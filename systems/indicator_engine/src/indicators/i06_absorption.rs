use crate::indicators::context::{
    clip01, AbsorptionEventRow, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_indicator_event_id;
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use crate::indicators::shared::market_structure::{
    stacked_imbalance_flags, value_area_key_levels_ticks,
};
use crate::runtime::state_store::tick_to_price;
use chrono::Duration;
use serde_json::{json, Map, Value};

const TICK_SIZE: f64 = 0.01;
const ETA_REJECT: f64 = 0.70;
const MIN_RANGE_TICKS: f64 = 4.0;
const RDELTA_ABS_MIN: f64 = 0.15;
const KEY_DIST_TICKS: f64 = 6.0;
const MERGE_GAP_MINUTES: i64 = 1;
const CONFIRM_BARS: usize = 3;
const MAX_EVENT_MINUTES: usize = 30;
const THETA_RD_SPOT: f64 = 0.15;
const THETA_WHALE_SPOT: f64 = 100_000.0;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AbsorptionEventData {
    pub direction: i16,
    pub event_type: String,
    pub trigger_side: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub delta_sum: f64,
    pub rdelta_mean: f64,
    pub reject_ratio: f64,
    pub key_distance_ticks: f64,
    pub stacked_buy_imbalance: bool,
    pub stacked_sell_imbalance: bool,
    pub spot_rdelta_1m_mean: f64,
    pub spot_cvd_1m_change: f64,
    pub spot_flow_confirm_score: f64,
    pub spot_whale_confirm_score: f64,
    pub spot_confirm: bool,
    pub score_base: f64,
    pub score: f64,
    pub payload: Value,
}

fn compute_absorption_all_history(ctx: &IndicatorContext) -> Vec<AbsorptionEventData> {
    let series = ctx.basic_event_history_series();
    let n = series.n;
    if n < CONFIRM_BARS + 5 {
        return Vec::new();
    }

    let fut = &ctx.history_futures[ctx.history_futures.len() - n..];
    let open = &series.open;
    let high = &series.high;
    let low = &series.low;
    let close = &series.close;
    let delta = &series.delta;
    let rdelta = &series.rdelta;
    let spot_rdelta = &series.spot_rdelta;
    let spot_cvd = &series.spot_cvd;

    // 文档要求 key levels 逐分钟前向填充，这里按分钟 profile/avwap 生成并 ffill。
    let mut avwap = vec![0.0; n];
    let mut val_levels = vec![0.0; n];
    let mut vah_levels = vec![0.0; n];
    let mut poc_levels = vec![0.0; n];
    let mut stacked_buy = vec![false; n];
    let mut stacked_sell = vec![false; n];

    let mut avwap_ffill: Option<f64> = None;
    let mut val_ffill: Option<f64> = None;
    let mut vah_ffill: Option<f64> = None;
    let mut poc_ffill: Option<f64> = None;

    for (i, bar) in fut.iter().enumerate() {
        if let Some(v) = bar.avwap_minute {
            avwap_ffill = Some(v);
        }
        if let Some((val_tick, vah_tick, poc_tick)) = value_area_key_levels_ticks(&bar.profile) {
            val_ffill = Some(tick_to_price(val_tick));
            vah_ffill = Some(tick_to_price(vah_tick));
            poc_ffill = Some(tick_to_price(poc_tick));
        }
        let (buy_flag, sell_flag) = stacked_imbalance_flags(&bar.profile);

        avwap[i] = avwap_ffill.unwrap_or(close[i]);
        val_levels[i] = val_ffill.unwrap_or(low[i]);
        vah_levels[i] = vah_ffill.unwrap_or(high[i]);
        poc_levels[i] = poc_ffill.unwrap_or(close[i]);
        stacked_buy[i] = buy_flag;
        stacked_sell[i] = sell_flag;
    }

    let mut prev_session_high = vec![None; n];
    let mut prev_session_low = vec![None; n];
    for i in 0..n {
        if i == 0 {
            continue;
        }
        let start = i.saturating_sub(1440);
        let hs = &high[start..i];
        let ls = &low[start..i];
        if !hs.is_empty() {
            prev_session_high[i] = hs.iter().copied().reduce(f64::max);
            prev_session_low[i] = ls.iter().copied().reduce(f64::min);
        }
    }

    let mut sign = vec![0_i16; n];
    let mut reject_bull = vec![0.0; n];
    let mut reject_bear = vec![0.0; n];
    let mut d_low_key = vec![f64::INFINITY; n];
    let mut d_high_key = vec![f64::INFINITY; n];

    for i in 0..n {
        let range = (high[i] - low[i]).max(0.0);
        let rbull = (close[i] - low[i]) / (range + 1e-12);
        let rbear = (high[i] - close[i]) / (range + 1e-12);
        reject_bull[i] = rbull;
        reject_bear[i] = rbear;

        let vah = vah_levels[i];
        let val = val_levels[i];
        let poc = poc_levels[i];
        let keys = [
            vah,
            val,
            poc,
            avwap[i],
            prev_session_high[i].unwrap_or(vah),
            prev_session_low[i].unwrap_or(val),
        ];

        d_low_key[i] = keys
            .iter()
            .map(|k| (low[i] - *k).abs() / TICK_SIZE)
            .fold(f64::INFINITY, f64::min);
        d_high_key[i] = keys
            .iter()
            .map(|k| (high[i] - *k).abs() / TICK_SIZE)
            .fold(f64::INFINITY, f64::min);

        let cand_bull = stacked_sell[i]
            && range >= MIN_RANGE_TICKS * TICK_SIZE
            && rdelta[i] <= -RDELTA_ABS_MIN
            && close[i] > open[i]
            && rbull >= ETA_REJECT
            && d_low_key[i] <= KEY_DIST_TICKS;
        let cand_bear = stacked_buy[i]
            && range >= MIN_RANGE_TICKS * TICK_SIZE
            && rdelta[i] >= RDELTA_ABS_MIN
            && close[i] < open[i]
            && rbear >= ETA_REJECT
            && d_high_key[i] <= KEY_DIST_TICKS;

        sign[i] = match (cand_bull, cand_bear) {
            (true, false) => 1,
            (false, true) => -1,
            _ => 0,
        };
    }

    let mut groups: Vec<(i16, usize, usize)> = Vec::new();
    for i in 0..n {
        if sign[i] == 0 {
            continue;
        }
        if let Some(last) = groups.last_mut() {
            let gap = (fut[i].ts_bucket - fut[last.2].ts_bucket).num_minutes();
            if last.0 == sign[i]
                && gap <= MERGE_GAP_MINUTES
                && (i - last.1 + 1) <= MAX_EVENT_MINUTES
            {
                last.2 = i;
                continue;
            }
        }
        groups.push((sign[i], i, i));
    }

    let mut out = Vec::new();
    for (dir, s, e) in groups {
        if e + 1 + CONFIRM_BARS > n {
            continue;
        }
        let l_g = low[s..=e].iter().copied().fold(f64::INFINITY, f64::min);
        let h_g = high[s..=e]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let r_g = (h_g - l_g).max(TICK_SIZE);

        let lows_future_min = low[e + 1..=e + CONFIRM_BARS]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let highs_future_max = high[e + 1..=e + CONFIRM_BARS]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);

        let mut confirm_idx = None;
        for j in 1..=CONFIRM_BARS {
            let idx = e + j;
            let ok = if dir > 0 {
                close[idx] >= l_g + 0.5 * r_g && lows_future_min >= l_g - TICK_SIZE
            } else {
                close[idx] <= h_g - 0.5 * r_g && highs_future_max <= h_g + TICK_SIZE
            };
            if ok {
                confirm_idx = Some(idx);
                break;
            }
        }
        let Some(cidx) = confirm_idx else {
            continue;
        };
        let n_g = (e - s + 1) as f64;
        let rd_mean = rdelta[s..=e].iter().sum::<f64>() / n_g;
        let reject_g = if dir > 0 {
            reject_bull[s..=e].iter().sum::<f64>() / n_g
        } else {
            reject_bear[s..=e].iter().sum::<f64>() / n_g
        };
        let d_key = if dir > 0 {
            d_low_key[s..=e]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        } else {
            d_high_key[s..=e]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        };

        let score = 0.35 * clip01(rd_mean.abs() / 0.5)
            + 0.35 * clip01((reject_g - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.20 * clip01(1.0 - d_key / KEY_DIST_TICKS)
            + 0.10 * clip01(n_g / 5.0);

        let spot_rd_mean = spot_rdelta[s..=e].iter().sum::<f64>() / n_g;
        let spot_cvd_push = spot_cvd[cidx] - spot_cvd[e];
        let spot_whale_push = series.spot_whale_notional[s..=cidx].iter().sum::<f64>();

        let spot_flow_confirm = clip01((dir as f64 * spot_rd_mean) / (THETA_RD_SPOT + 1e-12));
        let spot_whale_confirm =
            clip01((dir as f64 * spot_whale_push) / (THETA_WHALE_SPOT + 1e-12));
        let score_xmk = 0.85 * score + 0.10 * spot_flow_confirm + 0.05 * spot_whale_confirm;

        let event_type = if dir > 0 {
            "bullish_absorption"
        } else {
            "bearish_absorption"
        };
        let pivot_price = if dir > 0 { l_g } else { h_g };
        let delta_sum = delta[s..=e].iter().sum::<f64>();
        let stacked_buy_imbalance = stacked_buy[s..=e].iter().any(|flag| *flag);
        let stacked_sell_imbalance = stacked_sell[s..=e].iter().any(|flag| *flag);
        let spot_confirm = spot_flow_confirm > 0.0 || spot_whale_confirm > 0.0;
        let start_ts = fut[s].ts_bucket;
        let end_ts = fut[e].ts_bucket + Duration::minutes(1);
        let confirm_ts = fut[cidx].ts_bucket + Duration::minutes(1);
        let trigger_side = if dir > 0 { "sell" } else { "buy" };

        out.push(AbsorptionEventData {
            direction: dir,
            event_type: event_type.to_string(),
            trigger_side: trigger_side.to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price,
            price_low: l_g,
            price_high: h_g,
            delta_sum,
            rdelta_mean: rd_mean,
            reject_ratio: reject_g,
            key_distance_ticks: d_key,
            stacked_buy_imbalance,
            stacked_sell_imbalance,
            spot_rdelta_1m_mean: spot_rd_mean,
            spot_cvd_1m_change: spot_cvd_push,
            spot_flow_confirm_score: spot_flow_confirm,
            spot_whale_confirm_score: spot_whale_confirm,
            spot_confirm,
            score_base: score,
            score: score_xmk,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": pivot_price,
                "price_low": l_g,
                "price_high": h_g,
                "trigger_side": trigger_side,
                "delta_sum": delta_sum,
                "rdelta_mean": rd_mean,
                "reject_ratio": reject_g,
                "key_distance_ticks": d_key,
                "stacked_buy_imbalance": stacked_buy_imbalance,
                "stacked_sell_imbalance": stacked_sell_imbalance,
                "spot_rdelta_1m_mean": spot_rd_mean,
                "spot_cvd_1m_change": spot_cvd_push,
                "spot_flow_confirm_score": spot_flow_confirm,
                "spot_whale_confirm_score": spot_whale_confirm,
                "spot_confirm": spot_confirm,
                "score_base": score,
                "strength_score_xmk": score_xmk,
                "sig_pass": true
            }),
        });
    }

    out
}

pub(crate) fn detect_absorption_all_history(
    ctx: &IndicatorContext,
) -> std::sync::Arc<Vec<AbsorptionEventData>> {
    ctx.absorption_all_events_or_init(compute_absorption_all_history)
}

pub(crate) fn detect_absorption_events(ctx: &IndicatorContext) -> Vec<AbsorptionEventData> {
    let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
    detect_absorption_all_history(ctx)
        .iter()
        .filter(|event| event.confirm_ts == current_available_ts)
        .cloned()
        .collect()
}

pub(crate) fn absorption_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &AbsorptionEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_indicator_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.start_ts,
        Some(event.end_ts),
        event.direction,
        None,
        None,
    );
    let mut base = Map::new();
    base.insert("event_id".to_string(), json!(event_id));
    base.insert("type".to_string(), json!(event.event_type));
    base.insert("direction".to_string(), json!(event.direction));
    base.insert("start_ts".to_string(), json!(event.start_ts.to_rfc3339()));
    base.insert("end_ts".to_string(), json!(event.end_ts.to_rfc3339()));
    base.insert(
        "event_available_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert(
        "confirm_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert("score".to_string(), json!(event.score));
    base.insert("indicator_code".to_string(), json!(indicator_code));
    base.insert("trigger_side".to_string(), json!(event.trigger_side));
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

fn append_absorption_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[AbsorptionEventData],
) {
    for event in events {
        let event_id = build_indicator_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.start_ts,
            Some(event.end_ts),
            event.direction,
            None,
            None,
        );
        let payload_json = absorption_event_json(symbol, indicator_code, event).1;
        out.absorption_rows.push(AbsorptionEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            trigger_side: Some(event.trigger_side.clone()),
            pivot_price: Some(event.pivot_price),
            price_low: Some(event.price_low),
            price_high: Some(event.price_high),
            delta_sum: Some(event.delta_sum),
            rdelta_mean: Some(event.rdelta_mean),
            reject_ratio: Some(event.reject_ratio),
            key_distance_ticks: Some(event.key_distance_ticks),
            stacked_buy_imbalance: Some(event.stacked_buy_imbalance),
            stacked_sell_imbalance: Some(event.stacked_sell_imbalance),
            spot_rdelta_1m_mean: Some(event.spot_rdelta_1m_mean),
            spot_cvd_1m_change: Some(event.spot_cvd_1m_change),
            spot_flow_confirm_score: Some(event.spot_flow_confirm_score),
            spot_whale_confirm_score: Some(event.spot_whale_confirm_score),
            spot_confirm: Some(event.spot_confirm),
            score_base: Some(event.score_base),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

pub struct I06Absorption;

impl Indicator for I06Absorption {
    fn code(&self) -> &'static str {
        "absorption"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_absorption_all_history(ctx);
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| absorption_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    )
                }),
            }),
            ..Default::default()
        };

        append_absorption_rows(&mut out, &ctx.symbol, self.code(), all_events.as_slice());

        out
    }
}

#[cfg(test)]
mod tests {
    use super::{
        absorption_event_json, compute_absorption_all_history, detect_absorption_all_history,
        detect_absorption_events, AbsorptionEventData,
    };
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorSharedCaches,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LevelAgg, MinuteHistory, MinuteWindowData};
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn sample_minute(
        ts_bucket: chrono::DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        buy_qty: f64,
        sell_qty: f64,
        spot_bias: f64,
    ) -> MinuteHistory {
        let mut profile = BTreeMap::new();
        profile.insert(
            100,
            LevelAgg {
                buy_qty: 1.0,
                sell_qty: 12.0,
            },
        );
        profile.insert(
            101,
            LevelAgg {
                buy_qty: 1.0,
                sell_qty: 12.0,
            },
        );
        profile.insert(
            102,
            LevelAgg {
                buy_qty: 1.0,
                sell_qty: 12.0,
            },
        );
        profile.insert(
            103,
            LevelAgg {
                buy_qty: 12.0,
                sell_qty: 1.0,
            },
        );
        MinuteHistory {
            ts_bucket,
            market: MarketKind::Futures,
            open_price: Some(open),
            high_price: Some(high),
            low_price: Some(low),
            close_price: Some(close),
            last_price: Some(close),
            buy_qty,
            sell_qty,
            total_qty: buy_qty + sell_qty,
            total_notional: (buy_qty + sell_qty) * close,
            delta: buy_qty - sell_qty,
            relative_delta: if (buy_qty + sell_qty) > 0.0 {
                (buy_qty - sell_qty) / (buy_qty + sell_qty)
            } else {
                0.0
            },
            force_liq: BTreeMap::new(),
            ofi: 0.0,
            spread_twa: None,
            topk_depth_twa: None,
            obi_twa: None,
            obi_l1_twa: None,
            obi_k_twa: None,
            obi_k_dw_twa: None,
            obi_k_dw_close: None,
            obi_k_dw_change: None,
            obi_k_dw_adj_twa: None,
            bbo_updates: 0,
            microprice_twa: None,
            microprice_classic_twa: None,
            microprice_kappa_twa: None,
            microprice_adj_twa: None,
            cvd: spot_bias,
            vpin: 0.0,
            avwap_minute: Some(close),
            whale_trade_count: 0,
            whale_buy_count: 0,
            whale_sell_count: 0,
            whale_notional_total: 0.0,
            whale_notional_buy: 0.0,
            whale_notional_sell: 0.0,
            whale_qty_eth_total: 0.0,
            whale_qty_eth_buy: 0.0,
            whale_qty_eth_sell: 0.0,
            whale_max_single_notional: 0.0,
            profile,
        }
    }

    fn test_ctx(
        ts_bucket: chrono::DateTime<Utc>,
        history_futures: Vec<MinuteHistory>,
        history_spot: Vec<MinuteHistory>,
    ) -> IndicatorContext {
        IndicatorContext {
            ts_bucket,
            symbol: "TESTUSDT".to_string(),
            futures: MinuteWindowData::empty(MarketKind::Futures, ts_bucket),
            spot: MinuteWindowData::empty(MarketKind::Spot, ts_bucket),
            history_futures: history_futures.into(),
            history_spot: history_spot.into(),
            trade_history_futures: Vec::new(),
            trade_history_spot: Vec::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes_in_window: Vec::new(),
            funding_points_in_window: Vec::new(),
            mark_points_in_window: Vec::new(),
            funding_changes_recent: Vec::new().into(),
            funding_points_recent: Vec::new().into(),
            mark_points_recent: Vec::new().into(),
            latest_common_oi_ratio_bucket: None,
            current_open_interest: None,
            open_interest_hist_5m: Vec::new(),
            global_account_ratio_5m: Vec::new(),
            top_account_ratio_5m: Vec::new(),
            top_position_ratio_5m: Vec::new(),
            latest_options_surface_bucket: None,
            options_surface_5m: Vec::new(),
            incremental_outputs: std::sync::Arc::new(
                crate::indicators::shared::incremental::IncrementalIndicatorOutputs::default(),
            ),
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_bars_3d: 120,
            kline_history_bars_7d: 120,
            kline_history_bars_30d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_db_bars_3d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            kline_history_futures_4h_db: Vec::new(),
            kline_history_futures_1d_db: Vec::new(),
            kline_history_spot_4h_db: Vec::new(),
            kline_history_spot_1d_db: Vec::new(),
            tpo_rows_nb: 64,
            tpo_value_area_pct: 0.70,
            tpo_session_windows: vec!["4h".to_string(), "1d".to_string()],
            tpo_ib_minutes: 60,
            tpo_dev_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            rvwap_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            high_volume_pulse_summary_windows: vec!["15m".to_string(), "1h".to_string()],
            high_volume_pulse_min_samples: 5,
            ema_base_periods: vec![13, 21, 34],
            ema_htf_periods: vec![100, 200],
            ema_htf_windows: vec!["4h".to_string(), "1d".to_string()],
            ema_output_windows: vec!["15m".to_string(), "1h".to_string()],
            ema_fill_from_db: true,
            ema_db_bars_4h: 256,
            ema_db_bars_1d: 256,
            ema_db_bars_3d: 256,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
            shared_caches: Arc::new(IndicatorSharedCaches::default()),
        }
    }

    #[test]
    fn absorption_event_json_exposes_trigger_side() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 10, 0).unwrap();
        let end_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 12, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 13, 0).unwrap();
        let event = AbsorptionEventData {
            direction: 1,
            event_type: "bullish_absorption".to_string(),
            trigger_side: "sell".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: 1950.4,
            price_low: 1948.8,
            price_high: 1953.1,
            delta_sum: -312.8,
            rdelta_mean: -0.22,
            reject_ratio: 0.91,
            key_distance_ticks: 2.0,
            stacked_buy_imbalance: false,
            stacked_sell_imbalance: true,
            spot_rdelta_1m_mean: -0.11,
            spot_cvd_1m_change: -201.8,
            spot_flow_confirm_score: 0.3,
            spot_whale_confirm_score: 0.0,
            spot_confirm: true,
            score_base: 0.72,
            score: 0.81,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "trigger_side": "sell",
                "score": 0.81
            }),
        };

        let (_, payload) = absorption_event_json("TESTUSDT", "absorption", &event);
        assert_eq!(
            payload.get("trigger_side").and_then(|v| v.as_str()),
            Some("sell")
        );
    }

    #[test]
    fn cached_absorption_all_history_matches_direct_compute() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 1, 0, 0).unwrap();
        let history_futures = (0..10)
            .map(|i| {
                let ts = base + chrono::Duration::minutes(i as i64);
                sample_minute(
                    ts,
                    100.0 + i as f64 * 0.1,
                    101.0 + i as f64 * 0.1,
                    99.0 + i as f64 * 0.1,
                    100.2 + i as f64 * 0.1,
                    5.0 + i as f64,
                    4.0 + (i % 3) as f64,
                    i as f64,
                )
            })
            .collect::<Vec<_>>();
        let history_spot = (0..10)
            .map(|i| {
                let ts = base + chrono::Duration::minutes(i as i64);
                sample_minute(
                    ts,
                    99.8 + i as f64 * 0.1,
                    100.8 + i as f64 * 0.1,
                    98.8 + i as f64 * 0.1,
                    100.0 + i as f64 * 0.1,
                    4.0 + i as f64,
                    3.5 + (i % 2) as f64,
                    (i * 2) as f64,
                )
            })
            .collect::<Vec<_>>();
        let ctx = test_ctx(
            base + chrono::Duration::minutes(9),
            history_futures,
            history_spot,
        );

        let direct = compute_absorption_all_history(&ctx);
        let cached = detect_absorption_all_history(&ctx);
        assert_eq!(direct, *cached);

        let expected_current = direct
            .iter()
            .filter(|event| event.confirm_ts == ctx.ts_bucket + chrono::Duration::minutes(1))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(detect_absorption_events(&ctx), expected_current);
    }
}
