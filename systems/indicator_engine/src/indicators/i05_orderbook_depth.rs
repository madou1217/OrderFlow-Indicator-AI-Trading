use crate::indicators::context::{
    clip01, mean, ols_slope, zscore, IndicatorComputation, IndicatorContext, IndicatorLevelRow,
    IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::{tick_to_price, BookLevelAgg, MinuteHistory};
use chrono::Duration;
use serde_json::{json, Map, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const OFI_NORM_LOOKBACK: usize = 60;
const WINDOW_SPECS: [(&str, i64); 2] = [("15m", 15), ("1h", 60)];
const RAW_AUDIT_TOP_TOTAL_LEVELS: usize = 64;
const RAW_AUDIT_TOP_ABS_NET_LEVELS: usize = 32;
const RAW_AUDIT_TOP_BID_LEVELS: usize = 16;
const RAW_AUDIT_TOP_ASK_LEVELS: usize = 16;

pub struct I05OrderbookDepth;

#[derive(Debug, Clone)]
pub struct OrderbookDepthPrecomputed {
    heatmap_entries: Vec<(i64, i32, BookLevelAgg)>,
    selected_audit_ticks: HashSet<i64>,
    peak_bid_tick: Option<i64>,
    peak_ask_tick: Option<i64>,
    peak_total_tick: Option<i64>,
    peak_abs_net_tick: Option<i64>,
    heatmap_summary_fut: Value,
    level_rows: Vec<IndicatorLevelRow>,
    levels: Vec<Value>,
}

impl Indicator for I05OrderbookDepth {
    fn code(&self) -> &'static str {
        "orderbook_depth"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let fut_obi_k_twa = ctx.futures.obi_k_twa.or(ctx.futures.obi_twa);
        let spot_obi_k_dw_twa = ctx.spot.obi_k_dw_twa.or(ctx.spot.obi_twa);
        let spot_confirms = spot_obi_k_dw_twa
            .zip(ctx.futures.obi_k_dw_twa.or(ctx.futures.obi_twa))
            .map(|(s, f)| s.signum() == f.signum())
            .unwrap_or(false);
        let exec_confirm = ctx.futures.delta.signum() == fut_obi_k_twa.unwrap_or(0.0).signum();
        let fake_order_risk = (fut_obi_k_twa.unwrap_or(0.0).abs()
            * (1.0 - ctx.futures.relative_delta.abs()))
        .max(0.0);
        let weak_price_resp = previous_close(&ctx.history_futures)
            .zip(ctx.futures.last_price)
            .map(|(prev_close, last_price)| {
                (last_price - prev_close).abs() <= 0.02
                    && ctx.futures.obi_k_dw_twa.unwrap_or(0.0).abs() >= 0.4
            })
            .unwrap_or(false);
        let spot_driven_divergence_flag = ctx.spot.delta.abs() > (ctx.futures.delta.abs() * 1.2)
            && ctx.spot.delta.signum() != ctx.futures.delta.signum();
        let cross_cvd_attribution = ctx.spot.delta - ctx.futures.delta;
        let obi_shock_fut = ctx.futures.obi_k_dw_change.map(f64::abs);
        let ofi_norm_fut = ofi_norm(ctx.futures.ofi, &ctx.history_futures);
        let ofi_norm_spot = ofi_norm(ctx.spot.ofi, &ctx.history_spot);
        let obi_k_dw_slope_fut = ols_slope(
            &ctx.history_futures
                .iter()
                .filter_map(|h| h.obi_k_dw_twa)
                .collect::<Vec<_>>(),
        );
        let orderbook_precomputed =
            ctx.orderbook_depth_precomputed_or_init(build_orderbook_depth_precomputed);
        let level_rows = orderbook_precomputed.level_rows.clone();
        let levels = orderbook_precomputed.levels.clone();

        let mut by_window = Map::new();
        for (window_code, window_minutes) in WINDOW_SPECS {
            let fut_rows = window_rows(&ctx.history_futures, ctx.ts_bucket, window_minutes);
            let spot_rows = window_rows(&ctx.history_spot, ctx.ts_bucket, window_minutes);
            by_window.insert(
                window_code.to_string(),
                build_window_payload(ctx, &fut_rows, &spot_rows, window_minutes),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "depth_k": ctx.futures.depth_k,
                    "levels": levels,
                    "heatmap_summary_fut": orderbook_precomputed.heatmap_summary_fut.clone(),
                    "spread_twa_fut": ctx.futures.spread_twa,
                    "topk_depth_twa_fut": ctx.futures.topk_depth_twa,
                    "obi": fut_obi_k_twa,
                    "obi_fut": fut_obi_k_twa,
                    "obi_l1_twa_fut": ctx.futures.obi_l1_twa,
                    "obi_k_twa_fut": fut_obi_k_twa,
                    "obi_k_dw_twa_fut": ctx.futures.obi_k_dw_twa,
                    "obi_k_dw_close_fut": ctx.futures.obi_k_dw_close,
                    "obi_k_dw_change_fut": ctx.futures.obi_k_dw_change,
                    "obi_k_dw_slope_fut": obi_k_dw_slope_fut,
                    "obi_k_dw_adj_twa_fut": ctx.futures.obi_k_dw_adj_twa,
                    "ofi_fut": ctx.futures.ofi,
                    "ofi_norm_fut": ofi_norm_fut,
                    "bbo_updates_fut": ctx.futures.bbo_updates,
                    "microprice_fut": ctx.futures.microprice_twa,
                    "microprice_classic_fut": ctx.futures.microprice_classic_twa,
                    "microprice_kappa_fut": ctx.futures.microprice_kappa_twa,
                    "microprice_adj_fut": ctx.futures.microprice_adj_twa,
                    "spread_twa_spot": ctx.spot.spread_twa,
                    "topk_depth_twa_spot": ctx.spot.topk_depth_twa,
                    "obi_k_dw_twa_spot": spot_obi_k_dw_twa,
                    "ofi_spot": ctx.spot.ofi,
                    "ofi_norm_spot": ofi_norm_spot,
                    "trade_delta_spot": ctx.spot.delta,
                    "relative_delta_spot": ctx.spot.relative_delta,
                    "cvd_slope_spot": ctx.cvd_slope_spot(20),
                    "exec_confirm_fut": exec_confirm,
                    "spot_confirm": spot_confirms,
                    "obi_shock_fut": obi_shock_fut,
                    "weak_price_resp_fut": weak_price_resp,
                    "fake_order_risk_fut": fake_order_risk,
                    "spot_driven_divergence_flag": spot_driven_divergence_flag,
                    "cross_cvd_attribution": cross_cvd_attribution,
                    "by_window": by_window,
                }),
            }),
            level_rows,
            ..Default::default()
        }
    }
}

fn build_orderbook_depth_precomputed(ctx: &IndicatorContext) -> OrderbookDepthPrecomputed {
    let mut heatmap_entries = Vec::with_capacity(ctx.futures.heatmap.len());
    let mut peak_bid: Option<(i64, f64)> = None;
    let mut peak_ask: Option<(i64, f64)> = None;
    let mut peak_total: Option<(i64, f64)> = None;
    let mut peak_abs_net: Option<(i64, f64, f64)> = None;

    for (idx, (tick, level)) in ctx.futures.heatmap.iter().enumerate() {
        let total = level.total();
        let abs_net = level.net().abs();

        if peak_bid
            .as_ref()
            .map(|(_, current)| level.bid_liquidity > *current)
            .unwrap_or(true)
        {
            peak_bid = Some((*tick, level.bid_liquidity));
        }
        if peak_ask
            .as_ref()
            .map(|(_, current)| level.ask_liquidity > *current)
            .unwrap_or(true)
        {
            peak_ask = Some((*tick, level.ask_liquidity));
        }
        if peak_total
            .as_ref()
            .map(|(_, current)| total > *current)
            .unwrap_or(true)
        {
            peak_total = Some((*tick, total));
        }
        if peak_abs_net
            .as_ref()
            .map(|(_, current_abs, _)| abs_net > *current_abs)
            .unwrap_or(true)
        {
            peak_abs_net = Some((*tick, abs_net, level.net()));
        }

        heatmap_entries.push((*tick, (idx + 1) as i32, level.clone()));
    }

    let selected_audit_ticks = select_heatmap_audit_ticks(&heatmap_entries);
    let heatmap_summary_fut = json!({
        "levels": heatmap_entries.len(),
        "peak_bid": peak_bid.map(|(tick, bid_liquidity)| {
            json!({
                "price_level": tick_to_price(tick),
                "bid_liquidity": bid_liquidity
            })
        }),
        "peak_ask": peak_ask.map(|(tick, ask_liquidity)| {
            json!({
                "price_level": tick_to_price(tick),
                "ask_liquidity": ask_liquidity
            })
        }),
        "peak_total": peak_total.map(|(tick, total_liquidity)| {
            json!({
                "price_level": tick_to_price(tick),
                "total_liquidity": total_liquidity
            })
        }),
        "max_abs_net": peak_abs_net.map(|(tick, _, net_liquidity)| {
            let entry = ctx
                .futures
                .heatmap
                .get(&tick)
                .cloned()
                .unwrap_or_default();
            json!({
                "price_level": tick_to_price(tick),
                "net_liquidity": net_liquidity,
                "level_imbalance": entry.imbalance()
            })
        }),
        "coverage_ratio": clip01((heatmap_entries.len() as f64) / 50.0)
    });

    let level_rows = heatmap_entries
        .iter()
        .filter(|(tick, _, _)| selected_audit_ticks.contains(tick))
        .map(|(tick, rank, level)| IndicatorLevelRow {
            indicator_code: "orderbook_depth",
            window_code: "1m",
            price_level: tick_to_price(*tick),
            level_rank: Some(*rank),
            metrics_json: json!({
                "bid_liquidity": level.bid_liquidity,
                "ask_liquidity": level.ask_liquidity,
                "total_liquidity": level.total(),
                "net_liquidity": level.net(),
                "level_imbalance": level.imbalance(),
                "is_peak_bid": Some(*tick) == peak_bid.map(|(tick, _)| tick),
                "is_peak_ask": Some(*tick) == peak_ask.map(|(tick, _)| tick),
                "is_peak_total": Some(*tick) == peak_total.map(|(tick, _)| tick),
                "is_peak_abs_net": Some(*tick) == peak_abs_net.map(|(tick, _, _)| tick),
                "audit_capture_policy": "top_liquidity_structural_subset"
            }),
        })
        .collect::<Vec<_>>();

    let levels = heatmap_entries
        .iter()
        .map(|(tick, rank, level)| {
            json!({
                "price_level": tick_to_price(*tick),
                "level_rank": rank,
                "bid_liquidity": level.bid_liquidity,
                "ask_liquidity": level.ask_liquidity,
                "total_liquidity": level.total(),
                "net_liquidity": level.net(),
                "level_imbalance": level.imbalance()
            })
        })
        .collect::<Vec<_>>();

    OrderbookDepthPrecomputed {
        heatmap_entries,
        selected_audit_ticks,
        peak_bid_tick: peak_bid.map(|(tick, _)| tick),
        peak_ask_tick: peak_ask.map(|(tick, _)| tick),
        peak_total_tick: peak_total.map(|(tick, _)| tick),
        peak_abs_net_tick: peak_abs_net.map(|(tick, _, _)| tick),
        heatmap_summary_fut,
        level_rows,
        levels,
    }
}

fn select_heatmap_audit_ticks(entries: &[(i64, i32, BookLevelAgg)]) -> HashSet<i64> {
    let mut out = HashSet::new();

    let mut by_total = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.total()))
        .collect::<Vec<_>>();
    by_total.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_total.into_iter().take(RAW_AUDIT_TOP_TOTAL_LEVELS) {
        out.insert(tick);
    }

    let mut by_abs_net = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.net().abs()))
        .collect::<Vec<_>>();
    by_abs_net.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_abs_net.into_iter().take(RAW_AUDIT_TOP_ABS_NET_LEVELS) {
        out.insert(tick);
    }

    let mut by_bid = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.bid_liquidity))
        .collect::<Vec<_>>();
    by_bid.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_bid.into_iter().take(RAW_AUDIT_TOP_BID_LEVELS) {
        out.insert(tick);
    }

    let mut by_ask = entries
        .iter()
        .map(|(tick, _, level)| (*tick, level.ask_liquidity))
        .collect::<Vec<_>>();
    by_ask.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    for (tick, _) in by_ask.into_iter().take(RAW_AUDIT_TOP_ASK_LEVELS) {
        out.insert(tick);
    }

    out
}

fn build_window_payload(
    ctx: &IndicatorContext,
    futures_rows: &[&MinuteHistory],
    spot_rows: &[&MinuteHistory],
    window_minutes: i64,
) -> Value {
    let requested_minutes = window_minutes.max(1);
    let returned_minutes = futures_rows.len() as i64;
    let missing_minutes = requested_minutes.saturating_sub(returned_minutes);
    let coverage_ratio = clip01(returned_minutes as f64 / requested_minutes as f64);
    let current_window_ofi_fut = futures_rows.iter().map(|row| row.ofi).sum::<f64>();
    let current_window_ofi_spot = spot_rows.iter().map(|row| row.ofi).sum::<f64>();
    let fut_obi = avg_metric(futures_rows, |row| row.obi_k_dw_twa.or(row.obi_twa));
    let spot_obi = avg_metric(spot_rows, |row| row.obi_k_dw_twa.or(row.obi_twa));
    let fut_delta = futures_rows.iter().map(|row| row.delta).sum::<f64>();
    let spot_delta = spot_rows.iter().map(|row| row.delta).sum::<f64>();
    let fut_qty = futures_rows.iter().map(|row| row.total_qty).sum::<f64>();
    let spot_qty = spot_rows.iter().map(|row| row.total_qty).sum::<f64>();
    let relative_delta_spot = if spot_qty > 0.0 {
        spot_delta / spot_qty
    } else {
        0.0
    };
    let fake_order_risk_fut = (fut_obi.unwrap_or(0.0).abs()
        * (1.0
            - if fut_qty > 0.0 {
                (fut_delta / fut_qty).abs()
            } else {
                0.0
            }))
    .max(0.0);

    let first_close_fut = futures_rows
        .first()
        .and_then(|row| row.close_price.or(row.last_price).or(row.open_price));
    let last_close_fut = futures_rows
        .last()
        .and_then(|row| row.close_price.or(row.last_price).or(row.open_price));
    let weak_price_resp_fut = first_close_fut
        .zip(last_close_fut)
        .map(|(open, close)| (close - open).abs() <= 0.02 && fut_obi.unwrap_or(0.0).abs() >= 0.4)
        .unwrap_or(false);

    let obi_series = futures_rows
        .iter()
        .filter_map(|row| row.obi_k_dw_twa.or(row.obi_twa))
        .collect::<Vec<_>>();
    let obi_k_dw_change_fut = obi_series
        .first()
        .zip(obi_series.last())
        .map(|(first, last)| last - first);
    let obi_shock_fut = obi_k_dw_change_fut.map(f64::abs);

    json!({
        "requested_minutes": requested_minutes,
        "returned_minutes": returned_minutes,
        "missing_minutes": missing_minutes,
        "coverage_ratio": coverage_ratio,
        "is_ready": returned_minutes >= requested_minutes,
        "spread_twa_fut": avg_metric(futures_rows, |row| row.spread_twa),
        "topk_depth_twa_fut": avg_metric(futures_rows, |row| row.topk_depth_twa),
        "obi_fut": fut_obi,
        "obi_l1_twa_fut": avg_metric(futures_rows, |row| row.obi_l1_twa),
        "obi_k_twa_fut": avg_metric(futures_rows, |row| row.obi_k_twa.or(row.obi_twa)),
        "obi_k_dw_twa_fut": fut_obi,
        "obi_k_dw_close_fut": futures_rows.last().and_then(|row| row.obi_k_dw_close.or(row.obi_k_dw_twa).or(row.obi_twa)),
        "obi_k_dw_change_fut": obi_k_dw_change_fut,
        "obi_k_dw_slope_fut": ols_slope(&obi_series),
        "obi_k_dw_adj_twa_fut": avg_metric(futures_rows, |row| row.obi_k_dw_adj_twa),
        "ofi_fut": current_window_ofi_fut,
        "ofi_norm_fut": window_ofi_norm(&ctx.history_futures, ctx.ts_bucket, window_minutes, current_window_ofi_fut),
        "bbo_updates_fut": futures_rows.iter().map(|row| row.bbo_updates).sum::<i64>(),
        "microprice_fut": avg_metric(futures_rows, |row| row.microprice_twa),
        "microprice_classic_fut": avg_metric(futures_rows, |row| row.microprice_classic_twa),
        "microprice_kappa_fut": avg_metric(futures_rows, |row| row.microprice_kappa_twa),
        "microprice_adj_fut": avg_metric(futures_rows, |row| row.microprice_adj_twa),
        "spread_twa_spot": avg_metric(spot_rows, |row| row.spread_twa),
        "topk_depth_twa_spot": avg_metric(spot_rows, |row| row.topk_depth_twa),
        "obi_k_dw_twa_spot": spot_obi,
        "ofi_spot": current_window_ofi_spot,
        "ofi_norm_spot": window_ofi_norm(&ctx.history_spot, ctx.ts_bucket, window_minutes, current_window_ofi_spot),
        "trade_delta_spot": spot_delta,
        "relative_delta_spot": relative_delta_spot,
        "cvd_slope_spot": ols_slope(&spot_rows.iter().map(|row| row.cvd).collect::<Vec<_>>()),
        "exec_confirm_fut": fut_delta.signum() == fut_obi.unwrap_or(0.0).signum(),
        "spot_confirm": spot_obi
            .zip(fut_obi)
            .map(|(spot, fut)| spot.signum() == fut.signum())
            .unwrap_or(false),
        "obi_shock_fut": obi_shock_fut,
        "weak_price_resp_fut": weak_price_resp_fut,
        "fake_order_risk_fut": fake_order_risk_fut,
        "spot_driven_divergence_flag": spot_delta.abs() > (fut_delta.abs() * 1.2)
            && spot_delta.signum() != fut_delta.signum(),
        "cross_cvd_attribution": spot_delta - fut_delta,
    })
}

fn previous_close(history: &[MinuteHistory]) -> Option<f64> {
    history
        .iter()
        .rev()
        .skip(1)
        .find_map(|h| h.close_price.or(h.last_price))
}

fn ofi_norm(current_ofi: f64, history: &[MinuteHistory]) -> Option<f64> {
    let values = history
        .iter()
        .rev()
        .skip(1)
        .take(OFI_NORM_LOOKBACK)
        .map(|h| h.ofi)
        .collect::<Vec<_>>();
    zscore(current_ofi, values)
}

fn window_ofi_norm(
    history: &[MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    window_minutes: i64,
    current_sum: f64,
) -> Option<f64> {
    let current_end = ts_bucket;
    let current_start = current_end - Duration::minutes(window_minutes);
    let mut samples = Vec::new();

    for idx in 1..=OFI_NORM_LOOKBACK {
        let sample_end = current_start - Duration::minutes(window_minutes * (idx as i64 - 1));
        let sample_start = sample_end - Duration::minutes(window_minutes);
        let sample = history
            .iter()
            .filter(|row| row.ts_bucket > sample_start && row.ts_bucket <= sample_end)
            .map(|row| row.ofi)
            .sum::<f64>();
        if sample.abs() > 1e-12 {
            samples.push(sample);
        }
    }

    zscore(current_sum, samples)
}

fn avg_metric<T>(rows: &[&MinuteHistory], extractor: T) -> Option<f64>
where
    T: Fn(&MinuteHistory) -> Option<f64>,
{
    let values = rows
        .iter()
        .filter_map(|row| extractor(row))
        .collect::<Vec<_>>();
    mean(&values)
}

fn window_rows<'a>(
    history: &'a [MinuteHistory],
    ts_bucket: chrono::DateTime<chrono::Utc>,
    window_minutes: i64,
) -> Vec<&'a MinuteHistory> {
    let start = ts_bucket - Duration::minutes(window_minutes);
    history
        .iter()
        .filter(|row| row.ts_bucket > start && row.ts_bucket <= ts_bucket)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorRuntimeOptions, KlineHistorySupplement,
    };
    use crate::indicators::indicator_trait::Indicator;
    use crate::ingest::decoder::{
        AggHeatmapLevel, AggOrderbook1mEvent, EngineEvent, MarketKind, MdData,
    };
    use crate::runtime::state_store::StateStore;
    use chrono::{Duration, TimeZone, Utc};
    use uuid::Uuid;

    fn test_runtime_options() -> IndicatorRuntimeOptions {
        IndicatorRuntimeOptions {
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
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
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
        }
    }

    fn orderbook_event(ts_bucket: chrono::DateTime<Utc>, heatmap_loaded: bool) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.orderbook.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.orderbook.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket,
            published_at: ts_bucket,
            data: MdData::AggOrderbook1m(AggOrderbook1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + Duration::minutes(1),
                source_event_count: 1,
                sample_count: 2,
                bbo_updates: 4,
                spread_sum: 0.4,
                topk_depth_sum: 20.0,
                obi_sum: 0.6,
                obi_l1_sum: 0.4,
                obi_k_sum: 0.8,
                obi_k_dw_sum: 1.0,
                obi_k_dw_change_sum: 0.2,
                obi_k_dw_adj_sum: 0.9,
                microprice_sum: 200.0,
                microprice_classic_sum: 200.0,
                microprice_kappa_sum: 200.0,
                microprice_adj_sum: 200.0,
                ofi_sum: 8.0,
                obi_k_dw_close: Some(0.5),
                heatmap_levels: if heatmap_loaded {
                    vec![
                        AggHeatmapLevel {
                            price: 100.0,
                            bid_liquidity: 8.0,
                            ask_liquidity: 3.0,
                        },
                        AggHeatmapLevel {
                            price: 101.0,
                            bid_liquidity: 2.0,
                            ask_liquidity: 7.0,
                        },
                    ]
                } else {
                    Vec::new()
                },
                heatmap_loaded,
            }),
        }
    }

    #[test]
    fn orderbook_raw_audit_ticks_are_bounded() {
        let entries = (1..=300_i64)
            .enumerate()
            .map(|(idx, tick)| {
                (
                    tick,
                    (idx + 1) as i32,
                    BookLevelAgg {
                        bid_liquidity: (301 - tick) as f64,
                        ask_liquidity: (tick % 23 + 1) as f64,
                    },
                )
            })
            .collect::<Vec<_>>();

        let ticks = select_heatmap_audit_ticks(&entries);
        assert!(ticks.contains(&1));
        assert!(ticks.contains(&22));
        assert!(
            ticks.len()
                <= RAW_AUDIT_TOP_TOTAL_LEVELS
                    + RAW_AUDIT_TOP_ABS_NET_LEVELS
                    + RAW_AUDIT_TOP_BID_LEVELS
                    + RAW_AUDIT_TOP_ASK_LEVELS
        );
    }

    #[test]
    fn current_minute_snapshot_and_level_rows_match_after_scalar_then_heatmap_upgrade() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 24, 2, 12, 0)
            .single()
            .unwrap();
        let runtime_options = test_runtime_options();

        let mut direct_store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        direct_store.ingest(orderbook_event(ts, true));
        let direct_bundle = direct_store.finalize_minute(ts);

        let mut upgraded_store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        upgraded_store.ingest(orderbook_event(ts, false));
        upgraded_store.ingest(orderbook_event(ts, true));
        let upgraded_bundle = upgraded_store.finalize_minute(ts);

        let direct_ctx = IndicatorContext::from_bundle(
            &direct_bundle,
            &runtime_options,
            KlineHistorySupplement::default(),
        );
        let upgraded_ctx = IndicatorContext::from_bundle(
            &upgraded_bundle,
            &runtime_options,
            KlineHistorySupplement::default(),
        );

        let indicator = I05OrderbookDepth;
        let direct = indicator.evaluate(&direct_ctx);
        let upgraded = indicator.evaluate(&upgraded_ctx);

        let direct_snapshot = direct.snapshot.expect("direct snapshot");
        let upgraded_snapshot = upgraded.snapshot.expect("upgraded snapshot");
        assert_eq!(direct_snapshot.payload_json, upgraded_snapshot.payload_json);
        let direct_levels = direct
            .level_rows
            .iter()
            .map(|row| {
                (
                    row.indicator_code,
                    row.window_code,
                    row.price_level,
                    row.level_rank,
                    row.metrics_json.clone(),
                )
            })
            .collect::<Vec<_>>();
        let upgraded_levels = upgraded
            .level_rows
            .iter()
            .map(|row| {
                (
                    row.indicator_code,
                    row.window_code,
                    row.price_level,
                    row.level_rank,
                    row.metrics_json.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(direct_levels, upgraded_levels);
        assert!(!direct.level_rows.is_empty());
    }

    #[test]
    fn precomputed_heatmap_outputs_match_legacy_direct_construction() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 24, 2, 18, 0)
            .single()
            .unwrap();
        let runtime_options = test_runtime_options();

        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        store.ingest(orderbook_event(ts, true));
        let bundle = store.finalize_minute(ts);
        let ctx = IndicatorContext::from_bundle(
            &bundle,
            &runtime_options,
            KlineHistorySupplement::default(),
        );

        let precomputed = build_orderbook_depth_precomputed(&ctx);

        let mut legacy_entries = Vec::with_capacity(ctx.futures.heatmap.len());
        let mut legacy_peak_bid: Option<(i64, f64)> = None;
        let mut legacy_peak_ask: Option<(i64, f64)> = None;
        let mut legacy_peak_total: Option<(i64, f64)> = None;
        let mut legacy_peak_abs_net: Option<(i64, f64, f64)> = None;
        for (idx, (tick, level)) in ctx.futures.heatmap.iter().enumerate() {
            let total = level.total();
            let abs_net = level.net().abs();
            if legacy_peak_bid
                .as_ref()
                .map(|(_, current)| level.bid_liquidity > *current)
                .unwrap_or(true)
            {
                legacy_peak_bid = Some((*tick, level.bid_liquidity));
            }
            if legacy_peak_ask
                .as_ref()
                .map(|(_, current)| level.ask_liquidity > *current)
                .unwrap_or(true)
            {
                legacy_peak_ask = Some((*tick, level.ask_liquidity));
            }
            if legacy_peak_total
                .as_ref()
                .map(|(_, current)| total > *current)
                .unwrap_or(true)
            {
                legacy_peak_total = Some((*tick, total));
            }
            if legacy_peak_abs_net
                .as_ref()
                .map(|(_, current_abs, _)| abs_net > *current_abs)
                .unwrap_or(true)
            {
                legacy_peak_abs_net = Some((*tick, abs_net, level.net()));
            }
            legacy_entries.push((*tick, (idx + 1) as i32, level.clone()));
        }
        let legacy_selected = select_heatmap_audit_ticks(&legacy_entries);
        let legacy_level_rows = legacy_entries
            .iter()
            .filter(|(tick, _, _)| legacy_selected.contains(tick))
            .map(|(tick, rank, level)| IndicatorLevelRow {
                indicator_code: "orderbook_depth",
                window_code: "1m",
                price_level: tick_to_price(*tick),
                level_rank: Some(*rank),
                metrics_json: json!({
                    "bid_liquidity": level.bid_liquidity,
                    "ask_liquidity": level.ask_liquidity,
                    "total_liquidity": level.total(),
                    "net_liquidity": level.net(),
                    "level_imbalance": level.imbalance(),
                    "is_peak_bid": Some(*tick) == legacy_peak_bid.map(|(tick, _)| tick),
                    "is_peak_ask": Some(*tick) == legacy_peak_ask.map(|(tick, _)| tick),
                    "is_peak_total": Some(*tick) == legacy_peak_total.map(|(tick, _)| tick),
                    "is_peak_abs_net": Some(*tick) == legacy_peak_abs_net.map(|(tick, _, _)| tick),
                    "audit_capture_policy": "top_liquidity_structural_subset"
                }),
            })
            .collect::<Vec<_>>();
        let legacy_levels = legacy_entries
            .iter()
            .map(|(tick, rank, level)| {
                json!({
                    "price_level": tick_to_price(*tick),
                    "level_rank": rank,
                    "bid_liquidity": level.bid_liquidity,
                    "ask_liquidity": level.ask_liquidity,
                    "total_liquidity": level.total(),
                    "net_liquidity": level.net(),
                    "level_imbalance": level.imbalance()
                })
            })
            .collect::<Vec<_>>();

        let precomputed_rows = precomputed
            .level_rows
            .iter()
            .map(|row| {
                (
                    row.indicator_code,
                    row.window_code,
                    row.price_level,
                    row.level_rank,
                    row.metrics_json.clone(),
                )
            })
            .collect::<Vec<_>>();
        let legacy_rows = legacy_level_rows
            .iter()
            .map(|row| {
                (
                    row.indicator_code,
                    row.window_code,
                    row.price_level,
                    row.level_rank,
                    row.metrics_json.clone(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(precomputed.selected_audit_ticks, legacy_selected);
        assert_eq!(precomputed_rows, legacy_rows);
        assert_eq!(precomputed.levels, legacy_levels);
    }
}
