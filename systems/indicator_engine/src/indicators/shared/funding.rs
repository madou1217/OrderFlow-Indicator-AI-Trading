use crate::indicators::context::IndicatorContext;
use crate::runtime::state_store::FundingChange;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct FundingWindowMetrics {
    pub funding_current: Option<f64>,
    pub funding_current_effective_ts: Option<DateTime<Utc>>,
    pub funding_twa: Option<f64>,
    pub mark_price_last: Option<f64>,
    pub mark_price_last_ts: Option<DateTime<Utc>>,
    pub mark_price_twap: Option<f64>,
    pub index_price_last: Option<f64>,
    pub changes_json: Value,
}

pub fn compute_funding_window_metrics(ctx: &IndicatorContext, mins: i64) -> FundingWindowMetrics {
    let end = ctx.ts_bucket + Duration::minutes(1);
    let start = end - Duration::minutes(mins);

    let mut funding_points = ctx
        .funding_points_recent
        .iter()
        .map(|p| (p.ts, p.funding_rate))
        .collect::<Vec<_>>();
    funding_points.sort_by_key(|(ts, _)| *ts);

    let mut mark_points = ctx
        .mark_points_recent
        .iter()
        .filter_map(|p| p.mark_price.map(|v| (p.ts, v)))
        .collect::<Vec<_>>();
    mark_points.sort_by_key(|(ts, _)| *ts);

    let funding_pair = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= end)
        .copied();
    let funding_current = funding_pair.map(|(_, v)| v);
    let funding_current_effective_ts = funding_pair.map(|(ts, _)| ts);
    let funding_fallback = funding_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(funding_current);
    let funding_twa = time_weighted_avg(start, end, &funding_points, funding_fallback);

    let mark_last_pair = mark_points.iter().rev().find(|(ts, _)| *ts <= end).copied();
    let mark_price_last = mark_last_pair.map(|(_, v)| v);
    let mark_price_last_ts = mark_last_pair.map(|(ts, _)| ts);
    let mark_fallback = mark_points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, v)| *v)
        .or(mark_price_last);
    let mark_price_twap = time_weighted_avg(start, end, &mark_points, mark_fallback);
    let index_price_last = ctx
        .mark_points_recent
        .iter()
        .rev()
        .find_map(|p| (p.ts <= end).then_some(p.index_price).flatten())
        .or_else(|| {
            ctx.mark_points_in_window
                .iter()
                .rev()
                .find_map(|p| (p.ts <= end).then_some(p.index_price).flatten())
        });

    let changes_json = json!(ctx
        .funding_changes_recent
        .iter()
        .filter(|c| c.ts_change >= start && c.ts_change < end)
        .map(funding_change_json)
        .collect::<Vec<_>>());

    FundingWindowMetrics {
        funding_current,
        funding_current_effective_ts,
        funding_twa,
        mark_price_last,
        mark_price_last_ts,
        mark_price_twap,
        index_price_last,
        changes_json,
    }
}

pub fn funding_change_json(change: &FundingChange) -> Value {
    json!({
        "change_ts": change.ts_change.to_rfc3339(),
        "funding_prev": change.prev,
        "funding_new": change.new,
        "funding_delta": change.delta,
        "mark_price_at_change": change.mark_price_at_change
    })
}

fn time_weighted_avg(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &[(DateTime<Utc>, f64)],
    fallback: Option<f64>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    if points.is_empty() {
        return fallback;
    }

    let mut weighted = 0.0;
    let mut total = 0.0;
    let mut cursor = start;
    let mut last_val = fallback.unwrap_or(points[0].1);

    for (ts, v) in points {
        if *ts <= start {
            last_val = *v;
            continue;
        }
        if *ts > end {
            break;
        }
        let dt = (*ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_val * dt;
            total += dt;
        }
        cursor = *ts;
        last_val = *v;
    }

    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_val * dt;
        total += dt;
    }

    if total <= 0.0 {
        fallback.or(Some(last_val))
    } else {
        Some(weighted / total)
    }
}

#[cfg(test)]
mod tests {
    use super::compute_funding_window_metrics;
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorRuntimeOptions, KlineHistorySupplement,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{
        FundingChange, LatestFundingState, LatestMarkState, MinuteWindowData, WindowBundle,
    };
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn window_metrics_use_latest_point_within_minute() {
        let ts_bucket = Utc.with_ymd_and_hms(2026, 3, 27, 8, 0, 0).single().unwrap();
        let state_ts = ts_bucket + Duration::seconds(59) + Duration::milliseconds(1);
        let bundle = WindowBundle {
            ts_bucket,
            symbol: "TESTUSDT".to_string(),
            futures: MinuteWindowData::empty(MarketKind::Futures, ts_bucket),
            spot: MinuteWindowData::empty(MarketKind::Spot, ts_bucket),
            history_futures: Vec::new(),
            history_spot: Vec::new(),
            trade_history_futures: Vec::new(),
            trade_history_spot: Vec::new(),
            latest_mark: Some(LatestMarkState {
                ts: state_ts,
                mark_price: Some(2000.0),
                index_price: Some(1999.0),
                funding_rate: Some(-0.00006),
                next_funding_time: None,
            }),
            latest_funding: Some(LatestFundingState {
                ts: state_ts,
                funding_rate: -0.00006,
                mark_price: Some(2000.0),
                next_funding_time: None,
            }),
            funding_changes_in_window: vec![FundingChange {
                ts_change: state_ts,
                prev: Some(-0.00005),
                new: -0.00006,
                delta: Some(-0.00001),
                mark_price_at_change: Some(2000.0),
            }],
            funding_points_in_window: vec![LatestFundingState {
                ts: state_ts,
                funding_rate: -0.00006,
                mark_price: Some(2000.0),
                next_funding_time: None,
            }],
            mark_points_in_window: vec![LatestMarkState {
                ts: state_ts,
                mark_price: Some(2000.0),
                index_price: Some(1999.0),
                funding_rate: Some(-0.00006),
                next_funding_time: None,
            }],
            funding_changes_recent: vec![FundingChange {
                ts_change: state_ts,
                prev: Some(-0.00005),
                new: -0.00006,
                delta: Some(-0.00001),
                mark_price_at_change: Some(2000.0),
            }],
            funding_points_recent: vec![LatestFundingState {
                ts: state_ts,
                funding_rate: -0.00006,
                mark_price: Some(2000.0),
                next_funding_time: None,
            }],
            mark_points_recent: vec![LatestMarkState {
                ts: state_ts,
                mark_price: Some(2000.0),
                index_price: Some(1999.0),
                funding_rate: Some(-0.00006),
                next_funding_time: None,
            }],
            latest_common_oi_ratio_bucket: None,
            current_open_interest: None,
            open_interest_hist_5m: Vec::new(),
            global_account_ratio_5m: Vec::new(),
            top_account_ratio_5m: Vec::new(),
            top_position_ratio_5m: Vec::new(),
            latest_options_surface_bucket: None,
            options_surface_5m: Vec::new(),
        };
        let options = IndicatorRuntimeOptions {
            whale_threshold_usdt: 100_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: Vec::new(),
            fvg_fill_from_db: false,
            fvg_db_bars_4h: 0,
            fvg_db_bars_1d: 0,
            fvg_epsilon_gap_ticks: 0,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.0,
            fvg_min_impulse_atr_ratio: 0.0,
            fvg_min_gap_atr_ratio: 0.0,
            fvg_max_gap_atr_ratio: 0.0,
            fvg_mitigated_fill_threshold: 0.0,
            fvg_invalid_close_bars: 0,
            tpo_rows_nb: 0,
            tpo_value_area_pct: 0.7,
            tpo_session_windows: Vec::new(),
            tpo_ib_minutes: 0,
            tpo_dev_output_windows: Vec::new(),
            rvwap_windows: Vec::new(),
            rvwap_output_windows: Vec::new(),
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: Vec::new(),
            high_volume_pulse_summary_windows: Vec::new(),
            high_volume_pulse_min_samples: 5,
            ema_base_periods: Vec::new(),
            ema_htf_periods: Vec::new(),
            ema_htf_windows: Vec::new(),
            ema_output_windows: Vec::new(),
            ema_fill_from_db: false,
            ema_db_bars_4h: 0,
            ema_db_bars_1d: 0,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec!["1m".to_string()],
        };
        let ctx =
            IndicatorContext::from_bundle(&bundle, &options, KlineHistorySupplement::default());

        let metrics = compute_funding_window_metrics(&ctx, 1);

        assert_eq!(metrics.funding_current, Some(-0.00006));
        assert_eq!(metrics.funding_current_effective_ts, Some(state_ts));
        assert_eq!(metrics.mark_price_last_ts, Some(state_ts));
    }
}
