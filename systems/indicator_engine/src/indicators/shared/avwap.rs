use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};

pub const AVWAP_LOOKBACK_DAYS: i64 = 7;
pub const AVWAP_LOOKBACK_MINUTES: i64 = AVWAP_LOOKBACK_DAYS * 24 * 60;

pub fn avwap_window_start(ts_bucket: DateTime<Utc>, lookback_minutes: i64) -> DateTime<Utc> {
    ts_bucket - Duration::minutes(lookback_minutes.max(1))
}

pub fn avwap_window_slices<'a>(
    fut_history: &'a [MinuteHistory],
    spot_history: &'a [MinuteHistory],
    ts_bucket: DateTime<Utc>,
    lookback_minutes: i64,
) -> (&'a [MinuteHistory], &'a [MinuteHistory]) {
    let lookback_start = avwap_window_start(ts_bucket, lookback_minutes);
    (
        history_window_slice(fut_history, lookback_start, ts_bucket),
        history_window_slice(spot_history, lookback_start, ts_bucket),
    )
}

pub fn avwap_lookback_start(ts_bucket: DateTime<Utc>) -> DateTime<Utc> {
    avwap_window_start(ts_bucket, AVWAP_LOOKBACK_MINUTES)
}

pub fn avwap_lookback_window_slices<'a>(
    fut_history: &'a [MinuteHistory],
    spot_history: &'a [MinuteHistory],
    ts_bucket: DateTime<Utc>,
) -> (&'a [MinuteHistory], &'a [MinuteHistory]) {
    avwap_window_slices(fut_history, spot_history, ts_bucket, AVWAP_LOOKBACK_MINUTES)
}

pub fn avwap_7d_window_slices<'a>(
    fut_history: &'a [MinuteHistory],
    spot_history: &'a [MinuteHistory],
    ts_bucket: DateTime<Utc>,
) -> (&'a [MinuteHistory], &'a [MinuteHistory]) {
    avwap_lookback_window_slices(fut_history, spot_history, ts_bucket)
}

pub fn avwap_window_code(lookback_minutes: i64) -> String {
    match lookback_minutes.max(1) {
        1 => "1m".to_string(),
        5 => "5m".to_string(),
        15 => "15m".to_string(),
        60 => "1h".to_string(),
        240 => "4h".to_string(),
        1440 => "1d".to_string(),
        4320 => "3d".to_string(),
        10_080 => "7d".to_string(),
        43_200 => "30d".to_string(),
        mins => format!("{mins}m"),
    }
}

pub fn avwap_window_snapshot_json(
    window_code: &str,
    lookback_minutes: i64,
    anchor_ts: DateTime<Utc>,
    observed_minutes: usize,
    fut_last_price: Option<f64>,
    fut_mark_price: Option<f64>,
    avwap_fut: Option<f64>,
    avwap_spot: Option<f64>,
    gap: Option<f64>,
    gap_zscore: Option<f64>,
) -> Value {
    let lookback_minutes = lookback_minutes.max(1);
    let missing_minutes = lookback_minutes.saturating_sub(observed_minutes as i64);
    let price_minus_avwap_fut = fut_last_price.zip(avwap_fut).map(|(p, a)| p - a);
    let price_minus_spot_avwap_fut = fut_last_price.zip(avwap_spot).map(|(p, a)| p - a);
    let price_minus_spot_avwap_futmark = fut_mark_price.zip(avwap_spot).map(|(p, a)| p - a);

    json!({
        "window_code": window_code,
        "lookback": window_code,
        "lookback_minutes": lookback_minutes,
        "anchor_ts": anchor_ts.to_rfc3339(),
        "window_semantics": "recent_n_window",
        "observed_minutes": observed_minutes,
        "missing_minutes": missing_minutes,
        "is_ready": missing_minutes == 0,
        "avwap_fut": avwap_fut,
        "avwap_spot": avwap_spot,
        "fut_last_price": fut_last_price,
        "fut_mark_price": fut_mark_price,
        "price_minus_avwap_fut": price_minus_avwap_fut,
        "price_minus_spot_avwap_fut": price_minus_spot_avwap_fut,
        "price_minus_spot_avwap_futmark": price_minus_spot_avwap_futmark,
        "xmk_avwap_gap_f_minus_s": gap,
        "zavwap_gap": gap_zscore,
    })
}

pub fn avwap_of_slice(history: &[MinuteHistory]) -> Option<f64> {
    let num = history.iter().map(|h| h.total_notional).sum::<f64>();
    let den = history.iter().map(|h| h.total_qty).sum::<f64>();
    if den > 0.0 {
        Some(num / den)
    } else {
        None
    }
}

pub fn avwap_gap_zscore(
    fut: &[MinuteHistory],
    spot: &[MinuteHistory],
    current_gap: Option<f64>,
) -> Option<f64> {
    let current = current_gap?;
    let n = fut.len().min(spot.len());
    if n < 10 {
        return None;
    }

    let mut gaps = Vec::new();
    for i in 0..n {
        let f = &fut[n - 1 - i];
        let s = &spot[n - 1 - i];
        if f.total_qty <= 0.0 || s.total_qty <= 0.0 {
            continue;
        }
        gaps.push((f.total_notional / f.total_qty) - (s.total_notional / s.total_qty));
    }
    if gaps.len() < 10 {
        return None;
    }

    let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
    let var = gaps
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / gaps.len() as f64;
    let sd = var.sqrt();
    if sd <= 1e-12 {
        Some(0.0)
    } else {
        Some((current - mean) / sd)
    }
}

fn history_window_slice<'a>(
    history: &'a [MinuteHistory],
    lookback_start: DateTime<Utc>,
    end_ts: DateTime<Utc>,
) -> &'a [MinuteHistory] {
    if history.is_empty() {
        return &[];
    }
    let start_idx = lower_bound_history_ts(history, lookback_start + Duration::minutes(1));
    let end_exclusive = upper_bound_history_ts(history, end_ts);
    if start_idx >= end_exclusive {
        &[]
    } else {
        &history[start_idx..end_exclusive]
    }
}

fn lower_bound_history_ts(history: &[MinuteHistory], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = history.len();
    while l < r {
        let m = (l + r) / 2;
        if history[m].ts_bucket < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn upper_bound_history_ts(history: &[MinuteHistory], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = history.len();
    while l < r {
        let m = (l + r) / 2;
        if history[m].ts_bucket <= target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[cfg(test)]
mod tests {
    use super::{
        avwap_gap_zscore, avwap_lookback_window_slices, avwap_window_code, avwap_window_slices,
        avwap_window_snapshot_json, AVWAP_LOOKBACK_DAYS,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LiqAgg, MinuteHistory};
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn history_row(ts: chrono::DateTime<Utc>, price: f64, qty: f64) -> MinuteHistory {
        MinuteHistory {
            ts_bucket: ts,
            market: MarketKind::Futures,
            open_price: Some(price),
            high_price: Some(price),
            low_price: Some(price),
            close_price: Some(price),
            last_price: Some(price),
            buy_qty: qty * 0.5,
            sell_qty: qty * 0.5,
            total_qty: qty,
            total_notional: price * qty,
            delta: 0.0,
            relative_delta: 0.0,
            force_liq: BTreeMap::<i64, LiqAgg>::new(),
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
            cvd: 0.0,
            vpin: 0.0,
            avwap_minute: Some(price),
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
            profile: BTreeMap::new(),
        }
    }

    #[test]
    fn avwap_default_window_slice_tracks_configured_lookback_days() {
        let end = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let fut = vec![
            history_row(end - Duration::days(8), 100.0, 1.0),
            history_row(end - Duration::days(7), 101.0, 1.0),
            history_row(end - Duration::days(6), 102.0, 1.0),
            history_row(end, 103.0, 1.0),
        ];
        let spot = fut.clone();
        let (fut_slice, spot_slice) = avwap_lookback_window_slices(&fut, &spot, end);
        assert_eq!(fut_slice.len(), 2);
        assert_eq!(spot_slice.len(), 2);
        assert_eq!(
            fut_slice.first().unwrap().ts_bucket,
            end - Duration::days(6)
        );
        assert_eq!(fut_slice.last().unwrap().ts_bucket, end);
        assert_eq!(AVWAP_LOOKBACK_DAYS, 7);
    }

    #[test]
    fn avwap_window_slices_use_requested_lookback_minutes() {
        let end = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let fut = vec![
            history_row(end - Duration::days(31), 100.0, 1.0),
            history_row(end - Duration::days(29), 101.0, 1.0),
            history_row(end - Duration::days(2), 102.0, 1.0),
            history_row(end - Duration::hours(12), 103.0, 1.0),
            history_row(end, 104.0, 1.0),
        ];
        let spot = fut.clone();

        let (fut_1d, _) = avwap_window_slices(&fut, &spot, end, 1440);
        assert_eq!(fut_1d.len(), 2);
        assert_eq!(fut_1d.first().unwrap().ts_bucket, end - Duration::hours(12));

        let (fut_30d, _) = avwap_window_slices(&fut, &spot, end, 43_200);
        assert_eq!(fut_30d.len(), 4);
        assert_eq!(fut_30d.first().unwrap().ts_bucket, end - Duration::days(29));
    }

    #[test]
    fn avwap_window_code_formats_supported_lookbacks() {
        assert_eq!(avwap_window_code(15), "15m");
        assert_eq!(avwap_window_code(1440), "1d");
        assert_eq!(avwap_window_code(43_200), "30d");
    }

    #[test]
    fn avwap_window_snapshot_marks_recent_n_coverage() {
        let end = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let payload = avwap_window_snapshot_json(
            "30d",
            43_200,
            end - Duration::days(30),
            128,
            Some(105.0),
            Some(104.5),
            Some(100.0),
            Some(99.0),
            Some(1.0),
            Some(0.5),
        );

        assert_eq!(payload["window_code"], json!("30d"));
        assert_eq!(payload["lookback"], json!("30d"));
        assert_eq!(payload["window_semantics"], json!("recent_n_window"));
        assert_eq!(payload["observed_minutes"], json!(128));
        assert_eq!(payload["missing_minutes"], json!(43_072));
        assert_eq!(payload["is_ready"], json!(false));
        assert_eq!(payload["price_minus_avwap_fut"], json!(5.0));
        assert_eq!(payload["price_minus_spot_avwap_futmark"], json!(5.5));
    }

    #[test]
    fn avwap_gap_zscore_uses_only_provided_slice() {
        let end = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let mut fut = Vec::new();
        let mut spot = Vec::new();
        for i in 0..12 {
            let ts = end - Duration::minutes((11 - i) as i64);
            fut.push(history_row(ts, 100.0 + i as f64, 1.0));
            spot.push(history_row(ts, 99.0 + i as f64, 1.0));
        }
        let z_full = avwap_gap_zscore(&fut, &spot, Some(1.0)).unwrap();
        fut.push(history_row(end + Duration::minutes(1), 500.0, 1.0));
        spot.push(history_row(end + Duration::minutes(1), 0.0, 1.0));
        let z_with_extra = avwap_gap_zscore(&fut[..12], &spot[..12], Some(1.0)).unwrap();
        assert!((z_full - z_with_extra).abs() < 1e-12);
    }
}
