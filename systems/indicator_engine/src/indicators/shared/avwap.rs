use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, Utc};

pub const AVWAP_LOOKBACK_DAYS: i64 = 7;

pub fn avwap_lookback_start(ts_bucket: DateTime<Utc>) -> DateTime<Utc> {
    ts_bucket - Duration::days(AVWAP_LOOKBACK_DAYS)
}

pub fn avwap_7d_window_slices<'a>(
    fut_history: &'a [MinuteHistory],
    spot_history: &'a [MinuteHistory],
    ts_bucket: DateTime<Utc>,
) -> (&'a [MinuteHistory], &'a [MinuteHistory]) {
    let lookback_start = avwap_lookback_start(ts_bucket);
    (
        history_window_slice(fut_history, lookback_start, ts_bucket),
        history_window_slice(spot_history, lookback_start, ts_bucket),
    )
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
    use super::{avwap_7d_window_slices, avwap_gap_zscore};
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LiqAgg, MinuteHistory};
    use chrono::{Duration, TimeZone, Utc};
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
    fn avwap_window_slice_excludes_older_than_7d() {
        let end = Utc.with_ymd_and_hms(2026, 3, 29, 12, 0, 0).unwrap();
        let fut = vec![
            history_row(end - Duration::days(8), 100.0, 1.0),
            history_row(end - Duration::days(7), 101.0, 1.0),
            history_row(end - Duration::days(6), 102.0, 1.0),
            history_row(end, 103.0, 1.0),
        ];
        let spot = fut.clone();
        let (fut_slice, spot_slice) = avwap_7d_window_slices(&fut, &spot, end);
        assert_eq!(fut_slice.len(), 2);
        assert_eq!(spot_slice.len(), 2);
        assert_eq!(
            fut_slice.first().unwrap().ts_bucket,
            end - Duration::days(6)
        );
        assert_eq!(fut_slice.last().unwrap().ts_bucket, end);
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
