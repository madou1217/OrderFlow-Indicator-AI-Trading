use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::avwap::{
    avwap_gap_zscore, avwap_lookback_start, avwap_lookback_window_slices, avwap_of_slice,
    avwap_window_slices, avwap_window_snapshot_json, AVWAP_LOOKBACK_DAYS, AVWAP_LOOKBACK_MINUTES,
};
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::Duration;
use serde_json::{json, Map, Value};

const WINDOWS: [(&str, i64); 7] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
    ("7d", 10_080),
    ("30d", 43_200),
];

pub struct I18Avwap;

impl Indicator for I18Avwap {
    fn code(&self) -> &'static str {
        "avwap"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        if let Some(payload) = ctx.incremental_outputs.avwap_snapshot.clone() {
            return snapshot_only(self.code(), payload);
        }
        let lookback_start = avwap_lookback_start(ctx.ts_bucket);
        let (fut_window, spot_window) =
            avwap_lookback_window_slices(&ctx.history_futures, &ctx.history_spot, ctx.ts_bucket);

        let fut_avwap = avwap_of_slice(fut_window);
        let spot_avwap = avwap_of_slice(spot_window);
        let fut_last_price = ctx.futures.last_price;
        let minute_end = ctx.ts_bucket + Duration::minutes(1);
        let fut_mark_price = ctx
            .latest_mark_pair_at_or_before(minute_end)
            .map(|(_, mark_price)| mark_price);

        let avwap_gap = fut_avwap.zip(spot_avwap).map(|(f, s)| f - s);
        let z_avwap_gap = avwap_gap_zscore(fut_window, spot_window, avwap_gap);

        let primary_window_payload = avwap_window_snapshot_json(
            "7d",
            AVWAP_LOOKBACK_MINUTES,
            lookback_start,
            fut_window.len().min(spot_window.len()),
            fut_last_price,
            fut_mark_price,
            fut_avwap,
            spot_avwap,
            avwap_gap,
            z_avwap_gap,
        );

        let mut by_window = Map::new();
        for (label, mins) in WINDOWS {
            let anchor_ts = ctx.ts_bucket - Duration::minutes(mins.max(1));
            let (fut_slice, spot_slice) =
                avwap_window_slices(&ctx.history_futures, &ctx.history_spot, ctx.ts_bucket, mins);
            let window_fut_avwap = avwap_of_slice(fut_slice);
            let window_spot_avwap = avwap_of_slice(spot_slice);
            let window_gap = window_fut_avwap.zip(window_spot_avwap).map(|(f, s)| f - s);
            let window_z = avwap_gap_zscore(fut_slice, spot_slice, window_gap);
            by_window.insert(
                label.to_string(),
                avwap_window_snapshot_json(
                    label,
                    mins,
                    anchor_ts,
                    fut_slice.len().min(spot_slice.len()),
                    fut_last_price,
                    fut_mark_price,
                    window_fut_avwap,
                    window_spot_avwap,
                    window_gap,
                    window_z,
                ),
            );
        }

        let mut series_by_window = Map::new();
        for (label, mins) in WINDOWS {
            series_by_window.insert(
                label.to_string(),
                Value::Array(build_series(
                    &ctx.history_futures,
                    &ctx.history_spot,
                    mins,
                    mins,
                )),
            );
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "indicator": "avwap_dual_market",
                    "anchor_ts": primary_window_payload.get("anchor_ts").cloned().unwrap_or(Value::Null),
                    "lookback": primary_window_payload.get("lookback").cloned().unwrap_or(json!(format!("{}d", AVWAP_LOOKBACK_DAYS))),
                    "window": "1m",
                    "avwap_fut": primary_window_payload.get("avwap_fut").cloned().unwrap_or(Value::Null),
                    "avwap_spot": primary_window_payload.get("avwap_spot").cloned().unwrap_or(Value::Null),
                    "fut_last_price": primary_window_payload.get("fut_last_price").cloned().unwrap_or(Value::Null),
                    "fut_mark_price": primary_window_payload.get("fut_mark_price").cloned().unwrap_or(Value::Null),
                    "price_minus_avwap_fut": primary_window_payload.get("price_minus_avwap_fut").cloned().unwrap_or(Value::Null),
                    "price_minus_spot_avwap_fut": primary_window_payload.get("price_minus_spot_avwap_fut").cloned().unwrap_or(Value::Null),
                    "price_minus_spot_avwap_futmark": primary_window_payload.get("price_minus_spot_avwap_futmark").cloned().unwrap_or(Value::Null),
                    "xmk_avwap_gap_f_minus_s": primary_window_payload.get("xmk_avwap_gap_f_minus_s").cloned().unwrap_or(Value::Null),
                    "zavwap_gap": primary_window_payload.get("zavwap_gap").cloned().unwrap_or(Value::Null),
                    "observed_minutes": primary_window_payload.get("observed_minutes").cloned().unwrap_or(Value::Null),
                    "missing_minutes": primary_window_payload.get("missing_minutes").cloned().unwrap_or(Value::Null),
                    "is_ready": primary_window_payload.get("is_ready").cloned().unwrap_or(Value::Null),
                    "by_window": Value::Object(by_window),
                    "series_by_window": Value::Object(series_by_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn build_series(
    fut_history: &[crate::runtime::state_store::MinuteHistory],
    spot_history: &[crate::runtime::state_store::MinuteHistory],
    lookback_minutes: i64,
    sample_interval_mins: i64,
) -> Vec<Value> {
    let n = fut_history.len().min(spot_history.len());
    if n == 0 || lookback_minutes <= 0 || sample_interval_mins <= 0 {
        return Vec::new();
    }

    let fut = &fut_history[fut_history.len() - n..];
    let spot = &spot_history[spot_history.len() - n..];

    let mut fut_num = Vec::with_capacity(n);
    let mut fut_den = Vec::with_capacity(n);
    let mut spot_num = Vec::with_capacity(n);
    let mut spot_den = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);

    let mut fn_acc = 0.0;
    let mut fd_acc = 0.0;
    let mut sn_acc = 0.0;
    let mut sd_acc = 0.0;
    for i in 0..n {
        fn_acc += fut[i].total_notional;
        fd_acc += fut[i].total_qty;
        sn_acc += spot[i].total_notional;
        sd_acc += spot[i].total_qty;
        fut_num.push(fn_acc);
        fut_den.push(fd_acc);
        spot_num.push(sn_acc);
        spot_den.push(sd_acc);
        ts.push(fut[i].ts_bucket);
    }

    let mut out = Vec::new();
    for i in 0..n {
        let t = ts[i];
        if t.timestamp().rem_euclid(sample_interval_mins * 60) != 0 {
            continue;
        }

        let start_ts = t - Duration::minutes(lookback_minutes.max(1));
        let j = lower_bound_ts(&ts, start_ts + Duration::minutes(1));

        let fn_prev = if j > 0 { fut_num[j - 1] } else { 0.0 };
        let fd_prev = if j > 0 { fut_den[j - 1] } else { 0.0 };
        let sn_prev = if j > 0 { spot_num[j - 1] } else { 0.0 };
        let sd_prev = if j > 0 { spot_den[j - 1] } else { 0.0 };

        let fn_seg = fut_num[i] - fn_prev;
        let fd_seg = fut_den[i] - fd_prev;
        let sn_seg = spot_num[i] - sn_prev;
        let sd_seg = spot_den[i] - sd_prev;

        let avwap_fut = if fd_seg > 0.0 {
            Some(fn_seg / fd_seg)
        } else {
            None
        };
        let avwap_spot = if sd_seg > 0.0 {
            Some(sn_seg / sd_seg)
        } else {
            None
        };
        let gap = avwap_fut.zip(avwap_spot).map(|(f, s)| f - s);

        out.push(json!({
            "ts": t.to_rfc3339(),
            "avwap_fut": avwap_fut,
            "avwap_spot": avwap_spot,
            "xmk_avwap_gap_f_minus_s": gap
        }));
    }

    out
}

fn lower_bound_ts(
    values: &[chrono::DateTime<chrono::Utc>],
    target: chrono::DateTime<chrono::Utc>,
) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m] < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[cfg(test)]
mod tests {
    use super::build_series;
    use crate::indicators::shared::avwap::{avwap_window_slices, avwap_window_snapshot_json};
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LiqAgg, MinuteHistory};
    use chrono::{TimeZone, Utc};
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
    fn avwap_series_uses_requested_window_lookback() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 29, 0, 0, 0)
            .single()
            .expect("valid ts");
        let fut = vec![
            history_row(ts - chrono::Duration::days(31), 50.0, 1.0),
            history_row(ts - chrono::Duration::days(6), 100.0, 1.0),
            history_row(ts - chrono::Duration::days(2), 200.0, 1.0),
            history_row(ts - chrono::Duration::days(1), 300.0, 1.0),
            history_row(ts, 400.0, 1.0),
        ];
        let spot = fut.clone();

        let series_1d = build_series(&fut, &spot, 1440, 1);
        let latest_1d = series_1d.last().expect("1d latest point");
        assert_eq!(latest_1d["avwap_fut"], json!(400.0));

        let series_3d = build_series(&fut, &spot, 4320, 1);
        let latest_3d = series_3d.last().expect("3d latest point");
        assert_eq!(latest_3d["avwap_fut"], json!(300.0));

        let series_7d = build_series(&fut, &spot, 10_080, 1);
        let latest_7d = series_7d.last().expect("7d latest point");
        assert_eq!(latest_7d["avwap_fut"], json!(250.0));

        let series_30d = build_series(&fut, &spot, 43_200, 1);
        let latest_30d = series_30d.last().expect("30d latest point");
        assert_eq!(latest_30d["avwap_fut"], json!(250.0));
    }

    #[test]
    fn avwap_snapshot_current_windows_report_recent_n_metrics() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 29, 0, 0, 0)
            .single()
            .expect("valid ts");
        let fut = vec![
            history_row(ts - chrono::Duration::days(31), 50.0, 1.0),
            history_row(ts - chrono::Duration::days(6), 100.0, 1.0),
            history_row(ts - chrono::Duration::days(2), 200.0, 1.0),
            history_row(ts - chrono::Duration::days(1), 300.0, 1.0),
            history_row(ts, 400.0, 1.0),
        ];
        let spot = fut.clone();

        let payload_7d = avwap_window_snapshot_json(
            "7d",
            10_080,
            ts - chrono::Duration::days(7),
            {
                let (fut_slice, spot_slice) = avwap_window_slices(&fut, &spot, ts, 10_080);
                fut_slice.len().min(spot_slice.len())
            },
            Some(410.0),
            Some(409.0),
            Some(250.0),
            Some(250.0),
            Some(0.0),
            Some(0.0),
        );
        assert_eq!(payload_7d["lookback"], json!("7d"));
        assert_eq!(payload_7d["avwap_fut"], json!(250.0));

        let payload_30d = avwap_window_snapshot_json(
            "30d",
            43_200,
            ts - chrono::Duration::days(30),
            {
                let (fut_slice, spot_slice) = avwap_window_slices(&fut, &spot, ts, 43_200);
                fut_slice.len().min(spot_slice.len())
            },
            Some(410.0),
            Some(409.0),
            Some(250.0),
            Some(250.0),
            Some(0.0),
            Some(0.0),
        );
        assert_eq!(payload_30d["lookback"], json!("30d"));
        assert_eq!(payload_30d["avwap_fut"], json!(250.0));
        assert_eq!(payload_30d["is_ready"], json!(false));
    }
}
