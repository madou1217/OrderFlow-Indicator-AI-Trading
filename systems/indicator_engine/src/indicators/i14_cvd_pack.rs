use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};

const WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const LOOKBACK_DAYS: i64 = 7;
const DELTA_Z_LOOKBACK: usize = 64;
const CVD_Z_LOOKBACK: usize = 64;
const MIN_RANGE: f64 = 0.01;
const PARTIAL_SERIES_TAIL_LIMIT: usize = 15;

#[derive(Debug, Clone, Copy)]
struct BarPoint {
    ts: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    delta: f64,
    relative_delta: f64,
    delta_slant: Option<f64>,
    cvd_7d: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct BarBuild {
    open: Option<f64>,
    high: f64,
    low: f64,
    close: Option<f64>,
    volume: f64,
    delta: f64,
}

pub struct I14CvdPack;

impl Indicator for I14CvdPack {
    fn code(&self) -> &'static str {
        "cvd_pack"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let delta_fut = ctx.futures.delta;
        let delta_spot = ctx.spot.delta;
        let fut_slope = ctx.cvd_slope_futures(30);
        let spot_slope = ctx.cvd_slope_spot(30);
        let spot_dom = delta_spot.abs() / (delta_spot.abs() + delta_fut.abs() + 1e-12);

        let mut by_window = serde_json::Map::new();
        let mut partial_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            let fut_bars = aggregate_bars(&ctx.history_futures, ctx.ts_bucket, mins);
            let spot_bars = aggregate_bars(&ctx.history_spot, ctx.ts_bucket, mins);
            let series = build_dual_series(&fut_bars, &spot_bars);
            let series_count = series.len();
            by_window.insert(
                label.to_string(),
                json!({
                    "window": label,
                    "series_count": series_count,
                    "series": series
                }),
            );
            if matches!(label, "15m" | "4h" | "1d") {
                let partial = build_partial_window(
                    &ctx.history_futures,
                    &ctx.history_spot,
                    ctx.ts_bucket,
                    mins,
                );
                if !partial.is_null() {
                    partial_window.insert(label.to_string(), partial);
                }
            }
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "delta_fut": delta_fut,
                    "delta_spot": delta_spot,
                    "relative_delta_fut": ctx.futures.relative_delta,
                    "relative_delta_spot": ctx.spot.relative_delta,
                    "xmk_delta_gap_s_minus_f": delta_spot - delta_fut,
                    "cvd_slope_fut": fut_slope,
                    "cvd_slope_spot": spot_slope,
                    "spot_flow_dominance": spot_dom,
                    "spot_lead_score": if delta_spot.abs() > delta_fut.abs() {1.0} else {0.5},
                    "likely_driver": if delta_spot.abs() > delta_fut.abs() {"spot"} else {"futures"},
                    "by_window": Value::Object(by_window),
                    "partial_window": Value::Object(partial_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn build_partial_window(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Value {
    let window_end = align_bar_end(ts_bucket + Duration::minutes(1), interval_mins);
    let window_start = window_end - Duration::minutes(interval_mins);
    let recent_series_limit = if interval_mins <= PARTIAL_SERIES_TAIL_LIMIT as i64 {
        interval_mins as usize
    } else {
        PARTIAL_SERIES_TAIL_LIMIT
    };

    let fut_minutes = history_futures
        .iter()
        .filter(|h| h.ts_bucket >= window_start && h.ts_bucket <= ts_bucket)
        .collect::<Vec<_>>();
    if fut_minutes.is_empty() {
        return Value::Null;
    }

    let spot_by_ts = history_spot
        .iter()
        .filter(|h| h.ts_bucket >= window_start && h.ts_bucket <= ts_bucket)
        .map(|h| (h.ts_bucket.timestamp(), h))
        .collect::<HashMap<_, _>>();

    let minutes_elapsed = fut_minutes.len();
    let minutes_total = interval_mins.max(0) as usize;
    let mut cum_delta_fut = 0.0;
    let mut cum_delta_spot = 0.0;
    let mut cum_volume_fut = 0.0;
    let mut minute_rows = Vec::with_capacity(fut_minutes.len());

    for fut in fut_minutes {
        let spot_delta = spot_by_ts
            .get(&fut.ts_bucket.timestamp())
            .map(|entry| entry.delta)
            .unwrap_or(0.0);
        cum_delta_fut += fut.delta;
        cum_delta_spot += spot_delta;
        cum_volume_fut += fut.total_qty;
        minute_rows.push(json!({
            "ts": fut.ts_bucket.to_rfc3339(),
            "delta_fut": round2(fut.delta),
            "delta_spot": round2(spot_delta),
            "volume_fut": round2(fut.total_qty),
            "cum_delta_fut": round2(cum_delta_fut),
            "cum_delta_spot": round2(cum_delta_spot),
        }));
    }

    let recent_3m_delta_fut = rolling_delta_sum(&minute_rows, "delta_fut", 3);
    let recent_5m_delta_fut = rolling_delta_sum(&minute_rows, "delta_fut", 5);
    let recent_15m_delta_fut = rolling_delta_sum(&minute_rows, "delta_fut", 15);
    let recent_5m_delta_spot = rolling_delta_sum(&minute_rows, "delta_spot", 5);
    let slope_recent_5m_fut = average_recent_delta(&minute_rows, "delta_fut", 5);
    let slope_prev_5m_fut = average_previous_delta(&minute_rows, "delta_fut", 5);
    let slope_change_ratio = slope_prev_5m_fut.and_then(|prev| {
        if prev.abs() <= 1e-12 {
            None
        } else {
            slope_recent_5m_fut.map(|recent| round2(recent / prev))
        }
    });
    let regime = classify_partial_regime(slope_recent_5m_fut, slope_prev_5m_fut, minutes_elapsed);
    let recent_series = minute_rows
        .into_iter()
        .rev()
        .take(recent_series_limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();

    json!({
        "window_start": window_start.to_rfc3339(),
        "window_end": window_end.to_rfc3339(),
        "last_minute_ts": ts_bucket.to_rfc3339(),
        "minutes_elapsed": minutes_elapsed,
        "minutes_total": minutes_total,
        "progress_pct": if minutes_total == 0 {
            Value::Null
        } else {
            json!(round2(minutes_elapsed as f64 / minutes_total as f64 * 100.0))
        },
        "cum_delta_fut": round2(cum_delta_fut),
        "cum_delta_spot": round2(cum_delta_spot),
        "cum_volume_fut": round2(cum_volume_fut),
        "recent_3m_delta_fut": recent_3m_delta_fut,
        "recent_5m_delta_fut": recent_5m_delta_fut,
        "recent_15m_delta_fut": recent_15m_delta_fut,
        "recent_5m_delta_spot": recent_5m_delta_spot,
        "slope_recent_5m_fut": slope_recent_5m_fut.map(round2),
        "slope_prev_5m_fut": slope_prev_5m_fut.map(round2),
        "slope_change_ratio": slope_change_ratio,
        "regime": regime,
        "recent_series": recent_series,
    })
}

fn build_dual_series(fut_bars: &[BarPoint], spot_bars: &[BarPoint]) -> Vec<Value> {
    if fut_bars.is_empty() || spot_bars.is_empty() {
        return Vec::new();
    }

    let mut spot_idx = HashMap::new();
    for (idx, b) in spot_bars.iter().enumerate() {
        spot_idx.insert(b.ts.timestamp(), idx);
    }

    let fut_delta_z = rolling_z(
        &fut_bars.iter().map(|b| b.delta).collect::<Vec<_>>(),
        DELTA_Z_LOOKBACK,
    );
    let fut_cvd_z = rolling_z(
        &fut_bars.iter().map(|b| b.cvd_7d).collect::<Vec<_>>(),
        CVD_Z_LOOKBACK,
    );
    let spot_delta_z = rolling_z(
        &spot_bars.iter().map(|b| b.delta).collect::<Vec<_>>(),
        DELTA_Z_LOOKBACK,
    );
    let spot_cvd_z = rolling_z(
        &spot_bars.iter().map(|b| b.cvd_7d).collect::<Vec<_>>(),
        CVD_Z_LOOKBACK,
    );

    let mut out = Vec::new();
    for (fi, fb) in fut_bars.iter().enumerate() {
        let Some(si) = spot_idx.get(&fb.ts.timestamp()).copied() else {
            continue;
        };
        let sb = spot_bars[si];

        let xmk_delta_gap = spot_delta_z[si].zip(fut_delta_z[fi]).map(|(s, f)| s - f);
        let xmk_cvd_gap = spot_cvd_z[si].zip(fut_cvd_z[fi]).map(|(s, f)| s - f);
        let spot_dom = spot_delta_z[si]
            .zip(fut_delta_z[fi])
            .map(|(s, f)| s.abs() / (s.abs() + f.abs() + 1e-12));

        out.push(json!({
            "ts": fb.ts.to_rfc3339(),
            "open_fut": fb.open,
            "high_fut": fb.high,
            "low_fut": fb.low,
            "close_fut": fb.close,
            "volume_fut": fb.volume,
            "delta_fut": fb.delta,
            "relative_delta_fut": fb.relative_delta,
            "delta_slant_fut": fb.delta_slant,
            "cvd_7d_fut": fb.cvd_7d,

            "open_spot": sb.open,
            "high_spot": sb.high,
            "low_spot": sb.low,
            "close_spot": sb.close,
            "volume_spot": sb.volume,
            "delta_spot": sb.delta,
            "relative_delta_spot": sb.relative_delta,
            "delta_slant_spot": sb.delta_slant,
            "cvd_7d_spot": sb.cvd_7d,

            "xmk_delta_gap_s_minus_f": xmk_delta_gap,
            "xmk_cvd_gap_s_minus_f": xmk_cvd_gap,
            "spot_flow_dominance": spot_dom
        }));
    }
    out
}

fn aggregate_bars(
    history: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Vec<BarPoint> {
    let start = ts_bucket - Duration::days(LOOKBACK_DAYS);
    // `ts_bucket` is minute-labeled, so the effective cutoff is this minute's close.
    // Keep only bars with bar-end <= cutoff to avoid future-dated samples.
    let cutoff = ts_bucket + Duration::minutes(1);
    let mut bars: BTreeMap<i64, BarBuild> = BTreeMap::new();

    for h in history {
        if h.ts_bucket <= start || h.ts_bucket > ts_bucket {
            continue;
        }
        let end_ts = align_bar_end(h.ts_bucket + Duration::minutes(1), interval_mins);
        if end_ts > cutoff {
            continue;
        }
        let key = end_ts.timestamp();
        let build = bars.entry(key).or_insert_with(|| BarBuild {
            open: None,
            high: f64::NEG_INFINITY,
            low: f64::INFINITY,
            close: None,
            volume: 0.0,
            delta: 0.0,
        });

        let o = h
            .open_price
            .or(h.close_price)
            .or(h.last_price)
            .unwrap_or(0.0);
        let c = h.close_price.or(h.last_price).unwrap_or(o);
        let hi = h.high_price.or(Some(c)).unwrap_or(c);
        let lo = h.low_price.or(Some(c)).unwrap_or(c);

        if build.open.is_none() {
            build.open = Some(o);
        }
        build.close = Some(c);
        build.high = build.high.max(hi);
        build.low = build.low.min(lo);
        build.volume += h.total_qty;
        build.delta += h.delta;
    }

    let mut points = bars
        .into_iter()
        .map(|(ts, b)| {
            let ts = Utc.timestamp_opt(ts, 0).single().unwrap_or(ts_bucket);
            let open = b.open.unwrap_or(0.0);
            let close = b.close.unwrap_or(open);
            let high = if b.high.is_finite() { b.high } else { close };
            let low = if b.low.is_finite() { b.low } else { close };
            let relative_delta = if b.volume > 0.0 {
                b.delta / b.volume
            } else {
                0.0
            };
            let range = high - low;
            let delta_slant = if range >= MIN_RANGE {
                Some(b.delta / (range + 1e-12))
            } else {
                None
            };

            BarPoint {
                ts,
                open,
                high,
                low,
                close,
                volume: b.volume,
                delta: b.delta,
                relative_delta,
                delta_slant,
                cvd_7d: 0.0,
            }
        })
        .collect::<Vec<_>>();

    // rolling 7d CVD across bars
    let mut q: VecDeque<(DateTime<Utc>, f64)> = VecDeque::new();
    let mut sum = 0.0;
    for p in &mut points {
        let bound = p.ts - Duration::days(LOOKBACK_DAYS);
        while let Some((ts, d)) = q.front().copied() {
            if ts <= bound {
                q.pop_front();
                sum -= d;
            } else {
                break;
            }
        }
        sum += p.delta;
        q.push_back((p.ts, p.delta));
        p.cvd_7d = sum;
    }

    points
}

fn align_bar_end(ts: DateTime<Utc>, interval_mins: i64) -> DateTime<Utc> {
    let sec = ts.timestamp();
    let span = interval_mins * 60;
    // Ceiling alignment: if `ts` is already on a boundary return it as-is;
    // otherwise round up to the next boundary.
    // The previous `((sec/span)+1)*span` always advanced one extra span when
    // `sec` was exactly divisible, which caused every bar's window to shift
    // two minutes early (BUG-1) and excluded the last minute of the preceding
    // window (BUG-2).
    let rem = sec.rem_euclid(span);
    let end = if rem == 0 { sec } else { sec - rem + span };
    Utc.timestamp_opt(end, 0).single().unwrap_or(ts)
}

fn rolling_z(values: &[f64], lookback: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    for i in 0..values.len() {
        if i < lookback {
            continue;
        }
        let hist = &values[i - lookback..i];
        let mean = hist.iter().sum::<f64>() / hist.len() as f64;
        let var = hist
            .iter()
            .map(|v| {
                let d = *v - mean;
                d * d
            })
            .sum::<f64>()
            / hist.len() as f64;
        let sd = var.sqrt();
        out[i] = if sd <= 1e-12 {
            Some(0.0)
        } else {
            Some((values[i] - mean) / sd)
        };
    }
    out
}

fn rolling_delta_sum(series: &[Value], field: &str, count: usize) -> Option<f64> {
    if series.is_empty() {
        return None;
    }
    let sum = series
        .iter()
        .rev()
        .take(count)
        .filter_map(|entry| entry.get(field).and_then(Value::as_f64))
        .sum::<f64>();
    Some(round2(sum))
}

fn average_recent_delta(series: &[Value], field: &str, count: usize) -> Option<f64> {
    let values = series
        .iter()
        .rev()
        .take(count)
        .filter_map(|entry| entry.get(field).and_then(Value::as_f64))
        .collect::<Vec<_>>();
    if values.len() < count {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn average_previous_delta(series: &[Value], field: &str, count: usize) -> Option<f64> {
    let values = series
        .iter()
        .rev()
        .skip(count)
        .take(count)
        .filter_map(|entry| entry.get(field).and_then(Value::as_f64))
        .collect::<Vec<_>>();
    if values.len() < count {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn classify_partial_regime(
    slope_recent_5m_fut: Option<f64>,
    slope_prev_5m_fut: Option<f64>,
    minutes_elapsed: usize,
) -> &'static str {
    if minutes_elapsed < 6 {
        return "insufficient_data";
    }
    match (slope_recent_5m_fut, slope_prev_5m_fut) {
        (Some(recent), Some(prev)) if prev > 0.0 && recent < 0.0 => "reversal_to_selling",
        (Some(recent), Some(prev)) if prev < 0.0 && recent > 0.0 => "reversal_to_buying",
        (Some(recent), Some(prev)) if prev.abs() > 1e-12 && recent.abs() > prev.abs() * 1.5 => {
            "accelerating"
        }
        (Some(recent), Some(prev)) if prev.abs() > 1e-12 && recent.abs() < prev.abs() * 0.5 => {
            "decelerating"
        }
        (Some(_), Some(_)) => "steady",
        _ => "insufficient_data",
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::{build_partial_window, classify_partial_regime};
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::MinuteHistory;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn sample_history(ts_bucket: &str, delta: f64, total_qty: f64) -> MinuteHistory {
        MinuteHistory {
            ts_bucket: Utc
                .datetime_from_str(ts_bucket, "%Y-%m-%dT%H:%M:%SZ")
                .expect("parse ts"),
            market: MarketKind::Futures,
            open_price: Some(100.0),
            high_price: Some(101.0),
            low_price: Some(99.0),
            close_price: Some(100.0),
            last_price: Some(100.0),
            buy_qty: total_qty.max(delta).max(0.0),
            sell_qty: (total_qty - delta).max(0.0),
            total_qty,
            total_notional: total_qty * 100.0,
            delta,
            relative_delta: if total_qty > 0.0 {
                delta / total_qty
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
            cvd: 0.0,
            vpin: 0.0,
            avwap_minute: None,
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
    fn partial_window_uses_recent_5m_regime_detection() {
        assert_eq!(
            classify_partial_regime(Some(-10.0), Some(12.0), 8),
            "reversal_to_selling"
        );
        assert_eq!(
            classify_partial_regime(Some(15.0), Some(5.0), 8),
            "accelerating"
        );
        assert_eq!(
            classify_partial_regime(Some(1.0), Some(5.0), 8),
            "decelerating"
        );
    }

    #[test]
    fn partial_window_keeps_full_15m_recent_series_and_sums_incrementally() {
        let futures = vec![
            sample_history("2026-03-26T03:30:00Z", 10.0, 100.0),
            sample_history("2026-03-26T03:31:00Z", 12.0, 110.0),
            sample_history("2026-03-26T03:32:00Z", 11.0, 90.0),
            sample_history("2026-03-26T03:33:00Z", 10.0, 95.0),
            sample_history("2026-03-26T03:34:00Z", 8.0, 105.0),
            sample_history("2026-03-26T03:35:00Z", -5.0, 100.0),
            sample_history("2026-03-26T03:36:00Z", -6.0, 99.0),
            sample_history("2026-03-26T03:37:00Z", -7.0, 98.0),
            sample_history("2026-03-26T03:38:00Z", -8.0, 97.0),
            sample_history("2026-03-26T03:39:00Z", -9.0, 96.0),
        ];
        let spot = vec![
            sample_history("2026-03-26T03:30:00Z", 1.0, 10.0),
            sample_history("2026-03-26T03:31:00Z", 2.0, 10.0),
            sample_history("2026-03-26T03:32:00Z", 3.0, 10.0),
            sample_history("2026-03-26T03:33:00Z", 4.0, 10.0),
            sample_history("2026-03-26T03:34:00Z", 5.0, 10.0),
            sample_history("2026-03-26T03:35:00Z", -1.0, 10.0),
            sample_history("2026-03-26T03:36:00Z", -2.0, 10.0),
            sample_history("2026-03-26T03:37:00Z", -3.0, 10.0),
            sample_history("2026-03-26T03:38:00Z", -4.0, 10.0),
            sample_history("2026-03-26T03:39:00Z", -5.0, 10.0),
        ];

        let value = build_partial_window(
            &futures,
            &spot,
            Utc.with_ymd_and_hms(2026, 3, 26, 3, 39, 0).unwrap(),
            15,
        );

        assert_eq!(value["minutes_elapsed"], json!(10));
        assert_eq!(value["minutes_total"], json!(15));
        assert_eq!(value["recent_series"].as_array().map(Vec::len), Some(10));
        assert_eq!(value["cum_delta_fut"], json!(16.0));
        assert_eq!(value["recent_5m_delta_fut"], json!(-35.0));
        assert_eq!(value["regime"], json!("reversal_to_selling"));
        assert_eq!(
            value["recent_series"][0]["ts"],
            json!("2026-03-26T03:30:00+00:00")
        );
        assert_eq!(value["recent_series"][9]["cum_delta_fut"], json!(16.0));
    }
}
