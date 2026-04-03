use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};

const WINDOWS: [(&str, i64); 8] = [
    ("5m", 5),
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
    ("7d", 10_080),
    ("30d", 43_200),
];
const DELTA_Z_LOOKBACK: usize = 64;
const CVD_Z_LOOKBACK: usize = 64;
const MIN_RANGE: f64 = 0.01;
const PARTIAL_SERIES_TAIL_LIMIT: usize = 15;
const MINUTES_PER_DAY: i64 = 1_440;
const SECONDS_PER_DAY: i64 = MINUTES_PER_DAY * 60;

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
    cvd_window: f64,
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

#[derive(Debug, Default)]
struct WindowDeltaDigest {
    minute_delta_by_ts: HashMap<i64, f64>,
    day_delta_by_start_ts: HashMap<i64, f64>,
    day_observed_minutes_by_start_ts: HashMap<i64, usize>,
}

#[derive(Debug, Default, Clone, Copy)]
struct CompactSeriesStats {
    minute_points: usize,
    day_points: usize,
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
                    "window_semantics": "aligned_completed_interval_bars",
                    "series_count": series_count,
                    "series": series,
                    "current_window": build_current_window_payload(
                        &ctx.history_futures,
                        &ctx.history_spot,
                        ctx.ts_bucket,
                        mins,
                    ),
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

fn build_current_window_payload(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Value {
    let window_end = ts_bucket + Duration::minutes(1);
    let window_start = window_end - Duration::minutes(interval_mins.max(1));
    let fut = aggregate_recent_window(history_futures, window_start, window_end);
    let spot = aggregate_recent_window(history_spot, window_start, window_end);
    let observed_minutes_fut = fut.as_ref().map(|(_, observed)| *observed).unwrap_or(0);
    let observed_minutes_spot = spot.as_ref().map(|(_, observed)| *observed).unwrap_or(0);
    let point = fut
        .as_ref()
        .zip(spot.as_ref())
        .map(|((fut_point, _), (spot_point, _))| build_dual_series_entry(fut_point, spot_point))
        .unwrap_or(Value::Null);
    let (compact_series, compact_stats) = build_current_window_compact_series(
        history_futures,
        history_spot,
        window_start,
        window_end,
    );

    json!({
        "window_start": window_start.to_rfc3339(),
        "window_end": window_end.to_rfc3339(),
        "requested_minutes": interval_mins,
        "window_semantics": "recent_n_window",
        "observed_minutes_fut": observed_minutes_fut,
        "observed_minutes_spot": observed_minutes_spot,
        "missing_minutes_fut": interval_mins.max(1).saturating_sub(observed_minutes_fut as i64),
        "missing_minutes_spot": interval_mins.max(1).saturating_sub(observed_minutes_spot as i64),
        "is_ready": observed_minutes_fut >= interval_mins.max(1) as usize
            && observed_minutes_spot >= interval_mins.max(1) as usize,
        "compact_series_policy": "edge_minutes_plus_full_days",
        "compact_series_point_count": compact_series.len(),
        "compact_series_minute_point_count": compact_stats.minute_points,
        "compact_series_day_point_count": compact_stats.day_points,
        "compact_series": compact_series,
        "point": point,
    })
}

fn build_current_window_compact_series(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> (Vec<Value>, CompactSeriesStats) {
    if window_start >= window_end {
        return (Vec::new(), CompactSeriesStats::default());
    }

    let fut = build_window_delta_digest(history_futures, window_start, window_end);
    let spot = build_window_delta_digest(history_spot, window_start, window_end);
    let mut out = Vec::new();
    let mut stats = CompactSeriesStats::default();
    let mut cum_delta_fut = 0.0;
    let mut cum_delta_spot = 0.0;

    let first_full_day_start = ceil_day_boundary(window_start);
    let last_full_day_start = floor_day_boundary(window_end);
    if first_full_day_start >= last_full_day_start {
        append_compact_minute_points(
            &mut out,
            &mut stats,
            &mut cum_delta_fut,
            &mut cum_delta_spot,
            &fut,
            &spot,
            window_start,
            window_end,
        );
        return (out, stats);
    }

    append_compact_minute_points(
        &mut out,
        &mut stats,
        &mut cum_delta_fut,
        &mut cum_delta_spot,
        &fut,
        &spot,
        window_start,
        first_full_day_start.min(window_end),
    );

    let mut day_start = first_full_day_start;
    while day_start < last_full_day_start {
        let day_key = day_start.timestamp();
        let delta_fut = fut
            .day_delta_by_start_ts
            .get(&day_key)
            .copied()
            .unwrap_or(0.0);
        let delta_spot = spot
            .day_delta_by_start_ts
            .get(&day_key)
            .copied()
            .unwrap_or(0.0);
        let observed_minutes_fut = fut
            .day_observed_minutes_by_start_ts
            .get(&day_key)
            .copied()
            .unwrap_or(0);
        let observed_minutes_spot = spot
            .day_observed_minutes_by_start_ts
            .get(&day_key)
            .copied()
            .unwrap_or(0);
        cum_delta_fut += delta_fut;
        cum_delta_spot += delta_spot;
        out.push(build_compact_series_point(
            day_start + Duration::days(1),
            "1d",
            "full_day",
            delta_fut,
            delta_spot,
            cum_delta_fut,
            cum_delta_spot,
            observed_minutes_fut,
            observed_minutes_spot,
        ));
        stats.day_points += 1;
        day_start += Duration::days(1);
    }

    append_compact_minute_points(
        &mut out,
        &mut stats,
        &mut cum_delta_fut,
        &mut cum_delta_spot,
        &fut,
        &spot,
        last_full_day_start.max(window_start),
        window_end,
    );

    (out, stats)
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
        &fut_bars.iter().map(|b| b.cvd_window).collect::<Vec<_>>(),
        CVD_Z_LOOKBACK,
    );
    let spot_delta_z = rolling_z(
        &spot_bars.iter().map(|b| b.delta).collect::<Vec<_>>(),
        DELTA_Z_LOOKBACK,
    );
    let spot_cvd_z = rolling_z(
        &spot_bars.iter().map(|b| b.cvd_window).collect::<Vec<_>>(),
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

        let mut entry = build_dual_series_entry(fb, &sb);
        if let Some(obj) = entry.as_object_mut() {
            obj.insert("xmk_delta_gap_s_minus_f".to_string(), json!(xmk_delta_gap));
            obj.insert("xmk_cvd_gap_s_minus_f".to_string(), json!(xmk_cvd_gap));
            obj.insert("spot_flow_dominance".to_string(), json!(spot_dom));
        }
        out.push(entry);
    }
    out
}

fn build_dual_series_entry(fb: &BarPoint, sb: &BarPoint) -> Value {
    json!({
        "ts": fb.ts.to_rfc3339(),
        "open_fut": fb.open,
        "high_fut": fb.high,
        "low_fut": fb.low,
        "close_fut": fb.close,
        "volume_fut": fb.volume,
        "delta_fut": fb.delta,
        "relative_delta_fut": fb.relative_delta,
        "delta_slant_fut": fb.delta_slant,
        "cvd_window_fut": fb.cvd_window,
        "cvd_7d_fut": fb.cvd_window,
        "open_spot": sb.open,
        "high_spot": sb.high,
        "low_spot": sb.low,
        "close_spot": sb.close,
        "volume_spot": sb.volume,
        "delta_spot": sb.delta,
        "relative_delta_spot": sb.relative_delta,
        "delta_slant_spot": sb.delta_slant,
        "cvd_window_spot": sb.cvd_window,
        "cvd_7d_spot": sb.cvd_window,
    })
}

fn build_window_delta_digest(
    history: &[MinuteHistory],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> WindowDeltaDigest {
    let mut out = WindowDeltaDigest::default();
    for h in history {
        if h.ts_bucket < window_start || h.ts_bucket >= window_end {
            continue;
        }
        *out.minute_delta_by_ts
            .entry(h.ts_bucket.timestamp())
            .or_insert(0.0) += h.delta;
        let day_start = floor_day_boundary(h.ts_bucket).timestamp();
        *out.day_delta_by_start_ts.entry(day_start).or_insert(0.0) += h.delta;
        *out.day_observed_minutes_by_start_ts
            .entry(day_start)
            .or_insert(0) += 1;
    }
    out
}

fn append_compact_minute_points(
    out: &mut Vec<Value>,
    stats: &mut CompactSeriesStats,
    cum_delta_fut: &mut f64,
    cum_delta_spot: &mut f64,
    fut: &WindowDeltaDigest,
    spot: &WindowDeltaDigest,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) {
    let mut minute = start;
    while minute < end {
        let minute_key = minute.timestamp();
        let delta_fut = fut
            .minute_delta_by_ts
            .get(&minute_key)
            .copied()
            .unwrap_or(0.0);
        let delta_spot = spot
            .minute_delta_by_ts
            .get(&minute_key)
            .copied()
            .unwrap_or(0.0);
        *cum_delta_fut += delta_fut;
        *cum_delta_spot += delta_spot;
        out.push(build_compact_series_point(
            minute + Duration::minutes(1),
            "1m",
            "edge_minute",
            delta_fut,
            delta_spot,
            *cum_delta_fut,
            *cum_delta_spot,
            usize::from(fut.minute_delta_by_ts.contains_key(&minute_key)),
            usize::from(spot.minute_delta_by_ts.contains_key(&minute_key)),
        ));
        stats.minute_points += 1;
        minute += Duration::minutes(1);
    }
}

fn build_compact_series_point(
    ts: DateTime<Utc>,
    resolution: &str,
    segment_kind: &str,
    delta_fut: f64,
    delta_spot: f64,
    cvd_window_fut: f64,
    cvd_window_spot: f64,
    observed_minutes_fut: usize,
    observed_minutes_spot: usize,
) -> Value {
    json!({
        "ts": ts.to_rfc3339(),
        "resolution": resolution,
        "segment_kind": segment_kind,
        "delta_fut": round2(delta_fut),
        "delta_spot": round2(delta_spot),
        "cvd_window_fut": round2(cvd_window_fut),
        "cvd_7d_fut": round2(cvd_window_fut),
        "cvd_window_spot": round2(cvd_window_spot),
        "cvd_7d_spot": round2(cvd_window_spot),
        "observed_minutes_fut": observed_minutes_fut,
        "observed_minutes_spot": observed_minutes_spot,
    })
}

fn aggregate_bars(
    history: &[MinuteHistory],
    ts_bucket: DateTime<Utc>,
    interval_mins: i64,
) -> Vec<BarPoint> {
    // `ts_bucket` is minute-labeled, so the effective cutoff is this minute's close.
    // Keep only bars with bar-end <= cutoff to avoid future-dated samples.
    let cutoff = ts_bucket + Duration::minutes(1);
    let mut bars: BTreeMap<i64, BarBuild> = BTreeMap::new();

    for h in history {
        if h.ts_bucket > ts_bucket {
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
                cvd_window: 0.0,
            }
        })
        .collect::<Vec<_>>();

    // Keep the legacy `cvd_7d_*` field names for compatibility, but compute
    // the cumulative delta over each requested recent-N window.
    let mut q: VecDeque<(DateTime<Utc>, f64)> = VecDeque::new();
    let mut sum = 0.0;
    for p in &mut points {
        let bound = p.ts - Duration::minutes(interval_mins);
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
        p.cvd_window = sum;
    }

    points
}

fn aggregate_recent_window(
    history: &[MinuteHistory],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Option<(BarPoint, usize)> {
    let mut build = BarBuild {
        open: None,
        high: f64::NEG_INFINITY,
        low: f64::INFINITY,
        close: None,
        volume: 0.0,
        delta: 0.0,
    };
    let mut observed_minutes = 0usize;

    for h in history {
        if h.ts_bucket < window_start || h.ts_bucket >= window_end {
            continue;
        }
        observed_minutes += 1;

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

    if observed_minutes == 0 {
        return None;
    }

    let open = build.open.unwrap_or(0.0);
    let close = build.close.unwrap_or(open);
    let high = if build.high.is_finite() {
        build.high
    } else {
        close
    };
    let low = if build.low.is_finite() {
        build.low
    } else {
        close
    };
    let relative_delta = if build.volume > 0.0 {
        build.delta / build.volume
    } else {
        0.0
    };
    let range = high - low;
    let delta_slant = if range >= MIN_RANGE {
        Some(build.delta / (range + 1e-12))
    } else {
        None
    };

    Some((
        BarPoint {
            ts: window_end,
            open,
            high,
            low,
            close,
            volume: build.volume,
            delta: build.delta,
            relative_delta,
            delta_slant,
            cvd_window: build.delta,
        },
        observed_minutes,
    ))
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

fn floor_day_boundary(ts: DateTime<Utc>) -> DateTime<Utc> {
    let sec = ts.timestamp();
    let day_start = sec - sec.rem_euclid(SECONDS_PER_DAY);
    Utc.timestamp_opt(day_start, 0).single().unwrap_or(ts)
}

fn ceil_day_boundary(ts: DateTime<Utc>) -> DateTime<Utc> {
    let floor = floor_day_boundary(ts);
    if floor == ts {
        ts
    } else {
        floor + Duration::days(1)
    }
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
    use super::{
        aggregate_bars, aggregate_recent_window, build_current_window_payload,
        build_partial_window, ceil_day_boundary, classify_partial_regime, floor_day_boundary,
        WINDOWS,
    };
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
    fn cvd_pack_windows_include_5m_confirmation_window() {
        assert!(WINDOWS
            .iter()
            .any(|(label, mins)| *label == "5m" && *mins == 5));
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

    #[test]
    fn aggregate_bars_30d_uses_full_recent_30d_history() {
        let interval_mins = 43_200i64;
        let boundary = Utc
            .timestamp_opt(interval_mins * 60 * 3, 0)
            .single()
            .expect("valid boundary");
        let ts_bucket = boundary - chrono::Duration::minutes(1);
        let start = boundary - chrono::Duration::minutes(interval_mins);

        let history = (0..interval_mins)
            .map(|offset| sample_history_ts(start + chrono::Duration::minutes(offset), 1.0, 1.0))
            .collect::<Vec<_>>();

        let bars = aggregate_bars(&history, ts_bucket, interval_mins);
        let latest = bars.last().expect("latest 30d bar");

        assert_eq!(bars.len(), 1);
        assert_eq!(latest.delta, interval_mins as f64);
        assert_eq!(latest.cvd_window, interval_mins as f64);
    }

    #[test]
    fn recent_window_snapshot_uses_current_recent_n_minutes() {
        let ts_bucket = Utc.with_ymd_and_hms(2026, 3, 26, 3, 39, 0).unwrap();
        let window_end = ts_bucket + chrono::Duration::minutes(1);
        let window_start = window_end - chrono::Duration::minutes(5);
        let history = vec![
            sample_history("2026-03-26T03:33:00Z", 1.0, 10.0),
            sample_history("2026-03-26T03:35:00Z", 2.0, 10.0),
            sample_history("2026-03-26T03:36:00Z", 3.0, 10.0),
            sample_history("2026-03-26T03:37:00Z", 4.0, 10.0),
            sample_history("2026-03-26T03:39:00Z", 5.0, 10.0),
        ];

        let (point, observed) =
            aggregate_recent_window(&history, window_start, window_end).expect("current window");
        assert_eq!(observed, 4);
        assert_eq!(point.ts, window_end);
        assert_eq!(point.delta, 14.0);
        assert_eq!(point.cvd_window, 14.0);
    }

    #[test]
    fn current_window_payload_preserves_legacy_cvd_alias_for_snapshot() {
        let ts_bucket = Utc.with_ymd_and_hms(2026, 3, 26, 3, 39, 0).unwrap();
        let futures = vec![
            sample_history("2026-03-26T03:35:00Z", 10.0, 100.0),
            sample_history("2026-03-26T03:36:00Z", 20.0, 100.0),
            sample_history("2026-03-26T03:37:00Z", 30.0, 100.0),
            sample_history("2026-03-26T03:38:00Z", 40.0, 100.0),
            sample_history("2026-03-26T03:39:00Z", 50.0, 100.0),
        ];
        let spot = vec![
            sample_history("2026-03-26T03:35:00Z", 1.0, 10.0),
            sample_history("2026-03-26T03:36:00Z", 2.0, 10.0),
            sample_history("2026-03-26T03:37:00Z", 3.0, 10.0),
            sample_history("2026-03-26T03:38:00Z", 4.0, 10.0),
            sample_history("2026-03-26T03:39:00Z", 5.0, 10.0),
        ];

        let payload = build_current_window_payload(&futures, &spot, ts_bucket, 5);
        assert_eq!(payload["window_semantics"], json!("recent_n_window"));
        assert_eq!(payload["is_ready"], json!(true));
        assert_eq!(payload["point"]["cvd_window_fut"], json!(150.0));
        assert_eq!(payload["point"]["cvd_7d_fut"], json!(150.0));
        assert_eq!(payload["point"]["cvd_window_spot"], json!(15.0));
        assert_eq!(payload["point"]["cvd_7d_spot"], json!(15.0));
        assert_eq!(payload["compact_series_point_count"], json!(5));
        assert_eq!(payload["compact_series_minute_point_count"], json!(5));
        assert_eq!(payload["compact_series_day_point_count"], json!(0));
        assert_eq!(payload["compact_series"][4]["cvd_window_fut"], json!(150.0));
        assert_eq!(payload["compact_series"][4]["cvd_7d_fut"], json!(150.0));
    }

    #[test]
    fn current_window_payload_30d_compacts_to_edge_minutes_plus_full_days() {
        let interval_mins = 43_200i64;
        let ts_bucket = Utc.with_ymd_and_hms(2026, 4, 3, 11, 59, 0).unwrap();
        let window_end = ts_bucket + chrono::Duration::minutes(1);
        let window_start = window_end - chrono::Duration::minutes(interval_mins);
        let first_full_day_start = ceil_day_boundary(window_start);
        let last_full_day_start = floor_day_boundary(window_end);

        let futures = (0..interval_mins)
            .map(|offset| {
                sample_history_ts(window_start + chrono::Duration::minutes(offset), 1.0, 1.0)
            })
            .collect::<Vec<_>>();
        let spot = (0..interval_mins)
            .map(|offset| {
                sample_history_ts(window_start + chrono::Duration::minutes(offset), 2.0, 1.0)
            })
            .collect::<Vec<_>>();

        let payload = build_current_window_payload(&futures, &spot, ts_bucket, interval_mins);
        let compact_series = payload["compact_series"]
            .as_array()
            .expect("compact series array");
        let expected_minute_points = (first_full_day_start - window_start).num_minutes() as usize
            + (window_end - last_full_day_start).num_minutes() as usize;
        let expected_day_points = (last_full_day_start - first_full_day_start).num_days() as usize;

        assert_eq!(
            payload["compact_series_policy"],
            json!("edge_minutes_plus_full_days")
        );
        assert_eq!(
            payload["compact_series_minute_point_count"],
            json!(expected_minute_points)
        );
        assert_eq!(
            payload["compact_series_day_point_count"],
            json!(expected_day_points)
        );
        assert_eq!(
            payload["compact_series_point_count"],
            json!(expected_minute_points + expected_day_points)
        );
        assert_eq!(
            compact_series.len(),
            expected_minute_points + expected_day_points
        );
        assert_eq!(
            payload["point"]["cvd_window_fut"],
            json!(interval_mins as f64)
        );
        assert_eq!(
            payload["point"]["cvd_window_spot"],
            json!((interval_mins * 2) as f64)
        );
        assert_eq!(
            compact_series
                .last()
                .and_then(|point| point.get("cvd_window_fut")),
            Some(&json!(interval_mins as f64))
        );
        assert_eq!(
            compact_series
                .last()
                .and_then(|point| point.get("cvd_window_spot")),
            Some(&json!((interval_mins * 2) as f64))
        );
        assert!(compact_series
            .iter()
            .any(|point| point.get("resolution") == Some(&json!("1d"))));
    }

    fn sample_history_ts(
        ts_bucket: chrono::DateTime<Utc>,
        delta: f64,
        total_qty: f64,
    ) -> MinuteHistory {
        MinuteHistory {
            ts_bucket,
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
}
