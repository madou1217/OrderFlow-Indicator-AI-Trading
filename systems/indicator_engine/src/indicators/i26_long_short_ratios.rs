use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, LongShortRatioPoint,
};
use crate::indicators::indicator_trait::Indicator;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const LONG_SHORT_WINDOWS: [(&str, i64, usize); 5] = [
    ("5m", 5, 1),
    ("15m", 15, 3),
    ("4h", 240, 48),
    ("1d", 1440, 288),
    ("3d", 4320, 864),
];

const ZSCORE_LOOKBACK: usize = 60;
const EPS: f64 = 1e-9;

#[derive(Debug, Clone)]
pub struct LongShortRatioWindowMetrics {
    pub window_label: &'static str,
    pub window_minutes: i64,
    pub samples_used: usize,
    pub is_ready: bool,
    pub global_ratio_latest: Option<f64>,
    pub top_account_ratio_latest: Option<f64>,
    pub top_position_ratio_latest: Option<f64>,
    pub global_ratio_log: Option<f64>,
    pub top_account_ratio_log: Option<f64>,
    pub top_position_ratio_log: Option<f64>,
    pub global_ratio_change: Option<f64>,
    pub top_account_ratio_change: Option<f64>,
    pub top_position_ratio_change: Option<f64>,
    pub account_crowding_gap: Option<f64>,
    pub position_crowding_gap: Option<f64>,
    pub crowding_stretch: Option<f64>,
    pub crowding_zscore: Option<f64>,
    pub crowding_state: &'static str,
}

#[derive(Debug, Clone)]
pub struct LongShortRatioIndicatorView {
    pub as_of_ts: Option<DateTime<Utc>>,
    pub global_account_ratio: Option<f64>,
    pub top_account_ratio: Option<f64>,
    pub top_position_ratio: Option<f64>,
    pub crowding_state_current: &'static str,
    pub by_window: Vec<LongShortRatioWindowMetrics>,
}

#[derive(Debug, Clone)]
struct AlignedRatioPoint {
    global_account: f64,
    top_account: f64,
    top_position: f64,
}

pub struct I26LongShortRatios;

impl Indicator for I26LongShortRatios {
    fn code(&self) -> &'static str {
        "long_short_ratios"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let view = build_long_short_ratio_view(ctx);
        let mut by_window = Map::new();
        for metrics in &view.by_window {
            by_window.insert(
                metrics.window_label.to_string(),
                long_short_window_json(metrics),
            );
        }
        let mut snapshot_rows = Vec::new();
        for metrics in &view.by_window {
            if metrics.window_label == "5m" {
                continue;
            }
            snapshot_rows.push(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: metrics.window_label,
                payload_json: json!({
                    "as_of_ts": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "window_code": metrics.window_label,
                    "latest_common_complete_bucket": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "global_account_ratio": view.global_account_ratio,
                    "top_account_ratio": view.top_account_ratio,
                    "top_position_ratio": view.top_position_ratio,
                    "crowding_state_current": view.crowding_state_current,
                    "window": long_short_window_json(metrics),
                }),
            });
        }

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "5m",
                payload_json: json!({
                    "as_of_ts": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "window_code": "5m",
                    "global_account_ratio": view.global_account_ratio,
                    "top_account_ratio": view.top_account_ratio,
                    "top_position_ratio": view.top_position_ratio,
                    "crowding_state_current": view.crowding_state_current,
                    "latest_common_complete_bucket": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "by_window": Value::Object(by_window),
                }),
            }),
            snapshot_rows,
            ..Default::default()
        }
    }
}

pub fn build_long_short_ratio_view(ctx: &IndicatorContext) -> LongShortRatioIndicatorView {
    let aligned = align_ratio_series(
        &ctx.global_account_ratio_5m,
        &ctx.top_account_ratio_5m,
        &ctx.top_position_ratio_5m,
    );
    let latest = aligned.last();
    let by_window = LONG_SHORT_WINDOWS
        .into_iter()
        .map(|(label, minutes, samples)| {
            compute_long_short_window(&aligned, label, minutes, samples)
        })
        .collect::<Vec<_>>();

    LongShortRatioIndicatorView {
        as_of_ts: ctx.latest_common_oi_ratio_bucket,
        global_account_ratio: latest.map(|point| point.global_account),
        top_account_ratio: latest.map(|point| point.top_account),
        top_position_ratio: latest.map(|point| point.top_position),
        crowding_state_current: by_window
            .first()
            .map(|metrics| metrics.crowding_state)
            .unwrap_or("balanced"),
        by_window,
    }
}

fn compute_long_short_window(
    aligned: &[AlignedRatioPoint],
    window_label: &'static str,
    window_minutes: i64,
    sample_count: usize,
) -> LongShortRatioWindowMetrics {
    let Some((start, end)) = interval_endpoints(aligned, sample_count) else {
        return LongShortRatioWindowMetrics {
            window_label,
            window_minutes,
            samples_used: 0,
            is_ready: false,
            global_ratio_latest: None,
            top_account_ratio_latest: None,
            top_position_ratio_latest: None,
            global_ratio_log: None,
            top_account_ratio_log: None,
            top_position_ratio_log: None,
            global_ratio_change: None,
            top_account_ratio_change: None,
            top_position_ratio_change: None,
            account_crowding_gap: None,
            position_crowding_gap: None,
            crowding_stretch: None,
            crowding_zscore: None,
            crowding_state: "balanced",
        };
    };

    let account_crowding_gap = end.top_account - end.global_account;
    let position_crowding_gap = end.top_position - end.global_account;
    let crowding_stretch =
        account_crowding_gap.abs() + position_crowding_gap.abs() + (end.global_account - 1.0).abs();
    let global_ratio_change = Some(end.global_account - start.global_account);
    let top_account_ratio_change = Some(end.top_account - start.top_account);
    let top_position_ratio_change = Some(end.top_position - start.top_position);
    let crowding_state = classify_crowding_state(
        end.global_account,
        end.top_account,
        end.top_position,
        global_ratio_change,
        top_account_ratio_change,
        top_position_ratio_change,
    );

    LongShortRatioWindowMetrics {
        window_label,
        window_minutes,
        samples_used: sample_count,
        is_ready: true,
        global_ratio_latest: Some(end.global_account),
        top_account_ratio_latest: Some(end.top_account),
        top_position_ratio_latest: Some(end.top_position),
        global_ratio_log: ratio_log(end.global_account),
        top_account_ratio_log: ratio_log(end.top_account),
        top_position_ratio_log: ratio_log(end.top_position),
        global_ratio_change,
        top_account_ratio_change,
        top_position_ratio_change,
        account_crowding_gap: Some(account_crowding_gap),
        position_crowding_gap: Some(position_crowding_gap),
        crowding_stretch: Some(crowding_stretch),
        crowding_zscore: rolling_stretch_zscore(aligned, sample_count, crowding_stretch),
        crowding_state,
    }
}

fn align_ratio_series(
    global_account: &[LongShortRatioPoint],
    top_account: &[LongShortRatioPoint],
    top_position: &[LongShortRatioPoint],
) -> Vec<AlignedRatioPoint> {
    let global_map = global_account
        .iter()
        .map(|point| (point.ts_bucket, point.long_short_ratio))
        .collect::<BTreeMap<_, _>>();
    let top_account_map = top_account
        .iter()
        .map(|point| (point.ts_bucket, point.long_short_ratio))
        .collect::<BTreeMap<_, _>>();
    let top_position_map = top_position
        .iter()
        .map(|point| (point.ts_bucket, point.long_short_ratio))
        .collect::<BTreeMap<_, _>>();

    let global_keys = global_map.keys().copied().collect::<BTreeSet<_>>();
    let top_account_keys = top_account_map.keys().copied().collect::<BTreeSet<_>>();
    let top_position_keys = top_position_map.keys().copied().collect::<BTreeSet<_>>();

    let common = global_keys
        .intersection(&top_account_keys)
        .copied()
        .collect::<BTreeSet<_>>()
        .intersection(&top_position_keys)
        .copied()
        .collect::<BTreeSet<_>>();

    common
        .into_iter()
        .map(|ts_bucket| AlignedRatioPoint {
            global_account: *global_map.get(&ts_bucket).expect("global ts"),
            top_account: *top_account_map.get(&ts_bucket).expect("top account ts"),
            top_position: *top_position_map.get(&ts_bucket).expect("top position ts"),
        })
        .collect()
}

fn interval_endpoints(
    aligned: &[AlignedRatioPoint],
    sample_count: usize,
) -> Option<(&AlignedRatioPoint, &AlignedRatioPoint)> {
    if sample_count == 0 || aligned.len() <= sample_count {
        return None;
    }
    let end_idx = aligned.len() - 1;
    let start_idx = end_idx.checked_sub(sample_count)?;
    Some((&aligned[start_idx], &aligned[end_idx]))
}

fn rolling_stretch_zscore(
    aligned: &[AlignedRatioPoint],
    sample_count: usize,
    current_stretch: f64,
) -> Option<f64> {
    if aligned.len() <= sample_count {
        return None;
    }
    let mut values = Vec::new();
    for end_idx in sample_count..aligned.len() {
        let point = &aligned[end_idx];
        values.push(
            (point.top_account - point.global_account).abs()
                + (point.top_position - point.global_account).abs()
                + (point.global_account - 1.0).abs(),
        );
    }
    if values.len() < 3 {
        return None;
    }
    let tail = if values.len() > ZSCORE_LOOKBACK {
        &values[values.len() - ZSCORE_LOOKBACK..]
    } else {
        &values[..]
    };
    let mean = tail.iter().sum::<f64>() / tail.len() as f64;
    let var = tail
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / tail.len() as f64;
    if var <= EPS {
        None
    } else {
        Some((current_stretch - mean) / var.sqrt())
    }
}

fn classify_crowding_state(
    global_latest: f64,
    top_account_latest: f64,
    top_position_latest: f64,
    global_change: Option<f64>,
    top_account_change: Option<f64>,
    top_position_change: Option<f64>,
) -> &'static str {
    let global_change = global_change.unwrap_or(0.0);
    let top_account_change = top_account_change.unwrap_or(0.0);
    let top_position_change = top_position_change.unwrap_or(0.0);

    if global_latest > 1.02
        && top_account_latest > global_latest
        && top_position_latest > global_latest
    {
        if global_change < -EPS && top_account_change < -EPS && top_position_change < -EPS {
            "long_crowding_unwind"
        } else {
            "crowded_long"
        }
    } else if global_latest < 0.98
        && top_account_latest < global_latest
        && top_position_latest < global_latest
    {
        if global_change > EPS && top_account_change > EPS && top_position_change > EPS {
            "short_crowding_unwind"
        } else {
            "crowded_short"
        }
    } else {
        "balanced"
    }
}

fn ratio_log(value: f64) -> Option<f64> {
    if value <= EPS {
        None
    } else {
        Some(value.ln())
    }
}

fn long_short_window_json(metrics: &LongShortRatioWindowMetrics) -> Value {
    json!({
        "window_minutes": metrics.window_minutes,
        "samples_used": metrics.samples_used,
        "is_ready": metrics.is_ready,
        "global_ratio_latest": metrics.global_ratio_latest,
        "top_account_ratio_latest": metrics.top_account_ratio_latest,
        "top_position_ratio_latest": metrics.top_position_ratio_latest,
        "global_ratio_log": metrics.global_ratio_log,
        "top_account_ratio_log": metrics.top_account_ratio_log,
        "top_position_ratio_log": metrics.top_position_ratio_log,
        "global_ratio_change": metrics.global_ratio_change,
        "top_account_ratio_change": metrics.top_account_ratio_change,
        "top_position_ratio_change": metrics.top_position_ratio_change,
        "account_crowding_gap": metrics.account_crowding_gap,
        "position_crowding_gap": metrics.position_crowding_gap,
        "crowding_stretch": metrics.crowding_stretch,
        "crowding_zscore": metrics.crowding_zscore,
        "crowding_state": metrics.crowding_state,
    })
}
