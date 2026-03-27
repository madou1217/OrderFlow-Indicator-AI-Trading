use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, OptionsSurfacePoint,
};
use crate::indicators::indicator_trait::Indicator;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

pub const OPTIONS_SURFACE_WINDOWS: [(&str, i64, usize); 5] = [
    ("5m", 5, 1),
    ("15m", 15, 3),
    ("4h", 240, 48),
    ("1d", 1440, 288),
    ("3d", 4320, 864),
];

#[derive(Debug, Clone)]
pub struct OptionsSurfaceWindowMetrics {
    pub window_label: &'static str,
    pub window_minutes: i64,
    pub samples_used: usize,
    pub is_ready: bool,
    pub front_expiry_ts: Option<DateTime<Utc>>,
    pub second_expiry_ts: Option<DateTime<Utc>>,
    pub atm_strike_front: Option<f64>,
    pub atm_iv_front: Option<f64>,
    pub atm_iv_second: Option<f64>,
    pub atm_iv_30d_proxy: Option<f64>,
    pub atm_iv_regime: String,
    pub rr_25d_front: Option<f64>,
    pub rr_25d_second: Option<f64>,
    pub atm_iv_front_change: Option<f64>,
    pub rr_25d_front_change: Option<f64>,
    pub skew_state: String,
    pub term_structure_state: String,
}

#[derive(Debug, Clone)]
pub struct OptionsSurfaceIndicatorView {
    pub as_of_ts: Option<DateTime<Utc>>,
    pub by_window: Vec<OptionsSurfaceWindowMetrics>,
}

pub struct I27OptionsSurface;

impl Indicator for I27OptionsSurface {
    fn code(&self) -> &'static str {
        "options_surface"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let view = build_options_surface_view(ctx);
        let mut by_window = Map::new();
        for metrics in &view.by_window {
            by_window.insert(
                metrics.window_label.to_string(),
                options_surface_window_json(metrics),
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
                    "latest_options_surface_bucket": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "window": options_surface_window_json(metrics),
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
                    "latest_options_surface_bucket": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "by_window": Value::Object(by_window),
                }),
            }),
            snapshot_rows,
            ..Default::default()
        }
    }
}

pub fn build_options_surface_view(ctx: &IndicatorContext) -> OptionsSurfaceIndicatorView {
    let as_of_ts = ctx.latest_options_surface_bucket;
    let by_window = OPTIONS_SURFACE_WINDOWS
        .into_iter()
        .map(|(label, minutes, samples)| {
            compute_options_surface_window(&ctx.options_surface_5m, label, minutes, samples)
        })
        .collect::<Vec<_>>();
    OptionsSurfaceIndicatorView { as_of_ts, by_window }
}

fn compute_options_surface_window(
    series: &[OptionsSurfacePoint],
    window_label: &'static str,
    window_minutes: i64,
    sample_count: usize,
) -> OptionsSurfaceWindowMetrics {
    let Some((start, end)) = interval_endpoints(series, sample_count) else {
        return OptionsSurfaceWindowMetrics {
            window_label,
            window_minutes,
            samples_used: 0,
            is_ready: false,
            front_expiry_ts: None,
            second_expiry_ts: None,
            atm_strike_front: None,
            atm_iv_front: None,
            atm_iv_second: None,
            atm_iv_30d_proxy: None,
            atm_iv_regime: "unclear".to_string(),
            rr_25d_front: None,
            rr_25d_second: None,
            atm_iv_front_change: None,
            rr_25d_front_change: None,
            skew_state: "unclear".to_string(),
            term_structure_state: "unclear".to_string(),
        };
    };

    OptionsSurfaceWindowMetrics {
        window_label,
        window_minutes,
        samples_used: sample_count,
        is_ready: true,
        front_expiry_ts: end.front_expiry_ts,
        second_expiry_ts: end.second_expiry_ts,
        atm_strike_front: end.atm_strike_front,
        atm_iv_front: end.atm_iv_front,
        atm_iv_second: end.atm_iv_second,
        atm_iv_30d_proxy: end.atm_iv_30d_proxy,
        atm_iv_regime: classify_atm_iv_regime(end.atm_iv_front, end.atm_iv_30d_proxy).to_string(),
        rr_25d_front: end.rr_25d_front,
        rr_25d_second: end.rr_25d_second,
        atm_iv_front_change: diff(end.atm_iv_front, start.atm_iv_front),
        rr_25d_front_change: diff(end.rr_25d_front, start.rr_25d_front),
        skew_state: end.skew_state.clone(),
        term_structure_state: end.term_structure_state.clone(),
    }
}

fn interval_endpoints(
    series: &[OptionsSurfacePoint],
    sample_count: usize,
) -> Option<(&OptionsSurfacePoint, &OptionsSurfacePoint)> {
    if sample_count == 0 || series.len() <= sample_count {
        return None;
    }
    let end_idx = series.len() - 1;
    let start_idx = end_idx.checked_sub(sample_count)?;
    Some((&series[start_idx], &series[end_idx]))
}

fn diff(current: Option<f64>, previous: Option<f64>) -> Option<f64> {
    current.zip(previous).map(|(curr, prev)| curr - prev)
}

fn classify_atm_iv_regime(atm_iv_front: Option<f64>, atm_iv_30d_proxy: Option<f64>) -> &'static str {
    match atm_iv_front.zip(atm_iv_30d_proxy) {
        Some((front, proxy)) if (front - proxy) >= 0.02 => "elevated",
        Some((front, proxy)) if (proxy - front) >= 0.02 => "compressed",
        Some(_) => "neutral",
        None => "unclear",
    }
}

fn options_surface_window_json(metrics: &OptionsSurfaceWindowMetrics) -> Value {
    json!({
        "window_label": metrics.window_label,
        "window_minutes": metrics.window_minutes,
        "samples_used": metrics.samples_used,
        "is_ready": metrics.is_ready,
        "front_expiry_ts": metrics.front_expiry_ts.map(|ts| ts.to_rfc3339()),
        "second_expiry_ts": metrics.second_expiry_ts.map(|ts| ts.to_rfc3339()),
        "atm_strike_front": metrics.atm_strike_front,
        "atm_iv_front": metrics.atm_iv_front,
        "atm_iv_second": metrics.atm_iv_second,
        "atm_iv_30d_proxy": metrics.atm_iv_30d_proxy,
        "atm_iv_regime": metrics.atm_iv_regime,
        "rr_25d_front": metrics.rr_25d_front,
        "rr_25d_second": metrics.rr_25d_second,
        "atm_iv_front_change": metrics.atm_iv_front_change,
        "rr_25d_front_change": metrics.rr_25d_front_change,
        "skew_state": metrics.skew_state,
        "term_structure_state": metrics.term_structure_state,
    })
}
