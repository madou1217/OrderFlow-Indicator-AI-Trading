use crate::indicators::context::{
    IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, OpenInterestHistPoint,
};
use crate::indicators::indicator_trait::Indicator;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

pub const OI_WINDOWS: [(&str, i64, usize); 5] = [
    ("5m", 5, 1),
    ("15m", 15, 3),
    ("4h", 240, 48),
    ("1d", 1440, 288),
    ("3d", 4320, 864),
];

const ZSCORE_LOOKBACK: usize = 60;
const EPS: f64 = 1e-9;

#[derive(Debug, Clone)]
pub struct OpenInterestWindowMetrics {
    pub window_label: &'static str,
    pub window_minutes: i64,
    pub samples_used: usize,
    pub is_ready: bool,
    pub oi_start: Option<f64>,
    pub oi_end: Option<f64>,
    pub oi_delta_abs: Option<f64>,
    pub oi_delta_pct: Option<f64>,
    pub oi_log_return: Option<f64>,
    pub oi_zscore: Option<f64>,
    pub oi_accel: Option<f64>,
    pub price_start: Option<f64>,
    pub price_end: Option<f64>,
    pub price_delta_pct: Option<f64>,
    pub price_oi_relation: &'static str,
    pub state: &'static str,
}

#[derive(Debug, Clone)]
pub struct OpenInterestIndicatorView {
    pub as_of_ts: Option<DateTime<Utc>>,
    pub current_open_interest_contracts: Option<f64>,
    pub current_mark_price: Option<f64>,
    pub current_open_interest_value_usdt: Option<f64>,
    pub current_delta_abs: Option<f64>,
    pub current_delta_pct: Option<f64>,
    pub current_state: &'static str,
    pub by_window: Vec<OpenInterestWindowMetrics>,
}

pub struct I25OpenInterest;

impl Indicator for I25OpenInterest {
    fn code(&self) -> &'static str {
        "open_interest"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let view = build_open_interest_view(ctx);
        let mut by_window = Map::new();
        for metrics in &view.by_window {
            by_window.insert(metrics.window_label.to_string(), open_interest_window_json(metrics));
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
                    "current_open_interest_contracts": view.current_open_interest_contracts,
                    "current_mark_price": view.current_mark_price,
                    "current_open_interest_value_usdt": view.current_open_interest_value_usdt,
                    "current_delta_abs": view.current_delta_abs,
                    "current_delta_pct": view.current_delta_pct,
                    "current_state": view.current_state,
                    "window": open_interest_window_json(metrics),
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
                    "current_open_interest_contracts": view.current_open_interest_contracts,
                    "current_mark_price": view.current_mark_price,
                    "current_open_interest_value_usdt": view.current_open_interest_value_usdt,
                    "current_delta_abs": view.current_delta_abs,
                    "current_delta_pct": view.current_delta_pct,
                    "current_state": view.current_state,
                    "latest_common_complete_bucket": view.as_of_ts.map(|ts| ts.to_rfc3339()),
                    "by_window": Value::Object(by_window),
                }),
            }),
            snapshot_rows,
            ..Default::default()
        }
    }
}

pub fn build_open_interest_view(ctx: &IndicatorContext) -> OpenInterestIndicatorView {
    let as_of_ts = ctx.latest_common_oi_ratio_bucket;
    let latest_hist = ctx.open_interest_hist_5m.last();
    let current_open_interest_contracts = ctx
        .current_open_interest
        .as_ref()
        .map(|value| value.open_interest_contracts);
    let current_mark_price = ctx.current_open_interest.as_ref().and_then(|value| value.mark_price);
    let current_open_interest_value_usdt = ctx
        .current_open_interest
        .as_ref()
        .and_then(|value| value.open_interest_value_usdt);
    let current_delta_abs = current_open_interest_value_usdt
        .zip(latest_hist.map(|point| point.open_interest_value_usdt))
        .map(|(current, hist)| current - hist);
    let current_delta_pct = current_delta_abs.zip(latest_hist.map(|point| point.open_interest_value_usdt)).and_then(
        |(delta, base)| if base.abs() > EPS { Some(delta / base) } else { None },
    );
    let current_state = classify_price_oi_relation(
        current_mark_price
            .zip(latest_hist.and_then(|point| point.reference_price))
            .and_then(|(current, hist)| pct_change(hist, current)),
        current_delta_pct,
    );

    let by_window = OI_WINDOWS
        .into_iter()
        .map(|(label, minutes, samples)| {
            compute_open_interest_window(&ctx.open_interest_hist_5m, label, minutes, samples)
        })
        .collect();

    OpenInterestIndicatorView {
        as_of_ts,
        current_open_interest_contracts,
        current_mark_price,
        current_open_interest_value_usdt,
        current_delta_abs,
        current_delta_pct,
        current_state,
        by_window,
    }
}

pub fn compute_open_interest_window(
    series: &[OpenInterestHistPoint],
    window_label: &'static str,
    window_minutes: i64,
    sample_count: usize,
) -> OpenInterestWindowMetrics {
    let Some((start, end)) = interval_endpoints(series, sample_count) else {
        return OpenInterestWindowMetrics {
            window_label,
            window_minutes,
            samples_used: 0,
            is_ready: false,
            oi_start: None,
            oi_end: None,
            oi_delta_abs: None,
            oi_delta_pct: None,
            oi_log_return: None,
            oi_zscore: None,
            oi_accel: None,
            price_start: None,
            price_end: None,
            price_delta_pct: None,
            price_oi_relation: "neutral",
            state: "neutral",
        };
    };

    let oi_start = start.open_interest_value_usdt;
    let oi_end = end.open_interest_value_usdt;
    let oi_delta_abs = oi_end - oi_start;
    let oi_delta_pct = pct_change(oi_start, oi_end);
    let oi_log_return = log_return(oi_start, oi_end);
    let price_start = start.reference_price;
    let price_end = end.reference_price;
    let price_delta_pct = price_start.zip(price_end).and_then(|(a, b)| pct_change(a, b));
    let state = classify_price_oi_relation(price_delta_pct, oi_delta_pct);
    let oi_accel = compute_oi_accel(series, sample_count);
    let oi_zscore = rolling_window_zscore(series, sample_count, oi_delta_pct);

    OpenInterestWindowMetrics {
        window_label,
        window_minutes,
        samples_used: sample_count,
        is_ready: true,
        oi_start: Some(oi_start),
        oi_end: Some(oi_end),
        oi_delta_abs: Some(oi_delta_abs),
        oi_delta_pct,
        oi_log_return,
        oi_zscore,
        oi_accel,
        price_start,
        price_end,
        price_delta_pct,
        price_oi_relation: state,
        state,
    }
}

fn interval_endpoints(
    series: &[OpenInterestHistPoint],
    sample_count: usize,
) -> Option<(&OpenInterestHistPoint, &OpenInterestHistPoint)> {
    if sample_count == 0 || series.len() <= sample_count {
        return None;
    }
    let end_idx = series.len() - 1;
    let start_idx = end_idx.checked_sub(sample_count)?;
    Some((&series[start_idx], &series[end_idx]))
}

fn compute_oi_accel(series: &[OpenInterestHistPoint], sample_count: usize) -> Option<f64> {
    let current = interval_endpoints(series, sample_count)?;
    let current_pct = pct_change(
        current.0.open_interest_value_usdt,
        current.1.open_interest_value_usdt,
    )?;

    if series.len() <= sample_count * 2 {
        return None;
    }
    let prev_end_idx = series.len() - 1 - sample_count;
    let prev_start_idx = prev_end_idx.checked_sub(sample_count)?;
    let prev_pct = pct_change(
        series[prev_start_idx].open_interest_value_usdt,
        series[prev_end_idx].open_interest_value_usdt,
    )?;
    Some(current_pct - prev_pct)
}

fn rolling_window_zscore(
    series: &[OpenInterestHistPoint],
    sample_count: usize,
    current_value: Option<f64>,
) -> Option<f64> {
    let current_value = current_value?;
    if series.len() <= sample_count {
        return None;
    }

    let mut values = Vec::new();
    for end_idx in sample_count..series.len() {
        let start_idx = end_idx - sample_count;
        if let Some(value) = pct_change(
            series[start_idx].open_interest_value_usdt,
            series[end_idx].open_interest_value_usdt,
        ) {
            values.push(value);
        }
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
        Some((current_value - mean) / var.sqrt())
    }
}

fn classify_price_oi_relation(
    price_delta_pct: Option<f64>,
    oi_delta_pct: Option<f64>,
) -> &'static str {
    let price = price_delta_pct.unwrap_or(0.0);
    let oi = oi_delta_pct.unwrap_or(0.0);
    if price > EPS && oi > EPS {
        "leveraged_long_build"
    } else if price < -EPS && oi > EPS {
        "fresh_short_build"
    } else if price < -EPS && oi < -EPS {
        "long_unwind"
    } else if price > EPS && oi < -EPS {
        "short_cover"
    } else {
        "neutral"
    }
}

fn pct_change(start: f64, end: f64) -> Option<f64> {
    if start.abs() <= EPS {
        None
    } else {
        Some((end - start) / start)
    }
}

fn log_return(start: f64, end: f64) -> Option<f64> {
    if start <= EPS || end <= EPS {
        None
    } else {
        Some((end / start).ln())
    }
}

fn open_interest_window_json(metrics: &OpenInterestWindowMetrics) -> Value {
    json!({
        "window_minutes": metrics.window_minutes,
        "samples_used": metrics.samples_used,
        "is_ready": metrics.is_ready,
        "oi_start": metrics.oi_start,
        "oi_end": metrics.oi_end,
        "oi_delta_abs": metrics.oi_delta_abs,
        "oi_delta_pct": metrics.oi_delta_pct,
        "oi_log_return": metrics.oi_log_return,
        "oi_zscore": metrics.oi_zscore,
        "oi_accel": metrics.oi_accel,
        "price_start": metrics.price_start,
        "price_end": metrics.price_end,
        "price_delta_pct": metrics.price_delta_pct,
        "price_oi_relation": metrics.price_oi_relation,
        "state": metrics.state,
    })
}
