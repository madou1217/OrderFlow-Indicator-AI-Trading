use crate::indicators::context::IndicatorContext;
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::Duration;
use serde_json::{json, Map, Value};

const EPS: f64 = 1e-12;

pub struct I21RvwapSigmaBands;

impl Indicator for I21RvwapSigmaBands {
    fn code(&self) -> &'static str {
        "rvwap_sigma_bands"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let series = collect_weighted_series(ctx);
        let Some(last_idx) = series.len().checked_sub(1) else {
            return snapshot_only(
                self.code(),
                json!({
                    "indicator": self.code(),
                    "window": "1m",
                    "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                    "source_mode": "ohlcv_approx_1m",
                    "by_window": {},
                    "series_by_output_window": {},
                }),
            );
        };

        let mut by_window = Map::new();
        for window_code in &ctx.rvwap_windows {
            let Some(window_minutes) = window_to_minutes(window_code) else {
                continue;
            };
            let value = compute_stats_at(&series, last_idx, window_minutes, ctx.rvwap_min_samples)
                .map(stats_to_json)
                .unwrap_or_else(|| null_stats_json(window_minutes));
            by_window.insert(window_code.clone(), value);
        }

        let mut series_by_output_window = Map::new();
        for out_code in &ctx.rvwap_output_windows {
            let Some(out_minutes) = window_to_minutes(out_code) else {
                continue;
            };
            let mut output_series = Vec::new();
            for idx in 0..series.len() {
                let anchor = series.ts_bucket[idx] + Duration::minutes(1);
                if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
                    continue;
                }

                let mut row_windows = Map::new();
                for rolling_code in &ctx.rvwap_windows {
                    let Some(rolling_minutes) = window_to_minutes(rolling_code) else {
                        continue;
                    };
                    let value =
                        compute_stats_at(&series, idx, rolling_minutes, ctx.rvwap_min_samples)
                            .map(stats_to_json)
                            .unwrap_or_else(|| null_stats_json(rolling_minutes));
                    row_windows.insert(rolling_code.clone(), value);
                }

                output_series.push(json!({
                    "ts": anchor.to_rfc3339(),
                    "by_window": row_windows,
                }));
            }
            series_by_output_window.insert(out_code.clone(), Value::Array(output_series));
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": self.code(),
                "window": "1m",
                "as_of_ts": (ctx.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                "source_mode": "ohlcv_approx_1m",
                "by_window": by_window,
                "series_by_output_window": series_by_output_window,
            }),
        )
    }
}

#[derive(Debug, Clone)]
struct WeightedSeries {
    ts_bucket: Vec<chrono::DateTime<chrono::Utc>>,
    price: Vec<f64>,
    prefix_weight: Vec<f64>,
    prefix_price_weight: Vec<f64>,
    prefix_price_sq_weight: Vec<f64>,
    prefix_positive_samples: Vec<usize>,
}

impl WeightedSeries {
    fn len(&self) -> usize {
        self.ts_bucket.len()
    }

    fn is_empty(&self) -> bool {
        self.ts_bucket.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
struct RvwapStats {
    window_minutes: i64,
    rvwap: f64,
    sigma: f64,
    z: f64,
    sample_count: usize,
}

fn collect_weighted_series(ctx: &IndicatorContext) -> WeightedSeries {
    let mut ts_bucket = Vec::with_capacity(ctx.history_futures.len());
    let mut price = Vec::with_capacity(ctx.history_futures.len());
    let mut prefix_weight = Vec::with_capacity(ctx.history_futures.len());
    let mut prefix_price_weight = Vec::with_capacity(ctx.history_futures.len());
    let mut prefix_price_sq_weight = Vec::with_capacity(ctx.history_futures.len());
    let mut prefix_positive_samples = Vec::with_capacity(ctx.history_futures.len());
    let mut acc_weight = 0.0;
    let mut acc_price_weight = 0.0;
    let mut acc_price_sq_weight = 0.0;
    let mut acc_positive_samples = 0usize;

    for h in ctx.history_futures.iter() {
        let Some(p) = ohlcv_typical_price(h.high_price, h.low_price, h.close_price) else {
            continue;
        };
        let w = h.total_qty.max(0.0);
        ts_bucket.push(h.ts_bucket);
        price.push(p);
        if w > 0.0 {
            acc_weight += w;
            acc_price_weight += p * w;
            acc_price_sq_weight += p * p * w;
            acc_positive_samples += 1;
        }
        prefix_weight.push(acc_weight);
        prefix_price_weight.push(acc_price_weight);
        prefix_price_sq_weight.push(acc_price_sq_weight);
        prefix_positive_samples.push(acc_positive_samples);
    }

    WeightedSeries {
        ts_bucket,
        price,
        prefix_weight,
        prefix_price_weight,
        prefix_price_sq_weight,
        prefix_positive_samples,
    }
}

fn ohlcv_typical_price(high: Option<f64>, low: Option<f64>, close: Option<f64>) -> Option<f64> {
    Some((high? + low? + close?) / 3.0)
}

fn compute_stats_at(
    points: &WeightedSeries,
    end_idx: usize,
    window_minutes: i64,
    min_samples: usize,
) -> Option<RvwapStats> {
    if points.is_empty() || end_idx >= points.len() {
        return None;
    }

    let end_ts = points.ts_bucket[end_idx];
    let start_ts = end_ts - Duration::minutes(window_minutes.max(1));
    let start_idx = lower_bound_ts(&points.ts_bucket, start_ts + Duration::minutes(1));
    if start_idx > end_idx {
        return None;
    }

    let prefix_at = |values: &[f64], idx: usize| values.get(idx).copied().unwrap_or(0.0);
    let prefix_count_at = |values: &[usize], idx: usize| values.get(idx).copied().unwrap_or(0usize);
    let before_idx = start_idx.checked_sub(1);
    let sum_w = prefix_at(&points.prefix_weight, end_idx)
        - before_idx
            .map(|idx| prefix_at(&points.prefix_weight, idx))
            .unwrap_or(0.0);
    let sum_pv = prefix_at(&points.prefix_price_weight, end_idx)
        - before_idx
            .map(|idx| prefix_at(&points.prefix_price_weight, idx))
            .unwrap_or(0.0);
    let sum_p2v = prefix_at(&points.prefix_price_sq_weight, end_idx)
        - before_idx
            .map(|idx| prefix_at(&points.prefix_price_sq_weight, idx))
            .unwrap_or(0.0);
    let sample_count = prefix_count_at(&points.prefix_positive_samples, end_idx)
        - before_idx
            .map(|idx| prefix_count_at(&points.prefix_positive_samples, idx))
            .unwrap_or(0);

    if sample_count < min_samples || sum_w <= EPS {
        return None;
    }

    let rvwap = sum_pv / sum_w;
    let variance = (sum_p2v / sum_w - rvwap * rvwap).max(0.0);
    let sigma = variance.sqrt();
    let current_price = points.price[end_idx];
    let z = (current_price - rvwap) / (sigma + EPS);

    Some(RvwapStats {
        window_minutes,
        rvwap,
        sigma,
        z,
        sample_count,
    })
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

fn stats_to_json(stats: RvwapStats) -> Value {
    json!({
        "window_minutes": stats.window_minutes,
        "rvwap_w": stats.rvwap,
        "rvwap_sigma_w": stats.sigma,
        "rvwap_band_plus_1": stats.rvwap + stats.sigma,
        "rvwap_band_plus_2": stats.rvwap + 2.0 * stats.sigma,
        "rvwap_band_minus_1": stats.rvwap - stats.sigma,
        "rvwap_band_minus_2": stats.rvwap - 2.0 * stats.sigma,
        "z_price_minus_rvwap": stats.z,
        "samples_used": stats.sample_count,
    })
}

fn null_stats_json(window_minutes: i64) -> Value {
    json!({
        "window_minutes": window_minutes,
        "rvwap_w": null,
        "rvwap_sigma_w": null,
        "rvwap_band_plus_1": null,
        "rvwap_band_plus_2": null,
        "rvwap_band_minus_1": null,
        "rvwap_band_minus_2": null,
        "z_price_minus_rvwap": null,
        "samples_used": 0,
    })
}

fn window_to_minutes(code: &str) -> Option<i64> {
    match code {
        "15m" => Some(15),
        "1h" => Some(60),
        "4h" => Some(240),
        "1d" => Some(1440),
        "3d" => Some(4320),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_stats_at, ohlcv_typical_price, window_to_minutes, WeightedSeries};
    use chrono::{TimeZone, Utc};

    #[test]
    fn rvwap_weighted_stats_match_expected() {
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 0, 0, 0)
            .single()
            .expect("valid ts");
        let points = WeightedSeries {
            ts_bucket: vec![
                ts,
                ts + chrono::Duration::minutes(1),
                ts + chrono::Duration::minutes(2),
            ],
            price: vec![100.0, 102.0, 104.0],
            prefix_weight: vec![1.0, 2.0, 4.0],
            prefix_price_weight: vec![100.0, 202.0, 410.0],
            prefix_price_sq_weight: vec![10_000.0, 20_404.0, 42_036.0],
            prefix_positive_samples: vec![1, 2, 3],
        };

        let stats = compute_stats_at(&points, 2, 15, 2).expect("stats");
        assert!((stats.rvwap - 102.5).abs() < 1e-9);
        assert!(stats.sigma > 0.0);
    }

    #[test]
    fn ohlcv_typical_price_matches_formula() {
        let p = ohlcv_typical_price(Some(12.0), Some(6.0), Some(9.0)).expect("price");
        assert!((p - 9.0).abs() < 1e-12);
        assert!(ohlcv_typical_price(Some(12.0), None, Some(9.0)).is_none());
    }

    #[test]
    fn rvwap_window_to_minutes_supports_3d() {
        assert_eq!(window_to_minutes("3d"), Some(4320));
    }
}
