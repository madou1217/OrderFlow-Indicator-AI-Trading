use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::funding::{compute_funding_window_metrics, funding_change_json};
use crate::indicators::shared::output_mapper::snapshot_only;
use chrono::Duration;
use serde_json::{json, Value};

const WINDOWS: [(&str, i64); 7] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
    ("7d", 10_080),
    ("30d", 43_200),
];

pub struct I16FundingRate;

impl Indicator for I16FundingRate {
    fn code(&self) -> &'static str {
        "funding_rate"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        if let Some(payload) = ctx.incremental_outputs.funding_snapshot.clone() {
            return snapshot_only(self.code(), payload);
        }
        let mut by_window = serde_json::Map::new();
        for (label, mins) in WINDOWS {
            by_window.insert(label.to_string(), compute_window_metrics(ctx, mins, label));
        }

        let current_metrics = compute_funding_window_metrics(ctx, 1);
        let funding_current = current_metrics.funding_current;
        let funding_current_effective_ts = current_metrics
            .funding_current_effective_ts
            .map(|ts| ts.to_rfc3339());
        let funding_twa = current_metrics.funding_twa;
        let mark_price_last = current_metrics.mark_price_last;
        let mark_price_last_ts = current_metrics.mark_price_last_ts.map(|ts| ts.to_rfc3339());
        let mark_price_twap = current_metrics.mark_price_twap;
        let recent_7d = ctx
            .funding_recent_7d_payload_or_init(build_recent_7d_payload)
            .as_ref()
            .clone();

        IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "funding_current": funding_current,
                    "funding_current_effective_ts": funding_current_effective_ts,
                    "funding_twa": funding_twa,
                    "mark_price_last": mark_price_last,
                    "mark_price_last_ts": mark_price_last_ts,
                    "mark_price_twap": mark_price_twap,
                    "recent_7d": recent_7d,
                    "by_window": Value::Object(by_window)
                }),
            }),
            ..Default::default()
        }
    }
}

fn build_recent_7d_payload(ctx: &IndicatorContext) -> Vec<Value> {
    let end = ctx.ts_bucket + Duration::minutes(1);
    let recent_cutoff = end - Duration::days(7);
    ctx.funding_changes_recent
        .iter()
        .filter(|c| c.ts_change >= recent_cutoff && c.ts_change < end)
        .map(|c| funding_change_json(c))
        .collect()
}

fn compute_window_metrics(ctx: &IndicatorContext, mins: i64, label: &str) -> Value {
    let metrics = compute_funding_window_metrics(ctx, mins);
    let changes = metrics.changes_json.as_array().cloned().unwrap_or_default();

    json!({
        "window": label,
        "funding_current": metrics.funding_current,
        "funding_current_effective_ts": metrics.funding_current_effective_ts.map(|ts| ts.to_rfc3339()),
        "funding_twa": metrics.funding_twa,
        "mark_price_last": metrics.mark_price_last,
        "mark_price_last_ts": metrics.mark_price_last_ts.map(|ts| ts.to_rfc3339()),
        "mark_price_twap": metrics.mark_price_twap,
        "change_count": changes.len(),
        "changes": changes
    })
}
