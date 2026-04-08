use crate::indicators::context::{IndicatorComputation, IndicatorContext, IndicatorSnapshotRow};
use crate::indicators::i12_buying_exhaustion::{
    append_exhaustion_rows, detect_exhaustion_all_history, exhaustion_event_json,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_views::{build_event_window_view, build_recent_7d_payload};
use chrono::Duration;
use serde_json::json;

pub struct I13SellingExhaustion;

impl Indicator for I13SellingExhaustion {
    fn code(&self) -> &'static str {
        "selling_exhaustion"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = detect_exhaustion_all_history(ctx)
            .iter()
            .filter(|e| e.event_type == "selling_exhaustion")
            .cloned()
            .collect::<Vec<_>>();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| exhaustion_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    )
                }),
            }),
            ..Default::default()
        };

        let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
        let current_events = all_events
            .iter()
            .filter(|event| event.confirm_ts == current_available_ts)
            .cloned()
            .collect::<Vec<_>>();
        append_exhaustion_rows(&mut out, &ctx.symbol, self.code(), &current_events);

        out
    }
}
