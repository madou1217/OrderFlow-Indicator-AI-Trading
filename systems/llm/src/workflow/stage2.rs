use crate::workflow::schema::Stage1Output;
use anyhow::{anyhow, Result};

pub fn failure_level_breached(stage1_output: &Stage1Output, latest_price: f64) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    Ok(match path.side.as_str() {
        "LONG" => latest_price <= path.failure_level.low,
        "SHORT" => latest_price >= path.failure_level.high,
        other => return Err(anyhow!("unsupported path side {}", other)),
    })
}

#[cfg(test)]
mod tests {
    use super::failure_level_breached;
    use crate::workflow::schema::{
        CurrentPath, MapSummary, OpportunityAssessment, PriceZone, ReevaluationTrigger, Stage1Meta,
        Stage1Output,
    };
    use chrono::Utc;
    use serde_json::json;

    fn sample_stage1_output(side: &str) -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: MapSummary {
                location_3d: json!({}),
                location_1d: json!({}),
                location_4h: json!({}),
                price_location_class: "value_edge".to_string(),
                key_levels: json!({}),
            },
            opportunity_assessment: OpportunityAssessment::default(),
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(CurrentPath {
                id: "path_1".to_string(),
                side: side.to_string(),
                thesis: "thesis".to_string(),
                risk_grade: "aligned_trend".to_string(),
                activation_anchor_id: None,
                strategic_activation_level: PriceZone {
                    low: 100.0,
                    high: 101.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                first_path_target_anchor_id: None,
                first_path_target: PriceZone {
                    low: 105.0,
                    high: 106.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                next_path_target_anchor_id: None,
                next_path_target: PriceZone {
                    low: 110.0,
                    high: 111.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                failure_anchor_id: None,
                failure_level: PriceZone {
                    low: 98.0,
                    high: 99.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                failure_switch: None,
                setup_type: "continuation".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                tracked_zones: vec![],
            }),
        }
    }

    #[test]
    fn long_failure_level_breach_is_mechanical() {
        assert!(failure_level_breached(&sample_stage1_output("LONG"), 98.0).expect("check"));
        assert!(!failure_level_breached(&sample_stage1_output("LONG"), 101.0).expect("check"));
    }

    #[test]
    fn short_failure_level_breach_is_mechanical() {
        assert!(failure_level_breached(&sample_stage1_output("SHORT"), 99.0).expect("check"));
        assert!(!failure_level_breached(&sample_stage1_output("SHORT"), 98.5).expect("check"));
    }
}
