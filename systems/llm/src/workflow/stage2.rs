use crate::workflow::schema::{
    derive_max_drift_pct, CurrentPath, EntryPlan, Stage1Output, Stage2AOutput, Stage2AOutputDraft,
    TacticalEntryPlan,
};
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

pub fn stage1_stop_loss_for_path(path: &CurrentPath) -> Result<f64> {
    path.failure_level.risk_boundary(&path.side)
}

fn validate_finalized_stage2a_entry_plan(
    entry_plan: &EntryPlan,
    current_path: &CurrentPath,
) -> Result<()> {
    if entry_plan.entry_reason.trim().is_empty() {
        return Err(anyhow!("entry_plan.entry_reason must be non-empty"));
    }
    if !(1..=20).contains(&entry_plan.leverage) {
        return Err(anyhow!("entry_plan.leverage must be between 1 and 20"));
    }

    match current_path.side.as_str() {
        "LONG" => {
            if entry_plan.stop_loss > entry_plan.entry_zone.low + f64::EPSILON {
                return Err(anyhow!(
                    "Stage1 failure stop_loss must remain on the risk side of entry_plan.entry_zone for LONG"
                ));
            }
        }
        "SHORT" => {
            if entry_plan.stop_loss < entry_plan.entry_zone.high - f64::EPSILON {
                return Err(anyhow!(
                    "Stage1 failure stop_loss must remain on the risk side of entry_plan.entry_zone for SHORT"
                ));
            }
        }
        other => return Err(anyhow!("unsupported path side {}", other)),
    }

    Ok(())
}

pub fn finalize_stage2a_output_with_stage1_risk(
    draft: Stage2AOutputDraft,
    stage1_output: &Stage1Output,
) -> Result<Stage2AOutput> {
    match draft.stage2_decision.as_str() {
        "REQUEST_STAGE1_REEVALUATION" | "PATH_CONFIRMED_WAIT" => Ok(Stage2AOutput {
            stage2_decision: draft.stage2_decision,
            path_audit_note: draft.path_audit_note,
            tactical_entry_plan: None,
            reevaluation_reason: draft.reevaluation_reason,
            wait_reason: draft.wait_reason,
        }),
        "PATH_CONFIRMED_ENTRY" => {
            let current_path = stage1_output
                .current_path
                .as_ref()
                .ok_or_else(|| anyhow!("PATH_CONFIRMED_ENTRY requires Stage1 current_path"))?;
            let tactical_draft = draft
                .tactical_entry_plan
                .ok_or_else(|| anyhow!("PATH_CONFIRMED_ENTRY requires tactical_entry_plan"))?;
            if tactical_draft.path_id != current_path.id {
                return Err(anyhow!(
                    "tactical_entry_plan.path_id must match Stage1 current_path.id"
                ));
            }

            let entry_invalidation_level = current_path.failure_level.clone();
            let stop_loss = stage1_stop_loss_for_path(current_path)?;
            let max_drift_pct =
                derive_max_drift_pct(&current_path.side, &entry_invalidation_level, stop_loss)?;
            let entry_plan_draft = tactical_draft.entry_plan;
            let entry_plan = EntryPlan {
                side: current_path.side.clone(),
                entry_profile: entry_plan_draft.entry_profile,
                intent_mode: entry_plan_draft.intent_mode,
                entry_activation_level: entry_plan_draft.entry_activation_level,
                entry_zone: entry_plan_draft.entry_zone,
                entry_invalidation_level,
                stop_loss,
                leverage: entry_plan_draft.leverage,
                max_drift_pct,
                entry_reason: entry_plan_draft.entry_reason,
                invalidation_reason:
                    "Derived in workflow code from Stage1.current_path.failure_level.".to_string(),
                stop_loss_reason:
                    "Derived in workflow code from Stage1.current_path.failure_level.".to_string(),
            };
            validate_finalized_stage2a_entry_plan(&entry_plan, current_path)?;

            Ok(Stage2AOutput {
                stage2_decision: draft.stage2_decision,
                path_audit_note: draft.path_audit_note,
                tactical_entry_plan: Some(TacticalEntryPlan {
                    path_id: tactical_draft.path_id,
                    entry_plan,
                }),
                reevaluation_reason: draft.reevaluation_reason,
                wait_reason: draft.wait_reason,
            })
        }
        other => Err(anyhow!("unsupported stage2a decision {}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::{failure_level_breached, finalize_stage2a_output_with_stage1_risk};
    use crate::workflow::schema::{
        CurrentPath, MapSummary, OpportunityAssessment, PriceZone, ReevaluationTrigger, Stage1Meta,
        Stage1Output, Stage2AOutputDraft, TargetZone,
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
                    timeframe: Some("1d".to_string()),
                    label: None,
                    reason: None,
                },
                first_path_target_anchor_id: None,
                first_path_target: TargetZone {
                    low: 105.0,
                    high: 106.0,
                    timeframe: Some("1d".to_string()),
                    label: None,
                    reason: None,
                    tp_price: 105.0,
                },
                next_path_target_anchor_id: None,
                next_path_target: TargetZone {
                    low: 110.0,
                    high: 111.0,
                    timeframe: Some("3d".to_string()),
                    label: None,
                    reason: None,
                    tp_price: 110.0,
                },
                failure_anchor_id: None,
                failure_level: PriceZone {
                    low: 98.0,
                    high: 99.0,
                    timeframe: Some("1d".to_string()),
                    label: None,
                    reason: None,
                },
                realization_plan: None,
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

    fn sample_stage2a_draft(entry_low: f64, entry_high: f64) -> Stage2AOutputDraft {
        serde_json::from_value(json!({
            "stage2_decision": "PATH_CONFIRMED_ENTRY",
            "path_audit_note": "The 1D-3D path is still live and execution can be armed.",
            "tactical_entry_plan": {
                "path_id": "path_1",
                "entry_plan": {
                    "entry_profile": "reclaim_then_hold",
                    "intent_mode": "immediate",
                    "entry_activation_level": null,
                    "entry_zone": {
                        "low": entry_low,
                        "high": entry_high,
                        "timeframe": "1d",
                        "label": "entry",
                        "reason": "clean location"
                    },
                    "leverage": 5,
                    "entry_reason": "Entry is aligned with the remaining strategic path."
                }
            },
            "reevaluation_reason": null,
            "wait_reason": null
        }))
        .expect("draft")
    }

    #[test]
    fn stage2a_finalizer_adds_long_stop_from_stage1_failure_level() {
        let finalized = finalize_stage2a_output_with_stage1_risk(
            sample_stage2a_draft(100.0, 101.0),
            &sample_stage1_output("LONG"),
        )
        .expect("finalize");
        let entry_plan = &finalized
            .tactical_entry_plan
            .as_ref()
            .expect("tactical")
            .entry_plan;

        assert_eq!(entry_plan.side, "LONG");
        assert_eq!(entry_plan.entry_invalidation_level.low, 98.0);
        assert_eq!(entry_plan.entry_invalidation_level.high, 99.0);
        assert_eq!(entry_plan.stop_loss, 98.0);
        assert_eq!(entry_plan.max_drift_pct, 0.0);
    }

    #[test]
    fn stage2a_finalizer_adds_short_stop_from_stage1_failure_level() {
        let finalized = finalize_stage2a_output_with_stage1_risk(
            sample_stage2a_draft(96.0, 98.0),
            &sample_stage1_output("SHORT"),
        )
        .expect("finalize");
        let entry_plan = &finalized
            .tactical_entry_plan
            .as_ref()
            .expect("tactical")
            .entry_plan;

        assert_eq!(entry_plan.side, "SHORT");
        assert_eq!(entry_plan.stop_loss, 99.0);
        assert_eq!(entry_plan.entry_invalidation_level.low, 98.0);
        assert_eq!(entry_plan.entry_invalidation_level.high, 99.0);
    }

    #[test]
    fn stage2a_finalizer_rejects_entry_zone_beyond_stage1_failure_level() {
        let err = finalize_stage2a_output_with_stage1_risk(
            sample_stage2a_draft(97.0, 98.0),
            &sample_stage1_output("LONG"),
        )
        .expect_err("risk side violation");

        assert!(err.to_string().contains("risk side"));
    }
}
