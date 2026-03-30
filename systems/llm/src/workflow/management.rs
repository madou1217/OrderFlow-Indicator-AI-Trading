use crate::workflow::schema::{CurrentPath, EntrySnapshot, ExecutionIntent};
use chrono::{DateTime, Utc};

fn dedup_levels(levels: impl IntoIterator<Item = f64>) -> Vec<f64> {
    let mut out: Vec<f64> = Vec::new();
    for level in levels {
        if !level.is_finite() {
            continue;
        }
        if out
            .iter()
            .any(|existing| (*existing - level).abs() < f64::EPSILON)
        {
            continue;
        }
        out.push(level);
    }
    out
}

pub fn snapshot_from_execution_intent(
    symbol: &str,
    intent: &ExecutionIntent,
    current_path: &CurrentPath,
    now: DateTime<Utc>,
) -> EntrySnapshot {
    EntrySnapshot {
        symbol: symbol.to_ascii_uppercase(),
        context_key: intent.entry_snapshot.context_key.clone(),
        path_id: intent.path_id.clone(),
        side: intent.side.clone(),
        entry_profile: intent.entry_profile.clone(),
        intent_mode: Some(intent.intent_mode.clone()),
        entry_activation_level: intent.entry_activation_level.clone(),
        entry_zone: Some(intent.entry_zone.clone()),
        entry_invalidation_level: intent.entry_invalidation_level.clone(),
        max_drift_pct: Some(intent.max_drift_pct),
        stop_loss: intent.stop_loss,
        take_profit_1: intent.take_profit_1,
        take_profit_2: intent.take_profit_2,
        allowed_stop_loss_levels: dedup_levels([intent.stop_loss, current_path.failure_level.low]),
        allowed_take_profit_levels: dedup_levels([
            intent.take_profit_1,
            intent.take_profit_2,
            current_path.first_path_target.midpoint(),
            current_path.next_path_target.midpoint(),
        ]),
        tp1_realized: false,
        applied_driver_deterioration_signals: Vec::new(),
        created_at: now,
        updated_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::snapshot_from_execution_intent;
    use crate::workflow::schema::{
        CurrentPath, EntrySnapshotRef, ExecutionIntent, PriceZone, ReevaluationTrigger,
    };
    use chrono::Utc;

    #[test]
    fn snapshot_captures_allowed_management_levels_from_path_contract() {
        let intent = ExecutionIntent {
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: "immediate".to_string(),
            entry_activation_level: Some(PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_zone: PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            entry_invalidation_level: Some(PriceZone {
                low: 99.0,
                high: 99.5,
                timeframe: None,
                label: None,
                reason: None,
            }),
            trigger_price: Some(100.5),
            stop_loss: 99.0,
            take_profit_1: 103.0,
            take_profit_2: 105.0,
            ttl_minutes: 15,
            max_drift_pct: 0.2,
            path_id: "path_a".to_string(),
            entry_snapshot: EntrySnapshotRef {
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_a".to_string(),
            },
            reason: None,
            quantity_override: None,
        };
        let path = CurrentPath {
            id: "path_a".to_string(),
            side: "LONG".to_string(),
            thesis: "continuation".to_string(),
            risk_grade: "aligned_trend".to_string(),
            activation_anchor_id: None,
            strategic_activation_level: PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            first_path_target_anchor_id: None,
            first_path_target: PriceZone {
                low: 103.0,
                high: 103.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            next_path_target_anchor_id: None,
            next_path_target: PriceZone {
                low: 105.0,
                high: 105.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            failure_anchor_id: None,
            failure_level: PriceZone {
                low: 99.0,
                high: 99.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            failure_switch: Some("alt".to_string()),
            setup_type: "A_continuation".to_string(),
            reevaluation_trigger: ReevaluationTrigger::default(),
            tracked_zones: vec![],
        };
        let snapshot = snapshot_from_execution_intent("ETHUSDT", &intent, &path, Utc::now());
        assert_eq!(snapshot.allowed_stop_loss_levels, vec![99.0]);
        assert_eq!(snapshot.allowed_take_profit_levels, vec![103.0, 105.0]);
    }
}
