use crate::workflow::schema::{EntrySnapshot, ExecutionIntent, ManagementAction, PriceZone};
use anyhow::{anyhow, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptedExecutionIntent {
    pub side: String,
    pub intent_mode: String,
    pub entry_zone: PriceZone,
    pub trigger_price: Option<f64>,
    pub stop_loss: f64,
    pub take_profit_1: f64,
    pub take_profit_2: f64,
    pub ttl_minutes: u64,
    pub max_drift_pct: f64,
    pub path_id: String,
    pub context_key: String,
    pub reason: Option<String>,
    pub quantity_override: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptedManagementAction {
    pub action_type: String,
    pub context_key: String,
    pub path_id: String,
    pub execution_price: Option<f64>,
    pub reduce_ratio: Option<f64>,
    pub new_stop_loss: Option<f64>,
    pub take_profit_1: Option<f64>,
    pub take_profit_2: Option<f64>,
    pub reason: Option<String>,
}

pub fn adapt_execution_intent(intent: &ExecutionIntent) -> Result<AdaptedExecutionIntent> {
    if !matches!(intent.side.as_str(), "LONG" | "SHORT") {
        return Err(anyhow!("unsupported execution side {}", intent.side));
    }
    if !matches!(
        intent.intent_mode.as_str(),
        "immediate" | "pullback" | "breakout"
    ) {
        return Err(anyhow!(
            "unsupported execution intent_mode {}",
            intent.intent_mode
        ));
    }
    if intent.path_id.trim().is_empty() {
        return Err(anyhow!("execution path_id must be non-empty"));
    }
    if intent.entry_snapshot.context_key.trim().is_empty() {
        return Err(anyhow!("execution context_key must be non-empty"));
    }
    if intent.entry_snapshot.path_id != intent.path_id {
        return Err(anyhow!(
            "execution entry_snapshot.path_id must match execution path_id"
        ));
    }
    Ok(AdaptedExecutionIntent {
        side: intent.side.clone(),
        intent_mode: intent.intent_mode.clone(),
        entry_zone: intent.entry_zone.clone(),
        trigger_price: intent.trigger_price,
        stop_loss: intent.stop_loss,
        take_profit_1: intent.take_profit_1,
        take_profit_2: intent.take_profit_2,
        ttl_minutes: intent.ttl_minutes,
        max_drift_pct: intent.max_drift_pct,
        path_id: intent.path_id.clone(),
        context_key: intent.entry_snapshot.context_key.clone(),
        reason: intent.reason.clone(),
        quantity_override: intent.quantity_override,
    })
}

pub fn adapt_management_action(
    action: &ManagementAction,
    snapshot: &EntrySnapshot,
) -> Result<AdaptedManagementAction> {
    if action.context_key.trim().is_empty() {
        return Err(anyhow!("management action context_key must be non-empty"));
    }
    if action.path_id.trim().is_empty() {
        return Err(anyhow!("management action path_id must be non-empty"));
    }
    if snapshot.context_key != action.context_key {
        return Err(anyhow!(
            "management action context_key must match persisted entry snapshot"
        ));
    }
    if snapshot.path_id != action.path_id {
        return Err(anyhow!(
            "management action path_id must match persisted entry snapshot"
        ));
    }
    if let Some(execution_price) = action.execution_price {
        if !execution_price.is_finite() {
            return Err(anyhow!("management action execution_price must be finite"));
        }
    }
    match action.action_type.as_str() {
        "HOLD" => {
            if action.reduce_ratio.is_some()
                || action.new_stop_loss.is_some()
                || action.take_profit_1.is_some()
                || action.take_profit_2.is_some()
            {
                return Err(anyhow!("HOLD cannot include update fields"));
            }
        }
        "REDUCE_POSITION" => {
            let ratio = action
                .reduce_ratio
                .ok_or_else(|| anyhow!("REDUCE_POSITION requires reduce_ratio"))?;
            if !(0.0 < ratio && ratio <= 1.0) {
                return Err(anyhow!("reduce_ratio must be between 0 and 1"));
            }
        }
        "FLATTEN_POSITION" => {
            if action.new_stop_loss.is_some()
                || action.take_profit_1.is_some()
                || action.take_profit_2.is_some()
            {
                return Err(anyhow!(
                    "FLATTEN_POSITION must not include stop-loss or take-profit updates"
                ));
            }
        }
        "MOVE_STOP" => {
            if action.new_stop_loss.is_none() {
                return Err(anyhow!("MOVE_STOP requires new_stop_loss"));
            }
        }
        "UPDATE_TAKE_PROFIT" => {
            if action.take_profit_1.is_none() && action.take_profit_2.is_none() {
                return Err(anyhow!(
                    "UPDATE_TAKE_PROFIT requires take_profit_1 or take_profit_2"
                ));
            }
        }
        other => return Err(anyhow!("unsupported management action {}", other)),
    }

    Ok(AdaptedManagementAction {
        action_type: action.action_type.clone(),
        context_key: action.context_key.clone(),
        path_id: action.path_id.clone(),
        execution_price: action.execution_price,
        reduce_ratio: action.reduce_ratio,
        new_stop_loss: action.new_stop_loss,
        take_profit_1: action.take_profit_1,
        take_profit_2: action.take_profit_2,
        reason: action.reason.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{adapt_execution_intent, adapt_management_action};
    use crate::workflow::schema::{
        EntrySnapshot, EntrySnapshotRef, ExecutionIntent, ManagementAction, PriceZone,
    };
    use chrono::Utc;

    #[test]
    fn execution_adapter_preserves_exact_workflow_levels() {
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
                high: 102.0,
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
            trigger_price: Some(101.0),
            stop_loss: 99.0,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            ttl_minutes: 15,
            max_drift_pct: 0.2,
            path_id: "path_a".to_string(),
            entry_snapshot: EntrySnapshotRef {
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
            },
            reason: Some("execute".to_string()),
            quantity_override: None,
        };
        let adapted = adapt_execution_intent(&intent).expect("adapt");
        assert_eq!(adapted.side, "LONG");
        assert_eq!(adapted.trigger_price, Some(101.0));
        assert_eq!(adapted.take_profit_1, 104.0);
        assert_eq!(adapted.stop_loss, 99.0);
    }

    #[test]
    fn management_adapter_preserves_contract_fields() {
        let snapshot = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("immediate".to_string()),
            entry_activation_level: Some(PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_zone: Some(PriceZone {
                low: 100.0,
                high: 102.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_invalidation_level: Some(PriceZone {
                low: 99.0,
                high: 99.5,
                timeframe: None,
                label: None,
                reason: None,
            }),
            max_drift_pct: Some(0.2),
            stop_loss: 99.0,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            allowed_stop_loss_levels: vec![99.0, 100.0],
            allowed_take_profit_levels: vec![104.0, 107.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let action = ManagementAction {
            action_type: "MOVE_STOP".to_string(),
            context_key: snapshot.context_key.clone(),
            path_id: snapshot.path_id.clone(),
            execution_price: None,
            reduce_ratio: None,
            new_stop_loss: Some(100.0),
            take_profit_1: None,
            take_profit_2: None,
            reason: Some("protect".to_string()),
        };
        let adapted = adapt_management_action(&action, &snapshot).expect("adapt");
        assert_eq!(adapted.action_type, "MOVE_STOP");
        assert_eq!(adapted.new_stop_loss, Some(100.0));
    }

    #[test]
    fn management_adapter_rejects_path_mismatch() {
        let snapshot = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("immediate".to_string()),
            entry_activation_level: Some(PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_zone: Some(PriceZone {
                low: 100.0,
                high: 102.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_invalidation_level: Some(PriceZone {
                low: 99.0,
                high: 99.5,
                timeframe: None,
                label: None,
                reason: None,
            }),
            max_drift_pct: Some(0.2),
            stop_loss: 99.0,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            allowed_stop_loss_levels: vec![99.0, 100.0],
            allowed_take_profit_levels: vec![104.0, 107.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let action = ManagementAction {
            action_type: "FLATTEN_POSITION".to_string(),
            context_key: snapshot.context_key.clone(),
            path_id: "path_b".to_string(),
            execution_price: None,
            reduce_ratio: None,
            new_stop_loss: None,
            take_profit_1: None,
            take_profit_2: None,
            reason: None,
        };
        assert!(adapt_management_action(&action, &snapshot).is_err());
    }

    #[test]
    fn management_adapter_allows_take_profit_patch_with_both_targets() {
        let snapshot = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("immediate".to_string()),
            entry_activation_level: Some(PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_zone: Some(PriceZone {
                low: 100.0,
                high: 102.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_invalidation_level: Some(PriceZone {
                low: 99.0,
                high: 99.5,
                timeframe: None,
                label: None,
                reason: None,
            }),
            max_drift_pct: Some(0.2),
            stop_loss: 99.0,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            allowed_stop_loss_levels: vec![99.0, 100.0],
            allowed_take_profit_levels: vec![104.0, 107.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let action = ManagementAction {
            action_type: "UPDATE_TAKE_PROFIT".to_string(),
            context_key: snapshot.context_key.clone(),
            path_id: snapshot.path_id.clone(),
            execution_price: None,
            reduce_ratio: None,
            new_stop_loss: None,
            take_profit_1: Some(105.0),
            take_profit_2: Some(108.0),
            reason: Some("roll targets".to_string()),
        };
        let adapted = adapt_management_action(&action, &snapshot).expect("adapt");
        assert_eq!(adapted.take_profit_1, Some(105.0));
        assert_eq!(adapted.take_profit_2, Some(108.0));
    }
}
