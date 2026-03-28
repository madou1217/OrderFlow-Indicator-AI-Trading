use crate::workflow::schema::{
    ManagementAction, Stage1Output, Stage2Decision, WorkflowRuntimeContract,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;

const ALLOWED_SETUP_TYPES: &[&str] = &["A_continuation", "B_reversal", "C_value_return"];
const ALLOWED_REEVALUATION_SIGNALS: &[&str] =
    &["extreme_location", "reverse_confirmation", "driver_change"];
const ALLOWED_INTENT_MODES: &[&str] = &["immediate", "pullback", "breakout"];
const ALLOWED_MANAGEMENT_ACTIONS: &[&str] = &[
    "HOLD",
    "REDUCE_POSITION",
    "FLATTEN_POSITION",
    "MOVE_STOP",
    "UPDATE_TAKE_PROFIT",
];
const ALLOWED_STOP_MIGRATION_AFTER_TARGETS: &[&str] = &["take_profit_1", "take_profit_2"];
const ALLOWED_STOP_MIGRATION_BASES: &[&str] =
    &["activation_level", "first_path_target", "next_path_target"];
const ALLOWED_DRIVER_DETERIORATION_SIGNALS: &[&str] = &[
    "spot_confirmation_lost",
    "oi_support_lost",
    "fake_order_risk_rising",
    "driver_flip_confirmed",
];

fn approx_in_zone(level: f64, low: f64, high: f64) -> bool {
    level >= low && level <= high
}

fn approx_in_levels(level: f64, levels: &[f64]) -> bool {
    levels
        .iter()
        .any(|candidate| (*candidate - level).abs() < f64::EPSILON)
}

fn is_machine_identifier(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn stop_basis_zone<'a>(
    current_path: &'a crate::workflow::schema::CurrentPath,
    basis: &str,
) -> Option<&'a crate::workflow::schema::PriceZone> {
    match basis {
        "activation_level" => Some(&current_path.activation_level),
        "first_path_target" => Some(&current_path.first_path_target),
        "next_path_target" => Some(&current_path.next_path_target),
        _ => None,
    }
}

fn validate_stop_migration_rule(
    current_path: &crate::workflow::schema::CurrentPath,
    rule: &crate::workflow::schema::StopMigrationRule,
) -> Result<()> {
    if !ALLOWED_STOP_MIGRATION_AFTER_TARGETS.contains(&rule.after_target.as_str()) {
        return Err(anyhow!(
            "unsupported stop_migration_rules.after_target {}",
            rule.after_target
        ));
    }
    if !ALLOWED_STOP_MIGRATION_BASES.contains(&rule.new_stop_basis.as_str()) {
        return Err(anyhow!(
            "unsupported stop_migration_rules.new_stop_basis {}",
            rule.new_stop_basis
        ));
    }
    let zone = stop_basis_zone(current_path, &rule.new_stop_basis)
        .ok_or_else(|| anyhow!("unsupported stop migration basis"))?;
    if !approx_in_zone(rule.new_stop_level, zone.low, zone.high) {
        return Err(anyhow!(
            "stop_migration_rules.new_stop_level must align with {}",
            rule.new_stop_basis
        ));
    }
    Ok(())
}

fn validate_driver_deterioration_rule(
    rule: &crate::workflow::schema::DriverDeteriorationRule,
    require_reduce_ratio: bool,
    field_name: &str,
) -> Result<()> {
    if !ALLOWED_DRIVER_DETERIORATION_SIGNALS.contains(&rule.driver_signal.as_str()) {
        return Err(anyhow!(
            "{}.driver_signal must be one of [spot_confirmation_lost, oi_support_lost, fake_order_risk_rising, driver_flip_confirmed]",
            field_name
        ));
    }
    if require_reduce_ratio {
        let ratio = rule
            .reduce_ratio
            .ok_or_else(|| anyhow!("{}.reduce_ratio is required", field_name))?;
        if !(0.0 < ratio && ratio <= 1.0) {
            return Err(anyhow!(
                "{}.reduce_ratio must be between 0 and 1",
                field_name
            ));
        }
    } else if rule.reduce_ratio.is_some() {
        return Err(anyhow!("{} must not include reduce_ratio", field_name));
    }
    Ok(())
}

fn validate_runtime_context_key(
    symbol: &str,
    context_key: &str,
    expected_side: Option<&str>,
) -> Result<()> {
    let mut parts = context_key.split(':');
    let symbol_part = parts
        .next()
        .ok_or_else(|| anyhow!("context_key must include symbol prefix"))?;
    let side_part = parts
        .next()
        .ok_or_else(|| anyhow!("context_key must include side suffix"))?;
    let suffixes = parts.collect::<Vec<_>>();
    if suffixes.iter().any(|suffix| suffix.trim().is_empty()) {
        return Err(anyhow!(
            "context_key opaque suffix must be non-empty when present"
        ));
    }
    if !symbol_part.eq_ignore_ascii_case(symbol) {
        return Err(anyhow!(
            "context_key symbol {} does not match workflow symbol {}",
            symbol_part,
            symbol
        ));
    }
    if !matches!(side_part, "LONG" | "SHORT" | "BOTH") {
        return Err(anyhow!(
            "context_key side {} must be LONG, SHORT, or BOTH",
            side_part
        ));
    }
    if let Some(expected_side) = expected_side {
        if !side_part.eq_ignore_ascii_case(expected_side) && !side_part.eq_ignore_ascii_case("BOTH")
        {
            return Err(anyhow!(
                "context_key side {} must match expected side {} or BOTH",
                side_part,
                expected_side
            ));
        }
    }
    Ok(())
}

fn runtime_requires_reevaluation(runtime_contract: &WorkflowRuntimeContract) -> bool {
    runtime_contract.no_edge_reentered
        || runtime_contract.failure_level_breached
        || runtime_contract.reevaluation_trigger_hit
}

pub fn parse_stage1_output(value: Value) -> Result<Stage1Output> {
    let mut output: Stage1Output = serde_json::from_value(value)?;
    if output.monitoring_status.trim().is_empty() {
        return Err(anyhow!("monitoring_status must be non-empty"));
    }
    if output.monitoring_status == "active" {
        let current_script = output
            .current_script
            .as_ref()
            .ok_or_else(|| anyhow!("active stage1 output requires current_script"))?;
        if current_script.trim().is_empty() {
            return Err(anyhow!("current_script must be non-empty"));
        }
        let current_path = output
            .current_path
            .as_ref()
            .ok_or_else(|| anyhow!("active stage1 output requires current_path"))?;
        if current_path.id.trim().is_empty() {
            return Err(anyhow!("current_path.id must be non-empty"));
        }
        if current_path.id == current_path.failure_switch {
            return Err(anyhow!(
                "failure_switch must be a script name, not current path id"
            ));
        }
        if current_path.thesis.trim().is_empty() {
            return Err(anyhow!("current_path.thesis must be non-empty"));
        }
        if !matches!(current_path.side.as_str(), "LONG" | "SHORT") {
            return Err(anyhow!("current_path.side must be LONG or SHORT"));
        }
        if !ALLOWED_SETUP_TYPES.contains(&current_path.setup_type.as_str()) {
            return Err(anyhow!(
                "unsupported setup_type {}",
                current_path.setup_type
            ));
        }
        if current_path.failure_switch.trim().is_empty() {
            return Err(anyhow!("failure_switch must be non-empty"));
        }
        if !is_machine_identifier(&current_path.failure_switch) {
            return Err(anyhow!(
                "failure_switch must be an English machine-style script identifier"
            ));
        }
        if current_path.tracked_zones.is_empty() {
            return Err(anyhow!("tracked_zones must be non-empty"));
        }
        if current_path.management_plan.take_profit_1_basis != "first_path_target" {
            return Err(anyhow!("take_profit_1_basis must be first_path_target"));
        }
        if current_path.management_plan.take_profit_2_basis != "next_path_target" {
            return Err(anyhow!("take_profit_2_basis must be next_path_target"));
        }
        if !approx_in_zone(
            current_path.management_plan.take_profit_1_level,
            current_path.first_path_target.low,
            current_path.first_path_target.high,
        ) {
            return Err(anyhow!(
                "take_profit_1_level must align with first_path_target"
            ));
        }
        if !approx_in_zone(
            current_path.management_plan.take_profit_2_level,
            current_path.next_path_target.low,
            current_path.next_path_target.high,
        ) {
            return Err(anyhow!(
                "take_profit_2_level must align with next_path_target"
            ));
        }
        for signal in &current_path.reevaluation_trigger.signals {
            if !ALLOWED_REEVALUATION_SIGNALS.contains(&signal.as_str()) {
                return Err(anyhow!("unsupported reevaluation signal {}", signal));
            }
        }
        for rule in &current_path.management_plan.stop_migration_rules {
            validate_stop_migration_rule(current_path, rule)?;
        }
        for signal in &current_path.management_plan.reduce_on_driver_deterioration {
            validate_driver_deterioration_rule(signal, true, "reduce_on_driver_deterioration")?;
        }
        for signal in &current_path
            .management_plan
            .exit_full_on_driver_deterioration
        {
            validate_driver_deterioration_rule(signal, false, "exit_full_on_driver_deterioration")?;
        }
    } else if output.monitoring_status == "no_edge" {
        if output.current_script.is_some() || output.current_path.is_some() {
            output.current_script = None;
            output.current_path = None;
        }
    }
    Ok(output)
}

pub fn parse_stage2_decision(
    value: Value,
    symbol: &str,
    stage1_output: &Stage1Output,
    runtime_contract: &WorkflowRuntimeContract,
    has_positions: bool,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
) -> Result<Stage2Decision> {
    let decision: Stage2Decision = serde_json::from_value(value)?;
    match decision.decision.as_str() {
        "WAIT" | "EXECUTE" | "REQUEST_STAGE1_REEVALUATION" => {}
        other => return Err(anyhow!("unsupported stage2 decision {}", other)),
    }
    if decision.reason.trim().is_empty() {
        return Err(anyhow!("stage2 reason must be non-empty"));
    }
    let hard_gate = decision
        .hard_gate
        .as_ref()
        .ok_or_else(|| anyhow!("stage2 output requires hard_gate"))?;
    let soft_gate = decision
        .soft_gate
        .as_ref()
        .ok_or_else(|| anyhow!("stage2 output requires soft_gate"))?;
    if hard_gate != &runtime_contract.hard_gate {
        return Err(anyhow!(
            "stage2 hard_gate must match code-side runtime contract"
        ));
    }
    if soft_gate != &runtime_contract.soft_gate {
        return Err(anyhow!(
            "stage2 soft_gate must match code-side runtime contract"
        ));
    }
    if runtime_contract.monitoring_status == "no_edge" {
        if decision.execution_intent.is_some() {
            return Err(anyhow!(
                "no_edge stage2 output cannot include execution_intent"
            ));
        }
        if runtime_contract.no_edge_reentered {
            let request = decision
                .request_stage1_reevaluation
                .as_ref()
                .ok_or_else(|| anyhow!("no_edge_reentered requires reevaluation request"))?;
            if decision.decision != "REQUEST_STAGE1_REEVALUATION" {
                return Err(anyhow!(
                    "no_edge_reentered must request Stage1 reevaluation"
                ));
            }
            if request.refresh_reason != "no_edge_reentered" {
                return Err(anyhow!(
                    "no_edge_reentered must use refresh_reason=no_edge_reentered"
                ));
            }
            if request.trigger_source != "refresh_hint" {
                return Err(anyhow!(
                    "no_edge_reentered must use trigger_source=refresh_hint"
                ));
            }
        } else if decision.decision != "WAIT" || decision.request_stage1_reevaluation.is_some() {
            return Err(anyhow!(
                "no_edge without reentry must remain WAIT without reevaluation request"
            ));
        }
    }
    if runtime_contract.monitoring_status == "active"
        && runtime_requires_reevaluation(runtime_contract)
        && decision.decision != "REQUEST_STAGE1_REEVALUATION"
    {
        return Err(anyhow!(
            "runtime invalidation requires REQUEST_STAGE1_REEVALUATION"
        ));
    }
    if decision.decision == "REQUEST_STAGE1_REEVALUATION" {
        if !runtime_requires_reevaluation(runtime_contract) {
            return Err(anyhow!(
                "Stage2 cannot request reevaluation without code-side trigger"
            ));
        }
        let request = decision
            .request_stage1_reevaluation
            .as_ref()
            .ok_or_else(|| anyhow!("reevaluation decision requires request payload"))?;
        if !matches!(
            request.refresh_reason.as_str(),
            "thesis_invalidated" | "no_edge_reentered"
        ) {
            return Err(anyhow!("invalid refresh_reason {}", request.refresh_reason));
        }
        if decision.execution_intent.is_some() {
            return Err(anyhow!(
                "REQUEST_STAGE1_REEVALUATION cannot include execution_intent"
            ));
        }
    }
    if decision.decision == "WAIT" && decision.execution_intent.is_some() {
        return Err(anyhow!("WAIT cannot include execution_intent"));
    }
    if decision.decision == "EXECUTE" {
        if !runtime_contract.allow_execute {
            return Err(anyhow!(
                "EXECUTE is not allowed by code-side runtime contract"
            ));
        }
        let intent = decision
            .execution_intent
            .as_ref()
            .ok_or_else(|| anyhow!("EXECUTE requires execution_intent"))?;
        let current_path = stage1_output
            .current_path
            .as_ref()
            .ok_or_else(|| anyhow!("active stage1 path missing"))?;
        if intent.path_id != current_path.id {
            return Err(anyhow!(
                "execution_intent.path_id must match current_path.id"
            ));
        }
        if intent.side != current_path.side {
            return Err(anyhow!(
                "execution_intent.side must match current_path.side"
            ));
        }
        if !ALLOWED_INTENT_MODES.contains(&intent.intent_mode.as_str()) {
            return Err(anyhow!(
                "unsupported execution intent_mode {}",
                intent.intent_mode
            ));
        }
        if intent.entry_snapshot.context_key.trim().is_empty() {
            return Err(anyhow!(
                "execution_intent.entry_snapshot.context_key must be non-empty"
            ));
        }
        if let Some(snapshot) = entry_snapshots.get(&intent.entry_snapshot.context_key) {
            if snapshot.path_id != intent.path_id {
                return Err(anyhow!(
                    "execution_intent.context_key already exists with a different path_id"
                ));
            }
        }
        validate_runtime_context_key(
            symbol,
            &intent.entry_snapshot.context_key,
            Some(&intent.side),
        )?;
        if intent.entry_snapshot.path_id != intent.path_id {
            return Err(anyhow!(
                "entry_snapshot.path_id must match execution path id"
            ));
        }
        if !approx_in_zone(
            intent.stop_loss,
            current_path.failure_level.low,
            current_path.failure_level.high,
        ) {
            return Err(anyhow!("execution stop_loss must align with failure_level"));
        }
        if !approx_in_zone(
            intent.take_profit_1,
            current_path.first_path_target.low,
            current_path.first_path_target.high,
        ) {
            return Err(anyhow!("execution tp1 must align with first_path_target"));
        }
        if !approx_in_zone(
            intent.take_profit_2,
            current_path.next_path_target.low,
            current_path.next_path_target.high,
        ) {
            return Err(anyhow!("execution tp2 must align with next_path_target"));
        }
        if !(hard_gate.location_valid && hard_gate.trigger_confirmed) {
            return Err(anyhow!("EXECUTE requires hard_gate to fully pass"));
        }
        if soft_gate.passed_count < runtime_contract.soft_gate_min_required {
            return Err(anyhow!(
                "EXECUTE requires soft_gate.passed_count to meet runtime threshold"
            ));
        }
    }
    if !has_positions && !decision.management_actions.is_empty() {
        return Err(anyhow!("management_actions require active positions"));
    }
    for action in &decision.management_actions {
        validate_management_action(symbol, action, entry_snapshots)?;
    }
    Ok(decision)
}

fn validate_management_action(
    symbol: &str,
    action: &ManagementAction,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
) -> Result<()> {
    if !ALLOWED_MANAGEMENT_ACTIONS.contains(&action.action_type.as_str()) {
        return Err(anyhow!(
            "unsupported management action {}",
            action.action_type
        ));
    }
    if action.context_key.trim().is_empty() {
        return Err(anyhow!("management action context_key must be non-empty"));
    }
    let snapshot = entry_snapshots
        .get(&action.context_key)
        .ok_or_else(|| anyhow!("missing entry snapshot for {}", action.context_key))?;
    validate_runtime_context_key(symbol, &action.context_key, Some(&snapshot.side))?;
    if snapshot.path_id != action.path_id {
        return Err(anyhow!(
            "management action path_id must match persisted entry snapshot path_id"
        ));
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
        "FLATTEN_POSITION" => {}
        "MOVE_STOP" => {
            let new_stop_loss = action
                .new_stop_loss
                .ok_or_else(|| anyhow!("MOVE_STOP requires new_stop_loss"))?;
            if !approx_in_levels(new_stop_loss, &snapshot.allowed_stop_loss_levels) {
                return Err(anyhow!("MOVE_STOP requires new_stop_loss"));
            }
        }
        "UPDATE_TAKE_PROFIT" => {
            if action.take_profit_1.is_none() && action.take_profit_2.is_none() {
                return Err(anyhow!(
                    "UPDATE_TAKE_PROFIT requires take_profit_1 or take_profit_2"
                ));
            }
            if action.take_profit_1.is_some() && action.take_profit_2.is_some() {
                return Err(anyhow!(
                    "UPDATE_TAKE_PROFIT cannot include both take_profit_1 and take_profit_2"
                ));
            }
            if let Some(level) = action.take_profit_1 {
                if !approx_in_levels(level, &snapshot.allowed_take_profit_levels) {
                    return Err(anyhow!(
                        "take_profit_1 must come from persisted management plan/path levels"
                    ));
                }
            }
            if let Some(level) = action.take_profit_2 {
                if !approx_in_levels(level, &snapshot.allowed_take_profit_levels) {
                    return Err(anyhow!(
                        "take_profit_2 must come from persisted management plan/path levels"
                    ));
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub fn parse_json_from_text(raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Some(v);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<Value>(&trimmed[start..=end]).ok()
}

#[cfg(test)]
mod tests {
    use super::{parse_stage1_output, parse_stage2_decision};
    use crate::workflow::schema::{
        EntrySnapshot, HardGateEvaluation, PriceZone, SoftGateEvaluation, Stage1Meta, Stage1Output,
        WorkflowRuntimeContract,
    };
    use chrono::Utc;
    use serde_json::json;
    use std::collections::HashMap;

    fn runtime_contract(
        stage1_output: &Stage1Output,
        hard_gate: HardGateEvaluation,
        soft_gate: SoftGateEvaluation,
    ) -> WorkflowRuntimeContract {
        WorkflowRuntimeContract {
            monitoring_status: stage1_output.monitoring_status.clone(),
            no_edge_reentered: false,
            failure_level_breached: false,
            reevaluation_trigger_hit: false,
            activation_level_active: hard_gate.location_valid,
            setup_confirmed: hard_gate.trigger_confirmed,
            hard_gate: hard_gate.clone(),
            soft_gate: soft_gate.clone(),
            soft_gate_min_required: 3,
            allow_execute: stage1_output.monitoring_status == "active"
                && hard_gate.location_valid
                && hard_gate.trigger_confirmed
                && soft_gate.passed_count >= 3,
            request_refresh_reason: None,
            request_trigger_source: None,
            recommended_context_key: None,
        }
    }

    #[test]
    fn stage1_parser_requires_path_id() {
        let value = json!({
            "meta": { "stage1_ts": Utc::now() },
            "monitoring_status": "active",
            "current_script": "reclaim reversal",
            "driver_attribution": { "driver_bias": "spot_led" },
            "current_path": {
                "id": "",
                "side": "LONG",
                "thesis": "x",
                "activation_level": {"low": 1.0, "high": 1.0},
                "first_path_target": {"low": 2.0, "high": 2.0},
                "next_path_target": {"low": 3.0, "high": 3.0},
                "failure_level": {"low": 0.5, "high": 0.5},
                "failure_switch": "alt",
                "setup_type": "B_reversal",
                "reevaluation_trigger": { "signals": ["driver_change"] },
                "management_plan": {
                    "take_profit_1_basis": "first_path_target",
                    "take_profit_2_basis": "next_path_target",
                    "take_profit_1_level": 2.0,
                    "take_profit_2_level": 3.0,
                    "stop_migration_rules": [],
                    "reduce_on_driver_deterioration": [],
                    "exit_full_on_driver_deterioration": []
                },
                "tracked_zones": [{
                    "zone_id": "z1",
                    "timeframe": "4h",
                    "role": "activation",
                    "low": 1.0,
                    "high": 1.0
                }]
            }
        });
        assert!(parse_stage1_output(value).is_err());
    }

    #[test]
    fn stage2_parser_validates_management_context_path_binding() {
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: None,
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_new".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                activation_level: PriceZone {
                    low: 1.0,
                    high: 1.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2.0,
                    high: 2.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 3.0,
                    high: 3.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 0.5,
                    high: 0.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "alt".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2.0,
                    take_profit_2_level: 3.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![crate::workflow::schema::TrackedZone {
                    zone_id: "z1".to_string(),
                    timeframe: "4h".to_string(),
                    role: "activation".to_string(),
                    low: 1.0,
                    high: 1.0,
                    reason: None,
                }],
            }),
        };
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "ETHUSDT:LONG".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_old".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1.0,
                take_profit_1: 2.0,
                take_profit_2: 3.0,
                allowed_stop_loss_levels: vec![1.0, 1.1],
                allowed_take_profit_levels: vec![2.0, 3.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );
        let value = json!({
            "decision": "WAIT",
            "reason": "hold",
            "request_stage1_reevaluation": null,
            "execution_intent": null,
            "management_actions": [{
                "type": "MOVE_STOP",
                "context_key": "ETHUSDT:LONG",
                "path_id": "path_new",
                "new_stop_loss": 1.1
            }],
            "hard_gate": {
                "location_valid": true,
                "trigger_confirmed": true
            },
            "soft_gate": {
                "state_clear": true,
                "driver_clear": true,
                "orderflow_real": true,
                "invalidation_clear": true,
                "passed_count": 4
            }
        });
        let runtime_contract = runtime_contract(
            &stage1_output,
            HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            },
            SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            },
        );
        assert!(parse_stage2_decision(
            value,
            "ETHUSDT",
            &stage1_output,
            &runtime_contract,
            true,
            &snapshots,
        )
        .is_err());
    }

    #[test]
    fn stage2_parser_rejects_wait_with_execution_payload() {
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: None,
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_new".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                activation_level: PriceZone {
                    low: 1.0,
                    high: 1.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2.0,
                    high: 2.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 3.0,
                    high: 3.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 0.5,
                    high: 0.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "alt".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2.0,
                    take_profit_2_level: 3.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![crate::workflow::schema::TrackedZone {
                    zone_id: "z1".to_string(),
                    timeframe: "4h".to_string(),
                    role: "activation".to_string(),
                    low: 1.0,
                    high: 1.0,
                    reason: None,
                }],
            }),
        };
        let value = json!({
            "decision": "WAIT",
            "reason": "wait",
            "request_stage1_reevaluation": null,
            "execution_intent": {
                "side": "LONG",
                "intent_mode": "immediate",
                "entry_zone": {"low": 1.0, "high": 1.0},
                "stop_loss": 0.5,
                "take_profit_1": 2.0,
                "take_profit_2": 3.0,
                "ttl_minutes": 15,
                "max_drift_pct": 0.1,
                "path_id": "path_new",
                "entry_snapshot": {
                    "context_key": "ETHUSDT:LONG",
                    "path_id": "path_new"
                }
            },
            "management_actions": [],
            "hard_gate": {
                "location_valid": true,
                "trigger_confirmed": true
            },
            "soft_gate": {
                "state_clear": true,
                "driver_clear": true,
                "orderflow_real": true,
                "invalidation_clear": true,
                "passed_count": 4
            }
        });
        assert!(parse_stage2_decision(
            value,
            "ETHUSDT",
            &stage1_output,
            &runtime_contract(
                &stage1_output,
                HardGateEvaluation {
                    location_valid: true,
                    trigger_confirmed: true,
                },
                SoftGateEvaluation {
                    state_clear: true,
                    driver_clear: true,
                    orderflow_real: true,
                    invalidation_clear: true,
                    passed_count: 4,
                },
            ),
            false,
            &HashMap::new(),
        )
        .is_err());
    }

    #[test]
    fn stage2_parser_rejects_dual_take_profit_update() {
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: None,
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_new".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                activation_level: PriceZone {
                    low: 1.0,
                    high: 1.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2.0,
                    high: 2.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 3.0,
                    high: 3.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 0.5,
                    high: 0.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "alt".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2.0,
                    take_profit_2_level: 3.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![crate::workflow::schema::TrackedZone {
                    zone_id: "z1".to_string(),
                    timeframe: "4h".to_string(),
                    role: "activation".to_string(),
                    low: 1.0,
                    high: 1.0,
                    reason: None,
                }],
            }),
        };
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "ETHUSDT:LONG".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_new".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1.0,
                take_profit_1: 2.0,
                take_profit_2: 3.0,
                allowed_stop_loss_levels: vec![1.0, 1.1],
                allowed_take_profit_levels: vec![2.0, 3.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );
        let value = json!({
            "decision": "WAIT",
            "reason": "manage",
            "request_stage1_reevaluation": null,
            "execution_intent": null,
            "management_actions": [{
                "type": "UPDATE_TAKE_PROFIT",
                "context_key": "ETHUSDT:LONG",
                "path_id": "path_new",
                "take_profit_1": 2.1,
                "take_profit_2": 3.1
            }],
            "hard_gate": {
                "location_valid": true,
                "trigger_confirmed": true
            },
            "soft_gate": {
                "state_clear": true,
                "driver_clear": true,
                "orderflow_real": true,
                "invalidation_clear": true,
                "passed_count": 4
            }
        });
        let runtime_contract = runtime_contract(
            &stage1_output,
            HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            },
            SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            },
        );
        assert!(parse_stage2_decision(
            value,
            "ETHUSDT",
            &stage1_output,
            &runtime_contract,
            true,
            &snapshots,
        )
        .is_err());
    }

    #[test]
    fn stage2_parser_rejects_management_price_outside_persisted_contract() {
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: None,
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_new".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                activation_level: PriceZone {
                    low: 1.0,
                    high: 1.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2.0,
                    high: 2.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 3.0,
                    high: 3.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 0.5,
                    high: 0.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "alt".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2.0,
                    take_profit_2_level: 3.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![crate::workflow::schema::TrackedZone {
                    zone_id: "z1".to_string(),
                    timeframe: "4h".to_string(),
                    role: "activation".to_string(),
                    low: 1.0,
                    high: 1.0,
                    reason: None,
                }],
            }),
        };
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "ETHUSDT:LONG".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_new".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1.0,
                take_profit_1: 2.0,
                take_profit_2: 3.0,
                allowed_stop_loss_levels: vec![1.0, 1.1],
                allowed_take_profit_levels: vec![2.0, 3.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );
        let value = json!({
            "decision": "WAIT",
            "reason": "manage",
            "request_stage1_reevaluation": null,
            "execution_intent": null,
            "management_actions": [{
                "type": "MOVE_STOP",
                "context_key": "ETHUSDT:LONG",
                "path_id": "path_new",
                "new_stop_loss": 9.9
            }],
            "hard_gate": {
                "location_valid": true,
                "trigger_confirmed": true
            },
            "soft_gate": {
                "state_clear": true,
                "driver_clear": true,
                "orderflow_real": true,
                "invalidation_clear": true,
                "passed_count": 4
            }
        });
        let runtime_contract = runtime_contract(
            &stage1_output,
            HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            },
            SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            },
        );
        assert!(parse_stage2_decision(
            value,
            "ETHUSDT",
            &stage1_output,
            &runtime_contract,
            true,
            &snapshots,
        )
        .is_err());
    }

    #[test]
    fn stage2_parser_allows_wait_with_management_actions_when_positions_exist() {
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: None,
            current_script: Some("continuation".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_a".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                activation_level: PriceZone {
                    low: 1.0,
                    high: 1.1,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2.0,
                    high: 2.1,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 3.0,
                    high: 3.1,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 0.8,
                    high: 0.9,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "reversal".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2.0,
                    take_profit_2_level: 3.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![crate::workflow::schema::TrackedZone {
                    zone_id: "z1".to_string(),
                    timeframe: "15m".to_string(),
                    role: "activation".to_string(),
                    low: 1.0,
                    high: 1.1,
                    reason: None,
                }],
            }),
        };
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "ETHUSDT:LONG".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_a".to_string(),
                side: "LONG".to_string(),
                stop_loss: 0.9,
                take_profit_1: 2.0,
                take_profit_2: 3.0,
                allowed_stop_loss_levels: vec![0.9],
                allowed_take_profit_levels: vec![2.0, 3.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );
        let value = json!({
            "decision": "WAIT",
            "reason": "manage only",
            "request_stage1_reevaluation": null,
            "execution_intent": null,
            "management_actions": [{
                "type": "HOLD",
                "context_key": "ETHUSDT:LONG",
                "path_id": "path_a"
            }],
            "hard_gate": {
                "location_valid": false,
                "trigger_confirmed": false
            },
            "soft_gate": {
                "state_clear": true,
                "driver_clear": true,
                "orderflow_real": true,
                "invalidation_clear": true,
                "passed_count": 4
            }
        });

        let parsed = parse_stage2_decision(
            value,
            "ETHUSDT",
            &stage1_output,
            &runtime_contract(
                &stage1_output,
                HardGateEvaluation {
                    location_valid: false,
                    trigger_confirmed: false,
                },
                SoftGateEvaluation {
                    state_clear: true,
                    driver_clear: true,
                    orderflow_real: true,
                    invalidation_clear: true,
                    passed_count: 4,
                },
            ),
            true,
            &snapshots,
        )
        .expect("parse");
        assert_eq!(parsed.decision, "WAIT");
        assert_eq!(parsed.management_actions.len(), 1);
    }

    #[test]
    fn stage1_parser_rejects_natural_language_failure_switch() {
        let value = json!({
            "meta": { "stage1_ts": Utc::now() },
            "monitoring_status": "active",
            "no_trade_reason": null,
            "refresh_hints": [],
            "map_summary": null,
            "current_script": "value_return_long",
            "driver_attribution": null,
            "current_path": {
                "id": "path_a",
                "side": "LONG",
                "thesis": "English thesis",
                "activation_level": {"low": 100.0, "high": 101.0, "timeframe": null, "label": null, "reason": null},
                "first_path_target": {"low": 103.0, "high": 103.0, "timeframe": null, "label": null, "reason": null},
                "next_path_target": {"low": 105.0, "high": 105.0, "timeframe": null, "label": null, "reason": null},
                "failure_level": {"low": 99.0, "high": 99.0, "timeframe": null, "label": null, "reason": null},
                "failure_switch": "若失效则转空重评",
                "setup_type": "C_value_return",
                "reevaluation_trigger": { "signals": ["driver_change"] },
                "management_plan": {
                    "take_profit_1_basis": "first_path_target",
                    "take_profit_2_basis": "next_path_target",
                    "take_profit_1_level": 103.0,
                    "take_profit_2_level": 105.0,
                    "stop_migration_rules": [],
                    "reduce_on_driver_deterioration": [],
                    "exit_full_on_driver_deterioration": []
                },
                "tracked_zones": [{
                    "zone_id": "z1",
                    "timeframe": "4h",
                    "role": "activation",
                    "low": 100.0,
                    "high": 101.0,
                    "reason": null
                }]
            }
        });
        assert!(parse_stage1_output(value).is_err());
    }

    #[test]
    fn stage1_parser_rejects_freeform_management_plan_contract_fields() {
        let value = json!({
            "meta": { "stage1_ts": Utc::now() },
            "monitoring_status": "active",
            "no_trade_reason": null,
            "refresh_hints": [],
            "map_summary": null,
            "current_script": "value_return_long",
            "driver_attribution": null,
            "current_path": {
                "id": "path_a",
                "side": "LONG",
                "thesis": "English thesis",
                "activation_level": {"low": 100.0, "high": 101.0, "timeframe": null, "label": null, "reason": null},
                "first_path_target": {"low": 103.0, "high": 103.0, "timeframe": null, "label": null, "reason": null},
                "next_path_target": {"low": 105.0, "high": 105.0, "timeframe": null, "label": null, "reason": null},
                "failure_level": {"low": 99.0, "high": 99.0, "timeframe": null, "label": null, "reason": null},
                "failure_switch": "reevaluate_short",
                "setup_type": "C_value_return",
                "reevaluation_trigger": { "signals": ["driver_change"] },
                "management_plan": {
                    "take_profit_1_basis": "first_path_target",
                    "take_profit_2_basis": "next_path_target",
                    "take_profit_1_level": 103.0,
                    "take_profit_2_level": 105.0,
                    "stop_migration_rules": [{
                        "after_target": "TP1",
                        "new_stop_basis": "4h_tpo_poc",
                        "new_stop_level": 100.5
                    }],
                    "reduce_on_driver_deterioration": [{
                        "driver_signal": "若15m期货CVD转负则减仓",
                        "reduce_ratio": 0.5
                    }],
                    "exit_full_on_driver_deterioration": []
                },
                "tracked_zones": [{
                    "zone_id": "z1",
                    "timeframe": "4h",
                    "role": "activation",
                    "low": 100.0,
                    "high": 101.0,
                    "reason": null
                }]
            }
        });
        assert!(parse_stage1_output(value).is_err());
    }

    #[test]
    fn stage1_parser_normalizes_no_edge_output_with_extra_path_fields() {
        let value = json!({
            "meta": { "stage1_ts": Utc::now() },
            "monitoring_status": "no_edge",
            "no_trade_reason": "balanced inside value",
            "refresh_hints": [],
            "map_summary": null,
            "current_script": "should_be_dropped",
            "driver_attribution": null,
            "current_path": {
                "id": "path_a",
                "side": "LONG",
                "thesis": "English thesis",
                "activation_level": {"low": 100.0, "high": 101.0, "timeframe": null, "label": null, "reason": null},
                "first_path_target": {"low": 103.0, "high": 103.0, "timeframe": null, "label": null, "reason": null},
                "next_path_target": {"low": 105.0, "high": 105.0, "timeframe": null, "label": null, "reason": null},
                "failure_level": {"low": 99.0, "high": 99.0, "timeframe": null, "label": null, "reason": null},
                "failure_switch": "reevaluate_short",
                "setup_type": "C_value_return",
                "reevaluation_trigger": { "signals": ["driver_change"] },
                "management_plan": {
                    "take_profit_1_basis": "first_path_target",
                    "take_profit_2_basis": "next_path_target",
                    "take_profit_1_level": 103.0,
                    "take_profit_2_level": 105.0,
                    "stop_migration_rules": [],
                    "reduce_on_driver_deterioration": [],
                    "exit_full_on_driver_deterioration": []
                },
                "tracked_zones": [{
                    "zone_id": "z1",
                    "timeframe": "4h",
                    "role": "activation",
                    "low": 100.0,
                    "high": 101.0,
                    "reason": null
                }]
            }
        });

        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(parsed.monitoring_status, "no_edge");
        assert!(parsed.current_script.is_none());
        assert!(parsed.current_path.is_none());
    }
}
