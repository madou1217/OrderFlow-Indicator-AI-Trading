use crate::workflow::schema::{
    CurrentPath, EntryPlan, PendingOrderManagementAction, PendingOrderManagementPlan,
    PositionManagementAction, PositionManagementPlan, PriceTriggerCondition, PriceZone,
    Stage1Output, Stage2AOutput, Stage2BOutput, Stage2COutput,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashSet;

const ALLOWED_CURRENT_SCRIPTS: &[&str] = &["continuation", "crowded_reversal", "value_return"];
const ALLOWED_PRICE_LOCATION_CLASSES: &[&str] = &[
    "inside_value_middle",
    "value_edge",
    "outside_value_extended",
];
const ALLOWED_RISK_GRADES: &[&str] = &[
    "aligned_trend",
    "countertrend_repair",
    "high_conflict_repair",
];
const ALLOWED_SETUP_TYPES: &[&str] = &["A_continuation", "B_reversal", "C_value_return"];
const ALLOWED_ENTRY_PROFILES: &[&str] = &[
    "reclaim_then_hold",
    "pullback_acceptance",
    "failed_auction_reentry",
];
const ALLOWED_INTENT_MODES: &[&str] = &["immediate", "pullback", "breakout"];
const ALLOWED_FLOW_DRIVERS: &[&str] = &["spot_led", "futures_led", "mixed"];
const ALLOWED_OPPORTUNITY_QUALITIES: &[&str] = &["high", "medium", "low"];
const ALLOWED_ZONE_TRIGGER_KINDS: &[&str] = &[
    "accepted_into_zone",
    "accepted_beyond_zone",
    "rejected_from_zone",
    "reaccepted_through_zone",
];
const ALLOWED_STRATEGIC_TIMEFRAMES: &[&str] = &["4h", "1d", "4h-1d"];
const ALLOWED_TACTICAL_TIMEFRAMES: &[&str] = &["15m", "15m-4h"];
const ALLOWED_DRIVER_TRIGGER_KINDS: &[&str] = &[
    "driver_flip",
    "spot_confirmation_lost",
    "oi_support_lost",
    "state_regime_conflict",
];
const ALLOWED_DRIVER_SIGNALS: &[&str] = &[
    "spot_confirmation_lost",
    "oi_support_lost",
    "fake_order_risk_rising",
    "driver_flip_confirmed",
];
const ALLOWED_PATH_LIVE_ASSESSMENTS: &[&str] = &["live", "degraded", "invalidated"];
const ALLOWED_PENDING_ORDER_EXPOSURE_STATES: &[&str] = &[
    "flat_with_live_entry_orders",
    "in_position_with_live_entry_orders",
];
const ALLOWED_PRICE_TRIGGER_TYPES: &[&str] = &["price_above", "price_below"];

pub fn parse_json_from_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty JSON text"));
    }
    serde_json::from_str(trimmed).map_err(|err| anyhow!("parse JSON from model text failed: {err}"))
}

fn same_zone(level: f64, other: f64) -> bool {
    (level - other).abs() <= f64::EPSILON
}

fn validate_quality(name: &str, value: Option<&str>) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if !ALLOWED_OPPORTUNITY_QUALITIES.contains(&value) {
        return Err(anyhow!(
            "{name} must be one of [high, medium, low] when provided"
        ));
    }
    Ok(())
}

fn infer_anchor_id(path: &CurrentPath, zone: &PriceZone) -> Option<String> {
    path.tracked_zones
        .iter()
        .find(|tracked| {
            same_zone(tracked.low, zone.low)
                && same_zone(tracked.high, zone.high)
                && match (&tracked.timeframe, &zone.timeframe) {
                    (_, None) => true,
                    (tracked_tf, Some(zone_tf)) => tracked_tf == zone_tf,
                }
        })
        .map(|tracked| tracked.zone_id.clone())
}

fn validate_anchor_id(
    path: &CurrentPath,
    anchor_id: &Option<String>,
    expected_zone: &PriceZone,
    field: &str,
) -> Result<()> {
    let Some(anchor_id) = anchor_id.as_deref() else {
        return Ok(());
    };
    if anchor_id.trim().is_empty() {
        return Err(anyhow!("{field} must be non-empty when provided"));
    }
    let zone = path
        .tracked_zones
        .iter()
        .find(|tracked| tracked.zone_id == anchor_id)
        .ok_or_else(|| anyhow!("{field} must match one of current_path.tracked_zones[].zone_id"))?;
    if !same_zone(zone.low, expected_zone.low) || !same_zone(zone.high, expected_zone.high) {
        return Err(anyhow!(
            "{field} must align with the low/high of its corresponding current_path zone"
        ));
    }
    if let Some(expected_timeframe) = expected_zone.timeframe.as_deref() {
        if zone.timeframe != expected_timeframe {
            return Err(anyhow!(
                "{field} must align with the timeframe of its corresponding current_path zone"
            ));
        }
    }
    Ok(())
}

fn validate_zone_timeframe(
    field: &str,
    zone: &PriceZone,
    allowed: &[&str],
    required: bool,
) -> Result<()> {
    match zone.timeframe.as_deref() {
        Some(value) if allowed.contains(&value) => Ok(()),
        Some(_) => Err(anyhow!("{field}.timeframe has an unsupported value")),
        None if required => Err(anyhow!("{field}.timeframe is required")),
        None => Ok(()),
    }
}

fn validate_opportunity_assessment(output: &Stage1Output) -> Result<()> {
    let assessment = &output.opportunity_assessment;
    validate_quality(
        "opportunity_assessment.location_quality",
        assessment.location_quality.as_deref(),
    )?;
    validate_quality(
        "opportunity_assessment.state_quality",
        assessment.state_quality.as_deref(),
    )?;
    validate_quality(
        "opportunity_assessment.driver_quality",
        assessment.driver_quality.as_deref(),
    )?;
    validate_quality(
        "opportunity_assessment.geometry_quality",
        assessment.geometry_quality.as_deref(),
    )?;
    validate_quality(
        "opportunity_assessment.uniqueness_quality",
        assessment.uniqueness_quality.as_deref(),
    )?;
    validate_quality(
        "opportunity_assessment.overall_quality",
        assessment.overall_quality.as_deref(),
    )?;
    Ok(())
}

fn validate_zone_reevaluation_trigger(
    field: &str,
    trigger: &crate::workflow::schema::ZoneReevaluationTrigger,
    path: &CurrentPath,
) -> Result<()> {
    let has_structured_contract = trigger.kind.is_some()
        || trigger.zone_id.is_some()
        || trigger.timeframe.is_some()
        || trigger.min_confirmed_bars.is_some();
    if !has_structured_contract {
        return Ok(());
    }

    let kind = trigger
        .kind
        .as_deref()
        .ok_or_else(|| anyhow!("{field}.kind is required"))?;
    if !ALLOWED_ZONE_TRIGGER_KINDS.contains(&kind) {
        return Err(anyhow!("{field}.kind has an unsupported value"));
    }
    let zone_id = trigger
        .zone_id
        .as_deref()
        .ok_or_else(|| anyhow!("{field}.zone_id is required"))?;
    if zone_id.trim().is_empty() {
        return Err(anyhow!("{field}.zone_id must be non-empty"));
    }
    if !path
        .tracked_zones
        .iter()
        .any(|tracked| tracked.zone_id == zone_id)
    {
        return Err(anyhow!(
            "{field}.zone_id must match one of current_path.tracked_zones[].zone_id"
        ));
    }
    let timeframe = trigger
        .timeframe
        .as_deref()
        .ok_or_else(|| anyhow!("{field}.timeframe is required"))?;
    if !ALLOWED_STRATEGIC_TIMEFRAMES.contains(&timeframe) {
        return Err(anyhow!("{field}.timeframe must be one of [4h, 1d, 4h-1d]"));
    }
    let min_confirmed_bars = trigger
        .min_confirmed_bars
        .ok_or_else(|| anyhow!("{field}.min_confirmed_bars is required"))?;
    if min_confirmed_bars == 0 {
        return Err(anyhow!("{field}.min_confirmed_bars must be >= 1"));
    }
    if trigger.summary.trim().is_empty() {
        return Err(anyhow!("{field}.summary must be non-empty"));
    }
    if trigger.evidence.is_empty() {
        return Err(anyhow!("{field}.evidence must be non-empty"));
    }
    Ok(())
}

fn validate_driver_reevaluation_trigger(
    trigger: &crate::workflow::schema::DriverReevaluationTrigger,
    driver_attribution: Option<&crate::workflow::schema::DriverAttribution>,
) -> Result<()> {
    let has_structured_contract = trigger.kind.is_some()
        || trigger.expected_flow_driver.is_some()
        || !trigger.invalidate_when_drivers.is_empty()
        || trigger.require_spot_confirmation.is_some()
        || trigger.driver_signal.is_some()
        || trigger.min_confirmed_windows.is_some();
    if !has_structured_contract {
        return Ok(());
    }

    let kind = trigger
        .kind
        .as_deref()
        .ok_or_else(|| anyhow!("reevaluation_trigger.driver_change.kind is required"))?;
    if !ALLOWED_DRIVER_TRIGGER_KINDS.contains(&kind) {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.kind has an unsupported value"
        ));
    }
    let expected_flow_driver = trigger.expected_flow_driver.as_deref().ok_or_else(|| {
        anyhow!("reevaluation_trigger.driver_change.expected_flow_driver is required")
    })?;
    if !ALLOWED_FLOW_DRIVERS.contains(&expected_flow_driver) {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.expected_flow_driver has an unsupported value"
        ));
    }
    if let Some(driver) = driver_attribution {
        if driver.flow_driver != expected_flow_driver {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.expected_flow_driver must match driver_attribution.flow_driver"
            ));
        }
    }
    if trigger.invalidate_when_drivers.is_empty() {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.invalidate_when_drivers must be non-empty"
        ));
    }
    let mut seen = HashSet::new();
    for driver in &trigger.invalidate_when_drivers {
        if !ALLOWED_FLOW_DRIVERS.contains(&driver.as_str()) {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.invalidate_when_drivers has an unsupported value"
            ));
        }
        if !seen.insert(driver.as_str()) {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.invalidate_when_drivers must be unique"
            ));
        }
    }
    if let Some(driver_signal) = trigger.driver_signal.as_deref() {
        if !ALLOWED_DRIVER_SIGNALS.contains(&driver_signal) {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.driver_signal has an unsupported value"
            ));
        }
    }
    let min_confirmed_windows = trigger.min_confirmed_windows.ok_or_else(|| {
        anyhow!("reevaluation_trigger.driver_change.min_confirmed_windows is required")
    })?;
    if min_confirmed_windows == 0 {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.min_confirmed_windows must be >= 1"
        ));
    }
    if trigger.summary.trim().is_empty() {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.summary must be non-empty"
        ));
    }
    if trigger.evidence.is_empty() {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.evidence must be non-empty"
        ));
    }
    Ok(())
}

pub fn parse_stage1_output(value: Value) -> Result<Stage1Output> {
    let mut output: Stage1Output = serde_json::from_value(value)?;
    if !matches!(output.monitoring_status.as_str(), "active" | "no_edge") {
        return Err(anyhow!("monitoring_status must be active or no_edge"));
    }
    if !ALLOWED_PRICE_LOCATION_CLASSES.contains(&output.map_summary.price_location_class.as_str()) {
        return Err(anyhow!(
            "map_summary.price_location_class must be one of [inside_value_middle, value_edge, outside_value_extended]"
        ));
    }
    validate_opportunity_assessment(&output)?;

    match output.monitoring_status.as_str() {
        "active" => {
            let driver_attribution = output
                .driver_attribution
                .as_ref()
                .ok_or_else(|| anyhow!("active stage1 output requires driver_attribution"))?;
            if !ALLOWED_FLOW_DRIVERS.contains(&driver_attribution.flow_driver.as_str()) {
                return Err(anyhow!(
                    "driver_attribution.flow_driver must be one of [spot_led, futures_led, mixed]"
                ));
            }
            let script = output
                .current_script
                .as_ref()
                .ok_or_else(|| anyhow!("active stage1 output requires current_script"))?;
            if !ALLOWED_CURRENT_SCRIPTS.contains(&script.as_str()) {
                return Err(anyhow!(
                    "current_script must be one of [continuation, crowded_reversal, value_return]"
                ));
            }
            let path = output
                .current_path
                .as_mut()
                .ok_or_else(|| anyhow!("active stage1 output requires current_path"))?;
            if path.id.trim().is_empty() {
                return Err(anyhow!("current_path.id must be non-empty"));
            }
            if path.thesis.trim().is_empty() {
                return Err(anyhow!("current_path.thesis must be non-empty"));
            }
            if !matches!(path.side.as_str(), "LONG" | "SHORT") {
                return Err(anyhow!("current_path.side must be LONG or SHORT"));
            }
            if !ALLOWED_RISK_GRADES.contains(&path.risk_grade.as_str()) {
                return Err(anyhow!(
                    "risk_grade must be one of [aligned_trend, countertrend_repair, high_conflict_repair]"
                ));
            }
            if !ALLOWED_SETUP_TYPES.contains(&path.setup_type.as_str()) {
                return Err(anyhow!(
                    "setup_type must be one of [A_continuation, B_reversal, C_value_return]"
                ));
            }
            if let Some(failure_switch) = path.failure_switch.as_ref() {
                if failure_switch.trim().is_empty() {
                    return Err(anyhow!("failure_switch must be non-empty when provided"));
                }
            }

            validate_zone_timeframe(
                "current_path.strategic_activation_level",
                &path.strategic_activation_level,
                ALLOWED_STRATEGIC_TIMEFRAMES,
                true,
            )?;
            validate_zone_timeframe(
                "current_path.first_path_target",
                &path.first_path_target,
                ALLOWED_STRATEGIC_TIMEFRAMES,
                true,
            )?;
            validate_zone_timeframe(
                "current_path.next_path_target",
                &path.next_path_target,
                ALLOWED_STRATEGIC_TIMEFRAMES,
                true,
            )?;
            validate_zone_timeframe(
                "current_path.failure_level",
                &path.failure_level,
                ALLOWED_STRATEGIC_TIMEFRAMES,
                true,
            )?;

            if path.activation_anchor_id.is_none() {
                path.activation_anchor_id = infer_anchor_id(path, &path.strategic_activation_level);
            }
            if path.first_path_target_anchor_id.is_none() {
                path.first_path_target_anchor_id = infer_anchor_id(path, &path.first_path_target);
            }
            if path.next_path_target_anchor_id.is_none() {
                path.next_path_target_anchor_id = infer_anchor_id(path, &path.next_path_target);
            }
            if path.failure_anchor_id.is_none() {
                path.failure_anchor_id = infer_anchor_id(path, &path.failure_level);
            }
            validate_anchor_id(
                path,
                &path.activation_anchor_id,
                &path.strategic_activation_level,
                "current_path.activation_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.first_path_target_anchor_id,
                &path.first_path_target,
                "current_path.first_path_target_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.next_path_target_anchor_id,
                &path.next_path_target,
                "current_path.next_path_target_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.failure_anchor_id,
                &path.failure_level,
                "current_path.failure_anchor_id",
            )?;

            validate_zone_reevaluation_trigger(
                "reevaluation_trigger.extreme_location",
                &path.reevaluation_trigger.extreme_location,
                path,
            )?;
            validate_zone_reevaluation_trigger(
                "reevaluation_trigger.reverse_confirmation",
                &path.reevaluation_trigger.reverse_confirmation,
                path,
            )?;
            validate_driver_reevaluation_trigger(
                &path.reevaluation_trigger.driver_change,
                Some(driver_attribution),
            )?;
        }
        "no_edge" => {
            if let Some(reason) = output.no_trade_reason.as_ref() {
                if reason.trim().is_empty() {
                    return Err(anyhow!("no_trade_reason must be non-empty when provided"));
                }
            }
            output.current_script = None;
            output.current_path = None;
            output.driver_attribution = None;
        }
        _ => unreachable!(),
    }

    Ok(output)
}

fn validate_stage2a_entry_plan(entry_plan: &EntryPlan, current_path: &CurrentPath) -> Result<()> {
    if !matches!(entry_plan.side.as_str(), "LONG" | "SHORT") {
        return Err(anyhow!("entry_plan.side must be LONG or SHORT"));
    }
    if entry_plan.side != current_path.side {
        return Err(anyhow!(
            "entry_plan.side must match Stage1 current_path.side"
        ));
    }
    if !ALLOWED_ENTRY_PROFILES.contains(&entry_plan.entry_profile.as_str()) {
        return Err(anyhow!("entry_plan.entry_profile has an unsupported value"));
    }
    if !ALLOWED_INTENT_MODES.contains(&entry_plan.intent_mode.as_str()) {
        return Err(anyhow!("entry_plan.intent_mode has an unsupported value"));
    }
    validate_zone_timeframe(
        "entry_plan.entry_activation_level",
        &entry_plan.entry_activation_level,
        ALLOWED_TACTICAL_TIMEFRAMES,
        true,
    )?;
    validate_zone_timeframe(
        "entry_plan.entry_zone",
        &entry_plan.entry_zone,
        ALLOWED_TACTICAL_TIMEFRAMES,
        true,
    )?;
    validate_zone_timeframe(
        "entry_plan.entry_invalidation_level",
        &entry_plan.entry_invalidation_level,
        ALLOWED_TACTICAL_TIMEFRAMES,
        true,
    )?;
    if entry_plan.max_drift_pct < 0.0 {
        return Err(anyhow!("entry_plan.max_drift_pct must be >= 0"));
    }

    match current_path.side.as_str() {
        "LONG" => {
            if entry_plan.stop_loss > entry_plan.entry_zone.low + f64::EPSILON {
                return Err(anyhow!(
                    "entry_plan.stop_loss must remain on the risk side of entry_plan.entry_zone for LONG"
                ));
            }
            if entry_plan.stop_loss > entry_plan.entry_invalidation_level.high + f64::EPSILON {
                return Err(anyhow!(
                    "entry_plan.stop_loss must remain at or below entry_plan.entry_invalidation_level for LONG"
                ));
            }
        }
        "SHORT" => {
            if entry_plan.stop_loss < entry_plan.entry_zone.high - f64::EPSILON {
                return Err(anyhow!(
                    "entry_plan.stop_loss must remain on the risk side of entry_plan.entry_zone for SHORT"
                ));
            }
            if entry_plan.stop_loss < entry_plan.entry_invalidation_level.low - f64::EPSILON {
                return Err(anyhow!(
                    "entry_plan.stop_loss must remain at or above entry_plan.entry_invalidation_level for SHORT"
                ));
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

pub fn parse_stage2a_output(value: Value, stage1_output: &Stage1Output) -> Result<Stage2AOutput> {
    let output: Stage2AOutput = serde_json::from_value(value)?;
    if !matches!(
        output.stage2_decision.as_str(),
        "PATH_CONFIRMED" | "REQUEST_STAGE1_REEVALUATION"
    ) {
        return Err(anyhow!(
            "stage2_decision must be PATH_CONFIRMED or REQUEST_STAGE1_REEVALUATION"
        ));
    }

    if output.stage2_decision == "REQUEST_STAGE1_REEVALUATION" {
        if output
            .reevaluation_reason
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        {
            return Err(anyhow!(
                "REQUEST_STAGE1_REEVALUATION requires reevaluation_reason"
            ));
        }
        if output.tactical_entry_plan.is_some() {
            return Err(anyhow!(
                "REQUEST_STAGE1_REEVALUATION must set tactical_entry_plan=null"
            ));
        }
        return Ok(output);
    }

    if output.reevaluation_reason.is_some() {
        return Err(anyhow!("PATH_CONFIRMED must set reevaluation_reason=null"));
    }

    let current_path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("PATH_CONFIRMED requires Stage1 current_path"))?;
    let tactical_plan = output
        .tactical_entry_plan
        .as_ref()
        .ok_or_else(|| anyhow!("PATH_CONFIRMED requires tactical_entry_plan"))?;
    if tactical_plan.path_id != current_path.id {
        return Err(anyhow!(
            "tactical_entry_plan.path_id must match Stage1 current_path.id"
        ));
    }
    validate_stage2a_entry_plan(&tactical_plan.entry_plan, current_path)?;

    Ok(output)
}

fn validate_price_trigger_condition(field: &str, trigger: &PriceTriggerCondition) -> Result<()> {
    if !ALLOWED_PRICE_TRIGGER_TYPES.contains(&trigger.trigger_type.as_str()) {
        return Err(anyhow!(
            "{field}.trigger_type must be one of [price_above, price_below]"
        ));
    }
    if !trigger.trigger_price.is_finite() {
        return Err(anyhow!("{field}.trigger_price must be finite"));
    }
    Ok(())
}

fn ensure_action_field_absent(field: &str, present: bool, action_type: &str) -> Result<()> {
    if present {
        return Err(anyhow!("{field} must be null for {action_type}"));
    }
    Ok(())
}

fn validate_position_management_action(
    action: &PositionManagementAction,
    path_id: &str,
) -> Result<()> {
    if action.context_key.trim().is_empty() {
        return Err(anyhow!(
            "position_management_plan.actions[].context_key must be non-empty"
        ));
    }
    if action.path_id != path_id {
        return Err(anyhow!(
            "position_management_plan.actions[].path_id must match position_management_plan.path_id"
        ));
    }

    match action.action_type.as_str() {
        "add" => {
            validate_price_trigger_condition(
                "position_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("add requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "add",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reduce_ratio",
                action.reduce_ratio.is_some(),
                "add",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].new_stop_loss",
                action.new_stop_loss.is_some(),
                "add",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_bracket_template",
                action.reuse_current_bracket_template.is_some(),
                "add",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_1",
                action.take_profit_1.is_some(),
                "add",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_2",
                action.take_profit_2.is_some(),
                "add",
            )?;
            let add_ratio = action
                .add_ratio
                .ok_or_else(|| anyhow!("add requires add_ratio"))?;
            if !(0.0 < add_ratio && add_ratio <= 1.0) {
                return Err(anyhow!("add_ratio must be between 0 and 1"));
            }
            if action.reuse_current_entry_template != Some(true) {
                return Err(anyhow!("add requires reuse_current_entry_template=true"));
            }
        }
        "reduce" => {
            validate_price_trigger_condition(
                "position_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("reduce requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].add_ratio",
                action.add_ratio.is_some(),
                "reduce",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "reduce",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].new_stop_loss",
                action.new_stop_loss.is_some(),
                "reduce",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_bracket_template",
                action.reuse_current_bracket_template.is_some(),
                "reduce",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_1",
                action.take_profit_1.is_some(),
                "reduce",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_2",
                action.take_profit_2.is_some(),
                "reduce",
            )?;
            let reduce_ratio = action
                .reduce_ratio
                .ok_or_else(|| anyhow!("reduce requires reduce_ratio"))?;
            if !(0.0 < reduce_ratio && reduce_ratio <= 1.0) {
                return Err(anyhow!("reduce_ratio must be between 0 and 1"));
            }
            let execution_price = action
                .execution_price
                .ok_or_else(|| anyhow!("reduce requires execution_price"))?;
            if !execution_price.is_finite() {
                return Err(anyhow!("execution_price must be finite"));
            }
        }
        "exit_full" => {
            validate_price_trigger_condition(
                "position_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("exit_full requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].add_ratio",
                action.add_ratio.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reduce_ratio",
                action.reduce_ratio.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].new_stop_loss",
                action.new_stop_loss.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_bracket_template",
                action.reuse_current_bracket_template.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_1",
                action.take_profit_1.is_some(),
                "exit_full",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_2",
                action.take_profit_2.is_some(),
                "exit_full",
            )?;
            let execution_price = action
                .execution_price
                .ok_or_else(|| anyhow!("exit_full requires execution_price"))?;
            if !execution_price.is_finite() {
                return Err(anyhow!("execution_price must be finite"));
            }
        }
        "move_stop" => {
            validate_price_trigger_condition(
                "position_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("move_stop requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "move_stop",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].add_ratio",
                action.add_ratio.is_some(),
                "move_stop",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "move_stop",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reduce_ratio",
                action.reduce_ratio.is_some(),
                "move_stop",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_1",
                action.take_profit_1.is_some(),
                "move_stop",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].take_profit_2",
                action.take_profit_2.is_some(),
                "move_stop",
            )?;
            if action.new_stop_loss.is_none() {
                return Err(anyhow!("move_stop requires new_stop_loss"));
            }
            if action.reuse_current_bracket_template != Some(true) {
                return Err(anyhow!(
                    "move_stop requires reuse_current_bracket_template=true"
                ));
            }
        }
        "update_take_profit" => {
            validate_price_trigger_condition(
                "position_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("update_take_profit requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "update_take_profit",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].add_ratio",
                action.add_ratio.is_some(),
                "update_take_profit",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "update_take_profit",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].reduce_ratio",
                action.reduce_ratio.is_some(),
                "update_take_profit",
            )?;
            ensure_action_field_absent(
                "position_management_plan.actions[].new_stop_loss",
                action.new_stop_loss.is_some(),
                "update_take_profit",
            )?;
            if action.take_profit_1.is_none() && action.take_profit_2.is_none() {
                return Err(anyhow!(
                    "update_take_profit requires take_profit_1 or take_profit_2"
                ));
            }
            if action.reuse_current_bracket_template != Some(true) {
                return Err(anyhow!(
                    "update_take_profit requires reuse_current_bracket_template=true"
                ));
            }
        }
        other => {
            return Err(anyhow!(
                "position_management_plan.actions[].action_type has unsupported value {other}"
            ))
        }
    }

    Ok(())
}

fn validate_position_management_plan(
    plan: &PositionManagementPlan,
    stage1_output: &Stage1Output,
    expected_context_key: &str,
) -> Result<()> {
    let current_path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("Stage2B requires Stage1 current_path"))?;
    if plan.path_id != current_path.id {
        return Err(anyhow!(
            "position_management_plan.path_id must match Stage1 current_path.id"
        ));
    }
    if plan.exposure_state != "in_position" {
        return Err(anyhow!(
            "position_management_plan.exposure_state must be in_position"
        ));
    }
    if !ALLOWED_PATH_LIVE_ASSESSMENTS.contains(&plan.path_live_assessment.as_str()) {
        return Err(anyhow!(
            "position_management_plan.path_live_assessment must be one of [live, degraded, invalidated]"
        ));
    }
    for action in &plan.actions {
        validate_position_management_action(action, &plan.path_id)?;
        if action.context_key != expected_context_key {
            return Err(anyhow!(
                "position_management_plan.actions[].context_key must match the current Stage2B context_key"
            ));
        }
    }
    if plan.path_live_assessment == "invalidated"
        && !plan
            .actions
            .iter()
            .any(|action| matches!(action.action_type.as_str(), "reduce" | "exit_full"))
    {
        return Err(anyhow!(
            "invalidated position_management_plan must materially de-risk with reduce or exit_full"
        ));
    }
    Ok(())
}

pub fn parse_stage2b_output(
    value: Value,
    stage1_output: &Stage1Output,
    expected_context_key: &str,
) -> Result<Stage2BOutput> {
    let output: Stage2BOutput = serde_json::from_value(value)?;
    if output.stage2b_decision != "MANAGE_POSITION" {
        return Err(anyhow!("stage2b_decision must be MANAGE_POSITION"));
    }
    validate_position_management_plan(
        &output.position_management_plan,
        stage1_output,
        expected_context_key,
    )?;
    Ok(output)
}

fn validate_pending_order_management_action(
    action: &PendingOrderManagementAction,
    path_id: &str,
) -> Result<()> {
    if action.context_key.trim().is_empty() {
        return Err(anyhow!(
            "pending_order_management_plan.actions[].context_key must be non-empty"
        ));
    }
    if action.path_id != path_id {
        return Err(anyhow!(
            "pending_order_management_plan.actions[].path_id must match pending_order_management_plan.path_id"
        ));
    }

    match action.action_type.as_str() {
        "cancel_pending_order" => {
            validate_price_trigger_condition(
                "pending_order_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("cancel_pending_order requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "cancel_pending_order",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_entry_zone",
                action.replacement_entry_zone.is_some(),
                "cancel_pending_order",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_entry_invalidation_level",
                action.replacement_entry_invalidation_level.is_some(),
                "cancel_pending_order",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_stop_loss",
                action.replacement_stop_loss.is_some(),
                "cancel_pending_order",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "cancel_pending_order",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].post_fill_bracket_template",
                action.post_fill_bracket_template.is_some(),
                "cancel_pending_order",
            )?;
        }
        "replace_entry" => {
            validate_price_trigger_condition(
                "pending_order_management_plan.actions[].trigger_condition",
                action
                    .trigger_condition
                    .as_ref()
                    .ok_or_else(|| anyhow!("replace_entry requires trigger_condition"))?,
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "replace_entry",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].post_fill_bracket_template",
                action.post_fill_bracket_template.is_some(),
                "replace_entry",
            )?;
            validate_zone_timeframe(
                "pending_order_management_plan.actions[].replacement_entry_zone",
                action
                    .replacement_entry_zone
                    .as_ref()
                    .ok_or_else(|| anyhow!("replace_entry requires replacement_entry_zone"))?,
                ALLOWED_TACTICAL_TIMEFRAMES,
                true,
            )?;
            validate_zone_timeframe(
                "pending_order_management_plan.actions[].replacement_entry_invalidation_level",
                action
                    .replacement_entry_invalidation_level
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow!("replace_entry requires replacement_entry_invalidation_level")
                    })?,
                ALLOWED_TACTICAL_TIMEFRAMES,
                true,
            )?;
            if action.replacement_stop_loss.is_none() {
                return Err(anyhow!("replace_entry requires replacement_stop_loss"));
            }
            if action.reuse_current_entry_template != Some(true) {
                return Err(anyhow!(
                    "replace_entry requires reuse_current_entry_template=true"
                ));
            }
        }
        "update_post_fill_bracket_template" => {
            validate_price_trigger_condition(
                "pending_order_management_plan.actions[].trigger_condition",
                action.trigger_condition.as_ref().ok_or_else(|| {
                    anyhow!("update_post_fill_bracket_template requires trigger_condition")
                })?,
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].execution_price",
                action.execution_price.is_some(),
                "update_post_fill_bracket_template",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_entry_zone",
                action.replacement_entry_zone.is_some(),
                "update_post_fill_bracket_template",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_entry_invalidation_level",
                action.replacement_entry_invalidation_level.is_some(),
                "update_post_fill_bracket_template",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].replacement_stop_loss",
                action.replacement_stop_loss.is_some(),
                "update_post_fill_bracket_template",
            )?;
            ensure_action_field_absent(
                "pending_order_management_plan.actions[].reuse_current_entry_template",
                action.reuse_current_entry_template.is_some(),
                "update_post_fill_bracket_template",
            )?;
            if action.post_fill_bracket_template.is_none() {
                return Err(anyhow!(
                    "update_post_fill_bracket_template requires post_fill_bracket_template"
                ));
            }
        }
        other => {
            return Err(anyhow!(
                "pending_order_management_plan.actions[].action_type has unsupported value {other}"
            ))
        }
    }

    Ok(())
}

fn validate_pending_order_management_plan(
    plan: &PendingOrderManagementPlan,
    stage1_output: &Stage1Output,
    expected_exposure_state: &str,
    expected_context_key: &str,
) -> Result<()> {
    let current_path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("Stage2C requires Stage1 current_path"))?;
    if plan.path_id != current_path.id {
        return Err(anyhow!(
            "pending_order_management_plan.path_id must match Stage1 current_path.id"
        ));
    }
    if !ALLOWED_PENDING_ORDER_EXPOSURE_STATES.contains(&plan.exposure_state.as_str()) {
        return Err(anyhow!(
            "pending_order_management_plan.exposure_state must be one of [flat_with_live_entry_orders, in_position_with_live_entry_orders]"
        ));
    }
    if plan.exposure_state != expected_exposure_state {
        return Err(anyhow!(
            "pending_order_management_plan.exposure_state must match the current Stage2C exposure_state"
        ));
    }
    if !ALLOWED_PATH_LIVE_ASSESSMENTS.contains(&plan.path_live_assessment.as_str()) {
        return Err(anyhow!(
            "pending_order_management_plan.path_live_assessment must be one of [live, degraded, invalidated]"
        ));
    }
    for action in &plan.actions {
        validate_pending_order_management_action(action, &plan.path_id)?;
        if action.context_key != expected_context_key {
            return Err(anyhow!(
                "pending_order_management_plan.actions[].context_key must match the current Stage2C context_key"
            ));
        }
    }
    if plan.path_live_assessment == "invalidated"
        && !plan
            .actions
            .iter()
            .any(|action| action.action_type == "cancel_pending_order")
    {
        return Err(anyhow!(
            "invalidated pending_order_management_plan must include cancel_pending_order"
        ));
    }
    Ok(())
}

pub fn parse_stage2c_output(
    value: Value,
    stage1_output: &Stage1Output,
    expected_exposure_state: &str,
    expected_context_key: &str,
) -> Result<Stage2COutput> {
    let output: Stage2COutput = serde_json::from_value(value)?;
    if output.stage2c_decision != "MANAGE_PENDING_ORDERS" {
        return Err(anyhow!("stage2c_decision must be MANAGE_PENDING_ORDERS"));
    }
    validate_pending_order_management_plan(
        &output.pending_order_management_plan,
        stage1_output,
        expected_exposure_state,
        expected_context_key,
    )?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_stage1_output, parse_stage2a_output, parse_stage2b_output, parse_stage2c_output,
    };
    use crate::workflow::schema::{
        CurrentPath, DriverAttribution, MapSummary, OpportunityAssessment, PriceZone,
        ReevaluationTrigger, Stage1Meta, Stage1Output,
    };
    use chrono::Utc;
    use serde_json::json;

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: MapSummary {
                location_3d: json!({"bias": "bearish"}),
                location_1d: json!({"class": "value_edge"}),
                location_4h: json!({"class": "value_edge"}),
                price_location_class: "value_edge".to_string(),
                key_levels: json!({}),
            },
            opportunity_assessment: OpportunityAssessment {
                overall_quality: Some("high".to_string()),
                ..OpportunityAssessment::default()
            },
            current_script: Some("value_return".to_string()),
            driver_attribution: Some(DriverAttribution {
                flow_driver: "mixed".to_string(),
                spot_confirming: true,
                driver_note: "supportive".to_string(),
            }),
            current_path: Some(CurrentPath {
                id: "path_1".to_string(),
                side: "LONG".to_string(),
                thesis: "bounce".to_string(),
                risk_grade: "countertrend_repair".to_string(),
                activation_anchor_id: None,
                strategic_activation_level: PriceZone {
                    low: 1998.0,
                    high: 2002.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("activation".to_string()),
                    reason: None,
                },
                first_path_target_anchor_id: None,
                first_path_target: PriceZone {
                    low: 2020.0,
                    high: 2025.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("tp1".to_string()),
                    reason: None,
                },
                next_path_target_anchor_id: None,
                next_path_target: PriceZone {
                    low: 2030.0,
                    high: 2035.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("tp2".to_string()),
                    reason: None,
                },
                failure_anchor_id: None,
                failure_level: PriceZone {
                    low: 1989.0,
                    high: 1992.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("failure".to_string()),
                    reason: None,
                },
                failure_switch: Some("continuation".to_string()),
                setup_type: "B_reversal".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                tracked_zones: vec![],
            }),
        }
    }

    #[test]
    fn stage1_parser_accepts_location_3d_shape() {
        let value = serde_json::to_value(sample_stage1_output()).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(parsed.map_summary.price_location_class, "value_edge");
        assert!(parsed.current_path.is_some());
    }

    #[test]
    fn stage1_parser_accepts_active_medium_quality() {
        let mut sample = sample_stage1_output();
        sample.opportunity_assessment.overall_quality = Some("medium".to_string());
        let value = serde_json::to_value(sample).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(
            parsed.opportunity_assessment.overall_quality.as_deref(),
            Some("medium")
        );
        assert!(parsed.current_path.is_some());
    }

    #[test]
    fn stage1_parser_accepts_natural_language_failure_switch() {
        let mut sample = sample_stage1_output();
        sample.current_path.as_mut().expect("path").failure_switch = Some(
            "If 4H price is accepted back below 1996.81-2000.00, stop treating the move as the active repair long."
                .to_string(),
        );
        let value = serde_json::to_value(sample).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(
            parsed
                .current_path
                .as_ref()
                .and_then(|path| path.failure_switch.as_deref()),
            Some(
                "If 4H price is accepted back below 1996.81-2000.00, stop treating the move as the active repair long."
            )
        );
    }

    #[test]
    fn stage1_parser_accepts_support_resistance_tracked_zone_roles_for_anchor_ids() {
        let mut sample = sample_stage1_output();
        let path = sample.current_path.as_mut().expect("path");
        path.activation_anchor_id = Some("z_support".to_string());
        path.first_path_target_anchor_id = Some("z_res_1".to_string());
        path.next_path_target_anchor_id = Some("z_res_2".to_string());
        path.failure_anchor_id = Some("z_support_fail".to_string());
        path.tracked_zones = vec![
            crate::workflow::schema::TrackedZone {
                zone_id: "z_support".to_string(),
                timeframe: "4h".to_string(),
                role: "support".to_string(),
                low: path.strategic_activation_level.low,
                high: path.strategic_activation_level.high,
                reason: Some("reaccept support".to_string()),
            },
            crate::workflow::schema::TrackedZone {
                zone_id: "z_res_1".to_string(),
                timeframe: "4h".to_string(),
                role: "resistance".to_string(),
                low: path.first_path_target.low,
                high: path.first_path_target.high,
                reason: Some("first target".to_string()),
            },
            crate::workflow::schema::TrackedZone {
                zone_id: "z_res_2".to_string(),
                timeframe: "4h".to_string(),
                role: "resistance".to_string(),
                low: path.next_path_target.low,
                high: path.next_path_target.high,
                reason: Some("second target".to_string()),
            },
            crate::workflow::schema::TrackedZone {
                zone_id: "z_support_fail".to_string(),
                timeframe: "4h".to_string(),
                role: "support".to_string(),
                low: path.failure_level.low,
                high: path.failure_level.high,
                reason: Some("failure support".to_string()),
            },
        ];
        let value = serde_json::to_value(sample).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        let parsed_path = parsed.current_path.as_ref().expect("path");
        assert_eq!(
            parsed_path.activation_anchor_id.as_deref(),
            Some("z_support")
        );
        assert_eq!(
            parsed_path.first_path_target_anchor_id.as_deref(),
            Some("z_res_1")
        );
        assert_eq!(
            parsed_path.next_path_target_anchor_id.as_deref(),
            Some("z_res_2")
        );
        assert_eq!(
            parsed_path.failure_anchor_id.as_deref(),
            Some("z_support_fail")
        );
    }

    #[test]
    fn stage1_parser_accepts_driver_change_that_references_current_driver() {
        let mut sample = sample_stage1_output();
        sample
            .driver_attribution
            .as_mut()
            .expect("driver")
            .flow_driver = "futures_led".to_string();
        let driver_change = &mut sample
            .current_path
            .as_mut()
            .expect("path")
            .reevaluation_trigger
            .driver_change;
        driver_change.kind = Some("spot_confirmation_lost".to_string());
        driver_change.expected_flow_driver = Some("futures_led".to_string());
        driver_change.invalidate_when_drivers = vec!["futures_led".to_string()];
        driver_change.require_spot_confirmation = Some(true);
        driver_change.driver_signal = Some("spot_confirmation_lost".to_string());
        driver_change.min_confirmed_windows = Some(2);
        driver_change.summary = "If spot confirmation disappears, remap.".to_string();
        driver_change.evidence = vec!["Current flow remains futures-led.".to_string()];
        let value = serde_json::to_value(sample).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(
            parsed.current_path.as_ref().and_then(|path| path
                .reevaluation_trigger
                .driver_change
                .expected_flow_driver
                .as_deref()),
            Some("futures_led")
        );
    }

    #[test]
    fn stage1_parser_accepts_freeform_no_trade_reason() {
        let mut sample = sample_stage1_output();
        sample.monitoring_status = "no_edge".to_string();
        sample.no_trade_reason = Some(
            "ETH is pinned around the 4H/1D control cluster and the auction is balanced."
                .to_string(),
        );
        sample.current_script = Some("value_return".to_string());
        sample.current_path = Some(sample_stage1_output().current_path.expect("path"));
        sample.driver_attribution = Some(DriverAttribution {
            flow_driver: "mixed".to_string(),
            spot_confirming: true,
            driver_note: "balanced".to_string(),
        });
        let value = serde_json::to_value(sample).expect("encode");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(
            parsed.no_trade_reason.as_deref(),
            Some("ETH is pinned around the 4H/1D control cluster and the auction is balanced.")
        );
        assert!(parsed.current_script.is_none());
        assert!(parsed.current_path.is_none());
        assert!(parsed.driver_attribution.is_none());
    }

    #[test]
    fn stage2a_parser_accepts_single_entry_plan() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2_decision": "PATH_CONFIRMED",
            "tactical_entry_plan": {
                "path_id": "path_1",
                "entry_plan": {
                    "side": "LONG",
                    "entry_profile": "reclaim_then_hold",
                    "intent_mode": "immediate",
                    "entry_activation_level": {"low": 1998.0, "high": 2002.0, "timeframe": "15m", "label": "activation", "reason": "ok"},
                    "entry_zone": {"low": 1999.0, "high": 2001.0, "timeframe": "15m", "label": "entry", "reason": "ok"},
                    "entry_invalidation_level": {"low": 1992.0, "high": 1994.0, "timeframe": "15m", "label": "invalid", "reason": "ok"},
                    "stop_loss": 1993.0,
                    "max_drift_pct": 0.12,
                    "entry_note": "ok"
                }
            },
            "reevaluation_reason": null
        });
        let parsed = parse_stage2a_output(value, &stage1_output).expect("parse");
        assert_eq!(parsed.stage2_decision, "PATH_CONFIRMED");
    }

    #[test]
    fn stage2a_parser_allows_reevaluation_without_runtime_soft_invalidation() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2_decision": "REQUEST_STAGE1_REEVALUATION",
            "tactical_entry_plan": null,
            "reevaluation_reason": "Path quality degraded and needs a fresh strategic review."
        });
        let parsed = parse_stage2a_output(value, &stage1_output).expect("parse");
        assert_eq!(parsed.stage2_decision, "REQUEST_STAGE1_REEVALUATION");
    }

    #[test]
    fn stage2a_parser_allows_path_confirmation_without_runtime_path_verdict() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2_decision": "PATH_CONFIRMED",
            "tactical_entry_plan": {
                "path_id": "path_1",
                "entry_plan": {
                    "side": "LONG",
                    "entry_profile": "reclaim_then_hold",
                    "intent_mode": "immediate",
                    "entry_activation_level": {"low": 1998.0, "high": 2002.0, "timeframe": "15m", "label": "activation", "reason": "ok"},
                    "entry_zone": {"low": 1999.0, "high": 2001.0, "timeframe": "15m", "label": "entry", "reason": "ok"},
                    "entry_invalidation_level": {"low": 1992.0, "high": 1994.0, "timeframe": "15m", "label": "invalid", "reason": "ok"},
                    "stop_loss": 1993.0,
                    "max_drift_pct": 0.12,
                    "entry_note": "ok"
                }
            },
            "reevaluation_reason": null
        });
        let parsed = parse_stage2a_output(value, &stage1_output).expect("parse");
        assert_eq!(parsed.stage2_decision, "PATH_CONFIRMED");
    }

    #[test]
    fn stage2a_parser_allows_tactical_stop_beyond_stage1_failure_level_when_structure_is_valid() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2_decision": "PATH_CONFIRMED",
            "tactical_entry_plan": {
                "path_id": "path_1",
                "entry_plan": {
                    "side": "LONG",
                    "entry_profile": "reclaim_then_hold",
                    "intent_mode": "immediate",
                    "entry_activation_level": {"low": 1998.0, "high": 2002.0, "timeframe": "15m", "label": "activation", "reason": "ok"},
                    "entry_zone": {"low": 1999.0, "high": 2001.0, "timeframe": "15m", "label": "entry", "reason": "ok"},
                    "entry_invalidation_level": {"low": 1984.0, "high": 1988.0, "timeframe": "15m", "label": "invalid", "reason": "ok"},
                    "stop_loss": 1987.5,
                    "max_drift_pct": 0.12,
                    "entry_note": "ok"
                }
            },
            "reevaluation_reason": null
        });
        let parsed = parse_stage2a_output(value, &stage1_output).expect("parse");
        assert_eq!(parsed.stage2_decision, "PATH_CONFIRMED");
        assert_eq!(
            parsed
                .tactical_entry_plan
                .as_ref()
                .expect("tactical plan")
                .entry_plan
                .stop_loss,
            1987.5
        );
    }

    #[test]
    fn stage2b_parser_requires_manage_position() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2b_decision": "MANAGE_POSITION",
            "position_management_plan": {
                "path_id": "path_1",
                "exposure_state": "in_position",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [],
                "management_note": "no incremental changes"
            }
        });
        let parsed = parse_stage2b_output(value, &stage1_output, "ctx_1").expect("parse");
        assert_eq!(parsed.stage2b_decision, "MANAGE_POSITION");
    }

    #[test]
    fn stage2b_parser_rejects_extraneous_fields_for_add() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2b_decision": "MANAGE_POSITION",
            "position_management_plan": {
                "path_id": "path_1",
                "exposure_state": "in_position",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "add",
                    "context_key": "ctx_1",
                    "path_id": "path_1",
                    "trigger_condition": {
                        "trigger_type": "price_above",
                        "trigger_price": 2005.0
                    },
                    "execution_price": null,
                    "add_ratio": 0.25,
                    "reuse_current_entry_template": true,
                    "reduce_ratio": null,
                    "new_stop_loss": null,
                    "reuse_current_bracket_template": null,
                    "take_profit_1": 2015.0,
                    "take_profit_2": null,
                    "reason": "invalid"
                }],
                "management_note": "bad add"
            }
        });
        let err = parse_stage2b_output(value, &stage1_output, "ctx_1").expect_err("should fail");
        assert!(err
            .to_string()
            .contains("position_management_plan.actions[].take_profit_1 must be null for add"));
    }

    #[test]
    fn stage2b_parser_requires_execution_price_for_reduce() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2b_decision": "MANAGE_POSITION",
            "position_management_plan": {
                "path_id": "path_1",
                "exposure_state": "in_position",
                "path_live_assessment": "degraded",
                "path_assessment_reason": "risk is worsening",
                "actions": [{
                    "action_type": "reduce",
                    "context_key": "ctx_1",
                    "path_id": "path_1",
                    "trigger_condition": {
                        "trigger_type": "price_below",
                        "trigger_price": 1995.0
                    },
                    "execution_price": null,
                    "add_ratio": null,
                    "reuse_current_entry_template": null,
                    "reduce_ratio": 0.5,
                    "new_stop_loss": null,
                    "reuse_current_bracket_template": null,
                    "take_profit_1": null,
                    "take_profit_2": null,
                    "reason": "cut risk"
                }],
                "management_note": "de-risk if support breaks"
            }
        });

        let err = parse_stage2b_output(value, &stage1_output, "ctx_1").expect_err("should fail");
        assert!(err.to_string().contains("reduce requires execution_price"));
    }

    #[test]
    fn stage2c_parser_rejects_extraneous_fields_for_replace_entry() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2c_decision": "MANAGE_PENDING_ORDERS",
            "pending_order_management_plan": {
                "path_id": "path_1",
                "exposure_state": "flat_with_live_entry_orders",
                "path_live_assessment": "degraded",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "replace_entry",
                    "context_key": "ctx_1",
                    "path_id": "path_1",
                    "trigger_condition": {
                        "trigger_type": "price_above",
                        "trigger_price": 2005.0
                    },
                    "execution_price": null,
                    "replacement_entry_zone": {
                        "low": 2004.0,
                        "high": 2006.0,
                        "timeframe": "15m",
                        "label": "entry",
                        "reason": "shift"
                    },
                    "replacement_entry_invalidation_level": {
                        "low": 1998.0,
                        "high": 1999.0,
                        "timeframe": "15m",
                        "label": "invalid",
                        "reason": "shift"
                    },
                    "replacement_stop_loss": 1998.5,
                    "reuse_current_entry_template": true,
                    "post_fill_bracket_template": {
                        "take_profit_1": 2015.0,
                        "take_profit_2": 2020.0,
                        "stop_loss": 1998.5
                    },
                    "reason": "invalid"
                }],
                "management_note": "bad replace"
            }
        });
        let err = parse_stage2c_output(
            value,
            &stage1_output,
            "flat_with_live_entry_orders",
            "ctx_1",
        )
        .expect_err("should fail");
        assert!(
            err.to_string().contains(
                "pending_order_management_plan.actions[].post_fill_bracket_template must be null for replace_entry"
            )
        );
    }

    #[test]
    fn stage2c_parser_accepts_coexisting_position_and_pending_order_exposure_state() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2c_decision": "MANAGE_PENDING_ORDERS",
            "pending_order_management_plan": {
                "path_id": "path_1",
                "exposure_state": "in_position_with_live_entry_orders",
                "path_live_assessment": "degraded",
                "path_assessment_reason": "pending order still valid while the live position is already on",
                "actions": [],
                "management_note": "coexisting pending order plan"
            }
        });

        let parsed = parse_stage2c_output(
            value,
            &stage1_output,
            "in_position_with_live_entry_orders",
            "ctx_1",
        )
        .expect("coexisting exposure state should parse");
        assert_eq!(
            parsed.pending_order_management_plan.exposure_state,
            "in_position_with_live_entry_orders"
        );
    }

    #[test]
    fn stage2b_parser_rejects_mixed_context_keys() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2b_decision": "MANAGE_POSITION",
            "position_management_plan": {
                "path_id": "path_1",
                "exposure_state": "in_position",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "move_stop",
                    "context_key": "ctx_2",
                    "path_id": "path_1",
                    "trigger_condition": {
                        "trigger_type": "price_above",
                        "trigger_price": 2001.0
                    },
                    "execution_price": null,
                    "add_ratio": null,
                    "reuse_current_entry_template": null,
                    "reduce_ratio": null,
                    "new_stop_loss": 1997.5,
                    "reuse_current_bracket_template": true,
                    "take_profit_1": null,
                    "take_profit_2": null,
                    "reason": "tighten"
                }],
                "management_note": "tighten"
            }
        });

        let err = parse_stage2b_output(value, &stage1_output, "ctx_1").expect_err("should fail");
        assert!(err
            .to_string()
            .contains("position_management_plan.actions[].context_key must match the current Stage2B context_key"));
    }

    #[test]
    fn stage2c_parser_rejects_mixed_context_keys() {
        let stage1_output = sample_stage1_output();
        let value = json!({
            "stage2c_decision": "MANAGE_PENDING_ORDERS",
            "pending_order_management_plan": {
                "path_id": "path_1",
                "exposure_state": "flat_with_live_entry_orders",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "cancel_pending_order",
                    "context_key": "ctx_2",
                    "path_id": "path_1",
                    "trigger_condition": {
                        "trigger_type": "price_below",
                        "trigger_price": 1994.0
                    },
                    "execution_price": null,
                    "replacement_entry_zone": null,
                    "replacement_entry_invalidation_level": null,
                    "replacement_stop_loss": null,
                    "reuse_current_entry_template": null,
                    "post_fill_bracket_template": null,
                    "reason": "cancel"
                }],
                "management_note": "cancel"
            }
        });

        let err = parse_stage2c_output(
            value,
            &stage1_output,
            "flat_with_live_entry_orders",
            "ctx_1",
        )
        .expect_err("should fail");
        assert!(err
            .to_string()
            .contains("pending_order_management_plan.actions[].context_key must match the current Stage2C context_key"));
    }
}
