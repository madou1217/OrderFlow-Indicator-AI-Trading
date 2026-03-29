use crate::workflow::schema::{
    CurrentPath, EntryPlan, PathRuntimeState, Stage1Output, Stage2Output,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashSet;

const ALLOWED_CURRENT_SCRIPTS: &[&str] = &["continuation", "crowded_reversal", "value_return"];
const ALLOWED_NO_EDGE_REASONS: &[&str] = &[
    "conflict_no_edge",
    "script_not_unique",
    "path_not_actionable",
];
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
const ALLOWED_TRIGGER_TIMEFRAMES: &[&str] = &["15m", "4h", "1d"];
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

pub fn parse_json_from_text(text: &str) -> Result<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty JSON text"));
    }
    serde_json::from_str(trimmed).map_err(|err| anyhow!("parse JSON from model text failed: {err}"))
}

fn approx_in_zone(level: f64, low: f64, high: f64) -> bool {
    level >= low && level <= high
}

fn is_machine_identifier(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn validate_management_plan(path: &CurrentPath) -> Result<()> {
    if path.management_plan.take_profit_1_basis != "first_path_target" {
        return Err(anyhow!("take_profit_1_basis must be first_path_target"));
    }
    if path.management_plan.take_profit_2_basis != "next_path_target" {
        return Err(anyhow!("take_profit_2_basis must be next_path_target"));
    }
    if !approx_in_zone(
        path.management_plan.take_profit_1_level,
        path.first_path_target.low,
        path.first_path_target.high,
    ) {
        return Err(anyhow!(
            "take_profit_1_level must align with first_path_target"
        ));
    }
    if !approx_in_zone(
        path.management_plan.take_profit_2_level,
        path.next_path_target.low,
        path.next_path_target.high,
    ) {
        return Err(anyhow!(
            "take_profit_2_level must align with next_path_target"
        ));
    }
    for rule in &path.management_plan.stop_migration_rules {
        if !matches!(
            rule.after_target.as_str(),
            "take_profit_1" | "take_profit_2"
        ) {
            return Err(anyhow!(
                "stop_migration_rules.after_target must be take_profit_1 or take_profit_2"
            ));
        }
        if !matches!(
            rule.new_stop_basis.as_str(),
            "activation_level" | "first_path_target" | "next_path_target"
        ) {
            return Err(anyhow!(
                "stop_migration_rules.new_stop_basis must be activation_level, first_path_target, or next_path_target"
            ));
        }
    }
    for rule in &path.management_plan.reduce_on_driver_deterioration {
        let Some(reduce_ratio) = rule.reduce_ratio else {
            return Err(anyhow!(
                "reduce_on_driver_deterioration requires reduce_ratio"
            ));
        };
        if !(0.0 < reduce_ratio && reduce_ratio <= 1.0) {
            return Err(anyhow!(
                "reduce_on_driver_deterioration.reduce_ratio must be between 0 and 1"
            ));
        }
    }
    for rule in &path.management_plan.exit_full_on_driver_deterioration {
        if rule.reduce_ratio.is_some() {
            return Err(anyhow!(
                "exit_full_on_driver_deterioration.reduce_ratio must be null"
            ));
        }
    }
    Ok(())
}

fn same_zone(level: f64, other: f64) -> bool {
    (level - other).abs() <= f64::EPSILON
}

fn infer_anchor_id(
    path: &CurrentPath,
    role: &str,
    zone: &crate::workflow::schema::PriceZone,
) -> Option<String> {
    path.tracked_zones
        .iter()
        .find(|tracked| {
            tracked.role == role
                && same_zone(tracked.low, zone.low)
                && same_zone(tracked.high, zone.high)
        })
        .map(|tracked| tracked.zone_id.clone())
}

fn validate_anchor_id(
    path: &CurrentPath,
    anchor_id: &Option<String>,
    expected_role: &str,
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
    if zone.role != expected_role {
        return Err(anyhow!(
            "{field} must point to a tracked zone with role={expected_role}"
        ));
    }
    Ok(())
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
    if matches!(output.monitoring_status.as_str(), "active")
        && assessment.overall_quality.as_deref().is_some()
        && assessment.overall_quality.as_deref() != Some("high")
    {
        return Err(anyhow!(
            "active stage1 output requires opportunity_assessment.overall_quality=high"
        ));
    }
    if matches!(output.monitoring_status.as_str(), "active")
        && assessment.overall_quality.as_deref() == Some("high")
        && !assessment.disqualifiers.is_empty()
    {
        return Err(anyhow!(
            "active stage1 output with overall_quality=high must not carry disqualifiers"
        ));
    }
    Ok(())
}

fn validate_script_rejections(output: &Stage1Output) -> Result<()> {
    if output.script_rejections.is_empty() {
        return Ok(());
    }
    let mut seen = HashSet::new();
    for rejection in &output.script_rejections {
        if !ALLOWED_CURRENT_SCRIPTS.contains(&rejection.script.as_str()) {
            return Err(anyhow!(
                "script_rejections[].script must be one of [continuation, crowded_reversal, value_return]"
            ));
        }
        if rejection.reason.trim().is_empty() {
            return Err(anyhow!("script_rejections[].reason must be non-empty"));
        }
        if !seen.insert(rejection.script.as_str()) {
            return Err(anyhow!("script_rejections[].script must be unique"));
        }
    }

    if let Some(current_script) = output.current_script.as_deref() {
        let expected = ALLOWED_CURRENT_SCRIPTS
            .iter()
            .copied()
            .filter(|script| *script != current_script)
            .collect::<HashSet<_>>();
        let actual = output
            .script_rejections
            .iter()
            .map(|item| item.script.as_str())
            .collect::<HashSet<_>>();
        if actual != expected {
            return Err(anyhow!(
                "active stage1 output must reject exactly the two non-selected scripts"
            ));
        }
    }
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

    let kind = trigger.kind.as_deref().ok_or_else(|| {
        anyhow!("{field}.kind is required when using structured reevaluation_trigger")
    })?;
    if !ALLOWED_ZONE_TRIGGER_KINDS.contains(&kind) {
        return Err(anyhow!(
            "{field}.kind must be one of [accepted_into_zone, accepted_beyond_zone, rejected_from_zone, reaccepted_through_zone]"
        ));
    }
    let zone_id = trigger.zone_id.as_deref().ok_or_else(|| {
        anyhow!("{field}.zone_id is required when using structured reevaluation_trigger")
    })?;
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
    let timeframe = trigger.timeframe.as_deref().ok_or_else(|| {
        anyhow!("{field}.timeframe is required when using structured reevaluation_trigger")
    })?;
    if !ALLOWED_TRIGGER_TIMEFRAMES.contains(&timeframe) {
        return Err(anyhow!("{field}.timeframe must be one of [15m, 4h, 1d]"));
    }
    let min_confirmed_bars = trigger.min_confirmed_bars.ok_or_else(|| {
        anyhow!("{field}.min_confirmed_bars is required when using structured reevaluation_trigger")
    })?;
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

    let kind = trigger.kind.as_deref().ok_or_else(|| {
        anyhow!(
            "reevaluation_trigger.driver_change.kind is required when using structured reevaluation_trigger"
        )
    })?;
    if !ALLOWED_DRIVER_TRIGGER_KINDS.contains(&kind) {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.kind must be one of [driver_flip, spot_confirmation_lost, oi_support_lost, state_regime_conflict]"
        ));
    }
    let expected_flow_driver = trigger.expected_flow_driver.as_deref().ok_or_else(|| {
        anyhow!(
            "reevaluation_trigger.driver_change.expected_flow_driver is required when using structured reevaluation_trigger"
        )
    })?;
    if !ALLOWED_FLOW_DRIVERS.contains(&expected_flow_driver) {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.expected_flow_driver must be one of [spot_led, futures_led, mixed]"
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
                "reevaluation_trigger.driver_change.invalidate_when_drivers must only contain [spot_led, futures_led, mixed]"
            ));
        }
        if !seen.insert(driver.as_str()) {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.invalidate_when_drivers must be unique"
            ));
        }
    }
    if trigger
        .invalidate_when_drivers
        .iter()
        .any(|item| item == expected_flow_driver)
    {
        return Err(anyhow!(
            "reevaluation_trigger.driver_change.invalidate_when_drivers must exclude expected_flow_driver"
        ));
    }
    if let Some(driver_signal) = trigger.driver_signal.as_deref() {
        if !ALLOWED_DRIVER_SIGNALS.contains(&driver_signal) {
            return Err(anyhow!(
                "reevaluation_trigger.driver_change.driver_signal must be one of [spot_confirmation_lost, oi_support_lost, fake_order_risk_rising, driver_flip_confirmed]"
            ));
        }
    }
    let min_confirmed_windows = trigger.min_confirmed_windows.ok_or_else(|| {
        anyhow!(
            "reevaluation_trigger.driver_change.min_confirmed_windows is required when using structured reevaluation_trigger"
        )
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
    validate_script_rejections(&output)?;

    match output.monitoring_status.as_str() {
        "active" => {
            let driver_attribution = output.driver_attribution.clone();
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
                if !ALLOWED_CURRENT_SCRIPTS.contains(&failure_switch.as_str())
                    && !is_machine_identifier(failure_switch)
                {
                    return Err(anyhow!(
                        "failure_switch must be a script name or a machine-style reevaluation identifier"
                    ));
                }
            }
            if path.activation_anchor_id.is_none() {
                path.activation_anchor_id =
                    infer_anchor_id(path, "activation", &path.activation_level);
            }
            if path.first_path_target_anchor_id.is_none() {
                path.first_path_target_anchor_id =
                    infer_anchor_id(path, "target", &path.first_path_target);
            }
            if path.next_path_target_anchor_id.is_none() {
                path.next_path_target_anchor_id =
                    infer_anchor_id(path, "target", &path.next_path_target);
            }
            if path.failure_anchor_id.is_none() {
                path.failure_anchor_id = infer_anchor_id(path, "failure", &path.failure_level);
            }
            validate_anchor_id(
                path,
                &path.activation_anchor_id,
                "activation",
                "current_path.activation_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.first_path_target_anchor_id,
                "target",
                "current_path.first_path_target_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.next_path_target_anchor_id,
                "target",
                "current_path.next_path_target_anchor_id",
            )?;
            validate_anchor_id(
                path,
                &path.failure_anchor_id,
                "failure",
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
                driver_attribution.as_ref(),
            )?;
            validate_management_plan(path)?;
        }
        "no_edge" => {
            if let Some(reason) = output.no_trade_reason.as_ref() {
                if !ALLOWED_NO_EDGE_REASONS.contains(&reason.as_str()) {
                    return Err(anyhow!(
                        "no_trade_reason must be one of [conflict_no_edge, script_not_unique, path_not_actionable]"
                    ));
                }
            }
            output.current_script = None;
            output.current_path = None;
        }
        _ => unreachable!(),
    }

    Ok(output)
}

fn validate_entry_plan(
    entry_plan: &EntryPlan,
    current_path: &CurrentPath,
    plan_role: &str,
) -> Result<()> {
    if !matches!(entry_plan.side.as_str(), "LONG" | "SHORT") {
        return Err(anyhow!("{}.side must be LONG or SHORT", plan_role));
    }
    if entry_plan.side != current_path.side {
        return Err(anyhow!(
            "{}.side must match Stage1 current_path.side",
            plan_role
        ));
    }
    if !ALLOWED_ENTRY_PROFILES.contains(&entry_plan.entry_profile.as_str()) {
        return Err(anyhow!(
            "{}.entry_profile must be one of [reclaim_then_hold, pullback_acceptance, failed_auction_reentry]",
            plan_role
        ));
    }
    if !ALLOWED_INTENT_MODES.contains(&entry_plan.intent_mode.as_str()) {
        return Err(anyhow!(
            "{}.intent_mode must be one of [immediate, pullback, breakout]",
            plan_role
        ));
    }
    if entry_plan.entry_snapshot.path_id != current_path.id {
        return Err(anyhow!(
            "{}.entry_snapshot.path_id must match current_path.id",
            plan_role
        ));
    }
    if entry_plan.entry_snapshot.context_key.trim().is_empty() {
        return Err(anyhow!(
            "{}.entry_snapshot.context_key must be non-empty",
            plan_role
        ));
    }
    if entry_plan.ttl_minutes == 0 {
        return Err(anyhow!("{}.ttl_minutes must be > 0", plan_role));
    }
    if entry_plan.max_drift_pct < 0.0 {
        return Err(anyhow!("{}.max_drift_pct must be >= 0", plan_role));
    }
    if (entry_plan.take_profit_1 - current_path.management_plan.take_profit_1_level).abs()
        > f64::EPSILON
    {
        return Err(anyhow!(
            "{}.take_profit_1 must inherit Stage1 current_path take_profit_1_level",
            plan_role
        ));
    }
    if (entry_plan.take_profit_2 - current_path.management_plan.take_profit_2_level).abs()
        > f64::EPSILON
    {
        return Err(anyhow!(
            "{}.take_profit_2 must inherit Stage1 current_path take_profit_2_level",
            plan_role
        ));
    }

    match current_path.side.as_str() {
        "LONG" => {
            if entry_plan.stop_loss < current_path.failure_level.low - f64::EPSILON {
                return Err(anyhow!(
                    "{}.stop_loss must not widen beyond Stage1.failure_level.low",
                    plan_role
                ));
            }
            if entry_plan.entry_invalidation_level.low
                < current_path.failure_level.low - f64::EPSILON
            {
                return Err(anyhow!(
                    "{}.entry_invalidation_level must stay inside the Stage1 path envelope",
                    plan_role
                ));
            }
        }
        "SHORT" => {
            if entry_plan.stop_loss > current_path.failure_level.high + f64::EPSILON {
                return Err(anyhow!(
                    "{}.stop_loss must not widen beyond Stage1.failure_level.high",
                    plan_role
                ));
            }
            if entry_plan.entry_invalidation_level.high
                > current_path.failure_level.high + f64::EPSILON
            {
                return Err(anyhow!(
                    "{}.entry_invalidation_level must stay inside the Stage1 path envelope",
                    plan_role
                ));
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

pub fn parse_stage2_output(
    value: Value,
    stage1_output: &Stage1Output,
    path_runtime_state: &PathRuntimeState,
) -> Result<Stage2Output> {
    let output: Stage2Output = serde_json::from_value(value)?;
    if !matches!(
        output.stage2_decision.as_str(),
        "PATH_CONFIRMED" | "REQUEST_STAGE1_REEVALUATION"
    ) {
        return Err(anyhow!(
            "stage2_decision must be PATH_CONFIRMED or REQUEST_STAGE1_REEVALUATION"
        ));
    }

    let soft_invalidation_triplet = path_runtime_state.audit_flags.extreme_location
        && path_runtime_state.audit_flags.reverse_confirmation
        && path_runtime_state.audit_flags.driver_change;

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
                "REQUEST_STAGE1_REEVALUATION must not include tactical_entry_plan"
            ));
        }
        if !(path_runtime_state.hard_invalidation || soft_invalidation_triplet) {
            return Err(anyhow!(
                "REQUEST_STAGE1_REEVALUATION requires hard invalidation or the full soft-invalidation triplet"
            ));
        }
        return Ok(output);
    }

    if path_runtime_state.hard_invalidation || soft_invalidation_triplet {
        return Err(anyhow!(
            "path invalidation requires REQUEST_STAGE1_REEVALUATION"
        ));
    }
    if output.reevaluation_reason.is_some() {
        return Err(anyhow!(
            "PATH_CONFIRMED must not include reevaluation_reason"
        ));
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
    validate_entry_plan(
        &tactical_plan.primary_entry_plan,
        current_path,
        "primary_entry_plan",
    )?;
    validate_entry_plan(
        &tactical_plan.secondary_entry_plan,
        current_path,
        "secondary_entry_plan",
    )?;
    if tactical_plan.primary_entry_plan.entry_snapshot.context_key
        == tactical_plan
            .secondary_entry_plan
            .entry_snapshot
            .context_key
    {
        return Err(anyhow!(
            "primary_entry_plan and secondary_entry_plan must use distinct context_key values"
        ));
    }
    if tactical_plan.attempt_policy.max_filled_stopout_attempts != 2 {
        return Err(anyhow!(
            "attempt_policy.max_filled_stopout_attempts must be 2"
        ));
    }
    if tactical_plan.attempt_policy.count_unfilled_attempts {
        return Err(anyhow!(
            "attempt_policy.count_unfilled_attempts must remain false"
        ));
    }
    if tactical_plan.attempt_policy.time_window != "same_15m_window" {
        return Err(anyhow!(
            "attempt_policy.time_window must be same_15m_window"
        ));
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{parse_stage1_output, parse_stage2_output};
    use crate::workflow::schema::{
        AttemptPolicy, CurrentPath, DriverAttribution, DriverReevaluationTrigger, EntryPlan,
        ManagementPlan, MapSummary, OpportunityAssessment, PathAuditFlags, PathRuntimeState,
        PriceZone, ReevaluationTrigger, ScriptRejection, Stage1Meta, Stage1Output, Stage2Output,
        StopMigrationRule, TacticalEntryPlan, TacticalEntrySnapshot, ZoneReevaluationTrigger,
    };
    use chrono::Utc;
    use serde_json::{json, Value};

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: MapSummary {
                regime_3d: json!({}),
                location_1d: json!({}),
                location_4h: json!({}),
                price_location_class: "value_edge".to_string(),
                key_levels: json!({}),
            },
            opportunity_assessment: OpportunityAssessment {
                location_quality: Some("high".to_string()),
                state_quality: Some("high".to_string()),
                driver_quality: Some("high".to_string()),
                geometry_quality: Some("high".to_string()),
                uniqueness_quality: Some("high".to_string()),
                overall_quality: Some("high".to_string()),
                disqualifiers: vec![],
            },
            script_rejections: vec![
                ScriptRejection {
                    script: "continuation".to_string(),
                    reason: "wrong regime".to_string(),
                },
                ScriptRejection {
                    script: "crowded_reversal".to_string(),
                    reason: "no reversal trigger".to_string(),
                },
            ],
            current_script: Some("value_return".to_string()),
            driver_attribution: Some(DriverAttribution {
                flow_driver: "mixed".to_string(),
                spot_confirming: true,
                driver_note: "note".to_string(),
            }),
            current_path: Some(CurrentPath {
                id: "path_a".to_string(),
                side: "LONG".to_string(),
                thesis: "bounce".to_string(),
                risk_grade: "countertrend_repair".to_string(),
                activation_anchor_id: Some("z_act".to_string()),
                activation_level: PriceZone {
                    low: 100.0,
                    high: 101.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target_anchor_id: Some("z_tp1".to_string()),
                first_path_target: PriceZone {
                    low: 103.0,
                    high: 104.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target_anchor_id: Some("z_tp2".to_string()),
                next_path_target: PriceZone {
                    low: 106.0,
                    high: 107.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_anchor_id: Some("z_fail".to_string()),
                failure_level: PriceZone {
                    low: 98.0,
                    high: 99.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: Some("continuation".to_string()),
                setup_type: "C_value_return".to_string(),
                reevaluation_trigger: ReevaluationTrigger {
                    extreme_location: ZoneReevaluationTrigger {
                        kind: Some("accepted_into_zone".to_string()),
                        zone_id: Some("z_tp2".to_string()),
                        timeframe: Some("15m".to_string()),
                        min_confirmed_bars: Some(1),
                        summary: "accepted into target extension".to_string(),
                        evidence: vec!["price accepted into tp2".to_string()],
                    },
                    reverse_confirmation: ZoneReevaluationTrigger {
                        kind: Some("accepted_beyond_zone".to_string()),
                        zone_id: Some("z_fail".to_string()),
                        timeframe: Some("15m".to_string()),
                        min_confirmed_bars: Some(1),
                        summary: "accepted through failure".to_string(),
                        evidence: vec!["failure shelf lost".to_string()],
                    },
                    driver_change: DriverReevaluationTrigger {
                        kind: Some("driver_flip".to_string()),
                        expected_flow_driver: Some("mixed".to_string()),
                        invalidate_when_drivers: vec![
                            "spot_led".to_string(),
                            "futures_led".to_string(),
                        ],
                        require_spot_confirmation: Some(false),
                        driver_signal: Some("driver_flip_confirmed".to_string()),
                        min_confirmed_windows: Some(1),
                        summary: "driver flips away from mixed".to_string(),
                        evidence: vec!["driver attribution changed".to_string()],
                    },
                },
                management_plan: ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 103.5,
                    take_profit_2_level: 106.5,
                    stop_migration_rules: vec![StopMigrationRule {
                        after_target: "take_profit_1".to_string(),
                        new_stop_basis: "activation_level".to_string(),
                        new_stop_level: 101.0,
                    }],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![
                    crate::workflow::schema::TrackedZone {
                        zone_id: "z_act".to_string(),
                        timeframe: "4h".to_string(),
                        role: "activation".to_string(),
                        low: 100.0,
                        high: 101.0,
                        reason: None,
                    },
                    crate::workflow::schema::TrackedZone {
                        zone_id: "z_tp1".to_string(),
                        timeframe: "4h".to_string(),
                        role: "target".to_string(),
                        low: 103.0,
                        high: 104.0,
                        reason: None,
                    },
                    crate::workflow::schema::TrackedZone {
                        zone_id: "z_tp2".to_string(),
                        timeframe: "4h".to_string(),
                        role: "target".to_string(),
                        low: 106.0,
                        high: 107.0,
                        reason: None,
                    },
                    crate::workflow::schema::TrackedZone {
                        zone_id: "z_fail".to_string(),
                        timeframe: "4h".to_string(),
                        role: "failure".to_string(),
                        low: 98.0,
                        high: 99.0,
                        reason: None,
                    },
                ],
            }),
        }
    }

    #[test]
    fn stage1_parser_cleans_no_edge_path_fields() {
        let value = json!({
            "meta": {"stage1_ts": Utc::now()},
            "monitoring_status": "no_edge",
            "no_trade_reason": "conflict_no_edge",
            "refresh_hints": [],
            "map_summary": {
                "regime_3d": {},
                "location_1d": {},
                "location_4h": {},
                "price_location_class": "inside_value_middle",
                "key_levels": {}
            },
            "current_script": "value_return",
            "driver_attribution": null,
            "current_path": {
                "id": "x",
                "side": "LONG",
                "thesis": "bounce",
                "risk_grade": "countertrend_repair",
                "activation_level": {"low": 100.0, "high": 101.0},
                "first_path_target": {"low": 103.0, "high": 104.0},
                "next_path_target": {"low": 106.0, "high": 107.0},
                "failure_level": {"low": 98.0, "high": 99.0},
                "failure_switch": "continuation",
                "setup_type": "C_value_return",
                "reevaluation_trigger": {
                    "extreme_location": {},
                    "reverse_confirmation": {},
                    "driver_change": {}
                },
                "management_plan": {
                    "take_profit_1_basis": "first_path_target",
                    "take_profit_2_basis": "next_path_target",
                    "take_profit_1_level": 103.5,
                    "take_profit_2_level": 106.5,
                    "stop_migration_rules": [],
                    "reduce_on_driver_deterioration": [],
                    "exit_full_on_driver_deterioration": []
                },
                "tracked_zones": []
            }
        });
        let parsed = parse_stage1_output(value).expect("parse");
        assert!(parsed.current_script.is_none());
        assert!(parsed.current_path.is_none());
    }

    #[test]
    fn stage1_parser_accepts_structured_quality_contract() {
        let value = serde_json::to_value(sample_stage1_output()).expect("serialize");
        let parsed = parse_stage1_output(value).expect("parse");
        assert_eq!(
            parsed.opportunity_assessment.overall_quality.as_deref(),
            Some("high")
        );
        let path = parsed.current_path.expect("path");
        assert_eq!(path.activation_anchor_id.as_deref(), Some("z_act"));
        assert_eq!(
            path.reevaluation_trigger
                .driver_change
                .expected_flow_driver
                .as_deref(),
            Some("mixed")
        );
    }

    #[test]
    fn stage1_parser_backfills_missing_anchor_ids_from_tracked_zones() {
        let mut value = serde_json::to_value(sample_stage1_output()).expect("serialize");
        let path = value
            .get_mut("current_path")
            .and_then(Value::as_object_mut)
            .expect("current_path object");
        path.remove("activation_anchor_id");
        path.remove("first_path_target_anchor_id");
        path.remove("next_path_target_anchor_id");
        path.remove("failure_anchor_id");

        let parsed = parse_stage1_output(value).expect("parse");
        let path = parsed.current_path.expect("path");
        assert_eq!(path.activation_anchor_id.as_deref(), Some("z_act"));
        assert_eq!(path.first_path_target_anchor_id.as_deref(), Some("z_tp1"));
        assert_eq!(path.next_path_target_anchor_id.as_deref(), Some("z_tp2"));
        assert_eq!(path.failure_anchor_id.as_deref(), Some("z_fail"));
    }

    #[test]
    fn stage2_parser_accepts_path_confirmed_with_tactical_plan() {
        let stage1 = sample_stage1_output();
        let runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 100.5,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: true,
            activation_level_touched: true,
            opposing_pressure_detected: false,
            audit_flags: PathAuditFlags::default(),
            active_entry_context_keys: vec![],
            notes: vec![],
        };
        let output = Stage2Output {
            stage2_decision: "PATH_CONFIRMED".to_string(),
            tactical_entry_plan: Some(TacticalEntryPlan {
                path_id: "path_a".to_string(),
                primary_entry_plan: EntryPlan {
                    side: "LONG".to_string(),
                    entry_profile: "reclaim_then_hold".to_string(),
                    intent_mode: "immediate".to_string(),
                    entry_activation_level: PriceZone {
                        low: 100.0,
                        high: 101.0,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    entry_zone: PriceZone {
                        low: 100.0,
                        high: 101.0,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    entry_invalidation_level: PriceZone {
                        low: 98.5,
                        high: 99.0,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    stop_loss: 98.6,
                    take_profit_1: 103.5,
                    take_profit_2: 106.5,
                    ttl_minutes: 15,
                    max_drift_pct: 0.2,
                    entry_snapshot: TacticalEntrySnapshot {
                        context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                        path_id: "path_a".to_string(),
                        plan_role: "primary".to_string(),
                    },
                    entry_note: "note".to_string(),
                },
                secondary_entry_plan: EntryPlan {
                    side: "LONG".to_string(),
                    entry_profile: "failed_auction_reentry".to_string(),
                    intent_mode: "pullback".to_string(),
                    entry_activation_level: PriceZone {
                        low: 99.7,
                        high: 100.2,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    entry_zone: PriceZone {
                        low: 99.7,
                        high: 100.5,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    entry_invalidation_level: PriceZone {
                        low: 98.2,
                        high: 98.9,
                        timeframe: None,
                        label: None,
                        reason: None,
                    },
                    stop_loss: 98.3,
                    take_profit_1: 103.5,
                    take_profit_2: 106.5,
                    ttl_minutes: 15,
                    max_drift_pct: 0.25,
                    entry_snapshot: TacticalEntrySnapshot {
                        context_key: "ETHUSDT:LONG:path_a:secondary".to_string(),
                        path_id: "path_a".to_string(),
                        plan_role: "secondary".to_string(),
                    },
                    entry_note: "note".to_string(),
                },
                attempt_policy: AttemptPolicy {
                    max_filled_stopout_attempts: 2,
                    count_unfilled_attempts: false,
                    time_window: "same_15m_window".to_string(),
                },
            }),
            reevaluation_reason: None,
        };
        let value = serde_json::to_value(output).expect("serialize");
        let parsed = parse_stage2_output(value, &stage1, &runtime_state).expect("parse");
        assert_eq!(parsed.stage2_decision, "PATH_CONFIRMED");
    }

    #[test]
    fn stage2_parser_rejects_path_confirmed_when_audit_triplet_is_hit() {
        let stage1 = sample_stage1_output();
        let runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 100.5,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: false,
            activation_level_touched: true,
            opposing_pressure_detected: true,
            audit_flags: PathAuditFlags {
                extreme_location: true,
                reverse_confirmation: true,
                driver_change: true,
            },
            active_entry_context_keys: vec![],
            notes: vec![],
        };
        let value = json!({
            "stage2_decision": "PATH_CONFIRMED",
            "tactical_entry_plan": null,
            "reevaluation_reason": null
        });
        assert!(parse_stage2_output(value, &stage1, &runtime_state).is_err());
    }

    #[test]
    fn stage2_parser_rejects_reevaluation_without_hard_or_soft_invalidation() {
        let stage1 = sample_stage1_output();
        let runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 100.5,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: true,
            activation_level_touched: false,
            opposing_pressure_detected: true,
            audit_flags: PathAuditFlags {
                extreme_location: false,
                reverse_confirmation: true,
                driver_change: true,
            },
            active_entry_context_keys: vec![],
            notes: vec![],
        };
        let value = json!({
            "stage2_decision": "REQUEST_STAGE1_REEVALUATION",
            "tactical_entry_plan": null,
            "reevaluation_reason": "opposing_pressure"
        });
        assert!(parse_stage2_output(value, &stage1, &runtime_state).is_err());
    }

    #[test]
    fn stage2_parser_accepts_reevaluation_when_soft_invalidation_triplet_is_hit() {
        let stage1 = sample_stage1_output();
        let runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 100.5,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: false,
            activation_level_touched: false,
            opposing_pressure_detected: true,
            audit_flags: PathAuditFlags {
                extreme_location: true,
                reverse_confirmation: true,
                driver_change: true,
            },
            active_entry_context_keys: vec![],
            notes: vec![],
        };
        let value = json!({
            "stage2_decision": "REQUEST_STAGE1_REEVALUATION",
            "tactical_entry_plan": null,
            "reevaluation_reason": "soft_invalidation_triplet"
        });
        let parsed = parse_stage2_output(value, &stage1, &runtime_state).expect("parse");
        assert_eq!(parsed.stage2_decision, "REQUEST_STAGE1_REEVALUATION");
    }

    #[test]
    fn stage2_parser_rejects_path_confirmed_with_reevaluation_reason() {
        let stage1 = sample_stage1_output();
        let runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 100.5,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: true,
            activation_level_touched: true,
            opposing_pressure_detected: false,
            audit_flags: PathAuditFlags::default(),
            active_entry_context_keys: vec![],
            notes: vec![],
        };
        let output = json!({
            "stage2_decision": "PATH_CONFIRMED",
            "tactical_entry_plan": {
                "path_id": "path_a",
                "primary_entry_plan": {
                    "side": "LONG",
                    "entry_profile": "reclaim_then_hold",
                    "intent_mode": "immediate",
                    "entry_activation_level": {"low": 100.0, "high": 101.0},
                    "entry_zone": {"low": 100.0, "high": 101.0},
                    "entry_invalidation_level": {"low": 98.5, "high": 99.0},
                    "stop_loss": 98.6,
                    "take_profit_1": 103.5,
                    "take_profit_2": 106.5,
                    "ttl_minutes": 15,
                    "max_drift_pct": 0.2,
                    "entry_snapshot": {
                        "context_key": "ETHUSDT:LONG:path_a:primary",
                        "path_id": "path_a",
                        "plan_role": "primary"
                    },
                    "entry_note": "note"
                },
                "secondary_entry_plan": {
                    "side": "LONG",
                    "entry_profile": "failed_auction_reentry",
                    "intent_mode": "pullback",
                    "entry_activation_level": {"low": 99.7, "high": 100.2},
                    "entry_zone": {"low": 99.7, "high": 100.5},
                    "entry_invalidation_level": {"low": 98.2, "high": 98.9},
                    "stop_loss": 98.3,
                    "take_profit_1": 103.5,
                    "take_profit_2": 106.5,
                    "ttl_minutes": 15,
                    "max_drift_pct": 0.25,
                    "entry_snapshot": {
                        "context_key": "ETHUSDT:LONG:path_a:secondary",
                        "path_id": "path_a",
                        "plan_role": "secondary"
                    },
                    "entry_note": "note"
                },
                "attempt_policy": {
                    "max_filled_stopout_attempts": 2,
                    "count_unfilled_attempts": false,
                    "time_window": "same_15m_window"
                }
            },
            "reevaluation_reason": "should_be_null"
        });
        assert!(parse_stage2_output(output, &stage1, &runtime_state).is_err());
    }
}
