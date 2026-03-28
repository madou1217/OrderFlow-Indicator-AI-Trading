use crate::app::config::WorkflowSoftGateMinPassConfig;
use crate::execution::binance::TradingStateSnapshot;
use crate::workflow::predicate::{
    event_after_precondition, failed_auction_confirmed, price_above_on_close, price_below_on_close,
    reaccept_inside_value, zone_acceptance_above, zone_acceptance_below,
};
use crate::workflow::schema::{
    EntrySnapshot, HardGateEvaluation, IndicatorSummary, SoftGateEvaluation, Stage1Output,
    Stage2PromptInput, WorkflowAccountContext, WorkflowPosition, WorkflowRuntimeContract,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceState {
    Supporting,
    Conflicting,
    Missing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stage2RuntimeEvaluation {
    pub monitoring_status: String,
    pub latest_price: f64,
    pub no_edge_reentered: bool,
    pub failure_level_breached: bool,
    pub reevaluation_trigger_hit: bool,
    pub activation_level_active: bool,
    pub setup_confirmed: bool,
    pub hard_gate: HardGateEvaluation,
    pub soft_gate: SoftGateEvaluation,
    pub soft_gate_min_required: u8,
}

fn setup_type_min_pass(setup_type: &str, cfg: &WorkflowSoftGateMinPassConfig) -> u8 {
    match setup_type {
        "A_continuation" => cfg.a_continuation,
        "B_reversal" => cfg.b_reversal,
        "C_value_return" => cfg.c_value_return,
        _ => 4,
    }
}

fn flatten_blob(value: &serde_json::Value) -> String {
    value.to_string().to_ascii_lowercase()
}

fn contains_any_keyword(blob: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|keyword| blob.contains(keyword))
}

fn value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn find_bool_key(value: &Value, target: &str) -> Option<bool> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key.eq_ignore_ascii_case(target) {
                    if let Some(found) = child.as_bool() {
                        return Some(found);
                    }
                }
                if let Some(found) = find_bool_key(child, target) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|child| find_bool_key(child, target)),
        _ => None,
    }
}

fn find_f64_key(value: &Value, target: &str) -> Option<f64> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key.eq_ignore_ascii_case(target) {
                    if let Some(found) = child.as_f64() {
                        return Some(found);
                    }
                }
                if let Some(found) = find_f64_key(child, target) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|child| find_f64_key(child, target)),
        _ => None,
    }
}

fn context_child<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or(&Value::Null)
}

fn side_keywords(side: &str) -> (&'static [&'static str], &'static [&'static str]) {
    match side {
        "LONG" => (
            &["buy", "bull", "long", "bid", "up"],
            &["sell", "bear", "short", "ask", "down"],
        ),
        "SHORT" => (
            &["sell", "bear", "short", "ask", "down"],
            &["buy", "bull", "long", "bid", "up"],
        ),
        _ => (&[], &[]),
    }
}

fn blob_supports_side(blob: &str, side: &str) -> bool {
    let (positive, negative) = side_keywords(side);
    contains_any_keyword(blob, positive) && !contains_any_keyword(blob, negative)
}

fn blob_conflicts_side(blob: &str, side: &str) -> bool {
    let (_, negative) = side_keywords(side);
    contains_any_keyword(blob, negative)
}

fn orderbook_depth(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.driver_context, "orderbook_depth")
}

fn driver_change_evidence(indicator_summary: &IndicatorSummary) -> bool {
    let driver_blob = flatten_blob(&indicator_summary.driver_context);
    driver_blob.contains("flip")
        || driver_blob.contains("driver_change")
        || driver_blob.contains("driver_shift")
        || driver_blob.contains("spot_confirm")
        || driver_blob.contains("absorption")
        || driver_blob.contains("exhaust")
}

fn open_interest_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.state_context, "open_interest")
}

fn ratio_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.state_context, "long_short_ratios")
}

fn funding_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.state_context, "funding_rate")
}

fn vpin_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.state_context, "vpin")
}

fn footprint_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.trigger_context, "footprint")
}

fn divergence_context(indicator_summary: &IndicatorSummary) -> &Value {
    context_child(&indicator_summary.trigger_context, "divergence")
}

fn exhaustion_context<'a>(indicator_summary: &'a IndicatorSummary, side: &str) -> &'a Value {
    match side {
        "LONG" => context_child(&indicator_summary.trigger_context, "selling_exhaustion"),
        "SHORT" => context_child(&indicator_summary.trigger_context, "buying_exhaustion"),
        _ => &Value::Null,
    }
}

fn absorption_context<'a>(indicator_summary: &'a IndicatorSummary, side: &str) -> &'a Value {
    match side {
        "LONG" => {
            let bullish = context_child(&indicator_summary.driver_context, "bullish_absorption");
            if value_present(bullish) {
                bullish
            } else {
                context_child(&indicator_summary.driver_context, "absorption")
            }
        }
        "SHORT" => {
            let bearish = context_child(&indicator_summary.driver_context, "bearish_absorption");
            if value_present(bearish) {
                bearish
            } else {
                context_child(&indicator_summary.driver_context, "absorption")
            }
        }
        _ => &Value::Null,
    }
}

fn initiation_present(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let payload = match side {
        "LONG" => {
            let bullish = context_child(&indicator_summary.driver_context, "bullish_initiation");
            if value_present(bullish) {
                bullish
            } else {
                context_child(&indicator_summary.driver_context, "initiation")
            }
        }
        "SHORT" => {
            let bearish = context_child(&indicator_summary.driver_context, "bearish_initiation");
            if value_present(bearish) {
                bearish
            } else {
                context_child(&indicator_summary.driver_context, "initiation")
            }
        }
        _ => &Value::Null,
    };
    if !value_present(payload) {
        return false;
    }
    let blob = flatten_blob(payload);
    blob_supports_side(&blob, side) || blob.contains("initiation")
}

fn stacked_imbalance_present(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let footprint = footprint_context(indicator_summary);
    match side {
        "LONG" => find_bool_key(footprint, "stacked_buy").unwrap_or(false),
        "SHORT" => find_bool_key(footprint, "stacked_sell").unwrap_or(false),
        _ => false,
    }
}

fn orderflow_alignment_present(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let depth = orderbook_depth(indicator_summary);
    let obi = find_f64_key(depth, "obi_k_dw_twa_fut")
        .or_else(|| find_f64_key(depth, "obi_fut"))
        .or_else(|| find_f64_key(depth, "obi"));
    let ofi = find_f64_key(depth, "ofi_norm_fut").or_else(|| find_f64_key(depth, "ofi"));
    let microprice = find_f64_key(depth, "microprice_bias")
        .or_else(|| find_f64_key(depth, "microprice_delta"))
        .or_else(|| find_f64_key(depth, "microprice"));
    let sign_aligned = match side {
        "LONG" => [obi, ofi, microprice]
            .into_iter()
            .flatten()
            .any(|value| value > 0.0),
        "SHORT" => [obi, ofi, microprice]
            .into_iter()
            .flatten()
            .any(|value| value < 0.0),
        _ => false,
    };
    if sign_aligned {
        return true;
    }
    let blob = flatten_blob(depth);
    blob_supports_side(&blob, side)
        && contains_any_keyword(&blob, &["obi", "ofi", "microprice", "aligned"])
}

fn spot_confirm_support(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let depth = orderbook_depth(indicator_summary);
    if let Some(confirm) = find_bool_key(depth, "spot_confirm") {
        return confirm;
    }
    let blob = flatten_blob(depth);
    if blob.contains("spot_confirm\":true") || blob.contains("spot_confirming\":true") {
        return true;
    }
    blob.contains("spot_led") && blob_supports_side(&blob, side)
}

fn fake_order_risk_clear(indicator_summary: &IndicatorSummary) -> bool {
    let depth = orderbook_depth(indicator_summary);
    if let Some(clear) = find_bool_key(depth, "fake_order_risk_clear") {
        return clear;
    }
    if let Some(high) = find_bool_key(depth, "fake_order_risk_high") {
        return !high;
    }
    let blob = flatten_blob(depth);
    if contains_any_keyword(
        &blob,
        &["fake_order_risk_high", "fake_order_risk_rising", "spoof"],
    ) {
        return false;
    }
    if contains_any_keyword(&blob, &["fake_order_risk_clear", "fake_order_risk_low"]) {
        return true;
    }
    true
}

fn oi_support_state(indicator_summary: &IndicatorSummary, side: &str) -> EvidenceState {
    let oi_blob = flatten_blob(open_interest_context(indicator_summary));
    let ratio_blob = flatten_blob(ratio_context(indicator_summary));
    let state_blob = format!("{oi_blob} {ratio_blob}");
    if !value_present(open_interest_context(indicator_summary))
        && !value_present(ratio_context(indicator_summary))
    {
        return EvidenceState::Missing;
    }
    match side {
        "LONG" => {
            if contains_any_keyword(&state_blob, &["long_unwind", "short_cover", "crowded_long"]) {
                EvidenceState::Conflicting
            } else {
                EvidenceState::Supporting
            }
        }
        "SHORT" => {
            if contains_any_keyword(
                &state_blob,
                &["fresh_long", "crowded_short", "short_squeeze"],
            ) {
                EvidenceState::Conflicting
            } else {
                EvidenceState::Supporting
            }
        }
        _ => EvidenceState::Missing,
    }
}

fn oi_support_present(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    matches!(
        oi_support_state(indicator_summary, side),
        EvidenceState::Supporting
    )
}

fn divergence_present(indicator_summary: &IndicatorSummary) -> bool {
    value_present(divergence_context(indicator_summary))
        || flatten_blob(divergence_context(indicator_summary)).contains("divergence")
}

fn absorption_or_exhaustion_present(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    value_present(absorption_context(indicator_summary, side))
        || value_present(exhaustion_context(indicator_summary, side))
}

fn footprint_failure_signal(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let footprint = footprint_context(indicator_summary);
    let blob = flatten_blob(footprint);
    let unfinished = find_bool_key(footprint, "ua_top").unwrap_or(false)
        || find_bool_key(footprint, "ua_bottom").unwrap_or(false)
        || blob.contains("unfinished");
    let opposite_stack = match side {
        "LONG" => find_bool_key(footprint, "stacked_sell").unwrap_or(false),
        "SHORT" => find_bool_key(footprint, "stacked_buy").unwrap_or(false),
        _ => false,
    };
    unfinished || opposite_stack || contains_any_keyword(&blob, &["failed", "trap", "rejection"])
}

fn state_conflicts_with_path(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let state_blob = format!(
        "{} {} {} {}",
        flatten_blob(open_interest_context(indicator_summary)),
        flatten_blob(ratio_context(indicator_summary)),
        flatten_blob(funding_context(indicator_summary)),
        flatten_blob(vpin_context(indicator_summary))
    );
    match side {
        "LONG" => contains_any_keyword(
            &state_blob,
            &[
                "short_cover",
                "long_unwind",
                "crowded_long",
                "negative_funding_extreme",
            ],
        ),
        "SHORT" => contains_any_keyword(
            &state_blob,
            &[
                "fresh_long_build",
                "crowded_short",
                "positive_funding_extreme",
                "short_squeeze",
            ],
        ),
        _ => false,
    }
}

fn latest_price(indicator_summary: &IndicatorSummary) -> Option<f64> {
    indicator_summary
        .auction_context
        .recent_15m_bars
        .last()
        .map(|bar| bar.close)
}

fn failure_level_breached(stage1_output: &Stage1Output, latest_price: f64) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    Ok(match path.side.as_str() {
        "LONG" => latest_price <= path.failure_level.high,
        "SHORT" => latest_price >= path.failure_level.low,
        other => return Err(anyhow!("unsupported path side {}", other)),
    })
}

fn activation_level_active(stage1_output: &Stage1Output, latest_price: f64) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    Ok(path.activation_level.contains(latest_price))
}

fn continuation_confirmed(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    let close_confirmed = match path.side.as_str() {
        "LONG" => price_above_on_close(
            &indicator_summary.auction_context.recent_15m_bars,
            path.activation_level.low,
        ),
        "SHORT" => price_below_on_close(
            &indicator_summary.auction_context.recent_15m_bars,
            path.activation_level.high,
        ),
        other => return Err(anyhow!("unsupported path side {}", other)),
    };
    let checklist_confirmed = initiation_present(indicator_summary, &path.side)
        && (stacked_imbalance_present(indicator_summary, &path.side)
            || orderflow_alignment_present(indicator_summary, &path.side)
            || spot_confirm_support(indicator_summary, &path.side))
        && !matches!(
            oi_support_state(indicator_summary, &path.side),
            EvidenceState::Conflicting
        )
        && fake_order_risk_clear(indicator_summary);
    Ok(close_confirmed && checklist_confirmed)
}

fn reversal_confirmed_any(indicator_summary: &IndicatorSummary) -> bool {
    reversal_confirmed(indicator_summary, "LONG") || reversal_confirmed(indicator_summary, "SHORT")
}

fn value_return_confirmed_any(indicator_summary: &IndicatorSummary) -> bool {
    value_return_confirmed(indicator_summary, "LONG")
        || value_return_confirmed(indicator_summary, "SHORT")
}

fn refresh_hint_hit(indicator_summary: &IndicatorSummary, stage1_output: &Stage1Output) -> bool {
    stage1_output
        .refresh_hints
        .iter()
        .map(|hint| hint.trim().to_ascii_lowercase())
        .any(|hint| match hint.as_str() {
            "extreme_location" => {
                indicator_summary
                    .auction_context
                    .zone_states
                    .iter()
                    .any(|state| {
                        failed_auction_confirmed(state)
                            || reaccept_inside_value(state)
                            || zone_acceptance_above(state)
                            || zone_acceptance_below(state)
                    })
            }
            "reverse_confirmation" => {
                reversal_confirmed_any(indicator_summary)
                    || value_return_confirmed_any(indicator_summary)
            }
            "driver_change" => driver_change_evidence(indicator_summary),
            _ => {
                (hint.contains("extreme") || hint.contains("value") || hint.contains("auction"))
                    && indicator_summary
                        .auction_context
                        .zone_states
                        .iter()
                        .any(|state| {
                            failed_auction_confirmed(state)
                                || reaccept_inside_value(state)
                                || zone_acceptance_above(state)
                                || zone_acceptance_below(state)
                        })
                    || (hint.contains("reverse") || hint.contains("reversal"))
                        && (reversal_confirmed_any(indicator_summary)
                            || value_return_confirmed_any(indicator_summary))
                    || (hint.contains("driver") || hint.contains("flip"))
                        && driver_change_evidence(indicator_summary)
            }
        })
}

fn reversal_confirmed(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    absorption_or_exhaustion_present(indicator_summary, side)
        && divergence_present(indicator_summary)
        && !spot_confirm_support(indicator_summary, side)
        && footprint_failure_signal(indicator_summary, side)
}

fn value_return_confirmed(indicator_summary: &IndicatorSummary, side: &str) -> bool {
    let failed_auction = indicator_summary
        .auction_context
        .zone_states
        .iter()
        .any(failed_auction_confirmed);
    let reaccept = indicator_summary
        .auction_context
        .zone_states
        .iter()
        .any(|state| {
            reaccept_inside_value(state)
                || zone_acceptance_above(state)
                || zone_acceptance_below(state)
        });
    let lacking_oi_and_spot_support = !matches!(
        oi_support_state(indicator_summary, side),
        EvidenceState::Supporting
    ) && !spot_confirm_support(indicator_summary, side);
    failed_auction && reaccept && lacking_oi_and_spot_support
}

fn setup_confirmed(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    match path.setup_type.as_str() {
        "A_continuation" => continuation_confirmed(indicator_summary, stage1_output),
        "B_reversal" => Ok(reversal_confirmed(indicator_summary, &path.side)),
        "C_value_return" => Ok(value_return_confirmed(indicator_summary, &path.side)),
        other => Err(anyhow!("unsupported setup_type {}", other)),
    }
}

fn reevaluation_trigger_hit(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    if path.reevaluation_trigger.signals.is_empty() {
        return Ok(false);
    }
    let latest_close_time = indicator_summary
        .auction_context
        .recent_15m_bars
        .last()
        .map(|bar| bar.close_time);
    let signal_hit = path
        .reevaluation_trigger
        .signals
        .iter()
        .any(|signal| match signal.as_str() {
            "extreme_location" => {
                indicator_summary
                    .auction_context
                    .zone_states
                    .iter()
                    .any(|state| {
                        let ts_ok = latest_close_time
                            .zip(state.confirmed_at)
                            .map(|(now, confirmed)| event_after_precondition(now, confirmed))
                            .unwrap_or(false);
                        ts_ok && failed_auction_confirmed(state)
                    })
            }
            "reverse_confirmation" => {
                reversal_confirmed(indicator_summary, &path.side)
                    || value_return_confirmed(indicator_summary, &path.side)
            }
            "driver_change" => driver_change_evidence(indicator_summary),
            _ => false,
        });
    Ok(signal_hit)
}

fn build_soft_gate(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
) -> Result<SoftGateEvaluation> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    let driver_blob = flatten_blob(&indicator_summary.driver_context);
    let driver_bias = stage1_output
        .driver_attribution
        .as_ref()
        .map(|item| item.driver_bias.to_ascii_lowercase())
        .unwrap_or_default();

    let state_clear = !state_conflicts_with_path(indicator_summary, &path.side);
    let driver_clear = !driver_bias.is_empty()
        && !blob_conflicts_side(&driver_blob, &path.side)
        && (!driver_blob.contains("flip")
            || driver_blob.contains(&driver_bias)
            || stage1_output
                .driver_attribution
                .as_ref()
                .map(|item| item.conflicting_evidence.is_empty())
                .unwrap_or(false));
    let orderflow_real = match path.setup_type.as_str() {
        "A_continuation" => {
            orderflow_alignment_present(indicator_summary, &path.side)
                && fake_order_risk_clear(indicator_summary)
        }
        "B_reversal" => {
            absorption_or_exhaustion_present(indicator_summary, &path.side)
                && divergence_present(indicator_summary)
                && footprint_failure_signal(indicator_summary, &path.side)
        }
        "C_value_return" => {
            value_return_confirmed(indicator_summary, &path.side)
                && !oi_support_present(indicator_summary, &path.side)
        }
        _ => false,
    };
    let invalidation_clear = path.failure_level.low > 0.0 && path.failure_level.high > 0.0;
    let passed_count = [
        state_clear,
        driver_clear,
        orderflow_real,
        invalidation_clear,
    ]
    .into_iter()
    .filter(|item| *item)
    .count() as u8;

    Ok(SoftGateEvaluation {
        state_clear,
        driver_clear,
        orderflow_real,
        invalidation_clear,
        passed_count,
    })
}

pub fn evaluate_stage2_runtime(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
    soft_gate_cfg: &WorkflowSoftGateMinPassConfig,
) -> Result<Stage2RuntimeEvaluation> {
    let latest_price =
        latest_price(indicator_summary).ok_or_else(|| anyhow!("missing latest 15m close"))?;
    if stage1_output.monitoring_status == "no_edge" {
        return Ok(Stage2RuntimeEvaluation {
            monitoring_status: stage1_output.monitoring_status.clone(),
            latest_price,
            no_edge_reentered: refresh_hint_hit(indicator_summary, stage1_output),
            failure_level_breached: false,
            reevaluation_trigger_hit: false,
            activation_level_active: false,
            setup_confirmed: false,
            hard_gate: HardGateEvaluation::default(),
            soft_gate: SoftGateEvaluation::default(),
            soft_gate_min_required: 0,
        });
    }
    let failure_level_breached = failure_level_breached(stage1_output, latest_price)?;
    let reevaluation_trigger_hit = reevaluation_trigger_hit(indicator_summary, stage1_output)?;
    let activation_level_active = activation_level_active(stage1_output, latest_price)?;
    let setup_confirmed = setup_confirmed(indicator_summary, stage1_output)?;
    let hard_gate = HardGateEvaluation {
        location_valid: activation_level_active,
        trigger_confirmed: setup_confirmed,
    };
    let soft_gate = build_soft_gate(indicator_summary, stage1_output)?;
    let soft_gate_min_required = setup_type_min_pass(
        stage1_output
            .current_path
            .as_ref()
            .ok_or_else(|| anyhow!("active stage1 path missing"))?
            .setup_type
            .as_str(),
        soft_gate_cfg,
    );

    Ok(Stage2RuntimeEvaluation {
        monitoring_status: stage1_output.monitoring_status.clone(),
        latest_price,
        no_edge_reentered: false,
        failure_level_breached,
        reevaluation_trigger_hit,
        activation_level_active,
        setup_confirmed,
        hard_gate,
        soft_gate,
        soft_gate_min_required,
    })
}

pub fn runtime_contract_from_evaluation(
    indicator_summary: &IndicatorSummary,
    stage1_output: &Stage1Output,
    eval: &Stage2RuntimeEvaluation,
) -> WorkflowRuntimeContract {
    let recommended_context_key = stage1_output.current_path.as_ref().map(|path| {
        format!(
            "{}:{}:{}",
            indicator_summary.symbol.to_ascii_uppercase(),
            path.side.to_ascii_uppercase(),
            path.id
        )
    });
    let (request_refresh_reason, request_trigger_source) = if eval.monitoring_status == "no_edge" {
        if eval.no_edge_reentered {
            (
                Some("no_edge_reentered".to_string()),
                Some("refresh_hint".to_string()),
            )
        } else {
            (None, None)
        }
    } else if eval.failure_level_breached {
        (
            Some("thesis_invalidated".to_string()),
            Some("failure_level".to_string()),
        )
    } else if eval.reevaluation_trigger_hit {
        (
            Some("thesis_invalidated".to_string()),
            Some("reevaluation_trigger".to_string()),
        )
    } else {
        (None, None)
    };
    let allow_execute = eval.monitoring_status == "active"
        && !eval.failure_level_breached
        && !eval.reevaluation_trigger_hit
        && eval.hard_gate.location_valid
        && eval.hard_gate.trigger_confirmed
        && eval.soft_gate.passed_count >= eval.soft_gate_min_required;

    WorkflowRuntimeContract {
        monitoring_status: eval.monitoring_status.clone(),
        no_edge_reentered: eval.no_edge_reentered,
        failure_level_breached: eval.failure_level_breached,
        reevaluation_trigger_hit: eval.reevaluation_trigger_hit,
        activation_level_active: eval.activation_level_active,
        setup_confirmed: eval.setup_confirmed,
        hard_gate: eval.hard_gate.clone(),
        soft_gate: eval.soft_gate.clone(),
        soft_gate_min_required: eval.soft_gate_min_required,
        allow_execute,
        request_refresh_reason,
        request_trigger_source,
        recommended_context_key,
    }
}

fn snapshot_matches_direction(snapshot: &EntrySnapshot, symbol: &str, direction: &str) -> bool {
    snapshot.symbol.eq_ignore_ascii_case(symbol) && snapshot.side.eq_ignore_ascii_case(direction)
}

fn workflow_positions_for_active_position(
    symbol: &str,
    position: &crate::execution::binance::ActivePositionSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<WorkflowPosition> {
    let direction = if position.position_amt >= 0.0 {
        "LONG"
    } else {
        "SHORT"
    }
    .to_string();
    let matching_snapshots = entry_snapshots
        .values()
        .filter(|snapshot| snapshot_matches_direction(snapshot, symbol, &direction))
        .cloned()
        .collect::<Vec<_>>();
    if matching_snapshots.is_empty() {
        return vec![WorkflowPosition {
            context_key: format!("{}:{}", symbol.to_ascii_uppercase(), direction),
            position_side: position.position_side.clone(),
            direction,
            quantity: position.position_amt.abs(),
            leverage: position.leverage,
            entry_price: position.entry_price,
            mark_price: position.mark_price,
            unrealized_pnl: position.unrealized_pnl,
            current_tp_price: None,
            current_sl_price: None,
            entry_snapshot: None,
        }];
    }
    matching_snapshots
        .into_iter()
        .map(|snapshot| WorkflowPosition {
            context_key: snapshot.context_key.clone(),
            position_side: position.position_side.clone(),
            direction: snapshot.side.clone(),
            quantity: position.position_amt.abs(),
            leverage: position.leverage,
            entry_price: position.entry_price,
            mark_price: position.mark_price,
            unrealized_pnl: position.unrealized_pnl,
            current_tp_price: Some(snapshot.take_profit_1),
            current_sl_price: Some(snapshot.stop_loss),
            entry_snapshot: Some(snapshot),
        })
        .collect()
}

pub fn build_stage2_prompt_input(
    indicator_summary: IndicatorSummary,
    stage1_output: Stage1Output,
    runtime_contract: WorkflowRuntimeContract,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Stage2PromptInput {
    let active_positions = trading_state
        .active_positions
        .iter()
        .flat_map(|position| {
            workflow_positions_for_active_position(&trading_state.symbol, position, entry_snapshots)
        })
        .collect();

    Stage2PromptInput {
        task: "Evaluate the current path, confirm the setup, emit execution_intent when allowed, and manage active contexts.".to_string(),
        indicator_summary,
        stage1_output,
        runtime_contract,
        active_positions,
        account: WorkflowAccountContext {
            total_wallet_balance: trading_state.total_wallet_balance,
            available_balance: trading_state.available_balance,
            has_active_positions: trading_state.has_active_positions,
            has_open_orders: trading_state.has_open_orders,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_stage2_prompt_input, evaluate_stage2_runtime, runtime_contract_from_evaluation,
    };
    use crate::app::config::WorkflowSoftGateMinPassConfig;
    use crate::execution::binance::{ActivePositionSnapshot, TradingStateSnapshot};
    use crate::workflow::schema::{
        AuctionContext, CurrentPath, DriverAttribution, EntrySnapshot, HardGateEvaluation,
        IndicatorSummary, ManagementPlan, PriceZone, RecentBar, ReevaluationTrigger,
        SoftGateEvaluation, Stage1Meta, Stage1Output,
    };
    use chrono::{Duration, Utc};
    use serde_json::json;
    use std::collections::HashMap;

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: None,
            current_script: Some("continuation".to_string()),
            driver_attribution: Some(DriverAttribution {
                driver_bias: "buy".to_string(),
                primary_driver: None,
                supporting_evidence: vec![],
                conflicting_evidence: vec![],
            }),
            current_path: Some(CurrentPath {
                id: "path_1".to_string(),
                side: "LONG".to_string(),
                thesis: "continuation".to_string(),
                activation_level: PriceZone {
                    low: 1998.0,
                    high: 2005.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target: PriceZone {
                    low: 2020.0,
                    high: 2025.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target: PriceZone {
                    low: 2030.0,
                    high: 2035.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_level: PriceZone {
                    low: 1989.0,
                    high: 1992.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: "reversal".to_string(),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: ReevaluationTrigger {
                    signals: vec!["driver_change".to_string()],
                },
                management_plan: ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2022.0,
                    take_profit_2_level: 2032.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![],
            }),
        }
    }

    fn sample_indicator_summary(close: f64) -> IndicatorSummary {
        let now = Utc::now();
        IndicatorSummary {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            source_routing_key: "x".to_string(),
            indicator_count: 1,
            missing_indicator_codes: vec![],
            position_context: json!({}),
            state_context: json!({
                "open_interest": {"label": "fresh_short_build"},
                "long_short_ratios": {"state": "balanced"},
                "funding_rate": {"regime": "neutral"},
                "vpin": {"state": "normal"}
            }),
            driver_context: json!({
                "initiation": {"direction": "buy", "confirmed": true},
                "orderbook_depth": {
                    "obi_k_dw_twa_fut": 0.7,
                    "ofi_norm_fut": 0.5,
                    "microprice_bias": 0.2,
                    "spot_confirm": true,
                    "fake_order_risk_fut": 0.1
                }
            }),
            trigger_context: json!({
                "footprint": {"stacked_buy": true, "stacked_sell": false},
                "divergence": {"present": false}
            }),
            auction_context: AuctionContext {
                tracked_zones: vec![],
                zone_states: vec![],
                recent_15m_bars: vec![RecentBar {
                    open_time: now - Duration::minutes(15),
                    close_time: now,
                    open: close - 1.0,
                    high: close + 1.0,
                    low: close - 2.0,
                    close,
                    is_closed: true,
                }],
            },
            aux_context: json!({}),
        }
    }

    #[test]
    fn hard_and_soft_gate_evaluate_with_expected_defaults() {
        let eval = evaluate_stage2_runtime(
            &sample_indicator_summary(2001.0),
            &sample_stage1_output(),
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert_eq!(
            eval.hard_gate,
            HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            }
        );
        assert_eq!(eval.soft_gate_min_required, 3);
        assert_eq!(
            eval.soft_gate,
            SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            }
        );
    }

    #[test]
    fn failure_level_breach_is_detected_before_execution() {
        let eval = evaluate_stage2_runtime(
            &sample_indicator_summary(1991.0),
            &sample_stage1_output(),
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert!(eval.failure_level_breached);
    }

    #[test]
    fn continuation_does_not_fail_only_because_oi_and_ratio_are_missing() {
        let mut indicator_summary = sample_indicator_summary(2001.0);
        indicator_summary.state_context = json!({
            "funding_rate": {"regime": "neutral"},
            "vpin": {"state": "normal"}
        });

        let eval = evaluate_stage2_runtime(
            &indicator_summary,
            &sample_stage1_output(),
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert!(eval.hard_gate.trigger_confirmed);
    }

    #[test]
    fn activation_level_requires_price_inside_path_zone() {
        let eval = evaluate_stage2_runtime(
            &sample_indicator_summary(2006.0),
            &sample_stage1_output(),
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert!(!eval.hard_gate.location_valid);
    }

    #[test]
    fn numeric_fake_order_risk_does_not_create_implicit_threshold_rule() {
        let mut indicator_summary = sample_indicator_summary(2001.0);
        indicator_summary.driver_context = json!({
            "initiation": {"direction": "buy", "confirmed": true},
            "orderbook_depth": {
                "obi_k_dw_twa_fut": 0.7,
                "ofi_norm_fut": 0.5,
                "microprice_bias": 0.2,
                "spot_confirm": true,
                "fake_order_risk_fut": 0.95
            }
        });

        let eval = evaluate_stage2_runtime(
            &indicator_summary,
            &sample_stage1_output(),
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert!(eval.soft_gate.orderflow_real);
    }

    #[test]
    fn no_edge_runtime_allows_management_without_execution() {
        let mut stage1_output = sample_stage1_output();
        stage1_output.monitoring_status = "no_edge".to_string();
        stage1_output.current_script = None;
        stage1_output.current_path = None;
        stage1_output.refresh_hints = vec!["driver_change".to_string()];

        let eval = evaluate_stage2_runtime(
            &sample_indicator_summary(2001.0),
            &stage1_output,
            &WorkflowSoftGateMinPassConfig::default(),
        )
        .expect("stage2 runtime eval");

        assert_eq!(eval.monitoring_status, "no_edge");
        assert!(eval.no_edge_reentered);
        assert!(!eval.hard_gate.location_valid);
        assert!(!eval.hard_gate.trigger_confirmed);
    }

    #[test]
    fn prompt_input_expands_multiple_snapshots_for_same_direction() {
        let indicator_summary = sample_indicator_summary(2001.0);
        let stage1_output = sample_stage1_output();
        let runtime_contract = runtime_contract_from_evaluation(
            &indicator_summary,
            &stage1_output,
            &evaluate_stage2_runtime(
                &indicator_summary,
                &stage1_output,
                &WorkflowSoftGateMinPassConfig::default(),
            )
            .expect("eval"),
        );
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "BOTH".to_string(),
                position_amt: 0.2,
                entry_price: 2000.0,
                mark_price: 2001.0,
                unrealized_pnl: 1.0,
                leverage: 8,
            }],
            open_orders: vec![],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        };
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "ETHUSDT:LONG:path_a".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1990.0,
                take_profit_1: 2020.0,
                take_profit_2: 2030.0,
                allowed_stop_loss_levels: vec![1990.0],
                allowed_take_profit_levels: vec![2020.0, 2030.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );
        snapshots.insert(
            "ETHUSDT:LONG:path_b".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG:path_b".to_string(),
                path_id: "path_b".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1988.0,
                take_profit_1: 2022.0,
                take_profit_2: 2032.0,
                allowed_stop_loss_levels: vec![1988.0],
                allowed_take_profit_levels: vec![2022.0, 2032.0],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );

        let prompt_input = build_stage2_prompt_input(
            indicator_summary,
            stage1_output,
            runtime_contract,
            &trading_state,
            &snapshots,
        );

        assert_eq!(prompt_input.active_positions.len(), 2);
        assert!(prompt_input
            .active_positions
            .iter()
            .any(|position| position.context_key == "ETHUSDT:LONG:path_a"));
        assert!(prompt_input
            .active_positions
            .iter()
            .any(|position| position.context_key == "ETHUSDT:LONG:path_b"));
    }
}
