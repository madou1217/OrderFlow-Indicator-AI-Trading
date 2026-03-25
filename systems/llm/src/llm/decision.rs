use crate::llm::prompt;
use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::Value;

const PENDING_LEVEL_EXACT_EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeDecision {
    Long,
    Short,
    NoTrade,
}

impl TradeDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
            Self::NoTrade => "NO_TRADE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeIntent {
    pub decision: TradeDecision,
    pub entry_price: Option<f64>,
    pub take_profit: Option<f64>,
    pub stop_loss: Option<f64>,
    pub leverage: Option<f64>,
    pub risk_reward_ratio: Option<f64>,
    pub horizon: Option<String>,
    pub swing_logic: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PositionManagementDecision {
    Close,
    Add,
    Reduce,
    Hold,
    ModifyTpSl,
}

impl PositionManagementDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Close => "CLOSE",
            Self::Add => "ADD",
            Self::Reduce => "REDUCE",
            Self::Hold => "HOLD",
            Self::ModifyTpSl => "MODIFY_TPSL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PendingOrderManagementDecision {
    Hold,
    Close,
    ModifyMaker,
}

impl PendingOrderManagementDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hold => "HOLD",
            Self::Close => "CLOSE",
            Self::ModifyMaker => "MODIFY_MAKER",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionManagementIntent {
    pub decision: PositionManagementDecision,
    pub qty: Option<f64>,
    pub qty_ratio: Option<f64>,
    pub is_full_exit: Option<bool>,
    pub new_tp: Option<f64>,
    pub new_sl: Option<f64>,
    pub close_price: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingOrderManagementIntent {
    pub decision: PendingOrderManagementDecision,
    pub new_entry: Option<f64>,
    pub new_tp: Option<f64>,
    pub new_sl: Option<f64>,
    pub new_leverage: Option<f64>,
    pub reason: String,
}

/// Current state of the live pending order, used to determine HOLD vs MODIFY_MAKER.
#[derive(Debug, Clone, Default)]
pub struct PendingOrderContext {
    pub has_open_orders: bool,
    pub current_entry: Option<f64>,
    pub current_tp: Option<f64>,
    pub current_sl: Option<f64>,
    pub current_leverage: Option<f64>,
}

pub fn validate_model_output(
    value: &Value,
    management_mode: bool,
    pending_order_mode: bool,
) -> Option<String> {
    if pending_order_mode {
        pending_order_management_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    } else if management_mode {
        position_management_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    } else {
        trade_intent_from_value(value)
            .err()
            .map(|err| err.to_string())
    }
}

pub fn trade_intent_from_value(value: &Value) -> Result<TradeIntent> {
    validate_edge_assessment(value)?;
    validate_trade_quality(value)?;

    let decision_raw = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| {
            anyhow!(
                "decision must be {}/{}/{}",
                prompt::DECISION_LONG,
                prompt::DECISION_SHORT,
                prompt::DECISION_NO_TRADE
            )
        })?;
    let decision = parse_trade_decision(decision_raw)?;
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();

    let entry_price = find_f64(value, &["plan.entry"]);
    let take_profit = find_f64(value, &["plan.take_profit"]);
    let stop_loss = find_f64(value, &["plan.stop_loss"]);
    let leverage = find_f64(value, &["plan.leverage"]);
    let model_risk_reward_ratio = find_f64(value, &["plan.rr"]);
    let horizon = find_str(value, &["plan.horizon"]).map(str::to_string);
    let swing_logic = find_str(value, &["plan.swing_logic"]).map(str::to_string);

    match decision {
        TradeDecision::NoTrade => Ok(TradeIntent {
            decision,
            entry_price: None,
            take_profit: None,
            stop_loss: None,
            leverage: None,
            risk_reward_ratio: None,
            horizon: None,
            swing_logic,
            reason,
        }),
        TradeDecision::Long | TradeDecision::Short => {
            let entry_price = entry_price.ok_or_else(|| anyhow!("entry is missing"))?;
            let take_profit = take_profit.ok_or_else(|| anyhow!("tp is missing"))?;
            let stop_loss = stop_loss.ok_or_else(|| anyhow!("sl is missing"))?;
            let horizon = horizon
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow!("horizon is missing"))?;

            if entry_price <= 0.0 {
                return Err(anyhow!("entry must be > 0"));
            }
            if take_profit <= 0.0 {
                return Err(anyhow!("tp must be > 0"));
            }
            if stop_loss <= 0.0 {
                return Err(anyhow!("sl must be > 0"));
            }
            if leverage.is_some_and(|value| value <= 0.0) {
                return Err(anyhow!("leverage must be > 0"));
            }
            match decision {
                TradeDecision::Long => {
                    if !(take_profit > entry_price && stop_loss < entry_price) {
                        return Err(anyhow!("LONG requires tp > entry and sl < entry"));
                    }
                }
                TradeDecision::Short => {
                    if !(take_profit < entry_price && stop_loss > entry_price) {
                        return Err(anyhow!("SHORT requires tp < entry and sl > entry"));
                    }
                }
                TradeDecision::NoTrade => {}
            }
            let risk_reward_ratio = compute_rr_from_levels(entry_price, take_profit, stop_loss)
                .or_else(|| model_risk_reward_ratio.filter(|value| *value > 0.0))
                .ok_or_else(|| anyhow!("rr could not be computed from entry/tp/sl"))?;

            Ok(TradeIntent {
                decision,
                entry_price: Some(entry_price),
                take_profit: Some(take_profit),
                stop_loss: Some(stop_loss),
                leverage,
                risk_reward_ratio: Some(risk_reward_ratio),
                horizon: Some(horizon),
                swing_logic,
                reason,
            })
        }
    }
}

fn validate_edge_assessment(value: &Value) -> Result<()> {
    let edge_assessment = value
        .get("edge_assessment")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("edge_assessment must be an object"))?;

    validate_enum_field(edge_assessment, "side", &["LONG", "SHORT", "NONE"])?;

    edge_assessment
        .get("edge_exists_now")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("edge_exists_now must be a boolean"))?;

    validate_enum_field(
        edge_assessment,
        "edge_quality",
        &["strong", "moderate", "weak"],
    )?;
    validate_enum_field(
        edge_assessment,
        "location_quality",
        &["strong", "moderate", "weak"],
    )?;
    validate_enum_field(
        edge_assessment,
        "path_quality",
        &["clean", "contested", "poor"],
    )?;

    edge_assessment
        .get("why_no_trade_now")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("why_no_trade_now must be an array"))?;

    Ok(())
}

fn validate_trade_quality(value: &Value) -> Result<()> {
    let trade_quality = value
        .get("trade_quality")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("trade_quality must be an object"))?;

    validate_enum_field(
        trade_quality,
        "thesis_clarity",
        &["strong", "moderate", "weak"],
    )?;
    validate_enum_field(
        trade_quality,
        "execution_quality",
        &["strong", "moderate", "weak"],
    )?;
    validate_enum_field(
        trade_quality,
        "path_to_target_quality",
        &["clean", "contested", "poor"],
    )?;
    validate_enum_field(
        trade_quality,
        "stopout_risk_before_resolution",
        &["low", "medium", "high"],
    )?;
    validate_enum_field(
        trade_quality,
        "reward_to_risk_sufficiency",
        &["ample", "adequate", "insufficient"],
    )?;

    Ok(())
}

fn validate_enum_field(
    obj: &serde_json::Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<()> {
    let raw = obj
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("{key} must be a non-empty string"))?;

    if allowed
        .iter()
        .any(|candidate| raw.eq_ignore_ascii_case(candidate))
    {
        Ok(())
    } else {
        Err(anyhow!("{key} must be one of {}", allowed.join("/")))
    }
}

pub fn position_management_intent_from_value(value: &Value) -> Result<PositionManagementIntent> {
    let decision_raw = value
        .get("decision")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| {
            anyhow!(
                "decision must be {}/{}/{}/{}/{}",
                prompt::DECISION_HOLD,
                prompt::DECISION_REDUCE,
                prompt::DECISION_CLOSE,
                prompt::DECISION_ADJUST,
                prompt::DECISION_ADD,
            )
        })?;
    let decision = parse_management_decision(decision_raw)?;
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();
    let new_tp = find_f64(value, &["params.new_tp", "new_tp", "params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.new_sl", "new_sl", "params.sl", "sl"]);
    let close_price = find_f64(value, &["params.close_price", "close_price"]);
    let qty_ratio = find_f64(value, &["params.qty_ratio", "qty_ratio"]);

    let adjust_fields = value
        .pointer("/params/adjust_fields")
        .or_else(|| value.get("adjust_fields"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let decision = match decision {
        PositionManagementDecision::ModifyTpSl => {
            // Backward compatibility: legacy ADJUST may still encode add/reduce via adjust_fields.
            if adjust_fields.iter().any(|f| f == "add") {
                if qty_ratio.is_none() {
                    return Err(anyhow!("ADD requires params.qty_ratio"));
                }
                PositionManagementDecision::Add
            } else if adjust_fields.iter().any(|f| f == "reduce") {
                if qty_ratio.is_none() {
                    return Err(anyhow!("REDUCE requires params.qty_ratio"));
                }
                PositionManagementDecision::Reduce
            } else {
                if new_tp.is_none() && new_sl.is_none() {
                    return Err(anyhow!(
                        "ADJUST requires at least one of params.new_tp or params.new_sl"
                    ));
                }
                if new_tp.is_some_and(|v| v <= 0.0) {
                    return Err(anyhow!("new_tp must be > 0 when provided"));
                }
                if new_sl.is_some_and(|v| v <= 0.0) {
                    return Err(anyhow!("new_sl must be > 0 when provided"));
                }
                PositionManagementDecision::ModifyTpSl
            }
        }
        PositionManagementDecision::Add | PositionManagementDecision::Reduce => {
            let qty_ratio = qty_ratio
                .ok_or_else(|| anyhow!("{} requires params.qty_ratio", decision.as_str()))?;
            if !(qty_ratio > 0.0 && qty_ratio <= 1.0) {
                return Err(anyhow!("qty_ratio must be > 0 and <= 1"));
            }
            decision
        }
        PositionManagementDecision::Hold | PositionManagementDecision::Close => decision,
    };

    Ok(PositionManagementIntent {
        decision,
        qty: None,
        qty_ratio,
        is_full_exit: None,
        new_tp,
        new_sl,
        close_price,
        reason,
    })
}

pub fn position_management_intent_from_value_with_context(
    value: &Value,
    has_active_positions: bool,
    has_open_orders: bool,
) -> Result<PositionManagementIntent> {
    let intent = position_management_intent_from_value(value)?;
    if matches!(intent.decision, PositionManagementDecision::ModifyTpSl) && !has_active_positions {
        return Err(anyhow!("ADJUST is invalid when no active positions exist"));
    }
    if !has_active_positions
        && !has_open_orders
        && matches!(intent.decision, PositionManagementDecision::Close)
    {
        return Err(anyhow!(
            "CLOSE is not actionable when neither active positions nor open orders exist"
        ));
    }
    Ok(intent)
}

/// Parse the model's optimal order output (entry/tp/sl/leverage).
/// Decision is not read from the model — it is computed by comparison with the live order.
pub fn pending_order_management_intent_from_value(
    value: &Value,
) -> Result<PendingOrderManagementIntent> {
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| anyhow!("reason must be non-empty"))?
        .to_string();

    let new_entry = find_f64(value, &["params.entry", "entry"]);
    let new_tp = find_f64(value, &["params.tp", "tp"]);
    let new_sl = find_f64(value, &["params.sl", "sl"]);
    let new_leverage = find_f64(value, &["params.leverage", "leverage"]);

    if new_entry.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("entry must be > 0 when provided"));
    }
    if new_tp.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("tp must be > 0 when provided"));
    }
    if new_sl.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("sl must be > 0 when provided"));
    }
    if new_leverage.is_some_and(|v| v <= 0.0) {
        return Err(anyhow!("leverage must be > 0 when provided"));
    }

    // Preliminary decision: Close if model sees no valid setup; ModifyMaker otherwise.
    // The _with_context function refines this to Hold when levels match the live order.
    let decision = if new_entry.is_none() && new_tp.is_none() && new_sl.is_none() {
        PendingOrderManagementDecision::Close
    } else {
        PendingOrderManagementDecision::ModifyMaker
    };

    Ok(PendingOrderManagementIntent {
        decision,
        new_entry,
        new_tp,
        new_sl,
        new_leverage,
        reason,
    })
}

/// Compare the model's optimal order against the live order.
/// If the model's levels exactly match the live order → Hold. Otherwise → ModifyMaker.
///
/// Exchange-step-aware no-op suppression happens later in execution, where we have the
/// symbol tick size and can safely decide whether a requested reprice would place the
/// same effective live order.
pub fn pending_order_management_intent_from_value_with_context(
    value: &Value,
    ctx: &PendingOrderContext,
) -> Result<PendingOrderManagementIntent> {
    if !ctx.has_open_orders {
        return Err(anyhow!(
            "pending-order management is invalid when no open orders exist"
        ));
    }
    let mut intent = pending_order_management_intent_from_value(value)?;

    if matches!(intent.decision, PendingOrderManagementDecision::ModifyMaker) {
        if levels_match(intent.new_entry, ctx.current_entry)
            && levels_match(intent.new_tp, ctx.current_tp)
            && levels_match(intent.new_sl, ctx.current_sl)
            && levels_match(intent.new_leverage, ctx.current_leverage)
        {
            intent.decision = PendingOrderManagementDecision::Hold;
        }
    }

    Ok(intent)
}

/// Returns true if the model is effectively asking to keep the live level unchanged.
///
/// Omitted model levels (`None`) are treated as "leave unchanged". Concrete model levels require a
/// concrete live level to compare against; otherwise we must preserve MODIFY_MAKER instead of
/// collapsing into HOLD.
fn levels_match(model: Option<f64>, current: Option<f64>) -> bool {
    match (model, current) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(m), Some(c)) => (m - c).abs() <= PENDING_LEVEL_EXACT_EPSILON,
    }
}

fn compute_rr_from_levels(entry: f64, tp: f64, sl: f64) -> Option<f64> {
    let risk = (entry - sl).abs();
    let reward = (tp - entry).abs();
    if risk <= f64::EPSILON || reward <= f64::EPSILON {
        None
    } else {
        Some(reward / risk)
    }
}

pub fn normalize_decision(raw: &str) -> String {
    match raw.trim() {
        s if s.eq_ignore_ascii_case("LONG") => "LONG".to_string(),
        s if s.eq_ignore_ascii_case("SHORT") => "SHORT".to_string(),
        s if s.eq_ignore_ascii_case("NO_TRADE") => "NO_TRADE".to_string(),
        s if s == "鍋氬" => "LONG".to_string(),
        s if s == "鍋氱┖" => "SHORT".to_string(),
        s if s == "涓嶅仛" => "NO_TRADE".to_string(),
        s if s == "\u{4E0D}\u{4EA4}\u{6613}" => "NO_TRADE".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn parse_trade_decision(raw: &str) -> Result<TradeDecision> {
    match normalize_decision(raw).as_str() {
        "LONG" => Ok(TradeDecision::Long),
        "SHORT" => Ok(TradeDecision::Short),
        "NO_TRADE" => Ok(TradeDecision::NoTrade),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}",
            prompt::DECISION_LONG,
            prompt::DECISION_SHORT,
            prompt::DECISION_NO_TRADE
        )),
    }
}

fn parse_management_decision(raw: &str) -> Result<PositionManagementDecision> {
    match raw.trim().to_ascii_uppercase().as_str() {
        // Primary action-based decisions
        "HOLD" => Ok(PositionManagementDecision::Hold),
        "REDUCE" => Ok(PositionManagementDecision::Reduce),
        "CLOSE" => Ok(PositionManagementDecision::Close),
        "ADD" => Ok(PositionManagementDecision::Add),
        "ADJUST" | "MODIFY_TPSL" => Ok(PositionManagementDecision::ModifyTpSl),
        // Legacy review-style decisions, kept for parser compatibility
        "VALID" => Ok(PositionManagementDecision::Hold),
        "INVALID" => Ok(PositionManagementDecision::Close),
        _ => Err(anyhow!(
            "decision must be {}/{}/{}/{}/{}",
            prompt::DECISION_HOLD,
            prompt::DECISION_REDUCE,
            prompt::DECISION_CLOSE,
            prompt::DECISION_ADJUST,
            prompt::DECISION_ADD,
        )),
    }
}

fn find_f64<'a>(value: &'a Value, paths: &[&str]) -> Option<f64> {
    paths.iter().find_map(|path| {
        let v = get_path(value, path)?;
        if let Some(n) = v.as_f64() {
            return Some(n);
        }
        v.as_str().and_then(|raw| raw.trim().parse::<f64>().ok())
    })
}

fn find_str<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a str> {
    paths
        .iter()
        .find_map(|path| get_path(value, path).and_then(Value::as_str).map(str::trim))
}

fn find_bool<'a>(value: &'a Value, paths: &[&str]) -> Option<bool> {
    paths.iter().find_map(|path| {
        let v = get_path(value, path)?;
        if let Some(b) = v.as_bool() {
            return Some(b);
        }
        v.as_str()
            .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
    })
}

fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::{
        pending_order_management_intent_from_value,
        pending_order_management_intent_from_value_with_context,
        position_management_intent_from_value, trade_intent_from_value, PendingOrderContext,
        PendingOrderManagementDecision, PositionManagementDecision, TradeDecision,
    };
    use serde_json::json;

    #[test]
    fn pending_modify_maker_parses_entry_tp_sl() {
        let value = json!({
            "reason": "better structural zone available",
            "params": {
                "entry": 1965.5,
                "tp": 1974.0,
                "sl": 1961.2,
                "leverage": 3.0
            }
        });

        let intent = pending_order_management_intent_from_value(&value).expect("pending parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
        assert_eq!(intent.new_entry, Some(1965.5));
        assert_eq!(intent.new_tp, Some(1974.0));
        assert_eq!(intent.new_sl, Some(1961.2));
        assert_eq!(intent.new_leverage, Some(3.0));
    }

    #[test]
    fn pending_all_null_produces_close() {
        let value = json!({
            "reason": "no valid setup",
            "params": {
                "entry": null,
                "tp": null,
                "sl": null,
                "leverage": null
            }
        });

        let intent =
            pending_order_management_intent_from_value(&value).expect("pending null parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::Close);
        assert_eq!(intent.new_entry, None);
    }

    #[test]
    fn trade_intent_accepts_range_horizon_text() {
        let value = json!({
            "edge_assessment": {
                "side": "LONG",
                "edge_exists_now": true,
                "edge_quality": "strong",
                "location_quality": "strong",
                "path_quality": "clean",
                "why_no_trade_now": []
            },
            "decision": "LONG",
            "reason": "range horizon text should not block execution",
            "trade_quality": {
                "thesis_clarity": "strong",
                "execution_quality": "strong",
                "path_to_target_quality": "clean",
                "stopout_risk_before_resolution": "low",
                "reward_to_risk_sufficiency": "ample"
            },
            "plan": {
                "entry": 2018.33,
                "take_profit": 2054.96,
                "stop_loss": 2015.12,
                "leverage": 2,
                "rr": 11.41,
                "horizon": "2-5d"
            }
        });

        let intent = trade_intent_from_value(&value).expect("trade intent parses");
        assert_eq!(intent.decision, TradeDecision::Long);
        assert_eq!(intent.horizon.as_deref(), Some("2-5d"));
    }

    #[test]
    fn trade_intent_accepts_free_form_horizon_text() {
        let value = json!({
            "edge_assessment": {
                "side": "LONG",
                "edge_exists_now": true,
                "edge_quality": "moderate",
                "location_quality": "moderate",
                "path_quality": "clean",
                "why_no_trade_now": []
            },
            "decision": "LONG",
            "reason": "free-form horizon text should be preserved",
            "trade_quality": {
                "thesis_clarity": "strong",
                "execution_quality": "moderate",
                "path_to_target_quality": "clean",
                "stopout_risk_before_resolution": "medium",
                "reward_to_risk_sufficiency": "adequate"
            },
            "plan": {
                "entry": 2018.33,
                "take_profit": 2054.96,
                "stop_loss": 2015.12,
                "leverage": 2,
                "rr": 11.41,
                "horizon": "next week"
            }
        });

        let intent = trade_intent_from_value(&value).expect("free-form horizon should parse");
        assert_eq!(intent.horizon.as_deref(), Some("next week"));
    }

    #[test]
    fn trade_intent_derives_rr_when_model_omits_it() {
        let value = json!({
            "edge_assessment": {
                "side": "LONG",
                "edge_exists_now": true,
                "edge_quality": "moderate",
                "location_quality": "moderate",
                "path_quality": "contested",
                "why_no_trade_now": []
            },
            "decision": "LONG",
            "reason": "schema-compliant entry without rr should still parse",
            "trade_quality": {
                "thesis_clarity": "moderate",
                "execution_quality": "strong",
                "path_to_target_quality": "contested",
                "stopout_risk_before_resolution": "medium",
                "reward_to_risk_sufficiency": "adequate"
            },
            "plan": {
                "entry": 2000.0,
                "take_profit": 2040.0,
                "stop_loss": 1980.0,
                "leverage": 3,
                "horizon": "4h"
            }
        });

        let intent = trade_intent_from_value(&value).expect("trade intent should derive rr");
        assert_eq!(intent.risk_reward_ratio, Some(2.0));
    }

    #[test]
    fn trade_intent_requires_trade_quality() {
        let value = json!({
            "edge_assessment": {
                "side": "LONG",
                "edge_exists_now": true,
                "edge_quality": "strong",
                "location_quality": "strong",
                "path_quality": "clean",
                "why_no_trade_now": []
            },
            "decision": "LONG",
            "reason": "missing trade quality should fail",
            "plan": {
                "entry": 2000.0,
                "take_profit": 2040.0,
                "stop_loss": 1980.0,
                "leverage": 3,
                "horizon": "4h"
            }
        });

        let err = trade_intent_from_value(&value).expect_err("trade quality should be required");
        assert!(err.to_string().contains("trade_quality"));
    }

    #[test]
    fn trade_intent_requires_edge_assessment() {
        let value = json!({
            "decision": "LONG",
            "reason": "missing edge assessment should fail",
            "trade_quality": {
                "thesis_clarity": "strong",
                "execution_quality": "strong",
                "path_to_target_quality": "clean",
                "stopout_risk_before_resolution": "low",
                "reward_to_risk_sufficiency": "ample"
            },
            "plan": {
                "entry": 2000.0,
                "take_profit": 2040.0,
                "stop_loss": 1980.0,
                "leverage": 3,
                "horizon": "4h"
            }
        });

        let err = trade_intent_from_value(&value).expect_err("edge assessment should be required");
        assert!(err.to_string().contains("edge_assessment"));
    }

    #[test]
    fn pending_leverage_change_keeps_modify_maker() {
        let value = json!({
            "reason": "same prices but different leverage",
            "params": {
                "entry": 1965.5,
                "tp": 1974.0,
                "sl": 1961.2,
                "leverage": 5.0
            }
        });
        let ctx = PendingOrderContext {
            has_open_orders: true,
            current_entry: Some(1965.5),
            current_tp: Some(1974.0),
            current_sl: Some(1961.2),
            current_leverage: Some(3.0),
        };

        let intent = pending_order_management_intent_from_value_with_context(&value, &ctx)
            .expect("pending intent parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
    }

    #[test]
    fn pending_missing_live_levels_does_not_collapse_into_hold() {
        let value = json!({
            "reason": "re-anchor to a fresher pending short zone",
            "params": {
                "entry": 2194.29,
                "tp": 2162.25,
                "sl": 2219.0,
                "leverage": 3.0
            }
        });
        let ctx = PendingOrderContext {
            has_open_orders: true,
            current_entry: None,
            current_tp: None,
            current_sl: None,
            current_leverage: None,
        };

        let intent = pending_order_management_intent_from_value_with_context(&value, &ctx)
            .expect("pending intent parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
    }

    #[test]
    fn pending_exact_same_levels_collapse_into_hold() {
        let value = json!({
            "reason": "keep current pending order",
            "params": {
                "entry": 2164.22,
                "tp": 2141.79,
                "sl": 2170.21,
                "leverage": 16.0
            }
        });
        let ctx = PendingOrderContext {
            has_open_orders: true,
            current_entry: Some(2164.22),
            current_tp: Some(2141.79),
            current_sl: Some(2170.21),
            current_leverage: Some(16.0),
        };

        let intent = pending_order_management_intent_from_value_with_context(&value, &ctx)
            .expect("pending intent parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::Hold);
    }

    #[test]
    fn pending_entry_change_inside_old_five_bps_keeps_modify_maker() {
        let value = json!({
            "reason": "re-anchor higher to 2165",
            "params": {
                "entry": 2165.0,
                "tp": 2141.79,
                "sl": 2170.21,
                "leverage": 16.0
            }
        });
        let ctx = PendingOrderContext {
            has_open_orders: true,
            current_entry: Some(2164.22),
            current_tp: Some(2141.79),
            current_sl: Some(2170.21),
            current_leverage: Some(16.0),
        };

        let intent = pending_order_management_intent_from_value_with_context(&value, &ctx)
            .expect("pending intent parses");
        assert_eq!(intent.decision, PendingOrderManagementDecision::ModifyMaker);
    }

    #[test]
    fn management_action_decisions_parse_directly() {
        let hold = json!({
            "decision": "HOLD",
            "reason": "forward edge remains favorable",
            "params": {
                "close_price": null,
                "adjust_fields": null,
                "qty_ratio": null,
                "new_tp": null,
                "new_sl": null
            }
        });
        let reduce = json!({
            "decision": "REDUCE",
            "reason": "risk has increased enough to lower exposure",
            "params": {
                "close_price": null,
                "adjust_fields": null,
                "qty_ratio": 0.35,
                "new_tp": null,
                "new_sl": null
            }
        });
        let close = json!({
            "decision": "CLOSE",
            "reason": "exit now has higher expected value than holding",
            "params": {
                "close_price": null,
                "adjust_fields": null,
                "qty_ratio": null,
                "new_tp": null,
                "new_sl": null
            }
        });
        let add = json!({
            "decision": "ADD",
            "reason": "current structure supports increasing exposure",
            "params": {
                "close_price": null,
                "adjust_fields": null,
                "qty_ratio": 0.25,
                "new_tp": null,
                "new_sl": null
            }
        });
        let adjust = json!({
            "decision": "ADJUST",
            "reason": "tp and sl should be updated",
            "params": {
                "close_price": null,
                "adjust_fields": ["tp", "sl"],
                "qty_ratio": null,
                "new_tp": 2050.0,
                "new_sl": 2108.0
            }
        });

        assert_eq!(
            position_management_intent_from_value(&hold)
                .expect("hold parses")
                .decision,
            PositionManagementDecision::Hold
        );
        assert_eq!(
            position_management_intent_from_value(&reduce)
                .expect("reduce parses")
                .decision,
            PositionManagementDecision::Reduce
        );
        assert_eq!(
            position_management_intent_from_value(&close)
                .expect("close parses")
                .decision,
            PositionManagementDecision::Close
        );
        assert_eq!(
            position_management_intent_from_value(&add)
                .expect("add parses")
                .decision,
            PositionManagementDecision::Add
        );
        assert_eq!(
            position_management_intent_from_value(&adjust)
                .expect("adjust parses")
                .decision,
            PositionManagementDecision::ModifyTpSl
        );
    }

    #[test]
    fn management_legacy_adjust_with_reduce_still_maps_to_reduce() {
        let value = json!({
            "decision": "ADJUST",
            "reason": "legacy reduce path should remain parseable",
            "params": {
                "close_price": null,
                "adjust_fields": ["reduce"],
                "qty_ratio": 0.4,
                "new_tp": null,
                "new_sl": null
            }
        });

        let intent = position_management_intent_from_value(&value).expect("legacy reduce parses");
        assert_eq!(intent.decision, PositionManagementDecision::Reduce);
        assert_eq!(intent.qty_ratio, Some(0.4));
    }
}
