use crate::app::config::{LlmModelConfig, RootConfig};
use crate::llm::prompt::WorkflowPromptStage;
use crate::llm::provider;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct WorkflowModelOutput {
    pub model_name: String,
    pub provider: String,
    pub model: String,
    pub latency_ms: u128,
    pub raw_response_text: Option<String>,
    pub parsed_value: Option<Value>,
    pub error: Option<String>,
}

fn workflow_stage_name(stage: WorkflowPromptStage) -> &'static str {
    match stage {
        WorkflowPromptStage::Stage1 => "workflow_stage1",
        WorkflowPromptStage::Stage2A => "workflow_stage2a",
        WorkflowPromptStage::Stage2B => "workflow_stage2b",
        WorkflowPromptStage::Stage2C => "workflow_stage2c",
    }
}

fn workflow_stage_schema(stage: WorkflowPromptStage) -> Value {
    match stage {
        WorkflowPromptStage::Stage1 => workflow_stage1_schema(),
        WorkflowPromptStage::Stage2A => workflow_stage2a_schema(),
        WorkflowPromptStage::Stage2B => workflow_stage2b_schema(),
        WorkflowPromptStage::Stage2C => workflow_stage2c_schema(),
    }
}

fn nullable(inner: Value) -> Value {
    json!({
        "anyOf": [
            inner,
            {"type": "null"}
        ]
    })
}

fn location_summary_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["summary", "notes"],
        "properties": {
            "summary": {"type": "string"},
            "notes": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn key_levels_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["levels"],
        "properties": {
            "levels": {
                "type": "array",
                "items": tracked_zone_schema()
            }
        }
    })
}

fn opportunity_quality_schema() -> Value {
    json!({
        "type": ["string", "null"],
        "enum": ["high", "medium", "low", null]
    })
}

fn opportunity_assessment_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "location_quality",
            "state_quality",
            "driver_quality",
            "geometry_quality",
            "uniqueness_quality",
            "overall_quality",
            "disqualifiers"
        ],
        "properties": {
            "location_quality": opportunity_quality_schema(),
            "state_quality": opportunity_quality_schema(),
            "driver_quality": opportunity_quality_schema(),
            "geometry_quality": opportunity_quality_schema(),
            "uniqueness_quality": opportunity_quality_schema(),
            "overall_quality": opportunity_quality_schema(),
            "disqualifiers": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn zone_reevaluation_trigger_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "kind",
            "zone_id",
            "timeframe",
            "min_confirmed_bars",
            "summary",
            "evidence"
        ],
        "properties": {
            "kind": {
                "type": ["string", "null"],
                "enum": [
                    "accepted_into_zone",
                    "accepted_beyond_zone",
                    "rejected_from_zone",
                    "reaccepted_through_zone",
                    null
                ]
            },
            "zone_id": {"type": ["string", "null"]},
            "timeframe": {"type": ["string", "null"], "enum": ["4h", "1d", "4h-1d", null]},
            "min_confirmed_bars": {"type": ["integer", "null"], "minimum": 1},
            "summary": {"type": "string"},
            "evidence": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn driver_reevaluation_trigger_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "kind",
            "expected_flow_driver",
            "invalidate_when_drivers",
            "require_spot_confirmation",
            "driver_signal",
            "min_confirmed_windows",
            "summary",
            "evidence"
        ],
        "properties": {
            "kind": {
                "type": ["string", "null"],
                "enum": [
                    "driver_flip",
                    "spot_confirmation_lost",
                    "oi_support_lost",
                    "state_regime_conflict",
                    null
                ]
            },
            "expected_flow_driver": {
                "type": ["string", "null"],
                "enum": ["spot_led", "futures_led", "mixed", null]
            },
            "invalidate_when_drivers": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["spot_led", "futures_led", "mixed"]
                }
            },
            "require_spot_confirmation": {"type": ["boolean", "null"]},
            "driver_signal": {
                "type": ["string", "null"],
                "enum": [
                    "spot_confirmation_lost",
                    "oi_support_lost",
                    "fake_order_risk_rising",
                    "driver_flip_confirmed",
                    null
                ]
            },
            "min_confirmed_windows": {"type": ["integer", "null"], "minimum": 1},
            "summary": {"type": "string"},
            "evidence": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn price_zone_schema(timeframes: &[&str]) -> Value {
    let mut enums = timeframes
        .iter()
        .map(|value| Value::String((*value).to_string()))
        .collect::<Vec<_>>();
    enums.push(Value::Null);
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["low", "high", "timeframe", "label", "reason"],
        "properties": {
            "low": {"type": "number"},
            "high": {"type": "number"},
            "timeframe": {"type": ["string", "null"], "enum": enums},
            "label": {"type": ["string", "null"]},
            "reason": {"type": ["string", "null"]}
        }
    })
}

fn freeform_price_zone_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["low", "high", "timeframe", "label", "reason"],
        "properties": {
            "low": {"type": "number"},
            "high": {"type": "number"},
            "timeframe": {"type": ["string", "null"]},
            "label": {"type": ["string", "null"]},
            "reason": {"type": ["string", "null"]}
        }
    })
}

fn tracked_zone_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["zone_id", "timeframe", "role", "low", "high", "reason"],
        "properties": {
            "zone_id": {"type": "string"},
            "timeframe": {"type": "string"},
            "role": {"type": "string"},
            "low": {"type": "number"},
            "high": {"type": "number"},
            "reason": {"type": ["string", "null"]}
        }
    })
}

fn reevaluation_trigger_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["extreme_location", "reverse_confirmation", "driver_change"],
        "properties": {
            "extreme_location": zone_reevaluation_trigger_schema(),
            "reverse_confirmation": zone_reevaluation_trigger_schema(),
            "driver_change": driver_reevaluation_trigger_schema()
        }
    })
}

fn map_summary_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "location_3d",
            "location_1d",
            "location_4h",
            "price_location_class",
            "key_levels"
        ],
        "properties": {
            "location_3d": location_summary_schema(),
            "location_1d": location_summary_schema(),
            "location_4h": location_summary_schema(),
            "price_location_class": {
                "type": "string",
                "enum": ["inside_value_middle", "value_edge", "outside_value_extended"]
            },
            "key_levels": key_levels_schema()
        }
    })
}

fn driver_attribution_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["flow_driver", "spot_confirming", "driver_note"],
        "properties": {
            "flow_driver": {
                "type": "string",
                "enum": ["spot_led", "futures_led", "mixed"]
            },
            "spot_confirming": {"type": "boolean"},
            "driver_note": {"type": "string"}
        }
    })
}

fn current_path_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "id",
            "side",
            "thesis",
            "risk_grade",
            "activation_anchor_id",
            "strategic_activation_level",
            "first_path_target_anchor_id",
            "first_path_target",
            "next_path_target_anchor_id",
            "next_path_target",
            "failure_anchor_id",
            "failure_level",
            "failure_switch",
            "setup_type",
            "reevaluation_trigger",
            "tracked_zones"
        ],
        "properties": {
            "id": {"type": "string"},
            "side": {"type": "string", "enum": ["LONG", "SHORT"]},
            "thesis": {"type": "string"},
            "risk_grade": {
                "type": "string",
                "enum": ["aligned_trend", "countertrend_repair", "high_conflict_repair"]
            },
            "activation_anchor_id": {"type": ["string", "null"]},
            "strategic_activation_level": price_zone_schema(&["4h", "1d", "4h-1d"]),
            "first_path_target_anchor_id": {"type": ["string", "null"]},
            "first_path_target": price_zone_schema(&["4h", "1d", "4h-1d"]),
            "next_path_target_anchor_id": {"type": ["string", "null"]},
            "next_path_target": price_zone_schema(&["4h", "1d", "4h-1d"]),
            "failure_anchor_id": {"type": ["string", "null"]},
            "failure_level": price_zone_schema(&["4h", "1d", "4h-1d"]),
            "failure_switch": {"type": ["string", "null"]},
            "setup_type": {
                "type": "string",
                "enum": ["A_continuation", "B_reversal", "C_value_return"]
            },
            "reevaluation_trigger": reevaluation_trigger_schema(),
            "tracked_zones": {"type": "array", "items": tracked_zone_schema()}
        }
    })
}

fn workflow_stage1_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "meta",
            "monitoring_status",
            "no_trade_reason",
            "refresh_hints",
            "map_summary",
            "opportunity_assessment",
            "current_script",
            "driver_attribution",
            "current_path"
        ],
        "properties": {
            "meta": {
                "type": "object",
                "additionalProperties": false,
                "required": ["stage1_ts"],
                "properties": {
                    "stage1_ts": {"type": "string"}
                }
            },
            "monitoring_status": {"type": "string", "enum": ["active", "no_edge"]},
            "no_trade_reason": {"type": ["string", "null"]},
            "refresh_hints": {"type": "array", "items": {"type": "string"}},
            "map_summary": map_summary_schema(),
            "opportunity_assessment": opportunity_assessment_schema(),
            "current_script": {
                "type": ["string", "null"],
                "enum": ["continuation", "crowded_reversal", "value_return", null]
            },
            "driver_attribution": nullable(driver_attribution_schema()),
            "current_path": nullable(current_path_schema())
        }
    })
}

fn entry_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "entry_profile",
            "intent_mode",
            "entry_activation_level",
            "entry_zone",
            "entry_invalidation_level",
            "stop_loss",
            "leverage",
            "entry_reason",
            "invalidation_reason",
            "stop_loss_reason"
        ],
        "properties": {
            "entry_profile": {
                "type": "string",
                "enum": [
                    "reclaim_then_hold",
                    "pullback_acceptance",
                    "failed_auction_reentry"
                ]
            },
            "intent_mode": {"type": "string", "enum": ["immediate", "pullback", "breakout"]},
            "entry_activation_level": nullable(freeform_price_zone_schema()),
            "entry_zone": freeform_price_zone_schema(),
            "entry_invalidation_level": freeform_price_zone_schema(),
            "stop_loss": {"type": "number"},
            "leverage": {"type": "integer", "minimum": 1, "maximum": 20},
            "entry_reason": {"type": "string"},
            "invalidation_reason": {"type": "string"},
            "stop_loss_reason": {"type": "string"}
        }
    })
}

fn tactical_entry_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["path_id", "entry_plan"],
        "properties": {
            "path_id": {"type": "string"},
            "entry_plan": entry_plan_schema()
        }
    })
}

fn workflow_stage2a_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["stage2_decision", "path_audit_note", "tactical_entry_plan", "reevaluation_reason"],
        "properties": {
            "stage2_decision": {
                "type": "string",
                "enum": ["PATH_CONFIRMED", "REQUEST_STAGE1_REEVALUATION"]
            },
            "path_audit_note": {"type": "string"},
            "tactical_entry_plan": nullable(tactical_entry_plan_schema()),
            "reevaluation_reason": {"type": ["string", "null"]}
        }
    })
}

fn price_trigger_condition_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["trigger_type", "trigger_price"],
        "properties": {
            "trigger_type": {
                "type": "string",
                "enum": ["price_above", "price_below"]
            },
            "trigger_price": {"type": "number"}
        }
    })
}

fn position_management_action_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "action_type",
            "context_key",
            "path_id",
            "trigger_condition",
            "execution_price",
            "add_ratio",
            "reuse_current_entry_template",
            "reduce_ratio",
            "new_stop_loss",
            "reuse_current_bracket_template",
            "take_profit_1",
            "take_profit_2",
            "reason"
        ],
        "properties": {
            "action_type": {
                "type": "string",
                "enum": ["add", "reduce", "exit_full", "move_stop", "update_take_profit"]
            },
            "context_key": {"type": "string"},
            "path_id": {"type": "string"},
            "trigger_condition": nullable(price_trigger_condition_schema()),
            "execution_price": {"type": ["number", "null"]},
            "add_ratio": {"type": ["number", "null"]},
            "reuse_current_entry_template": {"type": ["boolean", "null"]},
            "reduce_ratio": {"type": ["number", "null"]},
            "new_stop_loss": {"type": ["number", "null"]},
            "reuse_current_bracket_template": {"type": ["boolean", "null"]},
            "take_profit_1": {"type": ["number", "null"]},
            "take_profit_2": {"type": ["number", "null"]},
            "reason": {"type": "string"}
        }
    })
}

fn position_management_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "path_id",
            "exposure_state",
            "path_live_assessment",
            "path_assessment_reason",
            "actions",
            "management_note"
        ],
        "properties": {
            "path_id": {"type": "string"},
            "exposure_state": {"type": "string", "enum": ["in_position"]},
            "path_live_assessment": {"type": "string", "enum": ["live", "degraded", "invalidated"]},
            "path_assessment_reason": {"type": ["string", "null"]},
            "actions": {"type": "array", "items": position_management_action_schema()},
            "management_note": {"type": "string"}
        }
    })
}

fn workflow_stage2b_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["stage2b_decision", "position_management_plan"],
        "properties": {
            "stage2b_decision": {"type": "string", "enum": ["MANAGE_POSITION"]},
            "position_management_plan": position_management_plan_schema()
        }
    })
}

fn post_fill_bracket_template_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["take_profit_1", "take_profit_2", "stop_loss"],
        "properties": {
            "take_profit_1": {"type": "number"},
            "take_profit_2": {"type": "number"},
            "stop_loss": {"type": "number"}
        }
    })
}

fn pending_order_management_action_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "action_type",
            "context_key",
            "path_id",
            "trigger_condition",
            "execution_price",
            "replacement_entry_zone",
            "replacement_entry_invalidation_level",
            "replacement_stop_loss",
            "reuse_current_entry_template",
            "post_fill_bracket_template",
            "reason"
        ],
        "properties": {
            "action_type": {
                "type": "string",
                "enum": ["cancel_pending_order", "replace_entry", "update_post_fill_bracket_template"]
            },
            "context_key": {"type": "string"},
            "path_id": {"type": "string"},
            "trigger_condition": nullable(price_trigger_condition_schema()),
            "execution_price": {"type": ["number", "null"]},
            "replacement_entry_zone": nullable(price_zone_schema(&["15m", "15m-4h"])),
            "replacement_entry_invalidation_level": nullable(price_zone_schema(&["15m", "15m-4h"])),
            "replacement_stop_loss": {"type": ["number", "null"]},
            "reuse_current_entry_template": {"type": ["boolean", "null"]},
            "post_fill_bracket_template": nullable(post_fill_bracket_template_schema()),
            "reason": {"type": "string"}
        }
    })
}

fn pending_order_management_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "path_id",
            "exposure_state",
            "path_live_assessment",
            "path_assessment_reason",
            "actions",
            "management_note"
        ],
        "properties": {
            "path_id": {"type": "string"},
            "exposure_state": {
                "type": "string",
                "enum": ["flat_with_live_entry_orders", "in_position_with_live_entry_orders"]
            },
            "path_live_assessment": {"type": "string", "enum": ["live", "degraded", "invalidated"]},
            "path_assessment_reason": {"type": ["string", "null"]},
            "actions": {"type": "array", "items": pending_order_management_action_schema()},
            "management_note": {"type": "string"}
        }
    })
}

fn workflow_stage2c_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["stage2c_decision", "pending_order_management_plan"],
        "properties": {
            "stage2c_decision": {"type": "string", "enum": ["MANAGE_PENDING_ORDERS"]},
            "pending_order_management_plan": pending_order_management_plan_schema()
        }
    })
}

fn workflow_reasoning(model: &LlmModelConfig, stage: WorkflowPromptStage) -> Option<String> {
    model
        .reasoning_for_stage(matches!(stage, WorkflowPromptStage::Stage1))
        .map(ToOwned::to_owned)
}

fn default_workflow_model(config: &RootConfig) -> Option<LlmModelConfig> {
    let default_provider = config.active_default_model();
    let default_model = match default_provider.as_str() {
        "qwen" => LlmModelConfig {
            name: "qwen_default".to_string(),
            provider: "qwen".to_string(),
            model: config.api.qwen.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        },
        "custom_llm" => LlmModelConfig {
            name: "custom_llm_default".to_string(),
            provider: "custom_llm".to_string(),
            model: config.api.custom_llm.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        },
        "gemini" => LlmModelConfig {
            name: "gemini_default".to_string(),
            provider: "gemini".to_string(),
            model: config.api.gemini.model.clone(),
            use_openrouter: Some(true),
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        },
        "grok" => LlmModelConfig {
            name: "grok_default".to_string(),
            provider: "grok".to_string(),
            model: config.api.grok.model.clone(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        },
        _ => return None,
    };
    Some(default_model)
}

fn selected_models(config: &RootConfig) -> Vec<LlmModelConfig> {
    let enabled = config.selected_enabled_models_for_default();
    if enabled.is_empty() {
        default_workflow_model(config).into_iter().collect()
    } else {
        enabled
    }
}

const STAGE2_RETRY_DELAY: Duration = Duration::from_millis(250);

fn is_retryable_transient_provider_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("status=408")
        || normalized.contains("status=429")
        || normalized.contains("status=500")
        || normalized.contains("status=502")
        || normalized.contains("status=503")
        || normalized.contains("status=504")
        || normalized.contains("bad gateway")
}

fn should_retry_workflow_stage_once(
    stage: WorkflowPromptStage,
    model: &LlmModelConfig,
    error: Option<&str>,
) -> bool {
    let Some(error) = error else {
        return false;
    };
    if is_retryable_transient_provider_error(error) {
        return true;
    }
    if stage == WorkflowPromptStage::Stage1 || !model.provider.eq_ignore_ascii_case("custom_llm") {
        return false;
    }
    let normalized = error.to_ascii_lowercase();
    normalized.contains("invalid schema for response_format")
        || normalized.contains("call workflow custom_llm api")
        || normalized.contains("finish_reason=length")
        || normalized.contains("response truncated")
        || normalized.contains("eof while parsing")
}

async fn invoke_provider_stage_once(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    model: &LlmModelConfig,
    prompt_template: &str,
    symbol: &str,
    stage: WorkflowPromptStage,
    input: &Value,
) -> provider::ProviderInvocationOutput {
    let stage_name = workflow_stage_name(stage);
    let stage_schema = workflow_stage_schema(stage);
    let request = provider::ProviderInvocationRequest {
        model,
        prompt_template,
        symbol,
        stage,
        input,
        schema_name: stage_name,
        schema: stage_schema,
        reasoning: workflow_reasoning(model, stage),
        enable_thinking: None,
    };

    if model.provider.eq_ignore_ascii_case("claude") {
        provider::invoke_claude_json_stage(http_client, config, request).await
    } else if model.provider.eq_ignore_ascii_case("custom_llm") {
        provider::invoke_openai_compatible_json_stage(
            http_client,
            loopback_http_client,
            &config.api.custom_llm.base_api_url,
            config.api.custom_llm.resolved_api_key(),
            request,
        )
        .await
    } else if model.provider.eq_ignore_ascii_case("qwen") {
        provider::invoke_openai_compatible_json_stage(
            http_client,
            loopback_http_client,
            &config.api.qwen.base_api_url,
            config.api.qwen.resolved_api_key(),
            provider::ProviderInvocationRequest {
                enable_thinking: Some(false),
                reasoning: None,
                ..request
            },
        )
        .await
    } else if model.provider.eq_ignore_ascii_case("gemini") && model.should_use_openrouter() {
        let mut routed_model = model.clone();
        routed_model.model = provider::openrouter_gemini_model_name(&model.model);
        provider::invoke_openai_compatible_json_stage(
            http_client,
            loopback_http_client,
            &config.api.openrouter.base_api_url,
            config.api.openrouter.resolved_api_key(),
            provider::ProviderInvocationRequest {
                model: &routed_model,
                reasoning: None,
                ..request
            },
        )
        .await
    } else if model.provider.eq_ignore_ascii_case("gemini") {
        provider::invoke_gemini_json_stage(http_client, config, request).await
    } else if model.provider.eq_ignore_ascii_case("grok") {
        provider::invoke_grok_json_stage(http_client, config, request).await
    } else {
        provider::ProviderInvocationOutput {
            latency_ms: 0,
            raw_response_text: None,
            parsed_value: None,
            error: Some(format!(
                "workflow provider for {} is not implemented",
                model.provider
            )),
        }
    }
}

async fn invoke_one_model_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    model: &LlmModelConfig,
    prompt_template: &str,
    symbol: &str,
    stage: WorkflowPromptStage,
    input: &Value,
) -> WorkflowModelOutput {
    let mut provider_output = invoke_provider_stage_once(
        http_client,
        loopback_http_client,
        config,
        model,
        prompt_template,
        symbol,
        stage,
        input,
    )
    .await;

    if should_retry_workflow_stage_once(stage, model, provider_output.error.as_deref()) {
        let first_attempt_latency = provider_output.latency_ms;
        warn!(
            symbol = %symbol,
            provider = %model.provider,
            model = %model.model,
            stage = workflow_stage_name(stage),
            error = provider_output.error.as_deref().unwrap_or("-"),
            "workflow stage invocation failed with retryable provider error; retrying once"
        );
        sleep(STAGE2_RETRY_DELAY).await;
        provider_output = invoke_provider_stage_once(
            http_client,
            loopback_http_client,
            config,
            model,
            prompt_template,
            symbol,
            stage,
            input,
        )
        .await;
        provider_output.latency_ms += first_attempt_latency + STAGE2_RETRY_DELAY.as_millis();
    }

    WorkflowModelOutput {
        model_name: model.name.clone(),
        provider: model.provider.clone(),
        model: model.model.clone(),
        latency_ms: provider_output.latency_ms,
        raw_response_text: provider_output.raw_response_text,
        parsed_value: provider_output.parsed_value,
        error: provider_output.error,
    }
}

async fn invoke_models_for_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
    stage: WorkflowPromptStage,
) -> Vec<WorkflowModelOutput> {
    let models = selected_models(config);
    if models.is_empty() {
        return vec![WorkflowModelOutput {
            model_name: workflow_stage_name(stage).to_string(),
            provider: config.active_default_model(),
            model: String::new(),
            latency_ms: 0,
            raw_response_text: None,
            parsed_value: None,
            error: Some(format!(
                "workflow provider requires an enabled model or a configured default provider, got {}",
                config.active_default_model()
            )),
        }];
    }
    let mut outputs = Vec::with_capacity(models.len());
    for model in models {
        outputs.push(
            invoke_one_model_stage(
                http_client,
                loopback_http_client,
                config,
                &model,
                &config.llm.prompt_template,
                symbol,
                stage,
                input,
            )
            .await,
        );
    }
    outputs
}

pub async fn invoke_stage1_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    invoke_models_for_stage(
        http_client,
        loopback_http_client,
        config,
        input,
        symbol,
        WorkflowPromptStage::Stage1,
    )
    .await
}

pub async fn invoke_stage2a_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    invoke_models_for_stage(
        http_client,
        loopback_http_client,
        config,
        input,
        symbol,
        WorkflowPromptStage::Stage2A,
    )
    .await
}

pub async fn invoke_stage2b_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    invoke_models_for_stage(
        http_client,
        loopback_http_client,
        config,
        input,
        symbol,
        WorkflowPromptStage::Stage2B,
    )
    .await
}

pub async fn invoke_stage2c_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    invoke_models_for_stage(
        http_client,
        loopback_http_client,
        config,
        input,
        symbol,
        WorkflowPromptStage::Stage2C,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        should_retry_workflow_stage_once, workflow_stage1_schema, workflow_stage2a_schema,
        workflow_stage2b_schema, workflow_stage2c_schema,
    };
    use crate::app::config::LlmModelConfig;
    use crate::llm::prompt::WorkflowPromptStage;
    use serde_json::{json, Value};

    fn assert_closed_object_schemas(schema: &Value) {
        match schema {
            Value::Object(map) => {
                if matches!(map.get("type"), Some(Value::String(kind)) if kind == "object") {
                    assert_eq!(
                        map.get("additionalProperties"),
                        Some(&json!(false)),
                        "object schema missing additionalProperties=false: {schema}"
                    );
                    let property_names = map
                        .get("properties")
                        .and_then(Value::as_object)
                        .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let required = map
                        .get("required")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(|value| value.as_str().map(|text| text.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    assert_eq!(
                        property_names.len(),
                        required.len(),
                        "object schema required/property length mismatch: {schema}"
                    );
                    for property_name in &property_names {
                        assert!(
                            required
                                .iter()
                                .any(|required_name| required_name == property_name),
                            "object schema missing required property '{property_name}': {schema}"
                        );
                    }
                }
                for value in map.values() {
                    assert_closed_object_schemas(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    assert_closed_object_schemas(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn stage1_schema_requires_location_3d() {
        let schema = workflow_stage1_schema();
        let required = schema["properties"]["map_summary"]["required"]
            .as_array()
            .expect("required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"location_3d"));
        assert!(required.contains(&"location_1d"));
        assert!(required.contains(&"location_4h"));
        assert_eq!(
            schema["properties"]["map_summary"]["properties"]["location_3d"]
                ["additionalProperties"],
            json!(false)
        );
        let location_required = schema["properties"]["map_summary"]["properties"]["location_3d"]
            ["required"]
            .as_array()
            .expect("location required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(location_required.contains(&"summary"));
        assert!(location_required.contains(&"notes"));
        assert_eq!(
            schema["properties"]["map_summary"]["properties"]["key_levels"]["additionalProperties"],
            json!(false)
        );
        let current_path_required = schema["properties"]["current_path"]["anyOf"][0]["required"]
            .as_array()
            .expect("current_path required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(current_path_required.contains(&"strategic_activation_level"));
        assert!(!current_path_required.contains(&"activation_level"));
        assert_eq!(
            schema["properties"]["no_trade_reason"],
            json!({"type": ["string", "null"]})
        );
    }

    #[test]
    fn stage2b_schema_requires_conditional_actions() {
        let schema = workflow_stage2b_schema();
        let action = schema["properties"]["position_management_plan"]["properties"]["actions"]
            ["items"]
            .clone();
        let required = action["required"]
            .as_array()
            .expect("required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"trigger_condition"));
        assert!(required.contains(&"execution_price"));
        assert!(required.contains(&"reuse_current_bracket_template"));
        let action_enum = action["properties"]["action_type"]["enum"]
            .as_array()
            .expect("action enum");
        let variants = action_enum
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(!variants.contains(&"hold"));
    }

    #[test]
    fn stage2a_schema_closes_all_object_nodes() {
        assert_closed_object_schemas(&workflow_stage2a_schema());
    }

    #[test]
    fn stage2a_schema_requires_nullable_entry_activation_level_key() {
        let schema = workflow_stage2a_schema();
        let required = schema["properties"]["tactical_entry_plan"]["anyOf"][0]["properties"]
            ["entry_plan"]["required"]
            .as_array()
            .expect("entry_plan required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"entry_activation_level"));
        assert_eq!(
            schema["properties"]["tactical_entry_plan"]["anyOf"][0]["properties"]["entry_plan"]
                ["properties"]["entry_activation_level"],
            json!({
                "anyOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["low", "high", "timeframe", "label", "reason"],
                        "properties": {
                            "low": {"type": "number"},
                            "high": {"type": "number"},
                            "timeframe": {"type": ["string", "null"]},
                            "label": {"type": ["string", "null"]},
                            "reason": {"type": ["string", "null"]}
                        }
                    },
                    {"type": "null"}
                ]
            })
        );
    }

    #[test]
    fn stage2a_schema_does_not_expose_max_drift_pct() {
        let schema = workflow_stage2a_schema();
        let entry_plan =
            &schema["properties"]["tactical_entry_plan"]["anyOf"][0]["properties"]["entry_plan"];
        assert!(entry_plan["properties"].get("max_drift_pct").is_none());
        let required = entry_plan["required"]
            .as_array()
            .expect("entry_plan required array");
        assert!(!required
            .iter()
            .any(|value| value.as_str() == Some("max_drift_pct")));
    }

    #[test]
    fn stage2a_schema_requires_confidence_leverage() {
        let schema = workflow_stage2a_schema();
        let entry_plan =
            &schema["properties"]["tactical_entry_plan"]["anyOf"][0]["properties"]["entry_plan"];
        assert_eq!(entry_plan["properties"]["leverage"]["type"], "integer");
        assert_eq!(entry_plan["properties"]["leverage"]["minimum"], 1);
        assert_eq!(entry_plan["properties"]["leverage"]["maximum"], 20);
        let required = entry_plan["required"]
            .as_array()
            .expect("entry_plan required array");
        assert!(required
            .iter()
            .any(|value| value.as_str() == Some("leverage")));
    }

    #[test]
    fn stage2b_schema_closes_all_object_nodes() {
        assert_closed_object_schemas(&workflow_stage2b_schema());
    }

    #[test]
    fn stage2c_schema_closes_all_object_nodes_and_supports_coexisting_exposure() {
        let schema = workflow_stage2c_schema();
        assert_closed_object_schemas(&schema);
        let action = schema["properties"]["pending_order_management_plan"]["properties"]["actions"]
            ["items"]
            .clone();
        let required = action["required"]
            .as_array()
            .expect("required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"trigger_condition"));
        assert!(required.contains(&"execution_price"));
        let action_enum = action["properties"]["action_type"]["enum"]
            .as_array()
            .expect("action enum");
        let variants = action_enum
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(!variants.contains(&"keep_order"));
        let enum_values = schema["properties"]["pending_order_management_plan"]["properties"]
            ["exposure_state"]["enum"]
            .as_array()
            .expect("exposure_state enum");
        let variants = enum_values
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(variants.contains(&"flat_with_live_entry_orders"));
        assert!(variants.contains(&"in_position_with_live_entry_orders"));
    }

    #[test]
    fn retry_policy_retries_transient_http_errors_once_even_for_stage1() {
        let custom_llm = LlmModelConfig {
            name: "custom_llm_default".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        };
        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage1,
            &custom_llm,
            Some("workflow custom_llm status=502 Bad Gateway body={\"error\":{\"message\":\"server_error\"}}"),
        ));
    }

    #[test]
    fn retry_policy_keeps_non_stage1_custom_llm_schema_retry() {
        let custom_llm = LlmModelConfig {
            name: "custom_llm_default".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1200,
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: None,
        };
        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2A,
            &custom_llm,
            Some("workflow custom_llm status=400 body={\"error\":{\"message\":\"Invalid schema for response_format 'workflow_stage2a'\"}}"),
        ));
        assert!(!should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage1,
            &custom_llm,
            Some("workflow custom_llm status=400 body={\"error\":{\"message\":\"Invalid schema for response_format 'workflow_stage1'\"}}"),
        ));
        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2A,
            &custom_llm,
            Some("workflow openai-compatible response truncated finish_reason=length"),
        ));
        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2A,
            &custom_llm,
            Some("parse JSON from model text failed: EOF while parsing a string at line 1 column 2121"),
        ));
    }
}
