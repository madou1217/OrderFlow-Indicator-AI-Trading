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
        WorkflowPromptStage::Stage2 => "workflow_stage2",
    }
}

fn workflow_stage_schema(stage: WorkflowPromptStage) -> Value {
    match stage {
        WorkflowPromptStage::Stage1 => workflow_stage1_schema(),
        WorkflowPromptStage::Stage2 => workflow_stage2_schema(),
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

fn any_object_schema() -> Value {
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
            "levels": {"type": "array", "items": tracked_zone_schema()}
        }
    })
}

fn opportunity_quality_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["high", "medium", "low"]
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

fn script_rejection_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["script", "reason"],
        "properties": {
            "script": {
                "type": "string",
                "enum": ["continuation", "crowded_reversal", "value_return"]
            },
            "reason": {"type": "string"}
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
                "type": "string",
                "enum": [
                    "accepted_into_zone",
                    "accepted_beyond_zone",
                    "rejected_from_zone",
                    "reaccepted_through_zone"
                ]
            },
            "zone_id": {"type": "string"},
            "timeframe": {"type": "string", "enum": ["15m", "4h", "1d"]},
            "min_confirmed_bars": {"type": "integer", "minimum": 1},
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
                "type": "string",
                "enum": [
                    "driver_flip",
                    "spot_confirmation_lost",
                    "oi_support_lost",
                    "state_regime_conflict"
                ]
            },
            "expected_flow_driver": {
                "type": "string",
                "enum": ["spot_led", "futures_led", "mixed"]
            },
            "invalidate_when_drivers": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["spot_led", "futures_led", "mixed"]
                }
            },
            "require_spot_confirmation": {"type": "boolean"},
            "driver_signal": {
                "type": "string",
                "enum": [
                    "spot_confirmation_lost",
                    "oi_support_lost",
                    "fake_order_risk_rising",
                    "driver_flip_confirmed"
                ]
            },
            "min_confirmed_windows": {"type": "integer", "minimum": 1},
            "summary": {"type": "string"},
            "evidence": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn price_zone_schema() -> Value {
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

fn stop_migration_rule_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["after_target", "new_stop_basis", "new_stop_level"],
        "properties": {
            "after_target": {
                "type": "string",
                "enum": ["take_profit_1", "take_profit_2"]
            },
            "new_stop_basis": {
                "type": "string",
                "enum": ["activation_level", "first_path_target", "next_path_target"]
            },
            "new_stop_level": {"type": "number"}
        }
    })
}

fn driver_deterioration_rule_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["driver_signal", "reduce_ratio"],
        "properties": {
            "driver_signal": {
                "type": "string",
                "enum": [
                    "spot_confirmation_lost",
                    "oi_support_lost",
                    "fake_order_risk_rising",
                    "driver_flip_confirmed"
                ]
            },
            "reduce_ratio": {"type": ["number", "null"]}
        }
    })
}

fn management_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "take_profit_1_basis",
            "take_profit_2_basis",
            "take_profit_1_level",
            "take_profit_2_level",
            "stop_migration_rules",
            "reduce_on_driver_deterioration",
            "exit_full_on_driver_deterioration"
        ],
        "properties": {
            "take_profit_1_basis": {"type": "string", "enum": ["first_path_target"]},
            "take_profit_2_basis": {"type": "string", "enum": ["next_path_target"]},
            "take_profit_1_level": {"type": "number"},
            "take_profit_2_level": {"type": "number"},
            "stop_migration_rules": {"type": "array", "items": stop_migration_rule_schema()},
            "reduce_on_driver_deterioration": {"type": "array", "items": driver_deterioration_rule_schema()},
            "exit_full_on_driver_deterioration": {"type": "array", "items": driver_deterioration_rule_schema()}
        }
    })
}

fn reevaluation_trigger_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "extreme_location",
            "reverse_confirmation",
            "driver_change"
        ],
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
            "regime_3d",
            "location_1d",
            "location_4h",
            "price_location_class",
            "key_levels"
        ],
        "properties": {
            "regime_3d": any_object_schema(),
            "location_1d": any_object_schema(),
            "location_4h": any_object_schema(),
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
            "activation_level",
            "first_path_target_anchor_id",
            "first_path_target",
            "next_path_target_anchor_id",
            "next_path_target",
            "failure_anchor_id",
            "failure_level",
            "failure_switch",
            "setup_type",
            "reevaluation_trigger",
            "management_plan",
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
            "activation_anchor_id": {"type": "string"},
            "activation_level": price_zone_schema(),
            "first_path_target_anchor_id": {"type": "string"},
            "first_path_target": price_zone_schema(),
            "next_path_target_anchor_id": {"type": "string"},
            "next_path_target": price_zone_schema(),
            "failure_anchor_id": {"type": "string"},
            "failure_level": price_zone_schema(),
            "failure_switch": {"type": ["string", "null"]},
            "setup_type": {
                "type": "string",
                "enum": ["A_continuation", "B_reversal", "C_value_return"]
            },
            "reevaluation_trigger": reevaluation_trigger_schema(),
            "management_plan": management_plan_schema(),
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
            "script_rejections",
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
            "no_trade_reason": {
                "type": ["string", "null"],
                "enum": [
                    "conflict_no_edge",
                    "script_not_unique",
                    "path_not_actionable",
                    null
                ]
            },
            "refresh_hints": {"type": "array", "items": {"type": "string"}},
            "map_summary": map_summary_schema(),
            "opportunity_assessment": opportunity_assessment_schema(),
            "script_rejections": {"type": "array", "items": script_rejection_schema()},
            "current_script": {
                "type": ["string", "null"],
                "enum": ["continuation", "crowded_reversal", "value_return", null]
            },
            "driver_attribution": nullable(driver_attribution_schema()),
            "current_path": nullable(current_path_schema())
        }
    })
}

fn tactical_entry_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["context_key", "path_id", "plan_role"],
        "properties": {
            "context_key": {"type": "string"},
            "path_id": {"type": "string"},
            "plan_role": {"type": "string", "enum": ["primary", "secondary"]}
        }
    })
}

fn entry_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "side",
            "entry_profile",
            "intent_mode",
            "entry_activation_level",
            "entry_zone",
            "entry_invalidation_level",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "ttl_minutes",
            "max_drift_pct",
            "entry_snapshot",
            "entry_note"
        ],
        "properties": {
            "side": {"type": "string", "enum": ["LONG", "SHORT"]},
            "entry_profile": {
                "type": "string",
                "enum": [
                    "reclaim_then_hold",
                    "pullback_acceptance",
                    "failed_auction_reentry"
                ]
            },
            "intent_mode": {"type": "string", "enum": ["immediate", "pullback", "breakout"]},
            "entry_activation_level": price_zone_schema(),
            "entry_zone": price_zone_schema(),
            "entry_invalidation_level": price_zone_schema(),
            "stop_loss": {"type": "number"},
            "take_profit_1": {"type": "number"},
            "take_profit_2": {"type": "number"},
            "ttl_minutes": {"type": "integer", "minimum": 1},
            "max_drift_pct": {"type": "number", "minimum": 0},
            "entry_snapshot": tactical_entry_snapshot_schema(),
            "entry_note": {"type": "string"}
        }
    })
}

fn attempt_policy_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "max_filled_stopout_attempts",
            "count_unfilled_attempts",
            "time_window"
        ],
        "properties": {
            "max_filled_stopout_attempts": {"type": "integer", "enum": [2]},
            "count_unfilled_attempts": {"type": "boolean", "enum": [false]},
            "time_window": {"type": "string", "enum": ["same_15m_window"]}
        }
    })
}

fn tactical_entry_plan_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "path_id",
            "primary_entry_plan",
            "secondary_entry_plan",
            "attempt_policy"
        ],
        "properties": {
            "path_id": {"type": "string"},
            "primary_entry_plan": entry_plan_schema(),
            "secondary_entry_plan": entry_plan_schema(),
            "attempt_policy": attempt_policy_schema()
        }
    })
}

fn workflow_stage2_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "stage2_decision",
            "tactical_entry_plan",
            "reevaluation_reason"
        ],
        "properties": {
            "stage2_decision": {
                "type": "string",
                "enum": ["PATH_CONFIRMED", "REQUEST_STAGE1_REEVALUATION"]
            },
            "tactical_entry_plan": nullable(tactical_entry_plan_schema()),
            "reevaluation_reason": {"type": ["string", "null"]}
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

fn should_retry_workflow_stage_once(
    stage: WorkflowPromptStage,
    model: &LlmModelConfig,
    error: Option<&str>,
) -> bool {
    if stage != WorkflowPromptStage::Stage2 || !model.provider.eq_ignore_ascii_case("custom_llm") {
        return false;
    }
    let Some(error) = error else {
        return false;
    };
    let normalized = error.to_ascii_lowercase();
    normalized.contains("invalid schema for response_format")
        || normalized.contains("call workflow custom_llm api")
        || normalized.contains("status=408")
        || normalized.contains("status=429")
        || normalized.contains("status=500")
        || normalized.contains("status=502")
        || normalized.contains("status=503")
        || normalized.contains("status=504")
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

#[cfg(test)]
mod tests {
    use super::{should_retry_workflow_stage_once, workflow_stage1_schema, workflow_stage2_schema};
    use crate::app::config::LlmModelConfig;
    use crate::llm::prompt::WorkflowPromptStage;

    #[test]
    fn stage1_schema_keeps_current_path_strict() {
        let schema = workflow_stage1_schema();
        let current_path = schema["properties"]["current_path"]["anyOf"][0].clone();
        assert_eq!(current_path["type"], "object");
        assert_eq!(current_path["additionalProperties"], false);
        assert!(current_path["required"]
            .as_array()
            .expect("required array")
            .iter()
            .any(|item| item == "risk_grade"));
        assert!(current_path["required"]
            .as_array()
            .expect("required array")
            .iter()
            .any(|item| item == "activation_anchor_id"));
        assert!(current_path["required"]
            .as_array()
            .expect("required array")
            .iter()
            .any(|item| item == "failure_anchor_id"));
    }

    #[test]
    fn stage2_schema_keeps_tactical_plan_strict() {
        let schema = workflow_stage2_schema();
        assert_eq!(schema["type"], "object");
        let tactical_plan = schema["properties"]["tactical_entry_plan"]["anyOf"][0].clone();
        assert_eq!(tactical_plan["type"], "object");
        assert_eq!(tactical_plan["additionalProperties"], false);
        let primary = tactical_plan["properties"]["primary_entry_plan"].clone();
        assert_eq!(primary["type"], "object");
        assert_eq!(primary["additionalProperties"], false);
    }

    #[test]
    fn stage1_schema_requires_new_map_summary_shape() {
        let schema = workflow_stage1_schema();
        let map_summary = schema["properties"]["map_summary"].clone();
        let required = map_summary["required"]
            .as_array()
            .expect("required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"regime_3d"));
        assert!(required.contains(&"location_1d"));
        assert!(required.contains(&"location_4h"));
        assert!(required.contains(&"price_location_class"));
        let stage1_required = schema["required"]
            .as_array()
            .expect("stage1 required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(stage1_required.contains(&"opportunity_assessment"));
        assert!(stage1_required.contains(&"script_rejections"));
    }

    #[test]
    fn stage1_schema_structures_reevaluation_triggers() {
        let schema = workflow_stage1_schema();
        let current_path = schema["properties"]["current_path"]["anyOf"][0].clone();
        let reevaluation = current_path["properties"]["reevaluation_trigger"].clone();
        let extreme_required = reevaluation["properties"]["extreme_location"]["required"]
            .as_array()
            .expect("extreme required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(extreme_required.contains(&"kind"));
        assert!(extreme_required.contains(&"zone_id"));
        assert!(extreme_required.contains(&"min_confirmed_bars"));

        let driver_required = reevaluation["properties"]["driver_change"]["required"]
            .as_array()
            .expect("driver required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(driver_required.contains(&"expected_flow_driver"));
        assert!(driver_required.contains(&"invalidate_when_drivers"));
        assert!(driver_required.contains(&"min_confirmed_windows"));
    }

    #[test]
    fn stage2_schema_requires_new_decision_fields() {
        let schema = workflow_stage2_schema();
        let required = schema["required"]
            .as_array()
            .expect("required array")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        assert!(required.contains(&"stage2_decision"));
        assert!(required.contains(&"tactical_entry_plan"));
        assert!(required.contains(&"reevaluation_reason"));
    }

    #[test]
    fn stage1_schema_constrains_management_plan_contract_enums() {
        let schema = workflow_stage1_schema();
        let current_path = schema["properties"]["current_path"]["anyOf"][0].clone();
        let stop_rule = current_path["properties"]["management_plan"]["properties"]
            ["stop_migration_rules"]["items"]
            .clone();
        let after_target = stop_rule["properties"]["after_target"]["enum"]
            .as_array()
            .expect("after_target enum");
        assert!(after_target.iter().any(|value| value == "take_profit_1"));
        assert!(after_target.iter().any(|value| value == "take_profit_2"));

        let driver_rule = current_path["properties"]["management_plan"]["properties"]
            ["reduce_on_driver_deterioration"]["items"]
            .clone();
        let driver_signal = driver_rule["properties"]["driver_signal"]["enum"]
            .as_array()
            .expect("driver_signal enum");
        assert!(driver_signal
            .iter()
            .any(|value| value == "spot_confirmation_lost"));
        assert!(driver_signal
            .iter()
            .any(|value| value == "driver_flip_confirmed"));
    }

    #[test]
    fn stage2_schema_constrains_attempt_policy_and_entry_profile() {
        let schema = workflow_stage2_schema();
        let tactical_plan = schema["properties"]["tactical_entry_plan"]["anyOf"][0].clone();
        let attempt_policy = tactical_plan["properties"]["attempt_policy"]["properties"].clone();
        assert_eq!(attempt_policy["max_filled_stopout_attempts"]["enum"][0], 2);
        assert_eq!(attempt_policy["count_unfilled_attempts"]["enum"][0], false);
        let entry_profile = tactical_plan["properties"]["primary_entry_plan"]["properties"]
            ["entry_profile"]["enum"]
            .as_array()
            .expect("entry_profile enum");
        assert!(entry_profile
            .iter()
            .any(|value| value == "reclaim_then_hold"));
        assert!(entry_profile
            .iter()
            .any(|value| value == "failed_auction_reentry"));
    }

    #[test]
    fn retry_policy_only_retries_stage2_custom_llm_errors_once() {
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
        let qwen = LlmModelConfig {
            provider: "qwen".to_string(),
            ..custom_llm.clone()
        };

        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2,
            &custom_llm,
            Some("workflow custom_llm status=400 body={\"error\":{\"message\":\"Invalid schema for response_format 'workflow_stage2'\"}}"),
        ));
        assert!(should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2,
            &custom_llm,
            Some("call workflow custom_llm api: connection reset by peer"),
        ));
        assert!(!should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage1,
            &custom_llm,
            Some("workflow custom_llm status=400 body={\"error\":{\"message\":\"Invalid schema for response_format 'workflow_stage2'\"}}"),
        ));
        assert!(!should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2,
            &qwen,
            Some("workflow qwen status=400 body={\"error\":{\"message\":\"Invalid schema for response_format 'workflow_stage2'\"}}"),
        ));
        assert!(!should_retry_workflow_stage_once(
            WorkflowPromptStage::Stage2,
            &custom_llm,
            Some("workflow custom_llm status=401 body=unauthorized"),
        ));
    }
}

pub async fn invoke_stage1_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    let models = selected_models(config);
    if models.is_empty() {
        return vec![WorkflowModelOutput {
            model_name: "workflow_stage1".to_string(),
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
                WorkflowPromptStage::Stage1,
                input,
            )
            .await,
        );
    }
    outputs
}

pub async fn invoke_stage2_models(
    http_client: &Client,
    loopback_http_client: &Client,
    config: &RootConfig,
    input: &Value,
    symbol: &str,
) -> Vec<WorkflowModelOutput> {
    let models = selected_models(config);
    if models.is_empty() {
        return vec![WorkflowModelOutput {
            model_name: "workflow_stage2".to_string(),
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
                WorkflowPromptStage::Stage2,
                input,
            )
            .await,
        );
    }
    outputs
}
