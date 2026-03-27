use crate::app::config::{LlmModelConfig, RootConfig};
use crate::llm::prompt::WorkflowPromptStage;
use crate::llm::provider;
use reqwest::Client;
use serde_json::{json, Value};

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

fn price_zone_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["low", "high"],
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
        "required": ["zone_id", "timeframe", "role", "low", "high"],
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
        "required": ["signals"],
        "properties": {
            "signals": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["extreme_location", "reverse_confirmation", "driver_change"]
                }
            }
        }
    })
}

fn stop_migration_rule_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["after_target", "new_stop_basis", "new_stop_level"],
        "properties": {
            "after_target": {"type": "string"},
            "new_stop_basis": {"type": "string"},
            "new_stop_level": {"type": "number"}
        }
    })
}

fn driver_deterioration_rule_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["driver_signal"],
        "properties": {
            "driver_signal": {"type": "string"},
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

fn map_summary_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["market_tradeable", "notes"],
        "properties": {
            "market_tradeable": {"type": "boolean"},
            "location_bias": {"type": ["string", "null"]},
            "state_summary": {"type": ["string", "null"]},
            "driver_summary": {"type": ["string", "null"]},
            "notes": {"type": "array", "items": {"type": "string"}}
        }
    })
}

fn driver_attribution_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["driver_bias", "supporting_evidence", "conflicting_evidence"],
        "properties": {
            "driver_bias": {"type": "string"},
            "primary_driver": {"type": ["string", "null"]},
            "supporting_evidence": {"type": "array", "items": {"type": "string"}},
            "conflicting_evidence": {"type": "array", "items": {"type": "string"}}
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
            "activation_level",
            "first_path_target",
            "next_path_target",
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
            "activation_level": price_zone_schema(),
            "first_path_target": price_zone_schema(),
            "next_path_target": price_zone_schema(),
            "failure_level": price_zone_schema(),
            "failure_switch": {"type": "string"},
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

fn hard_gate_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["location_valid", "trigger_confirmed"],
        "properties": {
            "location_valid": {"type": "boolean"},
            "trigger_confirmed": {"type": "boolean"}
        }
    })
}

fn soft_gate_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "state_clear",
            "driver_clear",
            "orderflow_real",
            "invalidation_clear",
            "passed_count"
        ],
        "properties": {
            "state_clear": {"type": "boolean"},
            "driver_clear": {"type": "boolean"},
            "orderflow_real": {"type": "boolean"},
            "invalidation_clear": {"type": "boolean"},
            "passed_count": {"type": "integer", "minimum": 0, "maximum": 4}
        }
    })
}

fn request_stage1_reevaluation_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["refresh_reason", "trigger_source"],
        "properties": {
            "refresh_reason": {"type": "string", "enum": ["thesis_invalidated", "no_edge_reentered"]},
            "trigger_source": {"type": "string"}
        }
    })
}

fn entry_snapshot_ref_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["context_key", "path_id"],
        "properties": {
            "context_key": {"type": "string"},
            "path_id": {"type": "string"}
        }
    })
}

fn execution_intent_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "side",
            "intent_mode",
            "entry_zone",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "ttl_minutes",
            "max_drift_pct",
            "path_id",
            "entry_snapshot"
        ],
        "properties": {
            "side": {"type": "string", "enum": ["LONG", "SHORT"]},
            "intent_mode": {"type": "string", "enum": ["immediate", "pullback", "breakout"]},
            "entry_zone": price_zone_schema(),
            "trigger_price": {"type": ["number", "null"]},
            "stop_loss": {"type": "number"},
            "take_profit_1": {"type": "number"},
            "take_profit_2": {"type": "number"},
            "ttl_minutes": {"type": "integer", "minimum": 1},
            "max_drift_pct": {"type": "number", "minimum": 0},
            "path_id": {"type": "string"},
            "entry_snapshot": entry_snapshot_ref_schema(),
            "reason": {"type": ["string", "null"]}
        }
    })
}

fn management_action_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["type", "context_key", "path_id"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["HOLD", "REDUCE_POSITION", "FLATTEN_POSITION", "MOVE_STOP", "UPDATE_TAKE_PROFIT"]
            },
            "context_key": {"type": "string"},
            "path_id": {"type": "string"},
            "reduce_ratio": {"type": ["number", "null"]},
            "new_stop_loss": {"type": ["number", "null"]},
            "take_profit_1": {"type": ["number", "null"]},
            "take_profit_2": {"type": ["number", "null"]},
            "reason": {"type": ["string", "null"]}
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
            "refresh_hints",
            "no_trade_reason",
            "map_summary",
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
            "map_summary": nullable(map_summary_schema()),
            "current_script": {"type": ["string", "null"]},
            "driver_attribution": nullable(driver_attribution_schema()),
            "current_path": nullable(current_path_schema())
        }
    })
}

fn workflow_stage2_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "decision",
            "reason",
            "request_stage1_reevaluation",
            "execution_intent",
            "management_actions",
            "hard_gate",
            "soft_gate"
        ],
        "properties": {
            "decision": {"type": "string", "enum": ["WAIT", "EXECUTE", "REQUEST_STAGE1_REEVALUATION"]},
            "reason": {"type": "string"},
            "request_stage1_reevaluation": nullable(request_stage1_reevaluation_schema()),
            "execution_intent": nullable(execution_intent_schema()),
            "management_actions": {"type": "array", "items": management_action_schema()},
            "hard_gate": hard_gate_schema(),
            "soft_gate": soft_gate_schema()
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

    let provider_output = if model.provider.eq_ignore_ascii_case("claude") {
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
    };

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
    use super::{workflow_stage1_schema, workflow_stage2_schema};

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
            .any(|item| item == "management_plan"));
    }

    #[test]
    fn stage2_schema_keeps_execution_and_management_strict() {
        let schema = workflow_stage2_schema();
        let execution_intent = schema["properties"]["execution_intent"]["anyOf"][0].clone();
        assert_eq!(execution_intent["type"], "object");
        assert_eq!(execution_intent["additionalProperties"], false);
        let management_action = schema["properties"]["management_actions"]["items"].clone();
        assert_eq!(management_action["type"], "object");
        assert_eq!(management_action["additionalProperties"], false);
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
