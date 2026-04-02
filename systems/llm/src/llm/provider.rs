use crate::app::config::{LlmModelConfig, RootConfig};
use crate::llm::prompt::{self, WorkflowPromptStage};
use crate::workflow::parser::parse_json_from_text;
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct ProviderInvocationOutput {
    pub latency_ms: u128,
    pub raw_response_text: Option<String>,
    pub parsed_value: Option<Value>,
    pub error: Option<String>,
}

pub(crate) struct ProviderInvocationRequest<'a> {
    pub model: &'a LlmModelConfig,
    pub prompt_template: &'a str,
    pub symbol: &'a str,
    pub stage: WorkflowPromptStage,
    pub input: &'a Value,
    pub schema_name: &'a str,
    pub schema: Value,
    pub reasoning: Option<String>,
    pub enable_thinking: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionsRequest {
    model: String,
    temperature: f64,
    max_tokens: u32,
    messages: Vec<ChatMessage>,
    response_format: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_thinking: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionsResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    #[serde(default)]
    content: Option<String>,
}

#[derive(Debug, Serialize)]
struct ClaudeMessageRequest {
    model: String,
    max_tokens: u32,
    temperature: f64,
    system: Vec<ClaudeTextBlock>,
    messages: Vec<ClaudeInputMessage>,
    tools: Vec<ClaudeTool>,
    tool_choice: ClaudeToolChoice,
}

#[derive(Debug, Serialize)]
struct ClaudeInputMessage {
    role: String,
    content: Vec<ClaudeTextBlock>,
}

#[derive(Debug, Serialize)]
struct ClaudeTextBlock {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

impl ClaudeTextBlock {
    fn text(text: String) -> Self {
        Self {
            kind: "text".to_string(),
            text,
        }
    }
}

#[derive(Debug, Serialize)]
struct ClaudeTool {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct ClaudeToolChoice {
    #[serde(rename = "type")]
    kind: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessageResponse {
    #[serde(default)]
    content: Vec<ClaudeContentBlock>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Debug, Serialize)]
struct GrokResponsesRequest {
    model: String,
    temperature: f64,
    max_output_tokens: u32,
    store: bool,
    input: Vec<GrokResponsesInputMessage>,
    text: GrokResponsesTextConfig,
}

#[derive(Debug, Serialize)]
struct GrokResponsesInputMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GrokResponsesApiResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    incomplete_details: Option<Value>,
    #[serde(default)]
    output: Vec<GrokResponseOutputItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GrokResponseOutputItem {
    #[serde(default)]
    pub content: Vec<GrokResponseContentItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GrokResponseContentItem {
    #[serde(default)]
    #[serde(rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Serialize)]
struct GrokResponsesTextConfig {
    format: GrokResponsesFormat,
}

#[derive(Debug, Serialize)]
struct GrokResponsesFormat {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    strict: bool,
    schema: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiInstruction>,
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
}

#[derive(Debug, Serialize)]
struct GeminiInstruction {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    temperature: f64,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_schema: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    content: Option<GeminiContentResponse>,
}

#[derive(Debug, Deserialize)]
struct GeminiContentResponse {
    #[serde(default)]
    parts: Vec<GeminiPartResponse>,
}

#[derive(Debug, Deserialize)]
struct GeminiPartResponse {
    text: Option<String>,
}

fn chat_completions_url(base_api_url: &str) -> String {
    format!("{}/chat/completions", base_api_url.trim_end_matches('/'))
}

fn json_schema_response_format(schema_name: &str, schema: Value) -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": schema_name,
            "strict": true,
            "schema": schema,
        }
    })
}

fn claude_tool(_schema_name: &str, schema: Value) -> ClaudeTool {
    ClaudeTool {
        name: "emit_workflow_json".to_string(),
        description: "Emit the final workflow JSON payload only.".to_string(),
        input_schema: schema,
    }
}

fn claude_tool_choice(tool: &ClaudeTool) -> ClaudeToolChoice {
    ClaudeToolChoice {
        kind: "tool".to_string(),
        name: tool.name.clone(),
    }
}

pub(crate) fn openrouter_gemini_model_name(model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return "google/gemini-3.1-pro-preview".to_string();
    }
    if let Some(stripped) = trimmed.strip_prefix("models/") {
        return format!("google/{}", stripped);
    }
    if trimmed.contains('/') {
        return trimmed.to_string();
    }
    format!("google/{}", trimmed)
}

fn gemini_model_path(model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.starts_with("models/") {
        trimmed.to_string()
    } else {
        format!("models/{}", trimmed)
    }
}

pub(crate) fn extract_grok_response_text(body: &GrokResponsesApiResponse) -> Option<String> {
    let text = body
        .output
        .iter()
        .flat_map(|item| item.content.iter())
        .filter(|content| content.kind.as_deref() == Some("output_text") || content.text.is_some())
        .filter_map(|content| content.text.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn prompt_text(
    prompt_template: &str,
    symbol: &str,
    stage: WorkflowPromptStage,
    input: &Value,
) -> Result<(String, String)> {
    let system = prompt::workflow_system_prompt(stage, prompt_template, symbol);
    let raw_input = serde_json::to_string(input).context("serialize workflow input")?;
    let user = format!(
        "{}{}",
        prompt::workflow_user_prompt_prefix(stage),
        raw_input
    );
    Ok((system, user))
}

pub(crate) async fn invoke_openai_compatible_json_stage(
    http_client: &Client,
    loopback_http_client: &Client,
    base_api_url: &str,
    api_key: String,
    request: ProviderInvocationRequest<'_>,
) -> ProviderInvocationOutput {
    let started = Instant::now();
    let (system, user) = match prompt_text(
        request.prompt_template,
        request.symbol,
        request.stage,
        request.input,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ProviderInvocationOutput {
                latency_ms: 0,
                raw_response_text: None,
                parsed_value: None,
                error: Some(format!("{error:#}")),
            };
        }
    };

    let payload = ChatCompletionsRequest {
        model: request.model.model.clone(),
        temperature: request.model.temperature,
        max_tokens: request.model.max_tokens,
        messages: vec![
            ChatMessage {
                role: "system".to_string(),
                content: system,
            },
            ChatMessage {
                role: "user".to_string(),
                content: user,
            },
        ],
        response_format: json_schema_response_format(request.schema_name, request.schema),
        reasoning: request.reasoning,
        enable_thinking: request.enable_thinking,
        stream: None,
    };

    let url = chat_completions_url(base_api_url);
    let client = if url.contains("127.0.0.1") || url.contains("localhost") {
        loopback_http_client
    } else {
        http_client
    };
    let response = client
        .post(&url)
        .bearer_auth(api_key)
        .header(
            "x-request-id",
            format!("workflow-{}-{}", request.symbol, Uuid::new_v4().simple()),
        )
        .json(&payload)
        .send()
        .await;

    match response {
        Ok(response) => {
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return ProviderInvocationOutput {
                    latency_ms: started.elapsed().as_millis(),
                    raw_response_text: None,
                    parsed_value: None,
                    error: Some(format!(
                        "workflow {} status={} body={}",
                        request.model.provider, status, body
                    )),
                };
            }

            let body = match response
                .json::<ChatCompletionsResponse>()
                .await
                .context("decode openai-compatible workflow response")
            {
                Ok(body) => body,
                Err(error) => {
                    return ProviderInvocationOutput {
                        latency_ms: started.elapsed().as_millis(),
                        raw_response_text: None,
                        parsed_value: None,
                        error: Some(format!("{error:#}")),
                    };
                }
            };

            let (text, finish_reason) = match extract_openai_compatible_text_and_finish_reason(body)
            {
                Ok(value) => value,
                Err(error) => {
                    return ProviderInvocationOutput {
                        latency_ms: started.elapsed().as_millis(),
                        raw_response_text: None,
                        parsed_value: None,
                        error: Some(format!("{error:#}")),
                    };
                }
            };

            if finish_reason
                .as_deref()
                .is_some_and(|reason| reason != "stop")
            {
                return ProviderInvocationOutput {
                    latency_ms: started.elapsed().as_millis(),
                    raw_response_text: Some(text),
                    parsed_value: None,
                    error: Some(format!(
                        "workflow openai-compatible response truncated finish_reason={}",
                        finish_reason.as_deref().unwrap_or("unknown")
                    )),
                };
            }

            match parse_json_from_text(&text) {
                Ok(parsed) => ProviderInvocationOutput {
                    latency_ms: started.elapsed().as_millis(),
                    raw_response_text: Some(text),
                    parsed_value: Some(parsed),
                    error: None,
                },
                Err(error) => ProviderInvocationOutput {
                    latency_ms: started.elapsed().as_millis(),
                    raw_response_text: Some(text),
                    parsed_value: None,
                    error: Some(format!("{error:#}")),
                },
            }
        }
        Err(error) => ProviderInvocationOutput {
            latency_ms: started.elapsed().as_millis(),
            raw_response_text: None,
            parsed_value: None,
            error: Some(format!(
                "{:#}",
                anyhow::Error::from(error)
                    .context(format!("call workflow {} api", request.model.provider))
            )),
        },
    }
}

fn extract_openai_compatible_text_and_finish_reason(
    body: ChatCompletionsResponse,
) -> Result<(String, Option<String>)> {
    let choice = body
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("workflow response choices are empty"))?;
    let text = choice
        .message
        .content
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    if text.is_empty() {
        return Err(anyhow!("workflow response text is empty"));
    }
    Ok((text, choice.finish_reason))
}

pub(crate) async fn invoke_claude_json_stage(
    http_client: &Client,
    config: &RootConfig,
    request: ProviderInvocationRequest<'_>,
) -> ProviderInvocationOutput {
    let started = Instant::now();
    let (system, user) = match prompt_text(
        request.prompt_template,
        request.symbol,
        request.stage,
        request.input,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ProviderInvocationOutput {
                latency_ms: 0,
                raw_response_text: None,
                parsed_value: None,
                error: Some(format!("{error:#}")),
            };
        }
    };
    let tool = claude_tool(request.schema_name, request.schema);
    let payload = ClaudeMessageRequest {
        model: request.model.model.clone(),
        max_tokens: request.model.max_tokens,
        temperature: request.model.temperature,
        system: vec![ClaudeTextBlock::text(system)],
        messages: vec![ClaudeInputMessage {
            role: "user".to_string(),
            content: vec![ClaudeTextBlock::text(user)],
        }],
        tools: vec![ClaudeTool {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: tool.input_schema.clone(),
        }],
        tool_choice: claude_tool_choice(&tool),
    };

    let response = http_client
        .post(&config.api.claude.api_url)
        .header("x-api-key", config.api.claude.resolved_api_key())
        .header("anthropic-version", &config.api.claude.api_version)
        .json(&payload)
        .send()
        .await;

    provider_result_from_response(started, "claude", response, |response| async move {
        let body = response
            .json::<ClaudeMessageResponse>()
            .await
            .context("decode workflow claude response")?;
        let tool_json = body
            .content
            .iter()
            .find(|block| {
                block.kind == "tool_use" && block.name.as_deref() == Some(tool.name.as_str())
            })
            .and_then(|block| block.input.clone())
            .or_else(|| {
                body.content
                    .iter()
                    .find(|block| block.kind == "tool_use")
                    .and_then(|block| block.input.clone())
            });
        if let Some(value) = tool_json {
            let text =
                serde_json::to_string(&value).context("serialize workflow claude tool input")?;
            return Ok((text, value));
        }
        let text = body
            .content
            .iter()
            .filter(|block| block.kind == "text")
            .filter_map(|block| block.text.as_deref())
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if text.is_empty() {
            return Err(anyhow!("workflow claude response text is empty"));
        }
        let parsed = parse_json_from_text(&text)?;
        Ok((text, parsed))
    })
    .await
}

pub(crate) async fn invoke_gemini_json_stage(
    http_client: &Client,
    config: &RootConfig,
    request: ProviderInvocationRequest<'_>,
) -> ProviderInvocationOutput {
    let started = Instant::now();
    let (system, user) = match prompt_text(
        request.prompt_template,
        request.symbol,
        request.stage,
        request.input,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ProviderInvocationOutput {
                latency_ms: 0,
                raw_response_text: None,
                parsed_value: None,
                error: Some(format!("{error:#}")),
            };
        }
    };

    let model_id = if request.model.model.trim().is_empty() {
        config.api.gemini.model.clone()
    } else {
        request.model.model.clone()
    };
    let endpoint = format!(
        "{}/{}:generateContent",
        config.api.gemini.base_api_url.trim_end_matches('/'),
        gemini_model_path(&model_id)
    );
    let mut url = match reqwest::Url::parse(&endpoint) {
        Ok(url) => url,
        Err(error) => {
            return ProviderInvocationOutput {
                latency_ms: 0,
                raw_response_text: None,
                parsed_value: None,
                error: Some(format!("parse gemini generateContent url failed: {error}")),
            };
        }
    };
    url.query_pairs_mut()
        .append_pair("key", &config.api.gemini.resolved_api_key());

    let response = http_client
        .post(url)
        .json(&GeminiGenerateContentRequest {
            system_instruction: Some(GeminiInstruction {
                parts: vec![GeminiPart { text: system }],
            }),
            contents: vec![GeminiContent {
                role: "user".to_string(),
                parts: vec![GeminiPart { text: user }],
            }],
            generation_config: Some(GeminiGenerationConfig {
                temperature: request.model.temperature,
                max_output_tokens: request.model.max_tokens,
                response_mime_type: Some("application/json".to_string()),
                response_schema: Some(request.schema),
            }),
        })
        .send()
        .await;

    provider_result_from_response(started, "gemini", response, |response| async move {
        let body = response
            .json::<GeminiGenerateContentResponse>()
            .await
            .context("decode workflow gemini response")?;
        let text = body
            .candidates
            .iter()
            .filter_map(|candidate| candidate.content.as_ref())
            .flat_map(|content| content.parts.iter())
            .filter_map(|part| part.text.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if text.is_empty() {
            return Err(anyhow!("workflow gemini response text is empty"));
        }
        let parsed = parse_json_from_text(&text)?;
        Ok((text, parsed))
    })
    .await
}

pub(crate) async fn invoke_grok_json_stage(
    http_client: &Client,
    config: &RootConfig,
    request: ProviderInvocationRequest<'_>,
) -> ProviderInvocationOutput {
    let started = Instant::now();
    let (system, user) = match prompt_text(
        request.prompt_template,
        request.symbol,
        request.stage,
        request.input,
    ) {
        Ok(value) => value,
        Err(error) => {
            return ProviderInvocationOutput {
                latency_ms: 0,
                raw_response_text: None,
                parsed_value: None,
                error: Some(format!("{error:#}")),
            };
        }
    };

    let response = http_client
        .post(format!(
            "{}/responses",
            config.api.grok.base_api_url.trim_end_matches('/')
        ))
        .bearer_auth(config.api.grok.resolved_api_key())
        .json(&GrokResponsesRequest {
            model: if request.model.model.trim().is_empty() {
                config.api.grok.model.clone()
            } else {
                request.model.model.clone()
            },
            temperature: request.model.temperature,
            max_output_tokens: request.model.max_tokens,
            store: false,
            input: vec![
                GrokResponsesInputMessage {
                    role: "system".to_string(),
                    content: system,
                },
                GrokResponsesInputMessage {
                    role: "user".to_string(),
                    content: user,
                },
            ],
            text: GrokResponsesTextConfig {
                format: GrokResponsesFormat {
                    kind: "json_schema".to_string(),
                    name: request.schema_name.to_string(),
                    strict: true,
                    schema: request.schema,
                },
            },
        })
        .send()
        .await;

    provider_result_from_response(started, "grok", response, |response| async move {
        let body = response
            .json::<GrokResponsesApiResponse>()
            .await
            .context("decode workflow grok response")?;
        let text = extract_grok_response_text(&body).ok_or_else(|| {
            anyhow!(
                "workflow grok response text is empty status={} incomplete_details={}",
                body.status.as_deref().unwrap_or("-"),
                body.incomplete_details
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_else(|| "null".to_string())
            )
        })?;
        let parsed = parse_json_from_text(&text)?;
        Ok((text, parsed))
    })
    .await
}

async fn provider_result_from_response<F, Fut>(
    started: Instant,
    provider: &str,
    response: Result<reqwest::Response, reqwest::Error>,
    on_success: F,
) -> ProviderInvocationOutput
where
    F: FnOnce(reqwest::Response) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<(String, Value)>>,
{
    let result = match response {
        Ok(response) => {
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                Err(anyhow!(
                    "workflow {} status={} body={}",
                    provider,
                    status,
                    body
                ))
            } else {
                on_success(response).await
            }
        }
        Err(error) => {
            Err(anyhow::Error::from(error).context(format!("call workflow {} api", provider)))
        }
    };

    match result {
        Ok((raw_response_text, parsed_value)) => ProviderInvocationOutput {
            latency_ms: started.elapsed().as_millis(),
            raw_response_text: Some(raw_response_text),
            parsed_value: Some(parsed_value),
            error: None,
        },
        Err(error) => ProviderInvocationOutput {
            latency_ms: started.elapsed().as_millis(),
            raw_response_text: None,
            parsed_value: None,
            error: Some(format!("{error:#}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extract_grok_response_text, extract_openai_compatible_text_and_finish_reason,
        openrouter_gemini_model_name, ChatChoice, ChatCompletionsResponse, ChatMessageResponse,
        GrokResponseContentItem, GrokResponseOutputItem, GrokResponsesApiResponse,
    };

    #[test]
    fn openrouter_gemini_model_name_normalizes_provider_prefix() {
        assert_eq!(
            openrouter_gemini_model_name("gemini-2.5-pro"),
            "google/gemini-2.5-pro"
        );
        assert_eq!(
            openrouter_gemini_model_name("models/gemini-2.5-pro"),
            "google/gemini-2.5-pro"
        );
        assert_eq!(
            openrouter_gemini_model_name("google/gemini-2.5-pro"),
            "google/gemini-2.5-pro"
        );
    }

    #[test]
    fn extract_grok_response_text_reads_output_text_items() {
        let body = GrokResponsesApiResponse {
            status: Some("completed".to_string()),
            incomplete_details: None,
            output: vec![GrokResponseOutputItem {
                content: vec![GrokResponseContentItem {
                    kind: Some("output_text".to_string()),
                    text: Some("{\"stage2_decision\":\"PATH_CONFIRMED\"}".to_string()),
                }],
            }],
        };
        assert_eq!(
            extract_grok_response_text(&body).as_deref(),
            Some("{\"stage2_decision\":\"PATH_CONFIRMED\"}")
        );
    }

    #[test]
    fn extract_openai_compatible_text_and_finish_reason_keeps_length_finish_reason() {
        let (text, finish_reason) =
            extract_openai_compatible_text_and_finish_reason(ChatCompletionsResponse {
                choices: vec![ChatChoice {
                    message: ChatMessageResponse {
                        content: Some("{\"foo\":\"bar".to_string()),
                    },
                    finish_reason: Some("length".to_string()),
                }],
            })
            .expect("extract response");

        assert_eq!(text, "{\"foo\":\"bar");
        assert_eq!(finish_reason.as_deref(), Some("length"));
    }
}
