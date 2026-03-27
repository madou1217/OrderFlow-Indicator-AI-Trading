use crate::app::config::TelegramApiConfig;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct TelegramOperator {
    token: String,
    chat_id: String,
    base_api_url: String,
}

#[derive(Debug, Clone)]
pub struct TradeSignalNotification {
    pub ts_bucket: DateTime<Utc>,
    pub trigger: String,
    pub symbol: String,
    pub model_name: String,
    pub decision: String,
    pub context_key: Option<String>,
    pub path_id: Option<String>,
    pub entry_price: Option<f64>,
    pub leverage: Option<f64>,
    pub risk_reward_ratio: Option<f64>,
    pub take_profit_1: Option<f64>,
    pub take_profit_2: Option<f64>,
    pub stop_loss: Option<f64>,
    pub reason: String,
}

impl TelegramOperator {
    pub fn from_config(cfg: &TelegramApiConfig) -> Option<Self> {
        let token = cfg.resolved_token().trim().to_string();
        let chat_id = normalize_chat_id(cfg.resolved_chat_id().trim());
        if token.is_empty() || chat_id.is_empty() {
            return None;
        }
        let mut base_api_url = cfg.base_api_url.trim().to_string();
        if base_api_url.is_empty() {
            base_api_url = "https://api.telegram.org".to_string();
        }
        base_api_url = base_api_url.trim_end_matches('/').to_string();
        Some(Self {
            token,
            chat_id,
            base_api_url,
        })
    }

    pub async fn send_trade_signal(
        &self,
        http_client: &Client,
        signal: &TradeSignalNotification,
    ) -> Result<()> {
        let url = format!("{}/bot{}/sendMessage", self.base_api_url, self.token);
        let payload = json!({
            "chat_id": self.chat_id,
            "text": build_trade_signal_message(signal),
            "disable_web_page_preview": true,
        });

        let response = http_client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("call telegram sendMessage")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read telegram sendMessage response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "telegram sendMessage failed status={} body={}",
                status,
                body
            ));
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&body) {
            if parsed.get("ok").and_then(Value::as_bool) == Some(false) {
                return Err(anyhow!(
                    "telegram sendMessage returned ok=false body={}",
                    body
                ));
            }
        }
        Ok(())
    }
}

fn normalize_chat_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with('-') || trimmed.chars().all(|c| c.is_ascii_digit()) {
        return trimmed.to_string();
    }
    if trimmed.starts_with('@') {
        return trimmed.to_string();
    }

    let mut candidate = trimmed
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("t.me/")
        .trim_start_matches("telegram.me/");
    candidate = candidate.trim_start_matches('@');
    candidate = candidate.trim_matches('/');
    if let Some((head, _)) = candidate.split_once('?') {
        candidate = head;
    }
    if let Some((head, _)) = candidate.split_once('#') {
        candidate = head;
    }

    if candidate.is_empty() {
        String::new()
    } else {
        format!("@{}", candidate)
    }
}

fn build_trade_signal_message(signal: &TradeSignalNotification) -> String {
    let mut lines = vec![
        "Workflow Trade Signal".to_string(),
        format!("Decision: {}", signal.decision),
        format!("Symbol: {}", signal.symbol),
    ];
    if let Some(context_key) = signal.context_key.as_deref() {
        lines.push(format!("Context: {}", context_key));
    }
    if let Some(path_id) = signal.path_id.as_deref() {
        lines.push(format!("Path: {}", path_id));
    }
    lines.push(format!("Entry: {}", format_opt_price(signal.entry_price)));
    lines.push(format!("TP1: {}", format_opt_price(signal.take_profit_1)));
    lines.push(format!("TP2: {}", format_opt_price(signal.take_profit_2)));
    lines.push(format!("SL: {}", format_opt_price(signal.stop_loss)));
    lines.push(format!(
        "Leverage: {}",
        format_opt_leverage(signal.leverage)
    ));
    lines.push(format!(
        "RR: {}",
        format_opt_ratio(signal.risk_reward_ratio)
    ));
    lines.push(format!("Time: {} UTC", signal.ts_bucket.format("%H:%M:%S")));
    lines.push(format!("Trigger: {}", signal.trigger));
    lines.push(format!("Model: {}", signal.model_name));
    lines.push(format!("Reason: {}", single_line_text(&signal.reason, 800)));
    lines.join("\n")
}

fn single_line_text(input: &str, max_len: usize) -> String {
    let mut output = input.split_whitespace().collect::<Vec<_>>().join(" ");
    if output.len() > max_len {
        output.truncate(max_len);
        output.push_str("...");
    }
    output
}

fn format_opt_price(value: Option<f64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_opt_ratio(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "-".to_string())
}

fn format_opt_leverage(value: Option<f64>) -> String {
    value
        .map(|v| {
            if (v - v.round()).abs() < f64::EPSILON {
                format!("{}", v.round() as i64)
            } else {
                format!("{:.2}", v)
            }
        })
        .unwrap_or_else(|| "-".to_string())
}
