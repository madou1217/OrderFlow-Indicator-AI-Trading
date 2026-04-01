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
            "text": format_trade_signal_message(signal),
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

pub fn format_trade_signal_message(signal: &TradeSignalNotification) -> String {
    [
        decision_heading(&signal.decision).to_string(),
        format!("📌 Symbol: {}", signal.symbol),
        format!("🟢 Entry: {}", format_opt_price(signal.entry_price)),
        format!("⚙️ Leverage: {}", format_opt_leverage(signal.leverage)),
        format!("📊 RR: {}", format_opt_ratio(signal.risk_reward_ratio)),
        format!("🎯 TP1: {}", format_opt_price(signal.take_profit_1)),
        format!("🎯 TP2: {}", format_opt_price(signal.take_profit_2)),
        format!("🛑 SL: {}", format_opt_price(signal.stop_loss)),
        format!("🕒 Time: {} UTC", signal.ts_bucket.format("%H:%M:%S")),
    ]
    .join("\n")
}

fn decision_heading(decision: &str) -> &'static str {
    if decision.eq_ignore_ascii_case("LONG") {
        "📈 LONG"
    } else if decision.eq_ignore_ascii_case("SHORT") {
        "📉 SHORT"
    } else if decision.eq_ignore_ascii_case("ADD") {
        "➕ ADD"
    } else if decision.eq_ignore_ascii_case("REDUCE") {
        "➖ REDUCE"
    } else if decision.eq_ignore_ascii_case("CLOSE") {
        "🔒 CLOSE"
    } else if decision.eq_ignore_ascii_case("MODIFY_TPSL") {
        "🛠 MODIFY_TPSL"
    } else if decision.eq_ignore_ascii_case("HOLD") {
        "⏸ HOLD"
    } else if decision.eq_ignore_ascii_case("NO_TRADE") {
        "🚫 NO_TRADE"
    } else {
        "🔔 SIGNAL"
    }
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

#[cfg(test)]
mod tests {
    use super::{format_trade_signal_message, TradeSignalNotification};
    use chrono::{DateTime, Utc};

    #[test]
    fn format_trade_signal_message_uses_structured_layout_without_reason() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-04-01T08:36:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let signal = TradeSignalNotification {
            ts_bucket,
            trigger: "watcher_fast_consumer".to_string(),
            symbol: "ETHUSDT".to_string(),
            model_name: "workflow_watcher_fast".to_string(),
            decision: "SHORT".to_string(),
            context_key: Some("ctx".to_string()),
            path_id: Some("path".to_string()),
            entry_price: Some(2142.89),
            leverage: Some(12.0),
            risk_reward_ratio: Some(6.84),
            take_profit_1: Some(2113.4),
            take_profit_2: Some(2101.8),
            stop_loss: Some(2147.2),
            reason: "do not show me".to_string(),
        };

        let rendered = format_trade_signal_message(&signal);

        assert_eq!(
            rendered,
            "📉 SHORT\n📌 Symbol: ETHUSDT\n🟢 Entry: 2142.89\n⚙️ Leverage: 12\n📊 RR: 6.84\n🎯 TP1: 2113.4\n🎯 TP2: 2101.8\n🛑 SL: 2147.2\n🕒 Time: 08:36:00 UTC"
        );
        assert!(!rendered.contains("Reason"));
        assert!(!rendered.contains("do not show me"));
        assert!(!rendered.contains("Context"));
        assert!(!rendered.contains("Path"));
    }

    #[test]
    fn format_trade_signal_message_keeps_fixed_shape_for_missing_fields() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-04-01T08:36:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let signal = TradeSignalNotification {
            ts_bucket,
            trigger: "schedule".to_string(),
            symbol: "ETHUSDT".to_string(),
            model_name: "custom_llm".to_string(),
            decision: "NO_TRADE".to_string(),
            context_key: None,
            path_id: None,
            entry_price: None,
            leverage: None,
            risk_reward_ratio: None,
            take_profit_1: None,
            take_profit_2: None,
            stop_loss: None,
            reason: "path_invalidated".to_string(),
        };

        let rendered = format_trade_signal_message(&signal);

        assert_eq!(
            rendered,
            "🚫 NO_TRADE\n📌 Symbol: ETHUSDT\n🟢 Entry: -\n⚙️ Leverage: -\n📊 RR: -\n🎯 TP1: -\n🎯 TP2: -\n🛑 SL: -\n🕒 Time: 08:36:00 UTC"
        );
    }
}
