use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct RootConfig {
    pub app: AppSection,
    pub api: ApiConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    pub mq: MqConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub llm: LlmConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppSection {
    pub name: String,
    pub env: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub claude: ClaudeApiConfig,
    #[serde(default)]
    pub qwen: QwenApiConfig,
    #[serde(default)]
    pub custom_llm: CustomLlmApiConfig,
    #[serde(default)]
    pub gemini: GeminiApiConfig,
    #[serde(default)]
    pub openrouter: OpenRouterApiConfig,
    #[serde(default)]
    pub grok: GrokApiConfig,
    #[serde(default)]
    pub telegram: TelegramApiConfig,
    #[serde(default)]
    pub x: XApiConfig,
    pub binance: BinanceApiConfig,
    #[serde(default)]
    pub default_model: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaudeApiConfig {
    pub api_key: String,
    #[serde(default = "default_claude_api_url")]
    pub api_url: String,
    #[serde(default = "default_claude_api_version")]
    pub api_version: String,
}

impl ClaudeApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct QwenApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_qwen_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_qwen_model")]
    pub model: String,
}

impl QwenApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CustomLlmApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub base_api_url: String,
    #[serde(default)]
    pub model: String,
}

impl CustomLlmApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GeminiApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_gemini_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_gemini_model")]
    pub model: String,
}

impl GeminiApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OpenRouterApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_openrouter_base_api_url")]
    pub base_api_url: String,
}

impl OpenRouterApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GrokApiConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_grok_base_api_url")]
    pub base_api_url: String,
    #[serde(default = "default_grok_model")]
    pub model: String,
}

impl GrokApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TelegramApiConfig {
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub chat_id: String,
    #[serde(default = "default_telegram_base_api_url")]
    pub base_api_url: String,
}

impl TelegramApiConfig {
    pub fn resolved_token(&self) -> String {
        resolve_secret(&self.token)
    }

    pub fn resolved_chat_id(&self) -> String {
        resolve_secret(&self.chat_id)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct XApiConfig {
    #[serde(default)]
    pub consumer_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub access_token_secret: String,
    #[serde(default = "default_x_base_api_url")]
    pub base_api_url: String,
}

impl XApiConfig {
    pub fn resolved_consumer_key(&self) -> String {
        resolve_secret(&self.consumer_key)
    }

    pub fn resolved_secret_key(&self) -> String {
        resolve_secret(&self.secret_key)
    }

    pub fn resolved_access_token(&self) -> String {
        resolve_secret(&self.access_token)
    }

    pub fn resolved_access_token_secret(&self) -> String {
        resolve_secret(&self.access_token_secret)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceApiConfig {
    pub api_key: String,
    pub api_secret: String,
    #[serde(default = "default_binance_futures_rest_api_url")]
    pub futures_rest_api_url: String,
}

impl BinanceApiConfig {
    pub fn resolved_api_key(&self) -> String {
        resolve_secret(&self.api_key)
    }

    pub fn resolved_api_secret(&self) -> String {
        resolve_secret(&self.api_secret)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NetworkConfig {
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub rest_proxy: ProxyConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabaseConfig {
    #[serde(default = "default_database_host")]
    pub host: String,
    #[serde(default = "default_database_port")]
    pub port: u16,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password_env: String,
    #[serde(default = "default_database_sslmode")]
    pub sslmode: String,
    #[serde(default)]
    pub connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub application_name: Option<String>,
    #[serde(default)]
    pub options: Option<String>,
    #[serde(default)]
    pub pool: Option<DatabasePoolConfig>,
}

impl DatabaseConfig {
    pub fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    pub fn postgres_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let resolved_password = self.resolved_password();
        let password = urlencoding::encode(&resolved_password);
        let database = urlencoding::encode(&self.database);
        let mut uri = format!(
            "postgres://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, database
        );

        let mut params = Vec::new();
        let sslmode = self.sslmode.trim();
        if !sslmode.is_empty() {
            params.push(format!("sslmode={}", urlencoding::encode(sslmode)));
        }
        if let Some(timeout) = self.connect_timeout_secs {
            params.push(format!("connect_timeout={}", timeout));
        }
        if let Some(app_name) = self.application_name.as_deref() {
            let app_name = app_name.trim();
            if !app_name.is_empty() {
                params.push(format!(
                    "application_name={}",
                    urlencoding::encode(app_name)
                ));
            }
        }
        if let Some(options) = self.options.as_deref() {
            let options = options.trim();
            if !options.is_empty() {
                params.push(format!("options={}", urlencoding::encode(options)));
            }
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabasePoolConfig {
    #[serde(default)]
    pub min_connections: Option<u32>,
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub acquire_timeout_secs: Option<u64>,
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_lifetime_secs: Option<u64>,
    #[serde(default)]
    pub test_before_acquire: Option<bool>,
}

impl NetworkConfig {
    pub fn effective_rest_proxy_url(&self) -> Option<String> {
        self.rest_proxy
            .effective_url()
            .or_else(|| self.proxy.effective_url())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    pub address: Option<String>,
}

impl ProxyConfig {
    pub fn effective_url(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        let raw = self.address.as_deref()?.trim();
        if raw.is_empty() {
            return None;
        }
        if raw.contains("://") {
            Some(raw.to_string())
        } else {
            Some(format!("http://{}", raw))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqConfig {
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub user: String,
    pub password_env: String,
    pub heartbeat_secs: Option<u16>,
    pub connection_timeout_secs: Option<u16>,
    pub exchanges: MqExchanges,
    pub queues: HashMap<String, MqQueueConfig>,
}

impl MqConfig {
    pub fn resolved_password(&self) -> String {
        resolve_secret(&self.password_env)
    }

    pub fn amqp_uri(&self) -> String {
        let user = urlencoding::encode(&self.user);
        let resolved_password = self.resolved_password();
        let password = urlencoding::encode(&resolved_password);
        let raw_vhost = self.vhost.trim();
        let vhost = if raw_vhost.is_empty() {
            urlencoding::encode("/")
        } else {
            urlencoding::encode(raw_vhost)
        };
        let mut uri = format!(
            "amqp://{}:{}@{}:{}/{}",
            user, password, self.host, self.port, vhost
        );

        let mut params = Vec::new();
        if let Some(v) = self.heartbeat_secs {
            params.push(format!("heartbeat={}", v));
        }
        if let Some(v) = self.connection_timeout_secs {
            params.push(format!("connection_timeout={}", v));
        }
        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqExchanges {
    pub md_live: MqExchangeConfig,
    pub md_replay: MqExchangeConfig,
    pub ind: MqExchangeConfig,
    pub dlx: MqExchangeConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqExchangeConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub durable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqQueueConfig {
    pub name: String,
    pub bind: Vec<MqBinding>,
    /// x-message-ttl in milliseconds. Messages older than this are dropped.
    #[serde(default)]
    pub message_ttl_ms: Option<u32>,
    /// x-max-length (message count). Combined with x-overflow=drop-head.
    #[serde(default)]
    pub max_length: Option<u32>,
    /// x-max-length-bytes. Combined with x-overflow=drop-head.
    #[serde(default)]
    pub max_length_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqBinding {
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    #[serde(default = "default_llm_request_enabled")]
    pub request_enabled: bool,
    #[serde(default = "default_llm_default_model")]
    pub default_model: String,
    #[serde(default = "default_llm_prompt_template")]
    pub prompt_template: String,
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default = "default_queue_key")]
    pub queue_key: String,
    #[serde(default = "default_llm_purge_queue_on_start")]
    pub purge_queue_on_start: bool,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_bundle_settle_ms")]
    pub bundle_settle_ms: u64,
    #[serde(default = "default_bundle_stale_secs")]
    pub bundle_stale_secs: u64,
    #[serde(default = "default_bundle_consume_stale_secs")]
    pub bundle_consume_stale_secs: u64,
    #[serde(default = "default_bundle_execution_stale_secs")]
    pub bundle_execution_stale_secs: u64,
    #[serde(default)]
    pub temp_cache_retention_hours: Option<u64>,
    #[serde(default, rename = "temp_cache_retention_minutes")]
    pub temp_cache_retention_minutes_legacy: Option<u64>,
    #[serde(default = "default_print_response")]
    pub print_response: bool,
    #[serde(default = "default_telegram_signal_decisions")]
    pub telegram_signal_decisions: Vec<String>,
    #[serde(default = "default_x_signal_decisions")]
    pub x_signal_decisions: Vec<String>,
    #[serde(default = "default_models")]
    pub models: Vec<LlmModelConfig>,
    #[serde(default)]
    pub execution: LlmExecutionConfig,
    #[serde(default)]
    pub workflow: WorkflowConfig,
    #[serde(default)]
    pub compatibility: LlmCompatibilityConfig,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            request_enabled: default_llm_request_enabled(),
            default_model: default_llm_default_model(),
            prompt_template: default_llm_prompt_template(),
            symbol: default_symbol(),
            queue_key: default_queue_key(),
            purge_queue_on_start: default_llm_purge_queue_on_start(),
            request_timeout_secs: default_request_timeout_secs(),
            bundle_settle_ms: default_bundle_settle_ms(),
            bundle_stale_secs: default_bundle_stale_secs(),
            bundle_consume_stale_secs: default_bundle_consume_stale_secs(),
            bundle_execution_stale_secs: default_bundle_execution_stale_secs(),
            temp_cache_retention_hours: Some(default_temp_cache_retention_hours()),
            temp_cache_retention_minutes_legacy: None,
            print_response: default_print_response(),
            telegram_signal_decisions: default_telegram_signal_decisions(),
            x_signal_decisions: default_x_signal_decisions(),
            models: default_models(),
            execution: LlmExecutionConfig::default(),
            workflow: WorkflowConfig::default(),
            compatibility: LlmCompatibilityConfig::default(),
        }
    }
}

impl LlmConfig {
    pub fn temp_cache_retention_minutes(&self) -> u64 {
        self.temp_cache_retention_hours
            .and_then(|hours| hours.checked_mul(60))
            .or(self.temp_cache_retention_minutes_legacy)
            .unwrap_or_else(default_temp_cache_retention_minutes)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub account_margin_ratio: f64,
    #[serde(default = "default_execution_max_margin_usdt")]
    pub max_margin_usdt: f64,
    #[serde(default = "default_execution_margin_usdt")]
    pub margin_usdt: f64,
    #[serde(
        default = "default_execution_default_leverage_ratio",
        alias = "default_leverage"
    )]
    pub default_leverage_ratio: f64,
    #[serde(default = "default_execution_max_leverage")]
    pub max_leverage: u32,
    #[serde(default = "default_execution_hedge_mode")]
    pub hedge_mode: bool,
    #[serde(default = "default_execution_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_execution_place_exit_orders")]
    pub place_exit_orders: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_workflow_stage1_refresh_hours")]
    pub stage1_refresh_hours: Vec<u8>,
    #[serde(default = "default_workflow_stage2_review_minutes")]
    pub stage2_review_minutes: Vec<u8>,
    #[serde(default = "default_workflow_state_dir")]
    pub state_dir: String,
    #[serde(default = "default_workflow_persist_prompt_inputs")]
    pub persist_prompt_inputs: bool,
    #[serde(default)]
    pub stage1: WorkflowStage1Config,
    #[serde(default)]
    pub limits: WorkflowLimitsConfig,
    #[serde(default)]
    pub watcher: WorkflowWatcherConfig,
}

impl Default for WorkflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            stage1_refresh_hours: default_workflow_stage1_refresh_hours(),
            stage2_review_minutes: default_workflow_stage2_review_minutes(),
            state_dir: default_workflow_state_dir(),
            persist_prompt_inputs: default_workflow_persist_prompt_inputs(),
            stage1: WorkflowStage1Config::default(),
            limits: WorkflowLimitsConfig::default(),
            watcher: WorkflowWatcherConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowStage1Config {
    #[serde(default = "default_stage1_min_overall_quality_for_new_entry_dispatch")]
    pub min_overall_quality_for_new_entry_dispatch: String,
}

impl Default for WorkflowStage1Config {
    fn default() -> Self {
        Self {
            min_overall_quality_for_new_entry_dispatch:
                default_stage1_min_overall_quality_for_new_entry_dispatch(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowLimitsConfig {
    #[serde(default = "default_workflow_max_live_positions_per_direction")]
    pub max_live_positions_per_direction: usize,
    #[serde(default = "default_workflow_max_live_entry_orders_per_direction")]
    pub max_live_entry_orders_per_direction: usize,
}

impl Default for WorkflowLimitsConfig {
    fn default() -> Self {
        Self {
            max_live_positions_per_direction: default_workflow_max_live_positions_per_direction(),
            max_live_entry_orders_per_direction:
                default_workflow_max_live_entry_orders_per_direction(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowWatcherConfig {
    #[serde(default = "default_watcher_evaluate_on")]
    pub evaluate_on: String,
    #[serde(default = "default_watcher_entry_attempt_window")]
    pub entry_attempt_window: String,
    #[serde(default = "default_watcher_entry_ttl_minutes")]
    pub entry_ttl_minutes: u64,
    #[serde(default = "default_watcher_max_filled_stopout_attempts")]
    pub max_filled_stopout_attempts: u8,
    #[serde(default = "default_watcher_count_unfilled_attempts")]
    pub count_unfilled_attempts: bool,
    #[serde(default)]
    pub price_predicates: WorkflowWatcherPricePredicatesConfig,
    #[serde(default)]
    pub entry_profile_rules: WorkflowWatcherEntryProfileRulesConfig,
    #[serde(default)]
    pub intent_mode_rules: WorkflowWatcherIntentModeRulesConfig,
}

impl Default for WorkflowWatcherConfig {
    fn default() -> Self {
        Self {
            evaluate_on: default_watcher_evaluate_on(),
            entry_attempt_window: default_watcher_entry_attempt_window(),
            entry_ttl_minutes: default_watcher_entry_ttl_minutes(),
            max_filled_stopout_attempts: default_watcher_max_filled_stopout_attempts(),
            count_unfilled_attempts: default_watcher_count_unfilled_attempts(),
            price_predicates: WorkflowWatcherPricePredicatesConfig::default(),
            entry_profile_rules: WorkflowWatcherEntryProfileRulesConfig::default(),
            intent_mode_rules: WorkflowWatcherIntentModeRulesConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowWatcherPricePredicatesConfig {
    #[serde(default)]
    pub price_above_on_close: ClosePredicateConfig,
    #[serde(default)]
    pub price_below_on_close: ClosePredicateConfig,
    #[serde(default)]
    pub entry_reclaim_confirmed: EntryReclaimPredicateConfig,
    #[serde(default)]
    pub entry_hold_confirmed: EntryHoldPredicateConfig,
    #[serde(default)]
    pub breakout_confirmed: BreakoutPredicateConfig,
    #[serde(default)]
    pub pullback_acceptance_confirmed: PullbackAcceptancePredicateConfig,
    #[serde(default)]
    pub failed_auction_reentry_confirmed: FailedAuctionReentryPredicateConfig,
}

impl Default for WorkflowWatcherPricePredicatesConfig {
    fn default() -> Self {
        Self {
            price_above_on_close: ClosePredicateConfig::default(),
            price_below_on_close: ClosePredicateConfig::default(),
            entry_reclaim_confirmed: EntryReclaimPredicateConfig::default(),
            entry_hold_confirmed: EntryHoldPredicateConfig::default(),
            breakout_confirmed: BreakoutPredicateConfig::default(),
            pullback_acceptance_confirmed: PullbackAcceptancePredicateConfig::default(),
            failed_auction_reentry_confirmed: FailedAuctionReentryPredicateConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClosePredicateConfig {
    #[serde(default = "default_watcher_confirm_bars")]
    pub confirm_bars: u8,
    #[serde(default)]
    pub min_close_bps: f64,
}

impl Default for ClosePredicateConfig {
    fn default() -> Self {
        Self {
            confirm_bars: default_watcher_confirm_bars(),
            min_close_bps: default_watcher_min_close_bps(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EntryReclaimPredicateConfig {
    #[serde(default = "default_watcher_confirm_bars")]
    pub confirm_bars: u8,
    #[serde(default = "default_true")]
    pub allow_equal: bool,
}

impl Default for EntryReclaimPredicateConfig {
    fn default() -> Self {
        Self {
            confirm_bars: default_watcher_confirm_bars(),
            allow_equal: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EntryHoldPredicateConfig {
    #[serde(default = "default_watcher_confirm_bars")]
    pub hold_bars: u8,
    #[serde(default = "default_watcher_retest_tolerance_bps")]
    pub retest_tolerance_bps: f64,
}

impl Default for EntryHoldPredicateConfig {
    fn default() -> Self {
        Self {
            hold_bars: default_watcher_confirm_bars(),
            retest_tolerance_bps: default_watcher_retest_tolerance_bps(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BreakoutPredicateConfig {
    #[serde(default = "default_watcher_confirm_bars")]
    pub confirm_bars: u8,
    #[serde(default)]
    pub min_break_bps: f64,
}

impl Default for BreakoutPredicateConfig {
    fn default() -> Self {
        Self {
            confirm_bars: default_watcher_confirm_bars(),
            min_break_bps: default_watcher_min_break_bps(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullbackAcceptancePredicateConfig {
    #[serde(default = "default_watcher_confirm_bars")]
    pub confirm_bars: u8,
    #[serde(default = "default_true")]
    pub require_touch_entry_zone: bool,
    #[serde(default = "default_watcher_pullback_overshoot_bps")]
    pub max_overshoot_bps: f64,
}

impl Default for PullbackAcceptancePredicateConfig {
    fn default() -> Self {
        Self {
            confirm_bars: default_watcher_confirm_bars(),
            require_touch_entry_zone: true,
            max_overshoot_bps: default_watcher_pullback_overshoot_bps(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FailedAuctionReentryPredicateConfig {
    #[serde(default = "default_true")]
    pub require_probe_invalidation: bool,
    #[serde(default = "default_watcher_probe_lookback_bars")]
    pub probe_lookback_bars: u8,
    #[serde(default = "default_watcher_confirm_bars")]
    pub reaccept_confirm_bars: u8,
}

impl Default for FailedAuctionReentryPredicateConfig {
    fn default() -> Self {
        Self {
            require_probe_invalidation: true,
            probe_lookback_bars: default_watcher_probe_lookback_bars(),
            reaccept_confirm_bars: default_watcher_confirm_bars(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowWatcherEntryProfileRulesConfig {
    #[serde(default = "default_reclaim_then_hold_predicates")]
    pub reclaim_then_hold: PredicateListRuleConfig,
    #[serde(default = "default_pullback_acceptance_predicates")]
    pub pullback_acceptance: PredicateListRuleConfig,
    #[serde(default = "default_failed_auction_reentry_predicates")]
    pub failed_auction_reentry: PredicateListRuleConfig,
}

impl Default for WorkflowWatcherEntryProfileRulesConfig {
    fn default() -> Self {
        Self {
            reclaim_then_hold: default_reclaim_then_hold_predicates(),
            pullback_acceptance: default_pullback_acceptance_predicates(),
            failed_auction_reentry: default_failed_auction_reentry_predicates(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowWatcherIntentModeRulesConfig {
    #[serde(default = "default_immediate_intent_rule")]
    pub immediate: IntentModeRuleConfig,
    #[serde(default = "default_pullback_intent_rule")]
    pub pullback: IntentModeRuleConfig,
    #[serde(default = "default_breakout_intent_rule")]
    pub breakout: IntentModeRuleConfig,
}

impl Default for WorkflowWatcherIntentModeRulesConfig {
    fn default() -> Self {
        Self {
            immediate: default_immediate_intent_rule(),
            pullback: default_pullback_intent_rule(),
            breakout: default_breakout_intent_rule(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredicateListRuleConfig {
    #[serde(default)]
    pub required_predicates: Vec<String>,
}

impl Default for PredicateListRuleConfig {
    fn default() -> Self {
        Self {
            required_predicates: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IntentModeRuleConfig {
    #[serde(default)]
    pub required_predicates: Vec<String>,
    #[serde(default)]
    pub require_price_inside_entry_zone: bool,
    #[serde(default)]
    pub disallow_breakout_chase: bool,
}

impl Default for IntentModeRuleConfig {
    fn default() -> Self {
        Self {
            required_predicates: Vec::new(),
            require_price_inside_entry_zone: false,
            disallow_breakout_chase: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LlmCompatibilityConfig {
    #[serde(default)]
    pub execution_policy: LlmCompatibilityExecutionPolicyConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmCompatibilityExecutionPolicyConfig {
    #[serde(default)]
    pub entry_sl_remap: ExecutionEntrySlRemapConfig,
    #[serde(default = "default_execution_min_distance_v")]
    pub min_distance_v: f64,
    #[serde(default = "default_execution_min_rr")]
    pub min_rr: f64,
}

impl Default for LlmCompatibilityExecutionPolicyConfig {
    fn default() -> Self {
        Self {
            entry_sl_remap: ExecutionEntrySlRemapConfig::default(),
            min_distance_v: default_execution_min_distance_v(),
            min_rr: default_execution_min_rr(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionEntrySlRemapConfig {
    #[serde(default = "default_execution_entry_sl_remap_enabled")]
    pub enabled: bool,
    #[serde(default = "default_execution_entry_to_sl_distance_pct")]
    pub entry_to_sl_distance_pct: f64,
}

impl Default for ExecutionEntrySlRemapConfig {
    fn default() -> Self {
        Self {
            enabled: default_execution_entry_sl_remap_enabled(),
            entry_to_sl_distance_pct: default_execution_entry_to_sl_distance_pct(),
        }
    }
}

impl Default for LlmExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: false,
            account_margin_ratio: 0.0,
            max_margin_usdt: default_execution_max_margin_usdt(),
            margin_usdt: default_execution_margin_usdt(),
            default_leverage_ratio: default_execution_default_leverage_ratio(),
            max_leverage: default_execution_max_leverage(),
            hedge_mode: default_execution_hedge_mode(),
            recv_window_ms: default_execution_recv_window_ms(),
            place_exit_orders: default_execution_place_exit_orders(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmModelConfig {
    pub name: String,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub use_openrouter: Option<bool>,
    #[serde(default = "default_model_enabled")]
    pub enabled: bool,
    #[serde(default = "default_model_temperature")]
    pub temperature: f64,
    #[serde(default = "default_model_max_tokens")]
    pub max_tokens: u32,
    /// Optional stage-1 workflow reasoning effort hint for OpenAI-compatible custom_llm backends.
    #[serde(default)]
    pub stage1_reasoning: Option<String>,
    /// Optional stage-2 finalize reasoning effort hint for OpenAI-compatible custom_llm backends.
    #[serde(default)]
    pub stage2_reasoning: Option<String>,
    /// Legacy fallback reasoning effort hint. Used when stage1/stage2 reasoning is not set.
    #[serde(default)]
    pub reasoning: Option<String>,
}

fn default_symbol() -> String {
    String::new()
}

fn default_queue_key() -> String {
    "llm_indicator_minute".to_string()
}

fn default_llm_purge_queue_on_start() -> bool {
    true
}

fn default_llm_request_enabled() -> bool {
    true
}

fn default_request_timeout_secs() -> u64 {
    1200
}

fn default_bundle_settle_ms() -> u64 {
    1000
}

fn default_bundle_stale_secs() -> u64 {
    59
}

fn default_bundle_consume_stale_secs() -> u64 {
    300
}

fn default_bundle_execution_stale_secs() -> u64 {
    300
}

fn default_workflow_stage1_refresh_hours() -> Vec<u8> {
    vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]
}

fn default_workflow_stage2_review_minutes() -> Vec<u8> {
    vec![0, 15, 30, 45]
}

fn default_workflow_state_dir() -> String {
    "systems/llm/state/workflow".to_string()
}

fn default_workflow_persist_prompt_inputs() -> bool {
    true
}

fn default_stage1_min_overall_quality_for_new_entry_dispatch() -> String {
    "medium".to_string()
}

fn default_workflow_max_live_positions_per_direction() -> usize {
    1
}

fn default_workflow_max_live_entry_orders_per_direction() -> usize {
    1
}

fn default_watcher_evaluate_on() -> String {
    "fast_price".to_string()
}

fn default_watcher_entry_attempt_window() -> String {
    "same_15m_window".to_string()
}

fn default_watcher_entry_ttl_minutes() -> u64 {
    15
}

fn default_watcher_max_filled_stopout_attempts() -> u8 {
    2
}

fn default_watcher_count_unfilled_attempts() -> bool {
    false
}

fn default_watcher_confirm_bars() -> u8 {
    3
}

fn default_watcher_min_close_bps() -> f64 {
    3.0
}

fn default_watcher_retest_tolerance_bps() -> f64 {
    5.0
}

fn default_watcher_min_break_bps() -> f64 {
    8.0
}

fn default_watcher_pullback_overshoot_bps() -> f64 {
    5.0
}

fn default_watcher_probe_lookback_bars() -> u8 {
    3
}

fn default_true() -> bool {
    true
}

fn default_reclaim_then_hold_predicates() -> PredicateListRuleConfig {
    PredicateListRuleConfig {
        required_predicates: vec![
            "entry_reclaim_confirmed".to_string(),
            "entry_hold_confirmed".to_string(),
        ],
    }
}

fn default_pullback_acceptance_predicates() -> PredicateListRuleConfig {
    PredicateListRuleConfig {
        required_predicates: vec!["pullback_acceptance_confirmed".to_string()],
    }
}

fn default_failed_auction_reentry_predicates() -> PredicateListRuleConfig {
    PredicateListRuleConfig {
        required_predicates: vec!["failed_auction_reentry_confirmed".to_string()],
    }
}

fn default_immediate_intent_rule() -> IntentModeRuleConfig {
    IntentModeRuleConfig {
        required_predicates: Vec::new(),
        require_price_inside_entry_zone: true,
        disallow_breakout_chase: false,
    }
}

fn default_pullback_intent_rule() -> IntentModeRuleConfig {
    IntentModeRuleConfig {
        required_predicates: Vec::new(),
        require_price_inside_entry_zone: false,
        disallow_breakout_chase: false,
    }
}

fn default_breakout_intent_rule() -> IntentModeRuleConfig {
    IntentModeRuleConfig {
        required_predicates: vec!["breakout_confirmed".to_string()],
        require_price_inside_entry_zone: false,
        disallow_breakout_chase: false,
    }
}

fn default_temp_cache_retention_hours() -> u64 {
    12
}

fn default_temp_cache_retention_minutes() -> u64 {
    default_temp_cache_retention_hours() * 60
}

fn validate_schedule_hours(hours: &[u8], field_name: &str) -> Result<()> {
    if hours.is_empty() {
        return Err(anyhow!("{} cannot be empty", field_name));
    }
    let mut seen = std::collections::HashSet::new();
    for hour in hours {
        if *hour > 23 {
            return Err(anyhow!(
                "{} contains invalid hour {}; expected 0..=23",
                field_name,
                hour
            ));
        }
        if !seen.insert(*hour) {
            return Err(anyhow!("{} contains duplicate hour {}", field_name, hour));
        }
    }
    Ok(())
}

fn validate_schedule_minutes(minutes: &[u8], field_name: &str) -> Result<()> {
    if minutes.is_empty() {
        return Err(anyhow!("{} cannot be empty", field_name));
    }
    let mut seen = std::collections::HashSet::new();
    for minute in minutes {
        if *minute > 59 {
            return Err(anyhow!(
                "{} contains invalid minute {}; expected 0..=59",
                field_name,
                minute
            ));
        }
        if !seen.insert(*minute) {
            return Err(anyhow!(
                "{} contains duplicate minute {}",
                field_name,
                minute
            ));
        }
    }
    Ok(())
}

fn validate_required_predicates(predicates: &[String], field_name: &str) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "price_above_on_close",
        "price_below_on_close",
        "entry_reclaim_confirmed",
        "entry_hold_confirmed",
        "breakout_confirmed",
        "pullback_acceptance_confirmed",
        "failed_auction_reentry_confirmed",
    ];
    for predicate in predicates {
        if !ALLOWED.iter().any(|allowed| predicate == allowed) {
            return Err(anyhow!(
                "{} contains unsupported predicate {}",
                field_name,
                predicate
            ));
        }
    }
    Ok(())
}

fn validate_workflow_watcher_config(cfg: &WorkflowWatcherConfig) -> Result<()> {
    if !matches!(cfg.evaluate_on.trim(), "1m_close" | "fast_price") {
        return Err(anyhow!(
            "llm.workflow.watcher.evaluate_on must be one of: 1m_close, fast_price"
        ));
    }
    if cfg.entry_attempt_window.trim() != "same_15m_window" {
        return Err(anyhow!(
            "llm.workflow.watcher.entry_attempt_window must be same_15m_window"
        ));
    }
    if cfg.entry_ttl_minutes == 0 {
        return Err(anyhow!(
            "llm.workflow.watcher.entry_ttl_minutes must be > 0"
        ));
    }
    if cfg.max_filled_stopout_attempts == 0 {
        return Err(anyhow!(
            "llm.workflow.watcher.max_filled_stopout_attempts must be > 0"
        ));
    }
    if cfg.count_unfilled_attempts {
        return Err(anyhow!(
            "llm.workflow.watcher.count_unfilled_attempts=true is not supported yet"
        ));
    }
    if cfg.price_predicates.price_above_on_close.confirm_bars == 0
        || cfg.price_predicates.price_below_on_close.confirm_bars == 0
        || cfg.price_predicates.entry_reclaim_confirmed.confirm_bars == 0
        || cfg.price_predicates.entry_hold_confirmed.hold_bars == 0
        || cfg.price_predicates.breakout_confirmed.confirm_bars == 0
        || cfg
            .price_predicates
            .pullback_acceptance_confirmed
            .confirm_bars
            == 0
        || cfg
            .price_predicates
            .failed_auction_reentry_confirmed
            .reaccept_confirm_bars
            == 0
    {
        return Err(anyhow!(
            "llm.workflow.watcher predicate confirm bars must be > 0"
        ));
    }
    if cfg.price_predicates.price_above_on_close.min_close_bps < 0.0
        || cfg.price_predicates.price_below_on_close.min_close_bps < 0.0
    {
        return Err(anyhow!(
            "llm.workflow.watcher price_above_on_close/price_below_on_close min_close_bps must be >= 0"
        ));
    }
    if cfg.price_predicates.breakout_confirmed.min_break_bps <= 0.0 {
        return Err(anyhow!(
            "llm.workflow.watcher.breakout_confirmed.min_break_bps must be > 0"
        ));
    }
    if cfg
        .price_predicates
        .entry_hold_confirmed
        .retest_tolerance_bps
        < 0.0
    {
        return Err(anyhow!(
            "llm.workflow.watcher.entry_hold_confirmed.retest_tolerance_bps must be >= 0"
        ));
    }
    if cfg
        .price_predicates
        .pullback_acceptance_confirmed
        .max_overshoot_bps
        < 0.0
    {
        return Err(anyhow!(
            "llm.workflow.watcher.pullback_acceptance_confirmed.max_overshoot_bps must be >= 0"
        ));
    }
    validate_required_predicates(
        &cfg.entry_profile_rules
            .reclaim_then_hold
            .required_predicates,
        "llm.workflow.watcher.entry_profile_rules.reclaim_then_hold.required_predicates",
    )?;
    validate_required_predicates(
        &cfg.entry_profile_rules
            .pullback_acceptance
            .required_predicates,
        "llm.workflow.watcher.entry_profile_rules.pullback_acceptance.required_predicates",
    )?;
    validate_required_predicates(
        &cfg.entry_profile_rules
            .failed_auction_reentry
            .required_predicates,
        "llm.workflow.watcher.entry_profile_rules.failed_auction_reentry.required_predicates",
    )?;
    validate_required_predicates(
        &cfg.intent_mode_rules.immediate.required_predicates,
        "llm.workflow.watcher.intent_mode_rules.immediate.required_predicates",
    )?;
    validate_required_predicates(
        &cfg.intent_mode_rules.pullback.required_predicates,
        "llm.workflow.watcher.intent_mode_rules.pullback.required_predicates",
    )?;
    validate_required_predicates(
        &cfg.intent_mode_rules.breakout.required_predicates,
        "llm.workflow.watcher.intent_mode_rules.breakout.required_predicates",
    )?;
    Ok(())
}

fn validate_temp_cache_retention_config(llm: &LlmConfig) -> Result<()> {
    if llm.temp_cache_retention_hours.is_some() && llm.temp_cache_retention_minutes_legacy.is_some()
    {
        return Err(anyhow!(
            "llm.temp_cache_retention_hours and llm.temp_cache_retention_minutes cannot both be set"
        ));
    }
    if llm.temp_cache_retention_hours == Some(0) {
        return Err(anyhow!("llm.temp_cache_retention_hours must be > 0"));
    }
    if llm.temp_cache_retention_minutes_legacy == Some(0) {
        return Err(anyhow!("llm.temp_cache_retention_minutes must be > 0"));
    }
    Ok(())
}

fn default_print_response() -> bool {
    true
}

fn default_telegram_signal_decisions() -> Vec<String> {
    vec!["long".to_string(), "short".to_string()]
}

fn default_x_signal_decisions() -> Vec<String> {
    default_telegram_signal_decisions()
}

fn default_model_enabled() -> bool {
    true
}

fn default_model_temperature() -> f64 {
    0.1
}

fn default_model_max_tokens() -> u32 {
    1200
}

fn default_claude_api_url() -> String {
    "https://api.anthropic.com/v1/messages".to_string()
}

fn default_llm_default_model() -> String {
    "claude".to_string()
}

fn default_llm_prompt_template() -> String {
    "big_opportunity".to_string()
}

fn default_qwen_base_api_url() -> String {
    "https://dashscope-intl.aliyuncs.com/compatible-mode/v1".to_string()
}

fn default_qwen_model() -> String {
    "qwen3-max".to_string()
}

fn default_gemini_base_api_url() -> String {
    "https://generativelanguage.googleapis.com/v1beta".to_string()
}

fn default_gemini_model() -> String {
    "gemini-2.5-pro".to_string()
}

fn default_openrouter_base_api_url() -> String {
    "https://openrouter.ai/api/v1".to_string()
}

fn default_grok_base_api_url() -> String {
    "https://api.x.ai/v1".to_string()
}

fn default_grok_model() -> String {
    "grok-4-1-fast-non-reasoning".to_string()
}

fn default_telegram_base_api_url() -> String {
    "https://api.telegram.org".to_string()
}

fn default_x_base_api_url() -> String {
    "https://api.x.com/2".to_string()
}

fn default_database_host() -> String {
    "127.0.0.1".to_string()
}

fn default_database_port() -> u16 {
    5432
}

fn default_database_sslmode() -> String {
    "disable".to_string()
}

fn default_claude_api_version() -> String {
    "2023-06-01".to_string()
}

fn default_binance_futures_rest_api_url() -> String {
    "https://fapi.binance.com".to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        validate_temp_cache_retention_config, validate_workflow_watcher_config, LlmConfig,
        LlmModelConfig, WorkflowWatcherConfig,
    };

    #[test]
    fn default_llm_request_enabled_is_true() {
        assert!(LlmConfig::default().request_enabled);
    }

    #[test]
    fn default_temp_cache_retention_is_twelve_hours() {
        let config = LlmConfig::default();
        assert_eq!(config.temp_cache_retention_hours, Some(12));
        assert_eq!(config.temp_cache_retention_minutes(), 12 * 60);
    }

    #[test]
    fn temp_cache_retention_uses_configured_hours() {
        let config: LlmConfig =
            serde_yaml::from_str("temp_cache_retention_hours: 6").expect("parse llm config");
        assert_eq!(config.temp_cache_retention_hours, Some(6));
        assert_eq!(config.temp_cache_retention_minutes(), 6 * 60);
    }

    #[test]
    fn temp_cache_retention_supports_legacy_minutes_field() {
        let config: LlmConfig =
            serde_yaml::from_str("temp_cache_retention_minutes: 90").expect("parse llm config");
        assert_eq!(config.temp_cache_retention_hours, None);
        assert_eq!(config.temp_cache_retention_minutes_legacy, Some(90));
        assert_eq!(config.temp_cache_retention_minutes(), 90);
    }

    #[test]
    fn temp_cache_retention_rejects_conflicting_units() {
        let config: LlmConfig = serde_yaml::from_str(
            "temp_cache_retention_hours: 12\ntemp_cache_retention_minutes: 60",
        )
        .expect("parse llm config");
        let err =
            validate_temp_cache_retention_config(&config).expect_err("expected conflict error");
        assert!(err
            .to_string()
            .contains("temp_cache_retention_hours and llm.temp_cache_retention_minutes"));
    }

    #[test]
    fn default_execution_trade_gates_are_expected_values() {
        let execution = LlmConfig::default().execution;
        let policy = LlmConfig::default().compatibility.execution_policy;
        assert!(!policy.entry_sl_remap.enabled);
        assert!((policy.min_distance_v - 0.0).abs() < f64::EPSILON);
        assert!((policy.min_rr - 0.0).abs() < f64::EPSILON);
        assert!((execution.max_margin_usdt - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn default_workflow_watcher_config_is_stricter_and_valid() {
        let watcher = WorkflowWatcherConfig::default();
        assert_eq!(watcher.evaluate_on, "fast_price");
        assert_eq!(watcher.entry_attempt_window, "same_15m_window");
        assert_eq!(watcher.entry_ttl_minutes, 15);
        assert_eq!(watcher.max_filled_stopout_attempts, 2);
        assert!(!watcher.count_unfilled_attempts);
        assert_eq!(
            watcher.price_predicates.price_above_on_close.confirm_bars,
            3
        );
        assert!(
            (watcher.price_predicates.price_above_on_close.min_close_bps - 3.0).abs()
                < f64::EPSILON
        );
        assert_eq!(watcher.price_predicates.breakout_confirmed.confirm_bars, 3);
        assert!(
            (watcher.price_predicates.breakout_confirmed.min_break_bps - 8.0).abs() < f64::EPSILON
        );
        assert_eq!(watcher.price_predicates.entry_hold_confirmed.hold_bars, 3);
        assert!(
            (watcher
                .price_predicates
                .entry_hold_confirmed
                .retest_tolerance_bps
                - 5.0)
                .abs()
                < f64::EPSILON
        );
        assert_eq!(
            watcher
                .price_predicates
                .pullback_acceptance_confirmed
                .confirm_bars,
            3
        );
        assert!(
            (watcher
                .price_predicates
                .pullback_acceptance_confirmed
                .max_overshoot_bps
                - 5.0)
                .abs()
                < f64::EPSILON
        );
        assert_eq!(
            watcher
                .entry_profile_rules
                .reclaim_then_hold
                .required_predicates,
            vec![
                "entry_reclaim_confirmed".to_string(),
                "entry_hold_confirmed".to_string()
            ]
        );
        validate_workflow_watcher_config(&watcher).expect("watcher config valid");
    }

    #[test]
    fn workflow_watcher_rejects_non_positive_breakout_bps() {
        let mut watcher = WorkflowWatcherConfig::default();
        watcher.price_predicates.breakout_confirmed.min_break_bps = 0.0;
        let err = validate_workflow_watcher_config(&watcher).expect_err("expected validation err");
        assert!(err
            .to_string()
            .contains("breakout_confirmed.min_break_bps must be > 0"));
    }

    #[test]
    fn workflow_watcher_rejects_non_positive_entry_ttl() {
        let mut watcher = WorkflowWatcherConfig::default();
        watcher.entry_ttl_minutes = 0;
        let err = validate_workflow_watcher_config(&watcher).expect_err("expected validation err");
        assert!(err
            .to_string()
            .contains("llm.workflow.watcher.entry_ttl_minutes must be > 0"));
    }

    #[test]
    fn model_reasoning_for_stage_prefers_stage_specific_then_legacy() {
        let model = LlmModelConfig {
            name: "custom_llm".to_string(),
            provider: "custom_llm".to_string(),
            model: "gpt-5.4-xhigh".to_string(),
            use_openrouter: None,
            enabled: true,
            temperature: 0.1,
            max_tokens: 1000,
            stage1_reasoning: Some("high".to_string()),
            stage2_reasoning: Some("medium".to_string()),
            reasoning: Some("low".to_string()),
        };
        assert_eq!(model.reasoning_for_stage(true), Some("high"));
        assert_eq!(model.reasoning_for_stage(false), Some("medium"));

        let legacy_only = LlmModelConfig {
            stage1_reasoning: None,
            stage2_reasoning: None,
            reasoning: Some("xhigh".to_string()),
            ..model
        };
        assert_eq!(legacy_only.reasoning_for_stage(true), Some("xhigh"));
        assert_eq!(legacy_only.reasoning_for_stage(false), Some("xhigh"));
    }
}

fn default_models() -> Vec<LlmModelConfig> {
    vec![LlmModelConfig {
        name: "claude46_sonnet".to_string(),
        provider: "claude".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        use_openrouter: None,
        enabled: true,
        temperature: default_model_temperature(),
        max_tokens: default_model_max_tokens(),
        stage1_reasoning: None,
        stage2_reasoning: None,
        reasoning: None,
    }]
}

fn default_execution_margin_usdt() -> f64 {
    50.0
}

fn default_execution_max_margin_usdt() -> f64 {
    0.0
}

fn default_execution_max_leverage() -> u32 {
    10
}

fn default_execution_default_leverage_ratio() -> f64 {
    30.0
}

fn default_execution_hedge_mode() -> bool {
    true
}

fn default_execution_recv_window_ms() -> u64 {
    5000
}

fn default_execution_place_exit_orders() -> bool {
    true
}

fn default_execution_entry_sl_remap_enabled() -> bool {
    false
}

fn default_execution_entry_to_sl_distance_pct() -> f64 {
    100.0
}

fn default_execution_min_distance_v() -> f64 {
    0.0
}

fn default_execution_min_rr() -> f64 {
    0.0
}

pub fn load_config(path: &str) -> Result<RootConfig> {
    let mut doc = load_config_document(path)?;
    let symbol = extract_global_symbol(&doc)
        .ok_or_else(|| anyhow!("config.instrument.symbol or llm.symbol is required"))?;
    ensure_yaml_string_path(&mut doc, &["llm", "symbol"], &symbol);
    let symbol_lower = symbol.to_ascii_lowercase();
    apply_symbol_placeholders(&mut doc, &symbol, &symbol_lower);
    let cfg: RootConfig = serde_yaml::from_value(doc)?;
    validate_config(&cfg)?;
    Ok(cfg)
}

fn load_config_document(path: &str) -> Result<serde_yaml::Value> {
    let text = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&text)?)
}

fn extract_global_symbol(doc: &serde_yaml::Value) -> Option<String> {
    for path in [
        &["instrument", "symbol"][..],
        &["llm", "symbol"][..],
        &["indicator", "symbol"][..],
        &["replayer", "symbol"][..],
        &["market_data", "symbol"][..],
    ] {
        let Some(value) = lookup_yaml_string(doc, path) else {
            continue;
        };
        let trimmed = value.trim();
        if !trimmed.is_empty() && !trimmed.contains("{symbol") {
            return Some(trimmed.to_ascii_uppercase());
        }
    }
    None
}

fn lookup_yaml_string(doc: &serde_yaml::Value, path: &[&str]) -> Option<String> {
    let mut node = doc;
    for segment in path {
        let map = node.as_mapping()?;
        node = map.get(serde_yaml::Value::String((*segment).to_string()))?;
    }
    node.as_str().map(ToOwned::to_owned)
}

fn ensure_yaml_string_path(doc: &mut serde_yaml::Value, path: &[&str], value: &str) {
    if path.is_empty() {
        return;
    }

    let mut node = doc;
    for segment in &path[..path.len() - 1] {
        let Some(map) = node.as_mapping_mut() else {
            return;
        };
        node = map
            .entry(serde_yaml::Value::String((*segment).to_string()))
            .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()));
    }

    if let Some(map) = node.as_mapping_mut() {
        map.entry(serde_yaml::Value::String(path[path.len() - 1].to_string()))
            .or_insert_with(|| serde_yaml::Value::String(value.to_string()));
    }
}

fn apply_symbol_placeholders(node: &mut serde_yaml::Value, symbol: &str, symbol_lower: &str) {
    match node {
        serde_yaml::Value::String(text) => {
            *text = text
                .replace("{symbol_lower}", symbol_lower)
                .replace("{symbol}", symbol);
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                apply_symbol_placeholders(item, symbol, symbol_lower);
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for value in map.values_mut() {
                apply_symbol_placeholders(value, symbol, symbol_lower);
            }
        }
        _ => {}
    }
}

impl RootConfig {
    fn active_default_model_selector(&self) -> String {
        let llm_default = self.llm.default_model.trim().to_ascii_lowercase();
        if !llm_default.is_empty() {
            return llm_default;
        }
        self.api.default_model.trim().to_ascii_lowercase()
    }

    pub fn active_default_model(&self) -> String {
        let selector = self.active_default_model_selector();
        if is_supported_provider_name(&selector) {
            return selector;
        }
        if let Some(model) = self
            .llm
            .models
            .iter()
            .find(|m| m.enabled && m.name.trim().eq_ignore_ascii_case(&selector))
        {
            return model.provider.trim().to_ascii_lowercase();
        }
        selector
    }

    pub fn selected_enabled_models_for_default(&self) -> Vec<LlmModelConfig> {
        let selector = self.active_default_model_selector();
        if !is_supported_provider_name(&selector) {
            let by_name = self
                .llm
                .models
                .iter()
                .filter(|m| m.enabled && m.name.trim().eq_ignore_ascii_case(&selector))
                .cloned()
                .collect::<Vec<_>>();
            if !by_name.is_empty() {
                return by_name;
            }
        }

        let provider = self.active_default_model();
        self.llm
            .models
            .iter()
            .filter(|m| m.enabled && m.provider.eq_ignore_ascii_case(&provider))
            .cloned()
            .collect::<Vec<_>>()
    }
}

impl LlmModelConfig {
    pub fn should_use_openrouter(&self) -> bool {
        if self.provider.eq_ignore_ascii_case("gemini") {
            self.use_openrouter.unwrap_or(true)
        } else {
            self.use_openrouter.unwrap_or(false)
        }
    }

    pub fn reasoning_for_stage(&self, is_stage1: bool) -> Option<&str> {
        let stage_specific = if is_stage1 {
            self.stage1_reasoning.as_deref()
        } else {
            self.stage2_reasoning.as_deref()
        };
        stage_specific
            .or(self.reasoning.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

fn validate_config(cfg: &RootConfig) -> Result<()> {
    let default_selector = cfg.active_default_model_selector();
    let default_provider = cfg.active_default_model();
    if !is_supported_provider_name(&default_provider) {
        return Err(anyhow!(
            "llm.default_model must be one of [claude, qwen, custom_llm, gemini, grok] or an enabled llm.models[].name, got {}",
            default_selector
        ));
    }
    let prompt_template = cfg.llm.prompt_template.trim().to_ascii_lowercase();
    if prompt_template != "big_opportunity" && prompt_template != "medium_large_opportunity" {
        return Err(anyhow!(
            "llm.prompt_template must be one of [big_opportunity, medium_large_opportunity]"
        ));
    }

    if cfg.llm.symbol.trim().is_empty() {
        return Err(anyhow!("llm.symbol is empty"));
    }
    if cfg.llm.request_timeout_secs == 0 {
        return Err(anyhow!("llm.request_timeout_secs must be > 0"));
    }
    if cfg.llm.bundle_settle_ms == 0 {
        return Err(anyhow!("llm.bundle_settle_ms must be > 0"));
    }
    validate_temp_cache_retention_config(&cfg.llm)?;
    validate_schedule_hours(
        &cfg.llm.workflow.stage1_refresh_hours,
        "llm.workflow.stage1_refresh_hours",
    )?;
    validate_schedule_minutes(
        &cfg.llm.workflow.stage2_review_minutes,
        "llm.workflow.stage2_review_minutes",
    )?;
    if cfg.llm.workflow.state_dir.trim().is_empty() {
        return Err(anyhow!("llm.workflow.state_dir is empty"));
    }
    if !matches!(
        cfg.llm
            .workflow
            .stage1
            .min_overall_quality_for_new_entry_dispatch
            .trim(),
        "high" | "medium" | "low"
    ) {
        return Err(anyhow!(
            "llm.workflow.stage1.min_overall_quality_for_new_entry_dispatch must be one of [high, medium, low]"
        ));
    }
    if cfg.llm.workflow.limits.max_live_positions_per_direction == 0 {
        return Err(anyhow!(
            "llm.workflow.limits.max_live_positions_per_direction must be > 0"
        ));
    }
    if cfg.llm.workflow.limits.max_live_entry_orders_per_direction == 0 {
        return Err(anyhow!(
            "llm.workflow.limits.max_live_entry_orders_per_direction must be > 0"
        ));
    }
    validate_workflow_watcher_config(&cfg.llm.workflow.watcher)?;
    validate_telegram_signal_decisions(&cfg.llm.telegram_signal_decisions)?;
    validate_x_signal_decisions(&cfg.llm.x_signal_decisions)?;
    if !cfg.mq.queues.contains_key(&cfg.llm.queue_key) {
        return Err(anyhow!(
            "llm.queue_key={} not found in mq.queues",
            cfg.llm.queue_key
        ));
    }

    let enabled_models = cfg
        .llm
        .models
        .iter()
        .filter(|m| m.enabled)
        .collect::<Vec<_>>();
    if enabled_models.is_empty() && default_provider == "claude" {
        return Err(anyhow!(
            "llm.models has no enabled model; claude requires at least one enabled llm.models item"
        ));
    }
    for model in &enabled_models {
        if model.name.trim().is_empty() {
            return Err(anyhow!("llm.models[].name is empty"));
        }
        if model.provider.trim().is_empty() {
            return Err(anyhow!("llm.models[].provider is empty"));
        }
        if model.model.trim().is_empty() {
            return Err(anyhow!("llm.models[].model is empty"));
        }
        if model.max_tokens == 0 {
            return Err(anyhow!("llm.models[].max_tokens must be > 0"));
        }
    }

    let selected_enabled_models = cfg.selected_enabled_models_for_default();

    let claude_needed = default_provider == "claude";
    if claude_needed && cfg.api.claude.resolved_api_key().trim().is_empty() {
        return Err(anyhow!("api.claude.api_key is empty"));
    }

    if default_provider == "qwen" {
        if cfg.api.qwen.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.qwen.api_key is empty"));
        }
        if cfg.api.qwen.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.qwen.base_api_url is empty"));
        }
        if cfg.api.qwen.model.trim().is_empty() {
            return Err(anyhow!("api.qwen.model is empty"));
        }
    }

    if default_provider == "custom_llm" {
        if cfg.api.custom_llm.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.custom_llm.api_key is empty"));
        }
        if cfg.api.custom_llm.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.custom_llm.base_api_url is empty"));
        }
        if cfg.api.custom_llm.model.trim().is_empty() {
            return Err(anyhow!("api.custom_llm.model is empty"));
        }
    }

    if default_provider == "gemini" {
        let selected_uses_openrouter = selected_enabled_models
            .iter()
            .any(|m| m.should_use_openrouter());
        let selected_uses_direct_gemini = selected_enabled_models
            .iter()
            .any(|m| !m.should_use_openrouter());

        if selected_uses_openrouter {
            if cfg.api.openrouter.resolved_api_key().trim().is_empty() {
                return Err(anyhow!("api.openrouter.api_key is empty"));
            }
            if cfg.api.openrouter.base_api_url.trim().is_empty() {
                return Err(anyhow!("api.openrouter.base_api_url is empty"));
            }
        }

        if selected_uses_direct_gemini || !selected_uses_openrouter {
            if cfg.api.gemini.resolved_api_key().trim().is_empty() {
                return Err(anyhow!("api.gemini.api_key is empty"));
            }
            if cfg.api.gemini.base_api_url.trim().is_empty() {
                return Err(anyhow!("api.gemini.base_api_url is empty"));
            }
            if cfg.api.gemini.model.trim().is_empty() {
                return Err(anyhow!("api.gemini.model is empty"));
            }
        }
    }

    if default_provider == "grok" {
        if cfg.api.grok.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.grok.api_key is empty"));
        }
        if cfg.api.grok.base_api_url.trim().is_empty() {
            return Err(anyhow!("api.grok.base_api_url is empty"));
        }
        if cfg.api.grok.model.trim().is_empty() {
            return Err(anyhow!("api.grok.model is empty"));
        }
    }

    if !cfg
        .llm
        .compatibility
        .execution_policy
        .entry_sl_remap
        .entry_to_sl_distance_pct
        .is_finite()
    {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.entry_sl_remap.entry_to_sl_distance_pct must be finite"
        ));
    }
    if !(0.0..=100.0).contains(
        &cfg.llm
            .compatibility
            .execution_policy
            .entry_sl_remap
            .entry_to_sl_distance_pct,
    ) {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.entry_sl_remap.entry_to_sl_distance_pct must be between 0 and 100"
        ));
    }
    if !cfg
        .llm
        .compatibility
        .execution_policy
        .min_distance_v
        .is_finite()
    {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.min_distance_v must be finite"
        ));
    }
    if cfg.llm.compatibility.execution_policy.min_distance_v < 0.0 {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.min_distance_v must be >= 0"
        ));
    }
    if !cfg.llm.compatibility.execution_policy.min_rr.is_finite() {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.min_rr must be finite"
        ));
    }
    if cfg.llm.compatibility.execution_policy.min_rr < 0.0 {
        return Err(anyhow!(
            "llm.compatibility.execution_policy.min_rr must be >= 0"
        ));
    }

    if cfg.llm.execution.enabled {
        if default_provider == "claude" && selected_enabled_models.len() != 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=claude requires exactly one enabled claude model"
            ));
        }
        if default_provider == "qwen" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=qwen supports at most one enabled qwen model"
            ));
        }
        if default_provider == "custom_llm" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=custom_llm supports at most one enabled custom_llm model"
            ));
        }
        if default_provider == "gemini" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=gemini supports at most one enabled gemini model"
            ));
        }
        if default_provider == "grok" && selected_enabled_models.len() > 1 {
            return Err(anyhow!(
                "llm.execution.enabled with llm.default_model=grok supports at most one enabled grok model"
            ));
        }
        if cfg.api.binance.resolved_api_key().trim().is_empty() {
            return Err(anyhow!("api.binance.api_key is empty"));
        }
        if cfg.api.binance.resolved_api_secret().trim().is_empty() {
            return Err(anyhow!("api.binance.api_secret is empty"));
        }
        if cfg.llm.execution.margin_usdt <= 0.0 {
            return Err(anyhow!("llm.execution.margin_usdt must be > 0"));
        }
        if !(0.0..=1.0).contains(&cfg.llm.execution.account_margin_ratio) {
            return Err(anyhow!(
                "llm.execution.account_margin_ratio must be between 0.0 and 1.0"
            ));
        }
        if !cfg.llm.execution.max_margin_usdt.is_finite() {
            return Err(anyhow!("llm.execution.max_margin_usdt must be finite"));
        }
        if cfg.llm.execution.max_margin_usdt < 0.0 {
            return Err(anyhow!("llm.execution.max_margin_usdt must be >= 0"));
        }
        if cfg.llm.execution.max_leverage == 0 {
            return Err(anyhow!("llm.execution.max_leverage must be > 0"));
        }
        if !cfg.llm.execution.default_leverage_ratio.is_finite() {
            return Err(anyhow!(
                "llm.execution.default_leverage_ratio must be finite"
            ));
        }
        if cfg.llm.execution.default_leverage_ratio <= 0.0 {
            return Err(anyhow!("llm.execution.default_leverage_ratio must be > 0"));
        }
        if cfg.llm.execution.recv_window_ms == 0 {
            return Err(anyhow!("llm.execution.recv_window_ms must be > 0"));
        }
    }

    Ok(())
}

fn resolve_secret(raw: &str) -> String {
    std::env::var(raw).unwrap_or_else(|_| raw.to_string())
}

fn is_supported_provider_name(value: &str) -> bool {
    matches!(value, "claude" | "qwen" | "custom_llm" | "gemini" | "grok")
}

fn validate_telegram_signal_decisions(decisions: &[String]) -> Result<()> {
    validate_signal_decisions(
        decisions,
        "llm.telegram_signal_decisions",
        "llm.telegram_signal_decisions",
    )
}

fn validate_x_signal_decisions(decisions: &[String]) -> Result<()> {
    validate_signal_decisions(
        decisions,
        "llm.x_signal_decisions",
        "llm.x_signal_decisions",
    )
}

fn validate_signal_decisions(
    decisions: &[String],
    field_label: &str,
    duplicate_field_label: &str,
) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for raw in decisions {
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(anyhow!("{} cannot contain empty values", field_label));
        }
        if !is_supported_signal_decision(&normalized) {
            return Err(anyhow!(
                "{} contains unsupported value {}; supported: [long, short, no_trade, close, add, reduce, hold, modify_tpsl, modify_maker]",
                field_label,
                raw
            ));
        }
        if !seen.insert(normalized.clone()) {
            return Err(anyhow!(
                "{} contains duplicate value {}",
                duplicate_field_label,
                normalized
            ));
        }
    }
    Ok(())
}

fn is_supported_signal_decision(value: &str) -> bool {
    matches!(
        value,
        "long"
            | "short"
            | "no_trade"
            | "close"
            | "add"
            | "reduce"
            | "hold"
            | "modify_tpsl"
            | "modify_maker"
    )
}
