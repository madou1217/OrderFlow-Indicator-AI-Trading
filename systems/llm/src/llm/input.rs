use crate::execution::binance::TradingStateSnapshot;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct ModelInvocationInput {
    pub symbol: String,
    pub ts_bucket: DateTime<Utc>,
    pub window_code: String,
    pub indicator_count: usize,
    pub source_routing_key: String,
    pub source_published_at: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub indicators: Value,
    pub missing_indicator_codes: Vec<String>,
    pub trading_state: Option<TradingStateSnapshot>,
    pub management_snapshot: Option<ManagementSnapshotForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManagementSnapshotForLlm {
    pub context_state: String,
    pub has_active_positions: bool,
    pub has_open_orders: bool,
    pub active_position_count: usize,
    pub open_order_count: usize,
    pub positions: Vec<PositionSummaryForLlm>,
    pub pending_order: Option<PendingOrderSummaryForLlm>,
    pub last_management_reason: Option<String>,
    pub position_context: Option<PositionContextForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionSummaryForLlm {
    pub position_side: String,
    pub direction: String,
    pub quantity: f64,
    pub leverage: u32,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub pnl_by_latest_price: f64,
    pub current_tp_price: Option<f64>,
    pub current_sl_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingOrderSummaryForLlm {
    pub position_side: String,
    pub direction: String,
    pub quantity: f64,
    pub leverage: Option<u32>,
    pub entry_price: Option<f64>,
    pub current_tp_price: Option<f64>,
    pub current_sl_price: Option<f64>,
    pub planned_tp_price: Option<f64>,
    pub planned_tp_source: Option<String>,
    pub planned_sl_price: Option<f64>,
    pub planned_sl_source: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionContextForLlm {
    pub original_qty: f64,
    pub current_qty: f64,
    pub current_pct_of_original: f64,
    pub effective_leverage: Option<u32>,
    pub effective_entry_price: Option<f64>,
    pub effective_take_profit: Option<f64>,
    pub effective_stop_loss: Option<f64>,
    pub reduction_history: Vec<ReductionHistoryItemForLlm>,
    pub times_reduced_at_current_level: usize,
    pub last_management_action: Option<String>,
    pub last_management_reason: Option<String>,
    pub entry_context: Option<EntryContextForLlm>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReductionHistoryItemForLlm {
    pub time: String,
    pub qty_ratio: f64,
    pub reason_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EntryContextForLlm {
    pub entry_strategy: Option<String>,
    pub stop_model: Option<String>,
    pub entry_mode: Option<String>,
    pub original_tp: Option<f64>,
    pub original_sl: Option<f64>,
    pub sweep_wick_extreme: Option<f64>,
    pub horizon: Option<String>,
    pub entry_reason: String,
}
