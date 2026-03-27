use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PriceZone {
    pub low: f64,
    pub high: f64,
    #[serde(default)]
    pub timeframe: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl PriceZone {
    pub fn contains(&self, value: f64) -> bool {
        value >= self.low && value <= self.high
    }

    pub fn midpoint(&self) -> f64 {
        (self.low + self.high) / 2.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecentBar {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub is_closed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrackedZone {
    pub zone_id: String,
    pub timeframe: String,
    pub role: String,
    pub low: f64,
    pub high: f64,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ZoneState {
    pub zone_id: String,
    pub position_relative: String,
    pub acceptance_state: String,
    pub failed_auction_state: String,
    #[serde(default)]
    pub last_close: Option<f64>,
    #[serde(default)]
    pub confirmed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct AuctionContext {
    #[serde(default)]
    pub tracked_zones: Vec<TrackedZone>,
    #[serde(default)]
    pub zone_states: Vec<ZoneState>,
    #[serde(default)]
    pub recent_15m_bars: Vec<RecentBar>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IndicatorSummary {
    pub symbol: String,
    pub ts_bucket: DateTime<Utc>,
    pub source_routing_key: String,
    pub indicator_count: usize,
    #[serde(default)]
    pub missing_indicator_codes: Vec<String>,
    pub position_context: Value,
    pub state_context: Value,
    pub driver_context: Value,
    pub trigger_context: Value,
    pub auction_context: AuctionContext,
    pub aux_context: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage1Meta {
    pub stage1_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct MapSummary {
    #[serde(default)]
    pub market_tradeable: bool,
    #[serde(default)]
    pub location_bias: Option<String>,
    #[serde(default)]
    pub state_summary: Option<String>,
    #[serde(default)]
    pub driver_summary: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct DriverAttribution {
    pub driver_bias: String,
    #[serde(default)]
    pub primary_driver: Option<String>,
    #[serde(default)]
    pub supporting_evidence: Vec<String>,
    #[serde(default)]
    pub conflicting_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ReevaluationTrigger {
    #[serde(default)]
    pub signals: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StopMigrationRule {
    pub after_target: String,
    pub new_stop_basis: String,
    pub new_stop_level: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DriverDeteriorationRule {
    pub driver_signal: String,
    #[serde(default)]
    pub reduce_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagementPlan {
    pub take_profit_1_basis: String,
    pub take_profit_2_basis: String,
    pub take_profit_1_level: f64,
    pub take_profit_2_level: f64,
    #[serde(default)]
    pub stop_migration_rules: Vec<StopMigrationRule>,
    #[serde(default)]
    pub reduce_on_driver_deterioration: Vec<DriverDeteriorationRule>,
    #[serde(default)]
    pub exit_full_on_driver_deterioration: Vec<DriverDeteriorationRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CurrentPath {
    pub id: String,
    pub side: String,
    pub thesis: String,
    pub activation_level: PriceZone,
    pub first_path_target: PriceZone,
    pub next_path_target: PriceZone,
    pub failure_level: PriceZone,
    pub failure_switch: String,
    pub setup_type: String,
    pub reevaluation_trigger: ReevaluationTrigger,
    pub management_plan: ManagementPlan,
    #[serde(default)]
    pub tracked_zones: Vec<TrackedZone>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage1Output {
    pub meta: Stage1Meta,
    pub monitoring_status: String,
    #[serde(default)]
    pub no_trade_reason: Option<String>,
    #[serde(default)]
    pub refresh_hints: Vec<String>,
    #[serde(default)]
    pub map_summary: Option<MapSummary>,
    #[serde(default)]
    pub current_script: Option<String>,
    #[serde(default)]
    pub driver_attribution: Option<DriverAttribution>,
    #[serde(default)]
    pub current_path: Option<CurrentPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct HardGateEvaluation {
    #[serde(default)]
    pub location_valid: bool,
    #[serde(default)]
    pub trigger_confirmed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct SoftGateEvaluation {
    #[serde(default)]
    pub state_clear: bool,
    #[serde(default)]
    pub driver_clear: bool,
    #[serde(default)]
    pub orderflow_real: bool,
    #[serde(default)]
    pub invalidation_clear: bool,
    #[serde(default)]
    pub passed_count: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRuntimeContract {
    pub monitoring_status: String,
    #[serde(default)]
    pub no_edge_reentered: bool,
    #[serde(default)]
    pub failure_level_breached: bool,
    #[serde(default)]
    pub reevaluation_trigger_hit: bool,
    #[serde(default)]
    pub activation_level_active: bool,
    #[serde(default)]
    pub setup_confirmed: bool,
    pub hard_gate: HardGateEvaluation,
    pub soft_gate: SoftGateEvaluation,
    #[serde(default)]
    pub soft_gate_min_required: u8,
    #[serde(default)]
    pub allow_execute: bool,
    #[serde(default)]
    pub request_refresh_reason: Option<String>,
    #[serde(default)]
    pub request_trigger_source: Option<String>,
    #[serde(default)]
    pub recommended_context_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestStage1Reevaluation {
    pub refresh_reason: String,
    pub trigger_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EntrySnapshotRef {
    pub context_key: String,
    pub path_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionIntent {
    pub side: String,
    pub intent_mode: String,
    pub entry_zone: PriceZone,
    #[serde(default)]
    pub trigger_price: Option<f64>,
    pub stop_loss: f64,
    pub take_profit_1: f64,
    pub take_profit_2: f64,
    pub ttl_minutes: u64,
    pub max_drift_pct: f64,
    pub path_id: String,
    pub entry_snapshot: EntrySnapshotRef,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManagementAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub context_key: String,
    pub path_id: String,
    #[serde(default)]
    pub reduce_ratio: Option<f64>,
    #[serde(default)]
    pub new_stop_loss: Option<f64>,
    #[serde(default)]
    pub take_profit_1: Option<f64>,
    #[serde(default)]
    pub take_profit_2: Option<f64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage2Decision {
    pub decision: String,
    pub reason: String,
    #[serde(default)]
    pub request_stage1_reevaluation: Option<RequestStage1Reevaluation>,
    #[serde(default)]
    pub execution_intent: Option<ExecutionIntent>,
    #[serde(default)]
    pub management_actions: Vec<ManagementAction>,
    #[serde(default)]
    pub hard_gate: Option<HardGateEvaluation>,
    #[serde(default)]
    pub soft_gate: Option<SoftGateEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EntrySnapshot {
    pub symbol: String,
    pub context_key: String,
    pub path_id: String,
    pub side: String,
    pub stop_loss: f64,
    pub take_profit_1: f64,
    pub take_profit_2: f64,
    #[serde(default)]
    pub allowed_stop_loss_levels: Vec<f64>,
    #[serde(default)]
    pub allowed_take_profit_levels: Vec<f64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage1PromptInput {
    pub task: String,
    pub indicator_summary: IndicatorSummary,
    #[serde(default)]
    pub previous_stage1_output: Option<Stage1Output>,
    pub refresh_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPosition {
    pub context_key: String,
    pub position_side: String,
    pub direction: String,
    pub quantity: f64,
    pub leverage: u32,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    #[serde(default)]
    pub current_tp_price: Option<f64>,
    #[serde(default)]
    pub current_sl_price: Option<f64>,
    #[serde(default)]
    pub entry_snapshot: Option<EntrySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowAccountContext {
    pub total_wallet_balance: f64,
    pub available_balance: f64,
    pub has_active_positions: bool,
    pub has_open_orders: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage2PromptInput {
    pub task: String,
    pub indicator_summary: IndicatorSummary,
    pub stage1_output: Stage1Output,
    pub runtime_contract: WorkflowRuntimeContract,
    #[serde(default)]
    pub active_positions: Vec<WorkflowPosition>,
    pub account: WorkflowAccountContext,
}

#[cfg(test)]
mod tests {
    use super::{
        EntrySnapshotRef, ExecutionIntent, HardGateEvaluation, ManagementAction, PriceZone,
        SoftGateEvaluation, Stage2Decision,
    };
    use serde_json::json;

    #[test]
    fn execution_intent_rejects_unknown_fields() {
        let value = json!({
            "side": "LONG",
            "intent_mode": "immediate",
            "entry_zone": {"low": 100.0, "high": 101.0},
            "trigger_price": 100.5,
            "stop_loss": 99.0,
            "take_profit_1": 103.0,
            "take_profit_2": 105.0,
            "ttl_minutes": 15,
            "max_drift_pct": 0.2,
            "path_id": "path_a",
            "entry_snapshot": {
                "context_key": "ETHUSDT:LONG",
                "path_id": "path_a"
            },
            "min_rr": 2.0
        });
        assert!(serde_json::from_value::<ExecutionIntent>(value).is_err());
    }

    #[test]
    fn stage2_decision_roundtrip_preserves_contract_fields() {
        let decision = Stage2Decision {
            decision: "EXECUTE".to_string(),
            reason: "confirmed".to_string(),
            request_stage1_reevaluation: None,
            execution_intent: Some(ExecutionIntent {
                side: "LONG".to_string(),
                intent_mode: "immediate".to_string(),
                entry_zone: PriceZone {
                    low: 100.0,
                    high: 101.0,
                    timeframe: Some("15m".to_string()),
                    label: Some("entry".to_string()),
                    reason: None,
                },
                trigger_price: Some(100.5),
                stop_loss: 99.0,
                take_profit_1: 103.0,
                take_profit_2: 105.0,
                ttl_minutes: 15,
                max_drift_pct: 0.2,
                path_id: "path_a".to_string(),
                entry_snapshot: EntrySnapshotRef {
                    context_key: "ETHUSDT:LONG".to_string(),
                    path_id: "path_a".to_string(),
                },
                reason: Some("go".to_string()),
            }),
            management_actions: vec![ManagementAction {
                action_type: "MOVE_STOP".to_string(),
                context_key: "ETHUSDT:LONG".to_string(),
                path_id: "path_prev".to_string(),
                reduce_ratio: None,
                new_stop_loss: Some(100.0),
                take_profit_1: None,
                take_profit_2: None,
                reason: Some("trail".to_string()),
            }],
            hard_gate: Some(HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            }),
            soft_gate: Some(SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            }),
        };
        let value = serde_json::to_value(&decision).expect("serialize");
        let decoded: Stage2Decision = serde_json::from_value(value).expect("decode");
        assert_eq!(decoded, decision);
    }
}
