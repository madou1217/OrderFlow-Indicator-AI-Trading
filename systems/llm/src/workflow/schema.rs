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
pub struct StrategicSummaryMeta {
    pub symbol: String,
    pub ts_bucket: DateTime<Utc>,
    pub source_routing_key: String,
    pub indicator_count: usize,
    #[serde(default)]
    pub missing_indicator_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StrategicIndicatorSummary {
    pub meta: StrategicSummaryMeta,
    pub position_layer: Value,
    pub state_layer: Value,
    pub driver_layer: Value,
    pub trigger_layer: Value,
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
    pub regime_3d: Value,
    pub location_1d: Value,
    pub location_4h: Value,
    pub price_location_class: String,
    pub key_levels: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct DriverAttribution {
    pub flow_driver: String,
    pub spot_confirming: bool,
    pub driver_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct OpportunityAssessment {
    #[serde(default)]
    pub location_quality: Option<String>,
    #[serde(default)]
    pub state_quality: Option<String>,
    #[serde(default)]
    pub driver_quality: Option<String>,
    #[serde(default)]
    pub geometry_quality: Option<String>,
    #[serde(default)]
    pub uniqueness_quality: Option<String>,
    #[serde(default)]
    pub overall_quality: Option<String>,
    #[serde(default)]
    pub disqualifiers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ScriptRejection {
    pub script: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ZoneReevaluationTrigger {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub zone_id: Option<String>,
    #[serde(default)]
    pub timeframe: Option<String>,
    #[serde(default)]
    pub min_confirmed_bars: Option<u64>,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct DriverReevaluationTrigger {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub expected_flow_driver: Option<String>,
    #[serde(default)]
    pub invalidate_when_drivers: Vec<String>,
    #[serde(default)]
    pub require_spot_confirmation: Option<bool>,
    #[serde(default)]
    pub driver_signal: Option<String>,
    #[serde(default)]
    pub min_confirmed_windows: Option<u64>,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ReevaluationTrigger {
    #[serde(default)]
    pub extreme_location: ZoneReevaluationTrigger,
    #[serde(default)]
    pub reverse_confirmation: ZoneReevaluationTrigger,
    #[serde(default)]
    pub driver_change: DriverReevaluationTrigger,
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
    pub risk_grade: String,
    #[serde(default)]
    pub activation_anchor_id: Option<String>,
    pub activation_level: PriceZone,
    #[serde(default)]
    pub first_path_target_anchor_id: Option<String>,
    pub first_path_target: PriceZone,
    #[serde(default)]
    pub next_path_target_anchor_id: Option<String>,
    pub next_path_target: PriceZone,
    #[serde(default)]
    pub failure_anchor_id: Option<String>,
    pub failure_level: PriceZone,
    #[serde(default)]
    pub failure_switch: Option<String>,
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
    pub map_summary: MapSummary,
    #[serde(default)]
    pub opportunity_assessment: OpportunityAssessment,
    #[serde(default)]
    pub script_rejections: Vec<ScriptRejection>,
    #[serde(default)]
    pub current_script: Option<String>,
    #[serde(default)]
    pub driver_attribution: Option<DriverAttribution>,
    #[serde(default)]
    pub current_path: Option<CurrentPath>,
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
    #[serde(default)]
    pub tp1_realized: bool,
    #[serde(default)]
    pub applied_driver_deterioration_signals: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage1PromptInput {
    pub task: String,
    pub strategic_indicator_summary: StrategicIndicatorSummary,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvent {
    pub event_type: String,
    pub event_ts: DateTime<Utc>,
    pub latest_price: f64,
    pub reason: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct PathAuditFlags {
    pub extreme_location: bool,
    pub reverse_confirmation: bool,
    pub driver_change: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct PathRuntimeState {
    pub path_id: String,
    pub monitoring_status: String,
    pub latest_price: f64,
    #[serde(default)]
    pub hard_invalidation: bool,
    #[serde(default)]
    pub failure_level_breached: bool,
    #[serde(default)]
    pub path_alive: bool,
    #[serde(default)]
    pub activation_level_touched: bool,
    #[serde(default)]
    pub opposing_pressure_detected: bool,
    pub audit_flags: PathAuditFlags,
    #[serde(default)]
    pub active_entry_context_keys: Vec<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TacticalEntrySnapshot {
    pub context_key: String,
    pub path_id: String,
    pub plan_role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EntryPlan {
    pub side: String,
    pub entry_profile: String,
    pub intent_mode: String,
    pub entry_activation_level: PriceZone,
    pub entry_zone: PriceZone,
    pub entry_invalidation_level: PriceZone,
    pub stop_loss: f64,
    pub take_profit_1: f64,
    pub take_profit_2: f64,
    pub ttl_minutes: u64,
    pub max_drift_pct: f64,
    pub entry_snapshot: TacticalEntrySnapshot,
    #[serde(default)]
    pub entry_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AttemptPolicy {
    pub max_filled_stopout_attempts: u8,
    pub count_unfilled_attempts: bool,
    pub time_window: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TacticalEntryPlan {
    pub path_id: String,
    pub primary_entry_plan: EntryPlan,
    pub secondary_entry_plan: EntryPlan,
    pub attempt_policy: AttemptPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage2Output {
    pub stage2_decision: String,
    #[serde(default)]
    pub tactical_entry_plan: Option<TacticalEntryPlan>,
    #[serde(default)]
    pub reevaluation_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stage2PromptInput {
    pub task: String,
    pub candidate_event: CandidateEvent,
    pub path_runtime_state: PathRuntimeState,
    #[serde(default)]
    pub previous_tactical_plan: Option<TacticalEntryPlan>,
    pub tactical_position_slice: Value,
    pub latest_15m_trigger_facts: Value,
    pub state_guardrail_snapshot: Value,
    pub driver_guardrail_snapshot: Value,
    #[serde(default)]
    pub options_guardrail_snapshot: Option<Value>,
    pub stage1_output: Stage1Output,
    #[serde(default)]
    pub active_positions: Vec<WorkflowPosition>,
    pub account: WorkflowAccountContext,
}

#[cfg(test)]
mod tests {
    use super::{
        AttemptPolicy, EntryPlan, EntrySnapshotRef, ExecutionIntent, PriceZone, Stage2Output,
        TacticalEntryPlan, TacticalEntrySnapshot,
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
    fn stage2_output_roundtrip_preserves_new_contract() {
        let output = Stage2Output {
            stage2_decision: "PATH_CONFIRMED".to_string(),
            tactical_entry_plan: Some(TacticalEntryPlan {
                path_id: "path_a".to_string(),
                primary_entry_plan: EntryPlan {
                    side: "LONG".to_string(),
                    entry_profile: "reclaim_then_hold".to_string(),
                    intent_mode: "immediate".to_string(),
                    entry_activation_level: PriceZone {
                        low: 100.0,
                        high: 101.0,
                        timeframe: Some("15m".to_string()),
                        label: Some("activation".to_string()),
                        reason: None,
                    },
                    entry_zone: PriceZone {
                        low: 100.0,
                        high: 101.0,
                        timeframe: Some("15m".to_string()),
                        label: Some("entry".to_string()),
                        reason: None,
                    },
                    entry_invalidation_level: PriceZone {
                        low: 98.5,
                        high: 99.0,
                        timeframe: Some("15m".to_string()),
                        label: Some("invalid".to_string()),
                        reason: None,
                    },
                    stop_loss: 98.8,
                    take_profit_1: 103.0,
                    take_profit_2: 105.0,
                    ttl_minutes: 15,
                    max_drift_pct: 0.2,
                    entry_snapshot: TacticalEntrySnapshot {
                        context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                        path_id: "path_a".to_string(),
                        plan_role: "primary".to_string(),
                    },
                    entry_note: "follow-through".to_string(),
                },
                secondary_entry_plan: EntryPlan {
                    side: "LONG".to_string(),
                    entry_profile: "failed_auction_reentry".to_string(),
                    intent_mode: "pullback".to_string(),
                    entry_activation_level: PriceZone {
                        low: 99.5,
                        high: 100.0,
                        timeframe: Some("15m".to_string()),
                        label: Some("activation".to_string()),
                        reason: None,
                    },
                    entry_zone: PriceZone {
                        low: 99.5,
                        high: 100.2,
                        timeframe: Some("15m".to_string()),
                        label: Some("entry".to_string()),
                        reason: None,
                    },
                    entry_invalidation_level: PriceZone {
                        low: 98.3,
                        high: 98.7,
                        timeframe: Some("15m".to_string()),
                        label: Some("invalid".to_string()),
                        reason: None,
                    },
                    stop_loss: 98.5,
                    take_profit_1: 103.0,
                    take_profit_2: 105.0,
                    ttl_minutes: 15,
                    max_drift_pct: 0.25,
                    entry_snapshot: TacticalEntrySnapshot {
                        context_key: "ETHUSDT:LONG:path_a:secondary".to_string(),
                        path_id: "path_a".to_string(),
                        plan_role: "secondary".to_string(),
                    },
                    entry_note: "deeper retry".to_string(),
                },
                attempt_policy: AttemptPolicy {
                    max_filled_stopout_attempts: 2,
                    count_unfilled_attempts: false,
                    time_window: "same_15m_window".to_string(),
                },
            }),
            reevaluation_reason: None,
        };

        let value = serde_json::to_value(&output).expect("serialize");
        let decoded: Stage2Output = serde_json::from_value(value).expect("decode");
        assert_eq!(decoded, output);
    }

    #[test]
    fn execution_intent_roundtrip_keeps_internal_execution_contract() {
        let intent = ExecutionIntent {
            side: "LONG".to_string(),
            intent_mode: "immediate".to_string(),
            entry_zone: PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
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
                context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                path_id: "path_a".to_string(),
            },
            reason: Some("execute".to_string()),
        };
        let value = serde_json::to_value(&intent).expect("serialize");
        let decoded: ExecutionIntent = serde_json::from_value(value).expect("decode");
        assert_eq!(decoded, intent);
    }
}
