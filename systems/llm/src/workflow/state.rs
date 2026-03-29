use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::workflow::schema::{
    PendingOrderManagementPlan, PositionManagementPlan, PostFillBracketTemplate, TacticalEntryPlan,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkflowState {
    pub symbol: String,
    #[serde(default)]
    pub pending_stage1_refresh_reason: Option<String>,
    #[serde(default)]
    pub last_stage1_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub approved_tactical_plan: Option<TacticalEntryPlan>,
    #[serde(default)]
    pub approved_tactical_plan_updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub approved_position_management_plans: BTreeMap<String, PositionManagementPlan>,
    #[serde(default)]
    pub approved_position_management_plans_updated_at: BTreeMap<String, DateTime<Utc>>,
    #[serde(default)]
    pub approved_pending_order_management_plans: BTreeMap<String, PendingOrderManagementPlan>,
    #[serde(default)]
    pub approved_pending_order_management_plans_updated_at: BTreeMap<String, DateTime<Utc>>,
    #[serde(default)]
    pub pending_entry_bracket_template_override: Option<PostFillBracketTemplate>,
    #[serde(default)]
    pub active_15m_window_start: Option<DateTime<Utc>>,
    #[serde(default)]
    pub filled_stopout_attempts: u8,
    #[serde(default)]
    pub last_filled_context_key: Option<String>,
}
