use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::workflow::schema::TacticalEntryPlan;

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
    pub active_15m_window_start: Option<DateTime<Utc>>,
    #[serde(default)]
    pub filled_stopout_attempts: u8,
    #[serde(default)]
    pub last_filled_context_key: Option<String>,
    #[serde(default)]
    pub last_executed_plan_role: Option<String>,
}
