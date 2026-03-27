use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct WorkflowState {
    pub symbol: String,
    #[serde(default)]
    pub pending_stage1_refresh_reason: Option<String>,
    #[serde(default)]
    pub last_stage1_ts: Option<DateTime<Utc>>,
}
