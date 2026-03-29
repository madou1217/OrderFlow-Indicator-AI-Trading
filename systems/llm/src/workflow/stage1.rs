use crate::workflow::schema::{Stage1Output, Stage1PromptInput, StrategicIndicatorSummary};

pub fn build_stage1_prompt_input(
    mut strategic_indicator_summary: StrategicIndicatorSummary,
    previous_stage1_output: Option<Stage1Output>,
    refresh_reason: String,
) -> Stage1PromptInput {
    if strategic_indicator_summary
        .structural_refresh_context
        .refresh_cause
        .trim()
        .is_empty()
    {
        strategic_indicator_summary
            .structural_refresh_context
            .refresh_cause = refresh_reason.clone();
    }
    Stage1PromptInput {
        task: "Build the 3D/4H/1D market map, choose the primary script, and construct the strategic path"
            .to_string(),
        strategic_indicator_summary,
        previous_stage1_output,
        refresh_reason,
    }
}
