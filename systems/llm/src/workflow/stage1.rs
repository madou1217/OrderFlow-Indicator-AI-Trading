use crate::workflow::schema::{Stage1Output, Stage1PromptInput, StrategicIndicatorSummary};

pub fn build_stage1_prompt_input(
    strategic_indicator_summary: StrategicIndicatorSummary,
    previous_stage1_output: Option<Stage1Output>,
    refresh_reason: String,
) -> Stage1PromptInput {
    Stage1PromptInput {
        task: "Execute the 3D/1D/4H map, choose exactly one current script, and build exactly one strategic path when the market has a valid edge.".to_string(),
        strategic_indicator_summary,
        previous_stage1_output,
        refresh_reason,
    }
}
