use crate::workflow::schema::{IndicatorSummary, Stage1Output, Stage1PromptInput};

pub fn build_stage1_prompt_input(
    indicator_summary: IndicatorSummary,
    previous_stage1_output: Option<Stage1Output>,
    refresh_reason: String,
) -> Stage1PromptInput {
    Stage1PromptInput {
        task: "Build the Stage1 map, choose exactly one script and path only when the map is active, and return null current_script/current_path when the result is no_edge.".to_string(),
        indicator_summary,
        previous_stage1_output,
        refresh_reason,
    }
}
