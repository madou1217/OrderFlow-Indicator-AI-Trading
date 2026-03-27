use crate::workflow::schema::{IndicatorSummary, Stage1Output, Stage1PromptInput};

pub fn build_stage1_prompt_input(
    indicator_summary: IndicatorSummary,
    previous_stage1_output: Option<Stage1Output>,
    refresh_reason: String,
) -> Stage1PromptInput {
    Stage1PromptInput {
        task: "执行地图、剧本选择、path object 构建、驱动归因".to_string(),
        indicator_summary,
        previous_stage1_output,
        refresh_reason,
    }
}
