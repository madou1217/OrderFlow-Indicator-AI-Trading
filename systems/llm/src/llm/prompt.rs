pub mod workflow_stage1;
pub mod workflow_stage2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowPromptStage {
    Stage1,
    Stage2,
}

fn load_prompt_asset(asset: &'static str, replacements: &[(&str, &str)]) -> String {
    let base = asset
        .strip_suffix("\r\n")
        .or_else(|| asset.strip_suffix('\n'))
        .unwrap_or(asset);
    replacements
        .iter()
        .fold(base.to_owned(), |text, (from, to)| text.replace(from, to))
}

pub fn workflow_system_prompt(
    stage: WorkflowPromptStage,
    prompt_template: &str,
    symbol: &str,
) -> String {
    let template = prompt_template.trim().to_ascii_lowercase();
    match stage {
        WorkflowPromptStage::Stage1 => workflow_stage1::system_prompt(&template, symbol),
        WorkflowPromptStage::Stage2 => workflow_stage2::system_prompt(&template, symbol),
    }
}

pub fn workflow_user_prompt_prefix(stage: WorkflowPromptStage) -> &'static str {
    match stage {
        WorkflowPromptStage::Stage1 => {
            "You are in workflow Stage1 mode. Build exactly one current script and exactly one current path object. Return only the workflow Stage1 JSON.\n\n"
        }
        WorkflowPromptStage::Stage2 => {
            "You are in workflow Stage2 mode. You may only WAIT, EXECUTE the current path, or REQUEST_STAGE1_REEVALUATION. Return only the workflow Stage2 JSON.\n\n"
        }
    }
}
