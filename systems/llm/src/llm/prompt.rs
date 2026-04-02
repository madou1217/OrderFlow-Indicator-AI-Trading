pub mod workflow_stage1;
pub mod workflow_stage2a;
pub mod workflow_stage2b;
pub mod workflow_stage2c;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowPromptStage {
    Stage1,
    Stage2A,
    Stage2B,
    Stage2C,
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
        WorkflowPromptStage::Stage2A => workflow_stage2a::system_prompt(&template, symbol),
        WorkflowPromptStage::Stage2B => workflow_stage2b::system_prompt(&template, symbol),
        WorkflowPromptStage::Stage2C => workflow_stage2c::system_prompt(&template, symbol),
    }
}

pub fn workflow_user_prompt_prefix(stage: WorkflowPromptStage) -> &'static str {
    match stage {
        WorkflowPromptStage::Stage1 => {
            "You are in workflow Stage1 mode. Return only the workflow Stage1 JSON.\n\n"
        }
        WorkflowPromptStage::Stage2A => {
            "You are a path auditor and tactical execution planner. Audit the current strategic path first, then output either PATH_CONFIRMED_ENTRY with tactical_entry_plan, PATH_CONFIRMED_WAIT without a trade, or REQUEST_STAGE1_REEVALUATION. Return only the workflow Stage2A JSON.\n\n"
        }
        WorkflowPromptStage::Stage2B => {
            "You are a top-tier 4h-1d order flow trader reviewing an active position. First audit whether the supporting path is still alive. If not, output conditional close/de-risk actions. If yes, output a watcher-managed conditional position management plan. Return only the workflow Stage2B JSON.\n\n"
        }
        WorkflowPromptStage::Stage2C => {
            "You are a top-tier 4h-1d order flow trader reviewing active pending orders. First audit whether the supporting path is still alive. If not, output conditional cancel/remove actions. If yes, output a watcher-managed conditional pending-order plan. Return only the workflow Stage2C JSON.\n\n"
        }
    }
}
