use super::load_prompt_asset;

const BASE_PROMPT: &str = include_str!("workflow_stage2b/base.txt");

pub fn system_prompt(_template: &str, symbol: &str) -> String {
    load_prompt_asset(BASE_PROMPT, &[("__SYMBOL__", symbol)])
}
