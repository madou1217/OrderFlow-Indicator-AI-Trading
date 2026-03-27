use crate::workflow::schema::{EntrySnapshot, Stage1Output, TrackedZone};
use crate::workflow::state::WorkflowState;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

fn ensure_state_dir(state_dir: &str) -> Result<PathBuf> {
    let path = PathBuf::from(state_dir);
    fs::create_dir_all(&path)
        .with_context(|| format!("create workflow state dir {}", path.display()))?;
    Ok(path)
}

fn symbol_prefix(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn sanitize_context_key(context_key: &str) -> String {
    urlencoding::encode(context_key.trim()).into_owned()
}

fn write_json<T: serde::Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    let data = serde_json::to_vec_pretty(value)
        .with_context(|| format!("serialize workflow json {}", path.display()))?;
    fs::write(path, data).with_context(|| format!("write workflow file {}", path.display()))?;
    Ok(())
}

fn read_json<T: for<'de> serde::Deserialize<'de>>(path: &Path) -> Result<T> {
    let data = fs::read(path).with_context(|| format!("read workflow file {}", path.display()))?;
    serde_json::from_slice(&data).with_context(|| format!("parse workflow json {}", path.display()))
}

pub fn workflow_state_path(state_dir: &str, symbol: &str) -> Result<PathBuf> {
    let dir = ensure_state_dir(state_dir)?;
    Ok(dir.join(format!("{}.workflow_state.json", symbol_prefix(symbol))))
}

pub fn stage1_output_path(state_dir: &str, symbol: &str) -> Result<PathBuf> {
    let dir = ensure_state_dir(state_dir)?;
    Ok(dir.join(format!("{}.stage1_output.json", symbol_prefix(symbol))))
}

pub fn tracked_zones_path(state_dir: &str, symbol: &str) -> Result<PathBuf> {
    let dir = ensure_state_dir(state_dir)?;
    Ok(dir.join(format!("{}.tracked_zones.json", symbol_prefix(symbol))))
}

pub fn entry_snapshot_path(state_dir: &str, symbol: &str, context_key: &str) -> Result<PathBuf> {
    let dir = ensure_state_dir(state_dir)?;
    Ok(dir.join(format!(
        "{}.entry_snapshot.{}.json",
        symbol_prefix(symbol),
        sanitize_context_key(context_key)
    )))
}

pub fn load_workflow_state(state_dir: &str, symbol: &str) -> Result<Option<WorkflowState>> {
    let path = workflow_state_path(state_dir, symbol)?;
    if !path.exists() {
        return Ok(None);
    }
    read_json(&path).map(Some)
}

pub fn save_workflow_state(state_dir: &str, state: &WorkflowState) -> Result<()> {
    let path = workflow_state_path(state_dir, &state.symbol)?;
    write_json(&path, state)
}

pub fn load_stage1_output(state_dir: &str, symbol: &str) -> Result<Option<Stage1Output>> {
    let path = stage1_output_path(state_dir, symbol)?;
    if !path.exists() {
        return Ok(None);
    }
    read_json(&path).map(Some)
}

pub fn save_stage1_output(state_dir: &str, symbol: &str, output: &Stage1Output) -> Result<()> {
    let path = stage1_output_path(state_dir, symbol)?;
    write_json(&path, output)
}

pub fn load_tracked_zones(state_dir: &str, symbol: &str) -> Result<Vec<TrackedZone>> {
    let path = tracked_zones_path(state_dir, symbol)?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    read_json(&path)
}

pub fn save_tracked_zones(state_dir: &str, symbol: &str, zones: &[TrackedZone]) -> Result<()> {
    let path = tracked_zones_path(state_dir, symbol)?;
    write_json(&path, zones)
}

pub fn save_entry_snapshot(state_dir: &str, snapshot: &EntrySnapshot) -> Result<()> {
    let path = entry_snapshot_path(state_dir, &snapshot.symbol, &snapshot.context_key)?;
    write_json(&path, snapshot)
}

pub fn delete_entry_snapshot(state_dir: &str, symbol: &str, context_key: &str) -> Result<()> {
    let path = entry_snapshot_path(state_dir, symbol, context_key)?;
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("remove workflow file {}", path.display()))?;
    }
    Ok(())
}

pub fn load_entry_snapshots_for_symbol(
    state_dir: &str,
    symbol: &str,
) -> Result<Vec<EntrySnapshot>> {
    let dir = ensure_state_dir(state_dir)?;
    let prefix = format!("{}.entry_snapshot.", symbol_prefix(symbol));
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(dir).context("read workflow state dir")? {
        let entry = entry.context("read workflow state entry")?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.starts_with(&prefix) || !file_name.ends_with(".json") {
            continue;
        }
        snapshots.push(read_json(&entry.path())?);
    }
    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use super::{
        delete_entry_snapshot, load_entry_snapshots_for_symbol, load_workflow_state,
        save_entry_snapshot, save_workflow_state,
    };
    use crate::workflow::schema::EntrySnapshot;
    use crate::workflow::state::WorkflowState;
    use chrono::Utc;
    use std::fs;
    use uuid::Uuid;

    #[test]
    fn entry_snapshots_do_not_override_across_context_keys() {
        let state_dir = format!("/tmp/workflow_test_{}", Uuid::new_v4());
        let now = Utc::now();
        let a = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            stop_loss: 100.0,
            take_profit_1: 110.0,
            take_profit_2: 120.0,
            allowed_stop_loss_levels: vec![100.0, 105.0],
            allowed_take_profit_levels: vec![110.0, 120.0],
            created_at: now,
            updated_at: now,
        };
        let b = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:SHORT".to_string(),
            path_id: "path_b".to_string(),
            side: "SHORT".to_string(),
            stop_loss: 120.0,
            take_profit_1: 110.0,
            take_profit_2: 100.0,
            allowed_stop_loss_levels: vec![120.0, 115.0],
            allowed_take_profit_levels: vec![110.0, 100.0],
            created_at: now,
            updated_at: now,
        };
        save_entry_snapshot(&state_dir, &a).expect("save a");
        save_entry_snapshot(&state_dir, &b).expect("save b");

        let all = load_entry_snapshots_for_symbol(&state_dir, "ETHUSDT").expect("load all");
        let loaded_a = all
            .iter()
            .find(|snapshot| snapshot.context_key == "ETHUSDT:LONG")
            .expect("find a");
        let loaded_b = all
            .iter()
            .find(|snapshot| snapshot.context_key == "ETHUSDT:SHORT")
            .expect("find b");

        assert_eq!(loaded_a.path_id, "path_a");
        assert_eq!(loaded_b.path_id, "path_b");
        assert_eq!(all.len(), 2);

        delete_entry_snapshot(&state_dir, "ETHUSDT", "ETHUSDT:LONG").expect("delete a");
        delete_entry_snapshot(&state_dir, "ETHUSDT", "ETHUSDT:SHORT").expect("delete b");
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn workflow_state_roundtrips() {
        let state_dir = format!("/tmp/workflow_test_state_{}", Uuid::new_v4());
        let state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            last_stage1_ts: Some(Utc::now()),
        };
        save_workflow_state(&state_dir, &state).expect("save workflow state");
        let loaded = load_workflow_state(&state_dir, "ETHUSDT")
            .expect("load workflow state")
            .expect("workflow state exists");
        assert_eq!(loaded, state);
        let _ = fs::remove_dir_all(&state_dir);
    }
}
