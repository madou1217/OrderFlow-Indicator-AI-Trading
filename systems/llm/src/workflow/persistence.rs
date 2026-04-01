use crate::workflow::parser::parse_stage1_output;
use crate::workflow::schema::{EntrySnapshot, Stage1Output, TrackedZone};
use crate::workflow::state::WorkflowState;
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::{Map, Value};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::warn;

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

fn plan_context_key(value: &Value) -> Option<String> {
    value
        .get("actions")
        .and_then(Value::as_array)
        .and_then(|actions| actions.first())
        .and_then(|action| action.get("context_key"))
        .and_then(Value::as_str)
        .map(|context_key| context_key.to_string())
}

fn remove_legacy_workflow_state_fields(object: &mut Map<String, Value>) {
    for field in [
        "last_executed_plan_role",
        "last_stage2_event_fingerprint",
        "last_stage2_event_at",
    ] {
        object.remove(field);
    }
}

fn migrate_legacy_stage1_output_fields(value: &mut Value) -> bool {
    let Some(root) = value.as_object_mut() else {
        return false;
    };
    let mut migrated = false;
    for field in ["script_rejections", "management_plan"] {
        migrated |= root.remove(field).is_some();
    }
    let Some(map_summary) = root.get_mut("map_summary").and_then(Value::as_object_mut) else {
        if let Some(current_path) = root.get_mut("current_path").and_then(Value::as_object_mut) {
            if !current_path.contains_key("strategic_activation_level") {
                if let Some(legacy_activation_level) = current_path.remove("activation_level") {
                    current_path.insert(
                        "strategic_activation_level".to_string(),
                        legacy_activation_level,
                    );
                    migrated = true;
                }
            } else {
                migrated |= current_path.remove("activation_level").is_some();
            }
        }
        return migrated;
    };

    if map_summary.contains_key("location_3d") {
        migrated |= map_summary.remove("regime_3d").is_some();
    } else if let Some(regime_3d) = map_summary.remove("regime_3d") {
        map_summary.insert("location_3d".to_string(), regime_3d);
        migrated = true;
    }

    if let Some(current_path) = root.get_mut("current_path").and_then(Value::as_object_mut) {
        if !current_path.contains_key("strategic_activation_level") {
            if let Some(legacy_activation_level) = current_path.remove("activation_level") {
                current_path.insert(
                    "strategic_activation_level".to_string(),
                    legacy_activation_level,
                );
                migrated = true;
            }
        } else {
            migrated |= current_path.remove("activation_level").is_some();
        }
    }

    migrated
}

fn migrate_legacy_plan_fields(
    object: &mut Map<String, Value>,
    legacy_plan_field: &str,
    legacy_updated_at_field: &str,
    next_plan_field: &str,
    next_updated_at_field: &str,
) {
    let Some(plan_value) = object.remove(legacy_plan_field) else {
        object.remove(legacy_updated_at_field);
        return;
    };
    let Some(context_key) = plan_context_key(&plan_value) else {
        object.remove(legacy_updated_at_field);
        return;
    };

    let mut plans = Map::new();
    plans.insert(context_key.clone(), plan_value);
    object.insert(next_plan_field.to_string(), Value::Object(plans));

    if let Some(updated_at_value) = object.remove(legacy_updated_at_field) {
        let mut timestamps = Map::new();
        timestamps.insert(context_key, updated_at_value);
        object.insert(next_updated_at_field.to_string(), Value::Object(timestamps));
    }
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

fn quarantine_invalid_file(path: &Path, reason: &str) -> Result<PathBuf> {
    let suffix = Utc::now().format("%Y%m%dT%H%M%SZ");
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workflow_state.json");
    let quarantined = path.with_file_name(format!("{file_name}.invalid.{suffix}.bak"));
    fs::rename(path, &quarantined).with_context(|| {
        format!(
            "quarantine invalid workflow file {} because {}",
            path.display(),
            reason
        )
    })?;
    Ok(quarantined)
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
    let data = fs::read(&path).with_context(|| format!("read workflow file {}", path.display()))?;
    let mut value: Value = serde_json::from_slice(&data)
        .with_context(|| format!("parse workflow json {}", path.display()))?;
    if let Some(object) = value.as_object_mut() {
        remove_legacy_workflow_state_fields(object);
        migrate_legacy_plan_fields(
            object,
            "approved_position_management_plan",
            "approved_position_management_plan_updated_at",
            "approved_position_management_plans",
            "approved_position_management_plans_updated_at",
        );
        migrate_legacy_plan_fields(
            object,
            "approved_pending_order_management_plan",
            "approved_pending_order_management_plan_updated_at",
            "approved_pending_order_management_plans",
            "approved_pending_order_management_plans_updated_at",
        );
    }
    serde_json::from_value(value)
        .with_context(|| format!("parse workflow json {}", path.display()))
        .map(Some)
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
    let data = fs::read(&path).with_context(|| format!("read workflow file {}", path.display()))?;
    let mut value: Value = match serde_json::from_slice(&data)
        .with_context(|| format!("parse workflow json {}", path.display()))
    {
        Ok(value) => value,
        Err(err) => {
            let quarantined = quarantine_invalid_file(&path, "invalid_json")?;
            warn!(
                path = %path.display(),
                quarantined = %quarantined.display(),
                error = %err,
                "quarantined invalid persisted stage1_output json"
            );
            return Ok(None);
        }
    };
    let migrated = migrate_legacy_stage1_output_fields(&mut value);
    match parse_stage1_output(value) {
        Ok(output) => {
            if migrated {
                write_json(&path, &output)?;
            }
            Ok(Some(output))
        }
        Err(err) => {
            let quarantined = quarantine_invalid_file(&path, "schema_mismatch")?;
            warn!(
                path = %path.display(),
                quarantined = %quarantined.display(),
                error = %err,
                "quarantined incompatible persisted stage1_output"
            );
            Ok(None)
        }
    }
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
        delete_entry_snapshot, load_entry_snapshots_for_symbol, load_stage1_output,
        load_workflow_state, save_entry_snapshot, save_workflow_state, stage1_output_path,
    };
    use crate::workflow::schema::EntrySnapshot;
    use crate::workflow::state::WorkflowState;
    use chrono::Utc;
    use serde_json::Value;
    use std::fs;
    use std::path::PathBuf;
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
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("immediate".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 100.0,
            take_profit_1: 110.0,
            take_profit_2: 120.0,
            allowed_stop_loss_levels: vec![100.0, 105.0],
            allowed_take_profit_levels: vec![110.0, 120.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: now,
            updated_at: now,
        };
        let b = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:SHORT".to_string(),
            path_id: "path_b".to_string(),
            side: "SHORT".to_string(),
            entry_profile: Some("pullback_acceptance".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 120.0,
            take_profit_1: 110.0,
            take_profit_2: 100.0,
            allowed_stop_loss_levels: vec![120.0, 115.0],
            allowed_take_profit_levels: vec![110.0, 100.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
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
            approved_tactical_plan_source_ts_bucket: Some(Utc::now()),
            ..WorkflowState::default()
        };
        save_workflow_state(&state_dir, &state).expect("save workflow state");
        let loaded = load_workflow_state(&state_dir, "ETHUSDT")
            .expect("load workflow state")
            .expect("workflow state exists");
        assert_eq!(loaded, state);
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn load_workflow_state_migrates_legacy_single_plan_fields() {
        let state_dir = format!("/tmp/workflow_test_state_legacy_{}", Uuid::new_v4());
        fs::create_dir_all(&state_dir).expect("create state dir");
        let path = super::workflow_state_path(&state_dir, "ETHUSDT").expect("state path");
        let now = Utc::now();
        let legacy = serde_json::json!({
            "symbol": "ETHUSDT",
            "pending_stage1_refresh_reason": null,
            "last_stage1_ts": null,
            "approved_tactical_plan": null,
            "approved_tactical_plan_updated_at": null,
            "approved_position_management_plan": {
                "path_id": "path_a",
                "exposure_state": "in_position",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "hold",
                    "context_key": "ETHUSDT:LONG:path_a",
                    "path_id": "path_a",
                    "trigger_condition": null,
                    "execution_price": null,
                    "add_ratio": null,
                    "reuse_current_entry_template": null,
                    "reduce_ratio": null,
                    "new_stop_loss": null,
                    "reuse_current_bracket_template": null,
                    "take_profit_1": null,
                    "take_profit_2": null,
                    "reason": "hold"
                }],
                "management_note": "hold"
            },
            "approved_position_management_plan_updated_at": now,
            "approved_pending_order_management_plan": {
                "path_id": "path_a",
                "exposure_state": "flat_with_live_entry_orders",
                "path_live_assessment": "live",
                "path_assessment_reason": null,
                "actions": [{
                    "action_type": "keep_order",
                    "context_key": "ETHUSDT:LONG:path_a",
                    "path_id": "path_a",
                    "trigger_condition": null,
                    "execution_price": null,
                    "replacement_entry_zone": null,
                    "replacement_entry_invalidation_level": null,
                    "replacement_stop_loss": null,
                    "reuse_current_entry_template": null,
                    "post_fill_bracket_template": null,
                    "reason": "keep"
                }],
                "management_note": "keep"
            },
            "approved_pending_order_management_plan_updated_at": now,
            "pending_entry_bracket_template_override": null,
            "active_15m_window_start": null,
            "filled_stopout_attempts": 0,
            "last_filled_context_key": null,
            "last_executed_plan_role": "secondary",
            "last_stage2_event_fingerprint": "legacy-stage2-fingerprint",
            "last_stage2_event_at": now
        });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy"),
        )
        .expect("write legacy state");

        let loaded = load_workflow_state(&state_dir, "ETHUSDT")
            .expect("load state")
            .expect("state exists");

        assert!(loaded
            .approved_position_management_plans
            .contains_key("ETHUSDT:LONG:path_a"));
        assert!(loaded
            .approved_position_management_plans_updated_at
            .contains_key("ETHUSDT:LONG:path_a"));
        assert!(loaded
            .approved_pending_order_management_plans
            .contains_key("ETHUSDT:LONG:path_a"));
        assert!(loaded
            .approved_pending_order_management_plans_updated_at
            .contains_key("ETHUSDT:LONG:path_a"));
        let loaded_position_plan = loaded
            .approved_position_management_plans
            .get("ETHUSDT:LONG:path_a")
            .expect("loaded position plan");
        assert!(loaded_position_plan.actions[0].trigger_condition.is_none());
        assert!(loaded_position_plan.actions[0].execution_price.is_none());
        let loaded_pending_plan = loaded
            .approved_pending_order_management_plans
            .get("ETHUSDT:LONG:path_a")
            .expect("loaded pending plan");
        assert!(loaded_pending_plan.actions[0].trigger_condition.is_none());
        assert!(loaded_pending_plan.actions[0].execution_price.is_none());

        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn load_stage1_output_quarantines_incompatible_legacy_file() {
        let state_dir = format!("/tmp/workflow_test_stage1_{}", Uuid::new_v4());
        fs::create_dir_all(&state_dir).expect("create state dir");
        let path = stage1_output_path(&state_dir, "ETHUSDT").expect("stage1 path");
        let legacy = serde_json::json!({
            "meta": {"stage1_ts": Utc::now()},
            "monitoring_status": "active",
            "no_trade_reason": null,
            "refresh_hints": [],
            "map_summary": {
                "market_tradeable": true,
                "location_bias": "legacy"
            },
            "current_script": "legacy prose",
            "driver_attribution": null,
            "current_path": null
        });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy"),
        )
        .expect("write legacy file");

        let loaded = load_stage1_output(&state_dir, "ETHUSDT").expect("load stage1");
        assert!(loaded.is_none());
        assert!(!path.exists());

        let backups = fs::read_dir(&state_dir)
            .expect("read dir")
            .filter_map(|entry| entry.ok().map(|item| item.path()))
            .collect::<Vec<PathBuf>>();
        assert_eq!(backups.len(), 1);
        let backup_name = backups[0]
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        assert!(backup_name.contains("ETHUSDT.stage1_output.json.invalid."));

        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn load_stage1_output_migrates_legacy_regime_3d_and_rewrites_file() {
        let state_dir = format!("/tmp/workflow_test_stage1_migrate_{}", Uuid::new_v4());
        fs::create_dir_all(&state_dir).expect("create state dir");
        let path = stage1_output_path(&state_dir, "ETHUSDT").expect("stage1 path");
        let legacy = serde_json::json!({
            "meta": {"stage1_ts": Utc::now()},
            "monitoring_status": "no_edge",
            "no_trade_reason": "path_not_actionable",
            "refresh_hints": [],
            "map_summary": {
                "regime_3d": {"summary": "legacy 3d"},
                "location_1d": {"summary": "legacy 1d"},
                "location_4h": {"summary": "legacy 4h"},
                "price_location_class": "value_edge",
                "key_levels": {}
            },
            "opportunity_assessment": {
                "location_quality": "low",
                "state_quality": "low",
                "driver_quality": "low",
                "geometry_quality": "low",
                "uniqueness_quality": "low",
                "overall_quality": "low",
                "disqualifiers": ["legacy cache"]
            },
            "script_rejections": [],
            "management_plan": {"legacy": true},
            "current_script": null,
            "driver_attribution": null,
            "current_path": null
        });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy"),
        )
        .expect("write legacy stage1");

        let loaded = load_stage1_output(&state_dir, "ETHUSDT")
            .expect("load stage1")
            .expect("stage1 exists");
        assert_eq!(loaded.map_summary.location_3d["summary"], "legacy 3d");

        let rewritten: Value =
            serde_json::from_slice(&fs::read(&path).expect("read rewritten stage1"))
                .expect("parse rewritten stage1");
        let map_summary = rewritten
            .get("map_summary")
            .and_then(Value::as_object)
            .expect("map summary");
        assert!(map_summary.contains_key("location_3d"));
        assert!(!map_summary.contains_key("regime_3d"));

        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn load_stage1_output_migrates_legacy_activation_level_to_strategic_name() {
        let state_dir = format!(
            "/tmp/workflow_test_stage1_activation_migrate_{}",
            Uuid::new_v4()
        );
        fs::create_dir_all(&state_dir).expect("create state dir");
        let path = stage1_output_path(&state_dir, "ETHUSDT").expect("stage1 path");
        let legacy = serde_json::json!({
            "meta": {"stage1_ts": Utc::now()},
            "monitoring_status": "active",
            "no_trade_reason": null,
            "refresh_hints": [],
            "map_summary": {
                "location_3d": {"summary": "legacy 3d", "notes": []},
                "location_1d": {"summary": "legacy 1d", "notes": []},
                "location_4h": {"summary": "legacy 4h", "notes": []},
                "price_location_class": "value_edge",
                "key_levels": {"levels": []}
            },
            "opportunity_assessment": {
                "overall_quality": "medium"
            },
            "current_script": "value_return",
            "driver_attribution": {
                "flow_driver": "mixed",
                "spot_confirming": true,
                "driver_note": "legacy"
            },
            "current_path": {
                "id": "path_a",
                "side": "LONG",
                "thesis": "legacy path",
                "risk_grade": "countertrend_repair",
                "activation_anchor_id": null,
                "activation_level": {
                    "low": 100.0,
                    "high": 101.0,
                    "timeframe": "4h",
                    "label": "activation",
                    "reason": "legacy"
                },
                "first_path_target_anchor_id": null,
                "first_path_target": {
                    "low": 103.0,
                    "high": 104.0,
                    "timeframe": "4h",
                    "label": "tp1",
                    "reason": "legacy"
                },
                "next_path_target_anchor_id": null,
                "next_path_target": {
                    "low": 106.0,
                    "high": 107.0,
                    "timeframe": "1d",
                    "label": "tp2",
                    "reason": "legacy"
                },
                "failure_anchor_id": null,
                "failure_level": {
                    "low": 98.0,
                    "high": 99.0,
                    "timeframe": "4h",
                    "label": "failure",
                    "reason": "legacy"
                },
                "failure_switch": null,
                "setup_type": "B_reversal",
                "reevaluation_trigger": {
                    "extreme_location": {},
                    "reverse_confirmation": {},
                    "driver_change": {}
                },
                "tracked_zones": []
            }
        });
        fs::write(
            &path,
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy"),
        )
        .expect("write legacy stage1");

        let loaded = load_stage1_output(&state_dir, "ETHUSDT")
            .expect("load stage1")
            .expect("stage1 exists");
        assert_eq!(
            loaded
                .current_path
                .as_ref()
                .expect("current path")
                .strategic_activation_level
                .low,
            100.0
        );

        let rewritten: Value =
            serde_json::from_slice(&fs::read(&path).expect("read rewritten stage1"))
                .expect("parse rewritten stage1");
        let current_path = rewritten
            .get("current_path")
            .and_then(Value::as_object)
            .expect("current path");
        assert!(current_path.contains_key("strategic_activation_level"));
        assert!(!current_path.contains_key("activation_level"));

        let _ = fs::remove_dir_all(&state_dir);
    }
}
