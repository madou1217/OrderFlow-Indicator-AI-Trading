use crate::execution::binance::TradingStateSnapshot;
use crate::workflow::predicate::{
    failed_auction_confirmed, reaccept_inside_value, zone_acceptance_above, zone_acceptance_below,
};
use crate::workflow::schema::{
    CandidateEvent, EntrySnapshot, PathAuditFlags, PathRuntimeState, Stage1Output,
    Stage2PromptInput, StrategicIndicatorSummary, TacticalEntryPlan, WorkflowAccountContext,
    WorkflowPosition,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

fn value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn flatten_blob(value: &Value) -> String {
    value.to_string().to_ascii_lowercase()
}

fn contains_any_keyword(blob: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|keyword| blob.contains(keyword))
}

fn find_bool_key(value: &Value, target: &str) -> Option<bool> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key.eq_ignore_ascii_case(target) {
                    if let Some(found) = child.as_bool() {
                        return Some(found);
                    }
                }
                if let Some(found) = find_bool_key(child, target) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|child| find_bool_key(child, target)),
        _ => None,
    }
}

fn find_f64_key(value: &Value, target: &str) -> Option<f64> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key.eq_ignore_ascii_case(target) {
                    if let Some(found) = child.as_f64() {
                        return Some(found);
                    }
                }
                if let Some(found) = find_f64_key(child, target) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|child| find_f64_key(child, target)),
        _ => None,
    }
}

fn context_child<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or(&Value::Null)
}

fn window_slice(value: &Value, key: &str, windows: &[&str]) -> Value {
    let Some(source) = value.get(key).and_then(Value::as_object) else {
        return Value::Null;
    };
    let selected = windows
        .iter()
        .filter_map(|window| {
            source
                .get(*window)
                .cloned()
                .map(|entry| ((*window).to_string(), entry))
        })
        .collect::<serde_json::Map<_, _>>();
    Value::Object(selected)
}

fn object_slice(value: &Value, keys: &[&str]) -> Value {
    let Some(source) = value.as_object() else {
        return Value::Null;
    };
    let selected = keys
        .iter()
        .filter_map(|key| {
            source
                .get(*key)
                .cloned()
                .map(|entry| ((*key).to_string(), entry))
        })
        .collect::<Map<_, _>>();
    Value::Object(selected)
}

fn latest_series_point(value: &Value, key: &str, window: &str) -> Value {
    value
        .get(key)
        .and_then(|series| series.get(window))
        .and_then(|window_data| window_data.get("latest_point"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn zone_envelope(stage1_output: &Stage1Output) -> Option<(f64, f64)> {
    let path = stage1_output.current_path.as_ref()?;
    let mut lows = vec![
        path.activation_level.low,
        path.first_path_target.low,
        path.next_path_target.low,
        path.failure_level.low,
    ];
    let mut highs = vec![
        path.activation_level.high,
        path.first_path_target.high,
        path.next_path_target.high,
        path.failure_level.high,
    ];
    for zone in &path.tracked_zones {
        lows.push(zone.low);
        highs.push(zone.high);
    }
    Some((
        lows.into_iter().fold(f64::INFINITY, f64::min),
        highs.into_iter().fold(f64::NEG_INFINITY, f64::max),
    ))
}

fn price_within_envelope(price: f64, envelope: (f64, f64)) -> bool {
    price >= envelope.0 && price <= envelope.1
}

fn latest_price(summary: &StrategicIndicatorSummary) -> Option<f64> {
    summary
        .auction_context
        .recent_15m_bars
        .last()
        .map(|bar| bar.close)
}

fn latest_bar_time(summary: &StrategicIndicatorSummary) -> Option<DateTime<Utc>> {
    summary
        .auction_context
        .recent_15m_bars
        .last()
        .map(|bar| bar.close_time)
}

fn orderbook_depth(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.trigger_layer, "orderbook_depth")
}

fn open_interest(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.state_layer, "open_interest")
}

fn long_short_ratios(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.state_layer, "long_short_ratios")
}

fn funding_rate(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.state_layer, "funding_rate")
}

fn vpin(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.state_layer, "vpin")
}

fn cvd_pack(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.driver_layer, "cvd_pack")
}

fn divergence(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.driver_layer, "divergence")
}

fn whale_trades(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.driver_layer, "whale_trades")
}

fn footprint(summary: &StrategicIndicatorSummary) -> &Value {
    context_child(&summary.trigger_layer, "footprint")
}

fn absorption<'a>(summary: &'a StrategicIndicatorSummary, side: &str) -> &'a Value {
    match side {
        "LONG" => {
            let bullish = context_child(&summary.trigger_layer, "bullish_absorption");
            if value_present(bullish) {
                bullish
            } else {
                context_child(&summary.trigger_layer, "absorption")
            }
        }
        "SHORT" => {
            let bearish = context_child(&summary.trigger_layer, "bearish_absorption");
            if value_present(bearish) {
                bearish
            } else {
                context_child(&summary.trigger_layer, "absorption")
            }
        }
        _ => &Value::Null,
    }
}

fn exhaustion<'a>(summary: &'a StrategicIndicatorSummary, side: &str) -> &'a Value {
    match side {
        "LONG" => context_child(&summary.trigger_layer, "selling_exhaustion"),
        "SHORT" => context_child(&summary.trigger_layer, "buying_exhaustion"),
        _ => &Value::Null,
    }
}

fn side_keywords(side: &str) -> (&'static [&'static str], &'static [&'static str]) {
    match side {
        "LONG" => (
            &["buy", "bull", "long", "bid", "up"],
            &["sell", "bear", "short", "ask", "down"],
        ),
        "SHORT" => (
            &["sell", "bear", "short", "ask", "down"],
            &["buy", "bull", "long", "bid", "up"],
        ),
        _ => (&[], &[]),
    }
}

fn blob_conflicts_side(blob: &str, side: &str) -> bool {
    let (_, negative) = side_keywords(side);
    contains_any_keyword(blob, negative)
}

fn failure_level_breached(stage1_output: &Stage1Output, latest_price: f64) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    Ok(match path.side.as_str() {
        "LONG" => latest_price <= path.failure_level.high,
        "SHORT" => latest_price >= path.failure_level.low,
        other => return Err(anyhow!("unsupported path side {}", other)),
    })
}

fn activation_level_touched(stage1_output: &Stage1Output, latest_price: f64) -> Result<bool> {
    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    Ok(path.activation_level.contains(latest_price))
}

fn extreme_location_hit(summary: &StrategicIndicatorSummary) -> bool {
    summary.auction_context.zone_states.iter().any(|state| {
        failed_auction_confirmed(state)
            || reaccept_inside_value(state)
            || zone_acceptance_above(state)
            || zone_acceptance_below(state)
    })
}

fn reverse_confirmation_hit(summary: &StrategicIndicatorSummary, side: &str) -> bool {
    let absorption_blob = flatten_blob(absorption(summary, side));
    let exhaustion_blob = flatten_blob(exhaustion(summary, side));
    let divergence_blob = flatten_blob(divergence(summary));
    let footprint_blob = flatten_blob(footprint(summary));
    let opposite_stack = match side {
        "LONG" => find_bool_key(footprint(summary), "stacked_sell").unwrap_or(false),
        "SHORT" => find_bool_key(footprint(summary), "stacked_buy").unwrap_or(false),
        _ => false,
    };
    blob_conflicts_side(&absorption_blob, side)
        || blob_conflicts_side(&exhaustion_blob, side)
        || blob_conflicts_side(&divergence_blob, side)
        || contains_any_keyword(&footprint_blob, &["failed", "trap", "rejection"])
        || opposite_stack
}

fn driver_change_hit(summary: &StrategicIndicatorSummary, stage1_output: &Stage1Output) -> bool {
    let driver_blob = flatten_blob(&summary.driver_layer);
    let current_driver = stage1_output
        .driver_attribution
        .as_ref()
        .map(|item| item.flow_driver.to_ascii_lowercase())
        .unwrap_or_default();
    driver_blob.contains("flip")
        || driver_blob.contains("driver_change")
        || driver_blob.contains("driver_shift")
        || (!current_driver.is_empty()
            && !driver_blob.contains(&current_driver)
            && contains_any_keyword(&driver_blob, &["spot_led", "futures_led", "mixed"]))
}

fn opposing_pressure_detected(summary: &StrategicIndicatorSummary, side: &str) -> bool {
    let depth = orderbook_depth(summary);
    let depth_blob = flatten_blob(depth);
    let state_blob = format!(
        "{} {} {} {}",
        flatten_blob(open_interest(summary)),
        flatten_blob(long_short_ratios(summary)),
        flatten_blob(funding_rate(summary)),
        flatten_blob(vpin(summary))
    );
    let microprice = find_f64_key(depth, "microprice")
        .or_else(|| find_f64_key(depth, "microprice_fut"))
        .or_else(|| find_f64_key(depth, "microprice_adj_fut"));
    let obi = find_f64_key(depth, "obi")
        .or_else(|| find_f64_key(depth, "obi_fut"))
        .or_else(|| find_f64_key(depth, "obi_k_dw_twa_fut"));
    let ofi = find_f64_key(depth, "ofi_fut").or_else(|| find_f64_key(depth, "ofi_norm_fut"));
    match side {
        "LONG" => {
            [microprice, obi, ofi]
                .into_iter()
                .flatten()
                .any(|value| value < 0.0)
                || blob_conflicts_side(&depth_blob, side)
                || contains_any_keyword(
                    &state_blob,
                    &[
                        "fresh_short_build",
                        "crowded_long",
                        "positive_funding_extreme",
                    ],
                )
        }
        "SHORT" => {
            [microprice, obi, ofi]
                .into_iter()
                .flatten()
                .any(|value| value > 0.0)
                || blob_conflicts_side(&depth_blob, side)
                || contains_any_keyword(
                    &state_blob,
                    &[
                        "fresh_long_build",
                        "crowded_short",
                        "negative_funding_extreme",
                    ],
                )
        }
        _ => false,
    }
}

fn active_context_keys_for_path(
    stage1_output: &Stage1Output,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<String> {
    let Some(path) = stage1_output.current_path.as_ref() else {
        return Vec::new();
    };
    entry_snapshots
        .values()
        .filter(|snapshot| snapshot.path_id == path.id)
        .map(|snapshot| snapshot.context_key.clone())
        .collect()
}

pub fn build_path_runtime_state(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Result<PathRuntimeState> {
    let latest_price = latest_price(summary).ok_or_else(|| anyhow!("missing latest 15m close"))?;
    let path_id = stage1_output
        .current_path
        .as_ref()
        .map(|path| path.id.clone())
        .unwrap_or_else(|| "no_path".to_string());
    if stage1_output.monitoring_status == "no_edge" {
        return Ok(PathRuntimeState {
            path_id,
            monitoring_status: stage1_output.monitoring_status.clone(),
            latest_price,
            hard_invalidation: false,
            failure_level_breached: false,
            path_alive: false,
            activation_level_touched: false,
            opposing_pressure_detected: false,
            audit_flags: PathAuditFlags::default(),
            active_entry_context_keys: Vec::new(),
            notes: vec!["stage1_no_edge".to_string()],
        });
    }

    let path = stage1_output
        .current_path
        .as_ref()
        .ok_or_else(|| anyhow!("active stage1 path missing"))?;
    let failure_breached = failure_level_breached(stage1_output, latest_price)?;
    let flags = PathAuditFlags {
        extreme_location: extreme_location_hit(summary),
        reverse_confirmation: reverse_confirmation_hit(summary, &path.side),
        driver_change: driver_change_hit(summary, stage1_output),
    };
    let hard_invalidation = failure_breached;
    let soft_invalidation_ready =
        flags.extreme_location && flags.reverse_confirmation && flags.driver_change;
    Ok(PathRuntimeState {
        path_id,
        monitoring_status: stage1_output.monitoring_status.clone(),
        latest_price,
        hard_invalidation,
        failure_level_breached: failure_breached,
        path_alive: !hard_invalidation && !soft_invalidation_ready,
        activation_level_touched: activation_level_touched(stage1_output, latest_price)?,
        opposing_pressure_detected: opposing_pressure_detected(summary, &path.side),
        audit_flags: flags,
        active_entry_context_keys: active_context_keys_for_path(stage1_output, entry_snapshots),
        notes: Vec::new(),
    })
}

pub fn build_candidate_event(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
    path_runtime_state: &PathRuntimeState,
) -> Result<Option<CandidateEvent>> {
    let Some(event_ts) = latest_bar_time(summary) else {
        return Ok(None);
    };
    let latest_price = path_runtime_state.latest_price;

    if stage1_output.monitoring_status == "no_edge" {
        return Ok(None);
    }

    if path_runtime_state.hard_invalidation {
        return Ok(Some(CandidateEvent {
            event_type: "hard_invalidation".to_string(),
            event_ts,
            latest_price,
            reason: "failure_level_breached".to_string(),
            details: json!({
                "failure_level_breached": true
            }),
        }));
    }

    if path_runtime_state.audit_flags.extreme_location
        && path_runtime_state.audit_flags.reverse_confirmation
        && path_runtime_state.audit_flags.driver_change
    {
        return Ok(Some(CandidateEvent {
            event_type: "path_review_candidate".to_string(),
            event_ts,
            latest_price,
            reason: "soft_invalidation_triplet".to_string(),
            details: json!({
                "extreme_location": true,
                "reverse_confirmation": true,
                "driver_change": true
            }),
        }));
    }

    if path_runtime_state.activation_level_touched || path_runtime_state.opposing_pressure_detected
    {
        return Ok(Some(CandidateEvent {
            event_type: if path_runtime_state.activation_level_touched {
                "entry_candidate".to_string()
            } else {
                "path_review_candidate".to_string()
            },
            event_ts,
            latest_price,
            reason: if path_runtime_state.activation_level_touched {
                "activation_level_touched".to_string()
            } else {
                "opposing_15m_pressure".to_string()
            },
            details: json!({
                "activation_level_touched": path_runtime_state.activation_level_touched,
                "opposing_pressure_detected": path_runtime_state.opposing_pressure_detected
            }),
        }));
    }

    Ok(None)
}

fn build_tactical_position_slice(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Value {
    let path = stage1_output.current_path.as_ref();
    let position_layer = &summary.position_layer;
    let price_volume_structure = context_child(position_layer, "price_volume_structure");
    let liquidation_density = context_child(position_layer, "liquidation_density");
    let tpo_market_profile = context_child(position_layer, "tpo_market_profile");
    let rvwap_sigma_bands = context_child(position_layer, "rvwap_sigma_bands");
    let avwap = context_child(position_layer, "avwap");
    let fvg = context_child(position_layer, "fvg");
    let ema_trend_regime = context_child(position_layer, "ema_trend_regime");
    let avwap_envelope = zone_envelope(stage1_output);
    let avwap_3d = latest_series_point(avwap, "series_by_window", "3d");
    let avwap_3d_in_path = avwap_envelope
        .and_then(|envelope| {
            avwap_3d
                .get("avwap_fut")
                .and_then(Value::as_f64)
                .filter(|price| price_within_envelope(*price, envelope))
        })
        .is_some();
    let avwap_7d_in_path = avwap_envelope
        .and_then(|envelope| {
            avwap
                .get("avwap_fut")
                .and_then(Value::as_f64)
                .filter(|price| price_within_envelope(*price, envelope))
        })
        .is_some();
    json!({
        "path_id": path.map(|item| item.id.clone()),
        "path_side": path.map(|item| item.side.clone()),
        "activation_level": path.map(|item| item.activation_level.clone()),
        "first_path_target": path.map(|item| item.first_path_target.clone()),
        "next_path_target": path.map(|item| item.next_path_target.clone()),
        "failure_level": path.map(|item| item.failure_level.clone()),
        "tracked_zones": path.map(|item| item.tracked_zones.clone()).unwrap_or_default(),
        "price_volume_structure": {
            "by_window": window_slice(price_volume_structure, "by_window", &["4h", "1d"]),
        },
        "liquidation_density": {
            "by_window": window_slice(liquidation_density, "by_window", &["4h", "1d"]),
        },
        "tpo_market_profile": {
            "as_of_ts": tpo_market_profile.get("as_of_ts").cloned().unwrap_or(Value::Null),
            "by_session": object_slice(
                tpo_market_profile.get("by_session").unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
        },
        "rvwap_sigma_bands": {
            "by_window": window_slice(rvwap_sigma_bands, "by_window", &["15m", "4h", "1d"]),
        },
        "avwap": {
            "lookback": avwap.get("lookback").cloned().unwrap_or(Value::Null),
            "anchors": {
                "4h": latest_series_point(avwap, "series_by_window", "4h"),
                "1d": latest_series_point(avwap, "series_by_window", "1d"),
                "3d": if avwap_3d_in_path { avwap_3d } else { Value::Null },
                "7d_lookback": if avwap_7d_in_path {
                    json!({
                        "anchor_ts": avwap.get("anchor_ts").cloned().unwrap_or(Value::Null),
                        "avwap_fut": avwap.get("avwap_fut").cloned().unwrap_or(Value::Null),
                        "avwap_spot": avwap.get("avwap_spot").cloned().unwrap_or(Value::Null),
                    })
                } else {
                    Value::Null
                },
            },
        },
        "fvg": {
            "by_window": window_slice(fvg, "by_window", &["4h", "1d"]),
        },
        "ema_trend_regime": {
            "ema_100_htf": object_slice(
                ema_trend_regime.get("ema_100_htf").unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
            "ema_200_htf": object_slice(
                ema_trend_regime.get("ema_200_htf").unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
            "trend_regime_by_tf": object_slice(
                ema_trend_regime.get("trend_regime_by_tf").unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
        },
    })
}

fn build_latest_15m_trigger_facts(summary: &StrategicIndicatorSummary) -> Value {
    json!({
        "footprint": footprint(summary),
        "orderbook_depth": orderbook_depth(summary),
        "absorption": context_child(&summary.trigger_layer, "absorption"),
        "initiation": context_child(&summary.trigger_layer, "initiation"),
        "buying_exhaustion": context_child(&summary.trigger_layer, "buying_exhaustion"),
        "selling_exhaustion": context_child(&summary.trigger_layer, "selling_exhaustion"),
        "high_volume_pulse": context_child(&summary.trigger_layer, "high_volume_pulse"),
        "open_interest_15m": context_child(open_interest(summary), "by_window").get("15m").cloned().unwrap_or(Value::Null),
        "long_short_ratios_15m": context_child(long_short_ratios(summary), "by_window").get("15m").cloned().unwrap_or(Value::Null),
    })
}

fn build_state_guardrail_snapshot(summary: &StrategicIndicatorSummary) -> Value {
    json!({
        "open_interest": window_slice(open_interest(summary), "by_window", &["15m", "4h", "1d", "3d"]),
        "long_short_ratios": window_slice(long_short_ratios(summary), "by_window", &["15m", "4h", "1d", "3d"]),
        "funding_rate": window_slice(funding_rate(summary), "by_window", &["4h", "1d"]),
        "vpin": window_slice(vpin(summary), "by_window", &["4h", "1d"]),
    })
}

fn build_driver_guardrail_snapshot(summary: &StrategicIndicatorSummary) -> Value {
    json!({
        "cvd_pack": window_slice(cvd_pack(summary), "by_window", &["4h", "1d"]),
        "divergence": divergence(summary),
        "whale_trades": window_slice(whale_trades(summary), "by_window", &["4h", "1d"]),
    })
}

fn build_options_guardrail_snapshot(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Option<Value> {
    let options_surface = summary.aux_context.get("options_surface")?;
    if !value_present(options_surface) {
        return None;
    }
    let Some(path) = stage1_output.current_path.as_ref() else {
        return None;
    };
    let tactical_guardrail = options_surface.get("tactical_guardrail")?;
    let ready_windows = tactical_guardrail
        .get("ready_windows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if ready_windows.is_empty() {
        return None;
    }
    let envelope = zone_envelope(stage1_output)?;
    let windows = tactical_guardrail
        .get("windows")
        .and_then(Value::as_object)?;
    let mut overlapping_windows = Map::new();
    for ready_window in ready_windows {
        let Some(window) = ready_window.as_str() else {
            continue;
        };
        let Some(window_payload) = windows.get(window) else {
            continue;
        };
        let overlaps = window_payload
            .get("atm_strike_front")
            .and_then(Value::as_f64)
            .map(|strike| price_within_envelope(strike, envelope))
            .unwrap_or(false);
        if overlaps {
            overlapping_windows.insert(window.to_string(), window_payload.clone());
        }
    }
    if overlapping_windows.is_empty() {
        return None;
    }
    Some(json!({
        "path_id": path.id,
        "options_guardrail": {
            "windows": overlapping_windows.clone(),
            "ready_windows": overlapping_windows.keys().cloned().collect::<Vec<_>>()
        },
    }))
}

fn snapshot_matches_direction(snapshot: &EntrySnapshot, symbol: &str, direction: &str) -> bool {
    snapshot.symbol.eq_ignore_ascii_case(symbol) && snapshot.side.eq_ignore_ascii_case(direction)
}

fn workflow_positions_for_active_position(
    symbol: &str,
    position: &crate::execution::binance::ActivePositionSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<WorkflowPosition> {
    let direction = if position.position_amt >= 0.0 {
        "LONG"
    } else {
        "SHORT"
    }
    .to_string();
    let matching_snapshots = entry_snapshots
        .values()
        .filter(|snapshot| snapshot_matches_direction(snapshot, symbol, &direction))
        .cloned()
        .collect::<Vec<_>>();
    if matching_snapshots.is_empty() {
        return vec![WorkflowPosition {
            context_key: format!("{}:{}", symbol.to_ascii_uppercase(), direction),
            position_side: position.position_side.clone(),
            direction,
            quantity: position.position_amt.abs(),
            leverage: position.leverage,
            entry_price: position.entry_price,
            mark_price: position.mark_price,
            unrealized_pnl: position.unrealized_pnl,
            current_tp_price: None,
            current_sl_price: None,
            entry_snapshot: None,
        }];
    }
    matching_snapshots
        .into_iter()
        .map(|snapshot| WorkflowPosition {
            context_key: snapshot.context_key.clone(),
            position_side: position.position_side.clone(),
            direction: snapshot.side.clone(),
            quantity: position.position_amt.abs(),
            leverage: position.leverage,
            entry_price: position.entry_price,
            mark_price: position.mark_price,
            unrealized_pnl: position.unrealized_pnl,
            current_tp_price: Some(snapshot.take_profit_1),
            current_sl_price: Some(snapshot.stop_loss),
            entry_snapshot: Some(snapshot),
        })
        .collect()
}

pub fn build_stage2_prompt_input(
    summary: StrategicIndicatorSummary,
    stage1_output: Stage1Output,
    candidate_event: CandidateEvent,
    path_runtime_state: PathRuntimeState,
    previous_tactical_plan: Option<TacticalEntryPlan>,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Stage2PromptInput {
    let active_positions = trading_state
        .active_positions
        .iter()
        .flat_map(|position| {
            workflow_positions_for_active_position(&trading_state.symbol, position, entry_snapshots)
        })
        .collect::<Vec<_>>();
    let tactical_position_slice = build_tactical_position_slice(&summary, &stage1_output);
    let latest_15m_trigger_facts = build_latest_15m_trigger_facts(&summary);
    let state_guardrail_snapshot = build_state_guardrail_snapshot(&summary);
    let driver_guardrail_snapshot = build_driver_guardrail_snapshot(&summary);
    let options_guardrail_snapshot = build_options_guardrail_snapshot(&summary, &stage1_output);

    Stage2PromptInput {
        task: "Audit whether the current strategic path is still alive, and if it is still alive, design the best tactical entry plan inside the existing path envelope.".to_string(),
        candidate_event,
        path_runtime_state,
        previous_tactical_plan,
        tactical_position_slice,
        latest_15m_trigger_facts,
        state_guardrail_snapshot,
        driver_guardrail_snapshot,
        options_guardrail_snapshot,
        stage1_output,
        active_positions,
        account: WorkflowAccountContext {
            total_wallet_balance: trading_state.total_wallet_balance,
            available_balance: trading_state.available_balance,
            has_active_positions: trading_state.has_active_positions,
            has_open_orders: trading_state.has_open_orders,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{build_candidate_event, build_path_runtime_state, build_stage2_prompt_input};
    use crate::execution::binance::TradingStateSnapshot;
    use crate::workflow::schema::{
        AuctionContext, CurrentPath, DriverAttribution, ManagementPlan, MapSummary, PriceZone,
        ReevaluationTrigger, Stage1Meta, Stage1Output, StrategicIndicatorSummary,
        StrategicSummaryMeta,
    };
    use chrono::{Duration, Utc};
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: MapSummary {
                regime_3d: json!({"bias": "bearish"}),
                location_1d: json!({"class": "value_edge"}),
                location_4h: json!({"class": "value_edge"}),
                price_location_class: "value_edge".to_string(),
                key_levels: json!({}),
            },
            opportunity_assessment: crate::workflow::schema::OpportunityAssessment::default(),
            script_rejections: vec![],
            current_script: Some("value_return".to_string()),
            driver_attribution: Some(DriverAttribution {
                flow_driver: "mixed".to_string(),
                spot_confirming: true,
                driver_note: "supportive".to_string(),
            }),
            current_path: Some(CurrentPath {
                id: "path_1".to_string(),
                side: "LONG".to_string(),
                thesis: "bounce".to_string(),
                risk_grade: "countertrend_repair".to_string(),
                activation_anchor_id: None,
                activation_level: PriceZone {
                    low: 1998.0,
                    high: 2002.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("activation".to_string()),
                    reason: None,
                },
                first_path_target_anchor_id: None,
                first_path_target: PriceZone {
                    low: 2020.0,
                    high: 2025.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("tp1".to_string()),
                    reason: None,
                },
                next_path_target_anchor_id: None,
                next_path_target: PriceZone {
                    low: 2030.0,
                    high: 2035.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("tp2".to_string()),
                    reason: None,
                },
                failure_anchor_id: None,
                failure_level: PriceZone {
                    low: 1989.0,
                    high: 1992.0,
                    timeframe: Some("4h".to_string()),
                    label: Some("failure".to_string()),
                    reason: None,
                },
                failure_switch: Some("crowded_reversal".to_string()),
                setup_type: "C_value_return".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                management_plan: ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 2022.0,
                    take_profit_2_level: 2032.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![],
            }),
        }
    }

    fn sample_summary(close: f64) -> StrategicIndicatorSummary {
        let now = Utc::now();
        StrategicIndicatorSummary {
            meta: StrategicSummaryMeta {
                symbol: "ETHUSDT".to_string(),
                ts_bucket: now,
                source_routing_key: "x".to_string(),
                indicator_count: 1,
                missing_indicator_codes: vec![],
            },
            position_layer: json!({
                "price_volume_structure": {"by_window": {"4h": {"poc_price": 2000.0}, "1d": {"poc_price": 2001.0}, "3d": {"poc_price": 1990.0}}},
                "rvwap_sigma_bands": {"by_window": {"15m": {"rvwap_w": 2000.0}, "4h": {"rvwap_w": 2001.0}, "1d": {"rvwap_w": 2002.0}}},
                "avwap": {
                    "lookback": "7d",
                    "anchor_ts": "2026-03-28T00:00:00Z",
                    "avwap_fut": 2100.0,
                    "avwap_spot": 2098.0,
                    "series_by_window": {
                        "4h": {"latest_point": {"avwap_fut": 2000.0, "avwap_spot": 1999.0}},
                        "1d": {"latest_point": {"avwap_fut": 2001.0, "avwap_spot": 2000.0}},
                        "3d": {"latest_point": {"avwap_fut": 2500.0, "avwap_spot": 2498.0}}
                    }
                },
                "tpo_market_profile": {"by_session": {"4h": {"tpo_poc": 2000.0}, "1d": {"tpo_poc": 2001.0}}},
                "liquidation_density": {"by_window": {"4h": {}, "1d": {}, "3d": {}}},
                "fvg": {"by_window": {"15m": {}, "4h": {}, "1d": {}, "3d": {}}}
            }),
            state_layer: json!({
                "open_interest": {"by_window": {"15m": {"state": "long_unwind"}, "4h": {"state": "long_unwind"}, "1d": {"state": "long_unwind"}}},
                "long_short_ratios": {"by_window": {"15m": {"crowding_state": "balanced"}, "4h": {"crowding_state": "balanced"}, "1d": {"crowding_state": "balanced"}}},
                "funding_rate": {"by_window": {"4h": {"funding_twa": -0.0001}, "1d": {"funding_twa": -0.0002}}},
                "vpin": {"by_window": {"4h": {"vpin_fut": 0.4}, "1d": {"vpin_fut": 0.5}}}
            }),
            driver_layer: json!({
                "cvd_pack": {"by_window": {"4h": {"series": [{"delta_fut": 1}]}, "1d": {"series": [{"delta_fut": 2}]}}},
                "divergence": {"signals": {"bullish_divergence": false}},
                "whale_trades": {"by_window": {"4h": {"window": "4h"}, "1d": {"window": "1d"}}}
            }),
            trigger_layer: json!({
                "footprint": {"by_window": {"15m": {"stacked_buy": true}, "4h": {}}},
                "orderbook_depth": {"by_window": {"15m": {}}, "spot_confirm": true, "fake_order_risk_fut": 0.1, "obi": 0.8, "ofi_fut": 2.0},
                "selling_exhaustion": {"recent_7d": {"events": []}},
                "buying_exhaustion": {"recent_7d": {"events": []}},
                "absorption": {"recent_7d": {"events": []}},
                "initiation": {"direction": "buy"},
                "high_volume_pulse": {"by_z_window": {"4h": {}, "1d": {}}},
                "divergence": {"signals": {"bullish_divergence": false}}
            }),
            auction_context: AuctionContext {
                tracked_zones: vec![],
                zone_states: vec![],
                recent_15m_bars: vec![crate::workflow::schema::RecentBar {
                    open_time: now - Duration::minutes(15),
                    close_time: now,
                    open: close - 1.0,
                    high: close + 1.0,
                    low: close - 2.0,
                    close,
                    is_closed: true,
                }],
            },
            aux_context: json!({
                "options_surface": {
                    "strategic_summary": {
                        "windows": {"4h": {"is_ready": true}},
                        "ready_windows": ["4h"]
                    },
                    "tactical_guardrail": {
                        "windows": {
                            "15m": {"is_ready": true, "atm_strike_front": 2000.0},
                            "4h": {"is_ready": true, "atm_strike_front": 2001.0}
                        },
                        "ready_windows": ["15m", "4h"]
                    }
                }
            }),
        }
    }

    #[test]
    fn path_runtime_state_marks_activation_touch() {
        let summary = sample_summary(2000.0);
        let state = build_path_runtime_state(&summary, &sample_stage1_output(), &HashMap::new())
            .expect("runtime");
        assert!(state.activation_level_touched);
        assert!(state.path_alive);
    }

    #[test]
    fn candidate_event_becomes_entry_candidate_when_activation_is_touched() {
        let summary = sample_summary(2000.0);
        let stage1 = sample_stage1_output();
        let runtime_state =
            build_path_runtime_state(&summary, &stage1, &HashMap::new()).expect("runtime");
        let event = build_candidate_event(&summary, &stage1, &runtime_state)
            .expect("candidate")
            .expect("event");
        assert_eq!(event.event_type, "entry_candidate");
    }

    #[test]
    fn stage2_prompt_input_uses_new_contract() {
        let summary = sample_summary(2000.0);
        let stage1 = sample_stage1_output();
        let runtime_state =
            build_path_runtime_state(&summary, &stage1, &HashMap::new()).expect("runtime");
        let event = build_candidate_event(&summary, &stage1, &runtime_state)
            .expect("candidate")
            .expect("event");
        let prompt = build_stage2_prompt_input(
            summary,
            stage1,
            event,
            runtime_state,
            None,
            &TradingStateSnapshot {
                symbol: "ETHUSDT".to_string(),
                has_active_context: false,
                has_active_positions: false,
                has_open_orders: false,
                active_positions: Vec::new(),
                open_orders: Vec::new(),
                total_wallet_balance: 0.0,
                available_balance: 0.0,
            },
            &HashMap::new(),
        );
        assert_eq!(prompt.account.has_active_positions, false);
        assert!(prompt.options_guardrail_snapshot.is_some());
        assert_eq!(prompt.candidate_event.event_type, "entry_candidate");
        assert!(prompt
            .tactical_position_slice
            .get("price_volume_structure")
            .and_then(|value| value.get("by_window"))
            .and_then(|value| value.get("3d"))
            .is_none());
        assert!(prompt
            .tactical_position_slice
            .get("avwap")
            .and_then(|value| value.get("anchors"))
            .and_then(|value| value.get("7d_lookback"))
            .and_then(Value::as_object)
            .is_none());
    }

    #[test]
    fn stage2_options_guardrail_is_omitted_when_no_obstacle_overlaps_path() {
        let mut summary = sample_summary(2000.0);
        summary.aux_context = json!({
            "options_surface": {
                "strategic_summary": {
                    "windows": {"4h": {"is_ready": true}},
                    "ready_windows": ["4h"]
                },
                "tactical_guardrail": {
                    "windows": {
                        "15m": {"is_ready": true, "atm_strike_front": 2500.0},
                        "4h": {"is_ready": true, "atm_strike_front": 2600.0}
                    },
                    "ready_windows": ["15m", "4h"]
                }
            }
        });
        let stage1 = sample_stage1_output();
        let runtime_state =
            build_path_runtime_state(&summary, &stage1, &HashMap::new()).expect("runtime");
        let event = build_candidate_event(&summary, &stage1, &runtime_state)
            .expect("candidate")
            .expect("event");
        let prompt = build_stage2_prompt_input(
            summary,
            stage1,
            event,
            runtime_state,
            None,
            &TradingStateSnapshot {
                symbol: "ETHUSDT".to_string(),
                has_active_context: false,
                has_active_positions: false,
                has_open_orders: false,
                active_positions: Vec::new(),
                open_orders: Vec::new(),
                total_wallet_balance: 0.0,
                available_balance: 0.0,
            },
            &HashMap::new(),
        );
        assert!(prompt.options_guardrail_snapshot.is_none());
    }
}
