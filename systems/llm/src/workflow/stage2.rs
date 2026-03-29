use crate::execution::binance::{OpenOrderSnapshot, TradingStateSnapshot};
use crate::workflow::predicate::{
    failed_auction_confirmed, reaccept_inside_value, zone_acceptance_above, zone_acceptance_below,
};
use crate::workflow::schema::{
    CandidateEvent, EntrySnapshot, PathAuditFlags, PathRuntimeState, PendingOrderManagementPlan,
    Stage1Output, Stage2APromptInput, Stage2BPromptInput, Stage2CPromptInput,
    StrategicIndicatorSummary, TacticalEntryPlan, WorkflowAccountContext, WorkflowPendingOrder,
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
    let funding = context_child(&summary.state_layer, "funding");
    if value_present(funding) {
        funding
    } else {
        context_child(&summary.state_layer, "funding_rate")
    }
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
        "funding": window_slice(funding_rate(summary), "by_window", &["4h", "1d"]),
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
            context_key: format!("{}:{}:pathless", symbol.to_ascii_uppercase(), direction),
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

fn order_direction(order: &OpenOrderSnapshot) -> Option<&'static str> {
    if order.reduce_only || order.close_position {
        return None;
    }
    if order.side.eq_ignore_ascii_case("BUY") {
        Some("LONG")
    } else if order.side.eq_ignore_ascii_case("SELL") {
        Some("SHORT")
    } else {
        None
    }
}

fn workflow_pending_order_for_open_order(
    symbol: &str,
    order: &OpenOrderSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Option<WorkflowPendingOrder> {
    let direction = order_direction(order)?;
    let entry_snapshot = entry_snapshots
        .values()
        .find(|snapshot| snapshot_matches_direction(snapshot, symbol, direction))
        .cloned();
    Some(WorkflowPendingOrder {
        context_key: entry_snapshot
            .as_ref()
            .map(|snapshot| snapshot.context_key.clone())
            .unwrap_or_else(|| {
                format!(
                    "{}:{}:pending",
                    symbol.to_ascii_uppercase(),
                    direction.to_ascii_uppercase()
                )
            }),
        order_id: order.order_id,
        side: direction.to_string(),
        position_side: order.position_side.clone(),
        order_type: order.order_type.clone(),
        status: order.status.clone(),
        quantity: order.orig_qty,
        executed_quantity: order.executed_qty,
        price: order.price,
        stop_price: order.stop_price,
        post_fill_bracket_template: entry_snapshot.as_ref().map(|snapshot| {
            crate::workflow::schema::PostFillBracketTemplate {
                take_profit_1: snapshot.take_profit_1,
                take_profit_2: snapshot.take_profit_2,
                stop_loss: snapshot.stop_loss,
            }
        }),
        entry_snapshot,
    })
}

pub fn stage2b_active_positions_for_current_path(
    stage1_output: &Stage1Output,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<WorkflowPosition> {
    let Some(current_path) = stage1_output.current_path.as_ref() else {
        return Vec::new();
    };
    trading_state
        .active_positions
        .iter()
        .flat_map(|position| {
            workflow_positions_for_active_position(&trading_state.symbol, position, entry_snapshots)
        })
        .filter(|position| position.direction.eq_ignore_ascii_case(&current_path.side))
        .filter(|position| {
            position
                .entry_snapshot
                .as_ref()
                .map(|snapshot| snapshot.path_id == current_path.id)
                .unwrap_or(true)
        })
        .collect()
}

pub fn stage2c_active_orders_for_current_path(
    stage1_output: &Stage1Output,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<WorkflowPendingOrder> {
    let Some(current_path) = stage1_output.current_path.as_ref() else {
        return Vec::new();
    };
    trading_state
        .open_orders
        .iter()
        .filter_map(|order| {
            workflow_pending_order_for_open_order(&trading_state.symbol, order, entry_snapshots)
        })
        .filter(|order| order.side.eq_ignore_ascii_case(&current_path.side))
        .filter(|order| {
            order
                .entry_snapshot
                .as_ref()
                .map(|snapshot| snapshot.path_id == current_path.id)
                .unwrap_or(true)
        })
        .collect()
}

fn build_account_context(trading_state: &TradingStateSnapshot) -> WorkflowAccountContext {
    WorkflowAccountContext {
        total_wallet_balance: trading_state.total_wallet_balance,
        available_balance: trading_state.available_balance,
        has_active_positions: trading_state.has_active_positions,
        has_open_orders: trading_state.has_open_orders,
    }
}

pub fn build_stage2a_prompt_input(
    summary: StrategicIndicatorSummary,
    stage1_output: Stage1Output,
    candidate_event: CandidateEvent,
    path_runtime_state: PathRuntimeState,
    previous_tactical_plan: Option<TacticalEntryPlan>,
    trading_state: &TradingStateSnapshot,
) -> Stage2APromptInput {
    Stage2APromptInput {
        task: "审核当前 strategic path，并基于 15m 战术输入设计 tactical entry".to_string(),
        candidate_event,
        path_runtime_state,
        previous_tactical_plan,
        exposure_state: "flat_no_orders".to_string(),
        tactical_position_slice: build_tactical_position_slice(&summary, &stage1_output),
        latest_15m_trigger_facts: build_latest_15m_trigger_facts(&summary),
        state_guardrail_snapshot: build_state_guardrail_snapshot(&summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(&summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(&summary, &stage1_output),
        stage1_output,
        account: build_account_context(trading_state),
    }
}

pub fn build_stage2b_prompt_input(
    summary: StrategicIndicatorSummary,
    stage1_output: Stage1Output,
    candidate_event: CandidateEvent,
    path_runtime_state: PathRuntimeState,
    active_position: WorkflowPosition,
    previous_management_plan: Option<crate::workflow::schema::PositionManagementPlan>,
    trading_state: &TradingStateSnapshot,
) -> Stage2BPromptInput {
    Stage2BPromptInput {
        task: "基于当前 strategic path 与 15m 战术输入管理持仓，以最大化收益为目标".to_string(),
        candidate_event,
        path_runtime_state,
        exposure_state: "in_position".to_string(),
        active_positions: vec![active_position],
        latest_15m_trigger_facts: build_latest_15m_trigger_facts(&summary),
        state_guardrail_snapshot: build_state_guardrail_snapshot(&summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(&summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(&summary, &stage1_output),
        stage1_output,
        previous_management_plan,
        account: build_account_context(trading_state),
    }
}

pub fn build_stage2c_prompt_input(
    summary: StrategicIndicatorSummary,
    stage1_output: Stage1Output,
    candidate_event: CandidateEvent,
    path_runtime_state: PathRuntimeState,
    exposure_state: &str,
    active_order: WorkflowPendingOrder,
    previous_pending_order_management_plan: Option<PendingOrderManagementPlan>,
    trading_state: &TradingStateSnapshot,
) -> Stage2CPromptInput {
    Stage2CPromptInput {
        task: "基于当前 strategic path 与 15m 战术输入管理未成交挂单，以最大化收益为目标"
            .to_string(),
        candidate_event,
        path_runtime_state,
        exposure_state: exposure_state.to_string(),
        active_orders: vec![active_order],
        latest_15m_trigger_facts: build_latest_15m_trigger_facts(&summary),
        state_guardrail_snapshot: build_state_guardrail_snapshot(&summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(&summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(&summary, &stage1_output),
        stage1_output,
        previous_pending_order_management_plan,
        account: build_account_context(trading_state),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        stage2b_active_positions_for_current_path, stage2c_active_orders_for_current_path,
    };
    use crate::execution::binance::{
        ActivePositionSnapshot, OpenOrderSnapshot, TradingStateSnapshot,
    };
    use crate::workflow::schema::{
        CurrentPath, EntrySnapshot, MapSummary, OpportunityAssessment, PriceZone,
        ReevaluationTrigger, Stage1Meta, Stage1Output,
    };
    use chrono::Utc;
    use std::collections::HashMap;

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: MapSummary {
                location_3d: serde_json::json!({}),
                location_1d: serde_json::json!({}),
                location_4h: serde_json::json!({}),
                price_location_class: "value_edge".to_string(),
                key_levels: serde_json::json!({}),
            },
            opportunity_assessment: OpportunityAssessment {
                overall_quality: Some("high".to_string()),
                ..OpportunityAssessment::default()
            },
            current_script: Some("value_return".to_string()),
            driver_attribution: None,
            current_path: Some(CurrentPath {
                id: "path_a".to_string(),
                side: "LONG".to_string(),
                thesis: "bounce".to_string(),
                risk_grade: "countertrend_repair".to_string(),
                activation_anchor_id: None,
                activation_level: price_zone(1998.0, 2002.0),
                first_path_target_anchor_id: None,
                first_path_target: price_zone(2020.0, 2025.0),
                next_path_target_anchor_id: None,
                next_path_target: price_zone(2030.0, 2035.0),
                failure_anchor_id: None,
                failure_level: price_zone(1989.0, 1992.0),
                failure_switch: Some("continuation".to_string()),
                setup_type: "B_reversal".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                tracked_zones: Vec::new(),
            }),
        }
    }

    fn price_zone(low: f64, high: f64) -> PriceZone {
        PriceZone {
            low,
            high,
            timeframe: Some("15m".to_string()),
            label: None,
            reason: None,
        }
    }

    fn sample_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![
                ActivePositionSnapshot {
                    position_side: "LONG".to_string(),
                    position_amt: 1.0,
                    entry_price: 2000.0,
                    mark_price: 2005.0,
                    unrealized_pnl: 5.0,
                    leverage: 5,
                },
                ActivePositionSnapshot {
                    position_side: "SHORT".to_string(),
                    position_amt: -0.5,
                    entry_price: 2100.0,
                    mark_price: 2090.0,
                    unrealized_pnl: 5.0,
                    leverage: 4,
                },
            ],
            open_orders: vec![
                OpenOrderSnapshot {
                    order_id: 11,
                    side: "BUY".to_string(),
                    position_side: "LONG".to_string(),
                    order_type: "LIMIT".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 1.0,
                    executed_qty: 0.0,
                    price: 1999.0,
                    stop_price: 0.0,
                    close_position: false,
                    reduce_only: false,
                    is_algo_order: false,
                },
                OpenOrderSnapshot {
                    order_id: 22,
                    side: "SELL".to_string(),
                    position_side: "SHORT".to_string(),
                    order_type: "LIMIT".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 0.5,
                    executed_qty: 0.0,
                    price: 2101.0,
                    stop_price: 0.0,
                    close_position: false,
                    reduce_only: false,
                    is_algo_order: false,
                },
            ],
            total_wallet_balance: 1000.0,
            available_balance: 500.0,
        }
    }

    fn sample_entry_snapshots() -> HashMap<String, EntrySnapshot> {
        HashMap::from([
            (
                "ETHUSDT:LONG:path_a".to_string(),
                EntrySnapshot {
                    symbol: "ETHUSDT".to_string(),
                    context_key: "ETHUSDT:LONG:path_a".to_string(),
                    path_id: "path_a".to_string(),
                    side: "LONG".to_string(),
                    entry_profile: Some("reclaim_then_hold".to_string()),
                    intent_mode: Some("immediate".to_string()),
                    entry_activation_level: Some(price_zone(1998.0, 2002.0)),
                    entry_zone: Some(price_zone(1999.0, 2001.0)),
                    entry_invalidation_level: Some(price_zone(1989.0, 1992.0)),
                    max_drift_pct: Some(0.2),
                    stop_loss: 1989.0,
                    take_profit_1: 2020.0,
                    take_profit_2: 2030.0,
                    allowed_stop_loss_levels: vec![1989.0],
                    allowed_take_profit_levels: vec![2020.0, 2030.0],
                    tp1_realized: false,
                    applied_driver_deterioration_signals: Vec::new(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            ),
            (
                "ETHUSDT:SHORT:path_b".to_string(),
                EntrySnapshot {
                    symbol: "ETHUSDT".to_string(),
                    context_key: "ETHUSDT:SHORT:path_b".to_string(),
                    path_id: "path_b".to_string(),
                    side: "SHORT".to_string(),
                    entry_profile: Some("reject_then_go".to_string()),
                    intent_mode: Some("maker_limit".to_string()),
                    entry_activation_level: Some(price_zone(2102.0, 2104.0)),
                    entry_zone: Some(price_zone(2101.0, 2103.0)),
                    entry_invalidation_level: Some(price_zone(2110.0, 2112.0)),
                    max_drift_pct: Some(0.2),
                    stop_loss: 2111.0,
                    take_profit_1: 2080.0,
                    take_profit_2: 2060.0,
                    allowed_stop_loss_levels: vec![2111.0],
                    allowed_take_profit_levels: vec![2080.0, 2060.0],
                    tp1_realized: false,
                    applied_driver_deterioration_signals: Vec::new(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            ),
        ])
    }

    #[test]
    fn stage2b_context_selection_keeps_only_current_path_position() {
        let contexts = stage2b_active_positions_for_current_path(
            &sample_stage1_output(),
            &sample_trading_state(),
            &sample_entry_snapshots(),
        );

        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].context_key, "ETHUSDT:LONG:path_a");
        assert_eq!(contexts[0].direction, "LONG");
    }

    #[test]
    fn stage2c_context_selection_keeps_only_current_path_order() {
        let contexts = stage2c_active_orders_for_current_path(
            &sample_stage1_output(),
            &sample_trading_state(),
            &sample_entry_snapshots(),
        );

        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].context_key, "ETHUSDT:LONG:path_a");
        assert_eq!(contexts[0].order_id, 11);
        assert_eq!(contexts[0].side, "LONG");
    }
}
