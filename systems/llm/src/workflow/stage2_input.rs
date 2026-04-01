use crate::execution::binance::{OpenOrderSnapshot, TradingStateSnapshot};
use crate::llm::input::ModelInvocationInput;
use crate::workflow::schema::{
    EntrySnapshot, PendingOrderManagementPlan, PositionManagementPlan, PostFillBracketTemplate,
    Stage1Output, Stage2APromptInput, Stage2BPromptInput, Stage2CPromptInput,
    StrategicIndicatorSummary, WorkflowAccountContext, WorkflowPendingOrder, WorkflowPosition,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

fn value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
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
        .collect::<Map<_, _>>();
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

fn raw_indicator_payload<'a>(input: &'a ModelInvocationInput, key: &str) -> &'a Value {
    input
        .indicators
        .get(key)
        .and_then(|value| value.get("payload"))
        .unwrap_or(&Value::Null)
}

fn latest_series_entry(value: &Value, key: &str, window: &str) -> Value {
    value
        .get(key)
        .and_then(|series| series.get(window))
        .and_then(Value::as_array)
        .and_then(|items| items.last())
        .cloned()
        .unwrap_or(Value::Null)
}

fn price_within_envelope(price: f64, envelope: (f64, f64)) -> bool {
    price >= envelope.0 && price <= envelope.1
}

fn zone_envelope(stage1_output: &Stage1Output) -> Option<(f64, f64)> {
    let path = stage1_output.current_path.as_ref()?;
    let mut lows = vec![
        path.strategic_activation_level.low,
        path.first_path_target.low,
        path.next_path_target.low,
        path.failure_level.low,
    ];
    let mut highs = vec![
        path.strategic_activation_level.high,
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

fn latest_15m_close(summary: &StrategicIndicatorSummary) -> Option<f64> {
    summary
        .auction_context
        .recent_15m_bars
        .last()
        .map(|bar| bar.close)
}

fn latest_closed_1m_price(input: &ModelInvocationInput) -> Option<f64> {
    raw_indicator_payload(input, "kline_history")
        .pointer("/intervals/1m/markets/futures/bars")
        .and_then(Value::as_array)
        .and_then(|bars| bars.last())
        .and_then(|bar| bar.get("close"))
        .and_then(Value::as_f64)
}

fn current_reference_price(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
) -> f64 {
    latest_closed_1m_price(input)
        .or_else(|| latest_15m_close(summary))
        .unwrap_or_default()
}

fn reference_windows_for_timeframe_hint(timeframe: &str) -> Vec<&'static str> {
    let mut windows = Vec::new();
    if timeframe.contains("3d") {
        windows.push("3d");
    }
    if timeframe.contains("1d") {
        windows.push("1d");
    }
    if timeframe.contains("4h") {
        windows.push("4h");
    }
    if timeframe.contains("15m") {
        windows.push("15m");
    }
    windows.push("7d_lookback");
    windows
}

fn primary_avwap_reference_window(timeframe: &str, avwap: &Value) -> &'static str {
    reference_windows_for_timeframe_hint(timeframe)
        .into_iter()
        .find(|window| {
            if *window == "7d_lookback" {
                return value_present(&avwap_reference_for_window(avwap, window));
            }
            value_present(&latest_series_entry(avwap, "series_by_window", window))
        })
        .unwrap_or("7d_lookback")
}

fn tracked_zone_for_anchor<'a>(
    stage1_output: &'a Stage1Output,
    anchor_id: Option<&str>,
) -> Option<&'a crate::workflow::schema::TrackedZone> {
    let anchor_id = anchor_id?;
    let path = stage1_output.current_path.as_ref()?;
    path.tracked_zones
        .iter()
        .find(|zone| zone.zone_id == anchor_id)
}

fn avwap_reference_for_window(avwap: &Value, window: &str) -> Value {
    match window {
        "15m" | "4h" | "1d" => latest_series_entry(avwap, "series_by_window", window),
        "3d" => {
            let reference = latest_series_entry(avwap, "series_by_window", "3d");
            if value_present(&reference) {
                reference
            } else {
                latest_series_entry(avwap, "series_by_window", "1d")
            }
        }
        "7d_lookback" => json!({
            "lookback": avwap.get("lookback").cloned().unwrap_or(Value::Null),
            "anchor_ts": avwap.get("anchor_ts").cloned().unwrap_or(Value::Null),
            "avwap_fut": avwap.get("avwap_fut").cloned().unwrap_or(Value::Null),
            "avwap_spot": avwap.get("avwap_spot").cloned().unwrap_or(Value::Null),
            "price_minus_avwap_fut": avwap.get("price_minus_avwap_fut").cloned().unwrap_or(Value::Null),
            "price_minus_spot_avwap_fut": avwap.get("price_minus_spot_avwap_fut").cloned().unwrap_or(Value::Null),
        }),
        _ => Value::Null,
    }
}

fn build_selected_avwap_anchors(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Value {
    let Some(path) = stage1_output.current_path.as_ref() else {
        return Value::Array(Vec::new());
    };
    let avwap = context_child(&summary.position_layer, "avwap");
    let current_price = latest_15m_close(summary).unwrap_or_default();
    let anchor_roles = [
        (
            "strategic_activation_level",
            path.activation_anchor_id.as_deref(),
            &path.strategic_activation_level,
        ),
        (
            "first_path_target",
            path.first_path_target_anchor_id.as_deref(),
            &path.first_path_target,
        ),
        (
            "next_path_target",
            path.next_path_target_anchor_id.as_deref(),
            &path.next_path_target,
        ),
        (
            "failure_level",
            path.failure_anchor_id.as_deref(),
            &path.failure_level,
        ),
    ];

    let anchors = anchor_roles
        .into_iter()
        .map(|(anchor_role, anchor_id, zone)| {
            let tracked_zone = tracked_zone_for_anchor(stage1_output, anchor_id);
            let timeframe_hint = tracked_zone
                .map(|item| item.timeframe.clone())
                .or_else(|| zone.timeframe.clone())
                .unwrap_or_else(|| "4h".to_string());
            let reference_windows = reference_windows_for_timeframe_hint(&timeframe_hint);
            let mapped_reference_window = primary_avwap_reference_window(&timeframe_hint, avwap);
            let avwap_references = reference_windows
                .into_iter()
                .map(|window| {
                    (
                        window.to_string(),
                        avwap_reference_for_window(avwap, window),
                    )
                })
                .collect::<Map<_, _>>();

            json!({
                "anchor_role": anchor_role,
                "anchor_id": anchor_id,
                "timeframe_hint": timeframe_hint,
                "zone": zone,
                "tracked_zone": tracked_zone,
                "current_price": current_price,
                "distance_to_zone_midpoint": current_price - zone.midpoint(),
                "mapped_reference_window": mapped_reference_window,
                "mapped_avwap_reference": avwap_reference_for_window(avwap, mapped_reference_window),
                "strategic_reference_7d": avwap_reference_for_window(avwap, "7d_lookback"),
                "avwap_references": avwap_references,
            })
        })
        .collect::<Vec<_>>();
    Value::Array(anchors)
}

fn build_avwap_anchor_distances(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Value {
    let current_price = current_reference_price(input, summary);
    let selected = build_selected_avwap_anchors(summary, stage1_output);
    let items = selected
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| {
            let anchor_role = item
                .get("anchor_role")
                .cloned()
                .unwrap_or(Value::String("unknown".to_string()));
            let anchor_id = item.get("anchor_id").cloned().unwrap_or(Value::Null);
            let timeframe_hint = item.get("timeframe_hint").cloned().unwrap_or(Value::Null);
            let zone = item.get("zone").cloned().unwrap_or(Value::Null);
            let mapped_reference_window = item
                .get("mapped_reference_window")
                .cloned()
                .unwrap_or(Value::String("7d_lookback".to_string()));
            let mapped_avwap_reference = item
                .get("mapped_avwap_reference")
                .cloned()
                .unwrap_or(Value::Null);
            let zone_midpoint = zone
                .get("low")
                .and_then(Value::as_f64)
                .zip(zone.get("high").and_then(Value::as_f64))
                .map(|(low, high)| (low + high) / 2.0);
            let avwap_references = item
                .get("avwap_references")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let distance_to_avwap_fut = avwap_references
                .iter()
                .map(|(window, reference)| {
                    (
                        window.clone(),
                        reference
                            .get("avwap_fut")
                            .and_then(Value::as_f64)
                            .map(|value| json!(current_price - value))
                            .unwrap_or(Value::Null),
                    )
                })
                .collect::<Map<_, _>>();
            let distance_to_avwap_spot = avwap_references
                .iter()
                .map(|(window, reference)| {
                    (
                        window.clone(),
                        reference
                            .get("avwap_spot")
                            .and_then(Value::as_f64)
                            .map(|value| json!(current_price - value))
                            .unwrap_or(Value::Null),
                    )
                })
                .collect::<Map<_, _>>();

            json!({
                "anchor_role": anchor_role,
                "anchor_id": anchor_id,
                "timeframe_hint": timeframe_hint,
                "mapped_reference_window": mapped_reference_window,
                "current_price": current_price,
                "zone": zone,
                "distance_to_zone_midpoint": zone_midpoint.map(|mid| current_price - mid),
                "distance_to_mapped_avwap_fut": mapped_avwap_reference
                    .get("avwap_fut")
                    .and_then(Value::as_f64)
                    .map(|value| json!(current_price - value))
                    .unwrap_or(Value::Null),
                "distance_to_mapped_avwap_spot": mapped_avwap_reference
                    .get("avwap_spot")
                    .and_then(Value::as_f64)
                    .map(|value| json!(current_price - value))
                    .unwrap_or(Value::Null),
                "distance_to_avwap_fut": distance_to_avwap_fut,
                "distance_to_avwap_spot": distance_to_avwap_spot,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "current_price": current_price,
        "selected_anchor_distances": items,
    })
}

fn summarize_divergence_15m(summary: &StrategicIndicatorSummary) -> Value {
    let divergence = context_child(&summary.driver_layer, "divergence");
    let recent_events = divergence
        .pointer("/recent_7d/events")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| divergence.get("events").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .into_iter()
        .rev()
        .take(3)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();

    json!({
        "signal": divergence.get("signal").cloned().unwrap_or(Value::Null),
        "signals": divergence.get("signals").cloned().unwrap_or(Value::Null),
        "divergence_type": divergence.get("divergence_type").cloned().unwrap_or(Value::Null),
        "likely_driver": divergence.get("likely_driver").cloned().unwrap_or(Value::Null),
        "spot_price_flow_confirm": divergence.get("spot_price_flow_confirm").cloned().unwrap_or(Value::Null),
        "spot_lead_score": divergence.get("spot_lead_score").cloned().unwrap_or(Value::Null),
        "latest": divergence.get("latest").cloned().unwrap_or(Value::Null),
        "latest_7d": divergence.get("latest_7d").cloned().unwrap_or(Value::Null),
        "recent_events": recent_events,
    })
}

fn summarize_footprint_15m(summary: &StrategicIndicatorSummary) -> Value {
    let footprint = context_child(&summary.trigger_layer, "footprint");
    json!({
        "stacked_buy": footprint.get("stacked_buy").cloned().unwrap_or(Value::Null),
        "stacked_sell": footprint.get("stacked_sell").cloned().unwrap_or(Value::Null),
        "ua_top": footprint.get("ua_top").cloned().unwrap_or(Value::Null),
        "ua_bottom": footprint.get("ua_bottom").cloned().unwrap_or(Value::Null),
        "unfinished_auction": footprint.get("unfinished_auction").cloned().unwrap_or(Value::Null),
        "window_delta": footprint.get("window_delta").cloned().unwrap_or(Value::Null),
        "window_15m": footprint
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

#[derive(Debug, Clone)]
struct AggregateFiveMinuteBar {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_base: f64,
    volume_quote: f64,
    count: usize,
}

fn floor_to_5m(ts: DateTime<Utc>) -> DateTime<Utc> {
    let epoch = ts.timestamp();
    let floored = epoch - epoch.rem_euclid(5 * 60);
    DateTime::<Utc>::from_timestamp(floored, 0).unwrap_or(ts)
}

fn aggregate_kline_history_5m(input: &ModelInvocationInput, max_bars: usize) -> Value {
    let bars = raw_indicator_payload(input, "kline_history")
        .pointer("/intervals/1m/markets/futures/bars")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut buckets: BTreeMap<DateTime<Utc>, AggregateFiveMinuteBar> = BTreeMap::new();
    for bar in bars {
        let Some(open_time_raw) = bar.get("open_time").and_then(Value::as_str) else {
            continue;
        };
        let Some(close_time_raw) = bar.get("close_time").and_then(Value::as_str) else {
            continue;
        };
        let Ok(open_time) = DateTime::parse_from_rfc3339(open_time_raw) else {
            continue;
        };
        let Ok(close_time) = DateTime::parse_from_rfc3339(close_time_raw) else {
            continue;
        };
        let Some(open) = bar.get("open").and_then(Value::as_f64) else {
            continue;
        };
        let Some(high) = bar.get("high").and_then(Value::as_f64) else {
            continue;
        };
        let Some(low) = bar.get("low").and_then(Value::as_f64) else {
            continue;
        };
        let Some(close) = bar.get("close").and_then(Value::as_f64) else {
            continue;
        };
        let bucket_start = floor_to_5m(open_time.with_timezone(&Utc));
        let bucket_end = bucket_start + ChronoDuration::minutes(5);
        let entry = buckets
            .entry(bucket_start)
            .or_insert(AggregateFiveMinuteBar {
                open_time: bucket_start,
                close_time: bucket_end,
                open,
                high,
                low,
                close,
                volume_base: 0.0,
                volume_quote: 0.0,
                count: 0,
            });
        if entry.count == 0 {
            entry.open = open;
        }
        entry.high = entry.high.max(high);
        entry.low = entry.low.min(low);
        entry.close = close;
        entry.volume_base += bar
            .get("volume_base")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        entry.volume_quote += bar
            .get("volume_quote")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        entry.count += 1;
        entry.close_time = close_time.with_timezone(&Utc);
    }

    let aggregated = buckets
        .into_values()
        .filter(|bar| bar.count >= 5)
        .map(|bar| {
            json!({
                "open_time": bar.open_time,
                "close_time": bar.close_time,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume_base": bar.volume_base,
                "volume_quote": bar.volume_quote,
                "aggregated_from_minutes": bar.count,
                "is_closed": true,
            })
        })
        .collect::<Vec<_>>();

    let start = aggregated.len().saturating_sub(max_bars);
    Value::Array(aggregated[start..].to_vec())
}

fn build_strategic_context_frozen(
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Value {
    let position_layer = &summary.position_layer;
    let state_layer = &summary.state_layer;
    let avwap = context_child(position_layer, "avwap");
    let tpo_market_profile = context_child(position_layer, "tpo_market_profile");
    let options_surface = context_child(&summary.aux_context, "options_surface");

    json!({
        "price_volume_structure_4h": context_child(position_layer, "price_volume_structure")
            .get("by_window")
            .and_then(|value| value.get("4h"))
            .cloned()
            .unwrap_or(Value::Null),
        "liquidation_density_4h": context_child(position_layer, "liquidation_density")
            .get("by_window")
            .and_then(|value| value.get("4h"))
            .cloned()
            .unwrap_or(Value::Null),
        "selected_avwap_anchors": build_selected_avwap_anchors(summary, stage1_output),
        "tpo_4h_1d": json!({
            "as_of_ts": tpo_market_profile.get("as_of_ts").cloned().unwrap_or(Value::Null),
            "by_session": object_slice(
                tpo_market_profile.get("by_session").unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
        }),
        "rvwap_sigma_bands_4h": context_child(position_layer, "rvwap_sigma_bands")
            .get("by_window")
            .and_then(|value| value.get("4h"))
            .cloned()
            .unwrap_or(Value::Null),
        "ema_trend_regime_4h_1d": json!({
            "ema_100_htf": object_slice(
                context_child(position_layer, "ema_trend_regime")
                    .get("ema_100_htf")
                    .unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
            "ema_200_htf": object_slice(
                context_child(position_layer, "ema_trend_regime")
                    .get("ema_200_htf")
                    .unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
            "trend_regime_by_tf": object_slice(
                context_child(position_layer, "ema_trend_regime")
                    .get("trend_regime_by_tf")
                    .unwrap_or(&Value::Null),
                &["4h", "1d"]
            ),
        }),
        "open_interest_4h": context_child(state_layer, "open_interest")
            .get("by_window")
            .and_then(|value| value.get("4h"))
            .cloned()
            .unwrap_or(Value::Null),
        "long_short_ratios_4h": context_child(state_layer, "long_short_ratios")
            .get("by_window")
            .and_then(|value| value.get("4h"))
            .cloned()
            .unwrap_or(Value::Null),
        "options_regime_1d": options_surface
            .get("strategic_summary")
            .and_then(|value| value.get("windows"))
            .and_then(|value| value.get("1d"))
            .cloned()
            .unwrap_or(Value::Null),
        "avwap_reference_7d": json!({
            "lookback": avwap.get("lookback").cloned().unwrap_or(Value::Null),
            "anchor_ts": avwap.get("anchor_ts").cloned().unwrap_or(Value::Null),
            "avwap_fut": avwap.get("avwap_fut").cloned().unwrap_or(Value::Null),
            "avwap_spot": avwap.get("avwap_spot").cloned().unwrap_or(Value::Null),
        }),
    })
}

fn build_entry_location_context_15m(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
) -> Value {
    let position_layer = &summary.position_layer;
    let state_layer = &summary.state_layer;
    let driver_layer = &summary.driver_layer;

    json!({
        "price_volume_structure_15m": context_child(position_layer, "price_volume_structure")
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
        "liquidation_density_15m": context_child(position_layer, "liquidation_density")
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
        "avwap_anchor_distances": build_avwap_anchor_distances(input, summary, stage1_output),
        "rvwap_sigma_bands_15m": context_child(position_layer, "rvwap_sigma_bands")
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
        "cvd_pack_15m": context_child(driver_layer, "cvd_pack")
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
        "divergence_15m_summary": summarize_divergence_15m(summary),
        "vpin_15m": context_child(state_layer, "vpin")
            .get("by_window")
            .and_then(|value| value.get("15m"))
            .cloned()
            .unwrap_or(Value::Null),
        "footprint_15m_summary": summarize_footprint_15m(summary),
    })
}

fn build_continuity_confirmation_context_5m(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
) -> Value {
    let state_layer = &summary.state_layer;
    json!({
        "orderbook_depth_5m": raw_indicator_payload(input, "orderbook_depth")
            .get("by_window")
            .and_then(|value| value.get("5m"))
            .cloned()
            .unwrap_or(Value::Null),
        "cvd_pack_5m": raw_indicator_payload(input, "cvd_pack")
            .get("by_window")
            .and_then(|value| value.get("5m"))
            .cloned()
            .unwrap_or(Value::Null),
        "open_interest_5m": context_child(state_layer, "open_interest")
            .get("by_window")
            .and_then(|value| value.get("5m"))
            .cloned()
            .unwrap_or(Value::Null),
        "long_short_ratios_5m": context_child(state_layer, "long_short_ratios")
            .get("by_window")
            .and_then(|value| value.get("5m"))
            .cloned()
            .unwrap_or(Value::Null),
        "kline_history_5m": aggregate_kline_history_5m(input, 6),
    })
}

fn build_state_guardrail_snapshot(summary: &StrategicIndicatorSummary) -> Value {
    let state_layer = &summary.state_layer;
    json!({
        "open_interest": window_slice(context_child(state_layer, "open_interest"), "by_window", &["15m", "4h", "1d", "3d"]),
        "long_short_ratios": window_slice(context_child(state_layer, "long_short_ratios"), "by_window", &["15m", "4h", "1d", "3d"]),
        "funding": window_slice(context_child(state_layer, "funding_rate"), "by_window", &["4h", "1d"]),
        "vpin": window_slice(context_child(state_layer, "vpin"), "by_window", &["4h", "1d"]),
    })
}

fn build_driver_guardrail_snapshot(summary: &StrategicIndicatorSummary) -> Value {
    let driver_layer = &summary.driver_layer;
    json!({
        "cvd_pack": window_slice(context_child(driver_layer, "cvd_pack"), "by_window", &["4h", "1d"]),
        "divergence": context_child(driver_layer, "divergence"),
        "whale_trades": window_slice(context_child(driver_layer, "whale_trades"), "by_window", &["4h", "1d"]),
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
    let path = stage1_output.current_path.as_ref()?;
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

fn build_account_context(trading_state: &TradingStateSnapshot) -> WorkflowAccountContext {
    WorkflowAccountContext {
        total_wallet_balance: trading_state.total_wallet_balance,
        available_balance: trading_state.available_balance,
        has_active_positions: trading_state.has_active_positions,
        has_open_orders: trading_state.has_open_orders,
    }
}

fn snapshot_matches_direction(snapshot: &EntrySnapshot, symbol: &str, direction: &str) -> bool {
    snapshot.symbol.eq_ignore_ascii_case(symbol) && snapshot.side.eq_ignore_ascii_case(direction)
}

fn live_exit_prices_for_position(
    position: &crate::execution::binance::ActivePositionSnapshot,
    open_orders: &[OpenOrderSnapshot],
) -> (Option<f64>, Option<f64>) {
    let direction = if position.position_amt >= 0.0 {
        "LONG"
    } else {
        "SHORT"
    };
    let exit_side = if direction == "LONG" { "SELL" } else { "BUY" };
    let is_take_profit = |price: f64| {
        if direction == "LONG" {
            price > position.entry_price
        } else {
            price < position.entry_price
        }
    };
    let is_stop_loss = |price: f64| {
        if direction == "LONG" {
            price < position.entry_price
        } else {
            price > position.entry_price
        }
    };
    let same_position_side = |order: &OpenOrderSnapshot| {
        position.position_side.eq_ignore_ascii_case("BOTH")
            || order.position_side.eq_ignore_ascii_case("BOTH")
            || order.position_side.eq_ignore_ascii_case(&position.position_side)
    };
    let exit_prices = open_orders
        .iter()
        .filter(|order| {
            (order.reduce_only || order.close_position)
                && order.side.eq_ignore_ascii_case(exit_side)
                && same_position_side(order)
        })
        .flat_map(|order| [order.price, order.stop_price])
        .filter(|price| *price > 0.0)
        .collect::<Vec<_>>();
    let current_tp_price = exit_prices
        .iter()
        .copied()
        .filter(|price| is_take_profit(*price))
        .min_by(|left, right| {
            (left - position.entry_price)
                .abs()
                .total_cmp(&(right - position.entry_price).abs())
        });
    let current_sl_price = exit_prices
        .iter()
        .copied()
        .filter(|price| is_stop_loss(*price))
        .min_by(|left, right| {
            (left - position.entry_price)
                .abs()
                .total_cmp(&(right - position.entry_price).abs())
        });
    (current_tp_price, current_sl_price)
}

fn workflow_positions_for_active_position(
    symbol: &str,
    position: &crate::execution::binance::ActivePositionSnapshot,
    open_orders: &[OpenOrderSnapshot],
    entry_snapshots: &HashMap<String, EntrySnapshot>,
) -> Vec<WorkflowPosition> {
    let direction = if position.position_amt >= 0.0 {
        "LONG"
    } else {
        "SHORT"
    }
    .to_string();
    let (current_tp_price, current_sl_price) =
        live_exit_prices_for_position(position, open_orders);
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
            current_tp_price,
            current_sl_price,
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
            current_tp_price,
            current_sl_price,
            entry_snapshot: Some(snapshot),
        })
        .collect()
}

fn workflow_position_relevance_key(position: &WorkflowPosition) -> (u8, i64, i64) {
    position
        .entry_snapshot
        .as_ref()
        .map(|snapshot| {
            (
                1,
                snapshot.updated_at.timestamp_millis(),
                snapshot.created_at.timestamp_millis(),
            )
        })
        .unwrap_or((0, i64::MIN, i64::MIN))
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
            PostFillBracketTemplate {
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
    let same_side_positions = trading_state
        .active_positions
        .iter()
        .flat_map(|position| {
            workflow_positions_for_active_position(
                &trading_state.symbol,
                position,
                &trading_state.open_orders,
                entry_snapshots,
            )
        })
        .filter(|position| position.direction.eq_ignore_ascii_case(&current_path.side))
        .collect::<Vec<_>>();

    let current_path_positions = same_side_positions
        .iter()
        .filter(|position| {
            position
                .entry_snapshot
                .as_ref()
                .map(|snapshot| snapshot.path_id == current_path.id)
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    if !current_path_positions.is_empty() {
        return current_path_positions;
    }

    same_side_positions
        .into_iter()
        .max_by_key(workflow_position_relevance_key)
        .into_iter()
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

pub fn build_stage2a_prompt_input(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
    trading_state: &TradingStateSnapshot,
) -> Stage2APromptInput {
    Stage2APromptInput {
        task: "Review the current strategic path and design the tactical entry from the dedicated Stage2 context"
            .to_string(),
        strategic_context_frozen: build_strategic_context_frozen(summary, stage1_output),
        entry_location_context_15m: build_entry_location_context_15m(input, summary, stage1_output),
        continuity_confirmation_context_5m: build_continuity_confirmation_context_5m(input, summary),
        state_guardrail_snapshot: build_state_guardrail_snapshot(summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(summary, stage1_output),
        stage1_output: stage1_output.clone(),
        account: build_account_context(trading_state),
    }
}

pub fn build_stage2b_prompt_input(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
    active_position: WorkflowPosition,
    previous_management_plan: Option<PositionManagementPlan>,
    trading_state: &TradingStateSnapshot,
) -> Stage2BPromptInput {
    Stage2BPromptInput {
        task: "Manage the active position from the dedicated Stage2 context to maximize returns"
            .to_string(),
        exposure_state: "in_position".to_string(),
        active_positions: vec![active_position],
        strategic_context_frozen: build_strategic_context_frozen(summary, stage1_output),
        entry_location_context_15m: build_entry_location_context_15m(input, summary, stage1_output),
        continuity_confirmation_context_5m: build_continuity_confirmation_context_5m(
            input, summary,
        ),
        state_guardrail_snapshot: build_state_guardrail_snapshot(summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(summary, stage1_output),
        stage1_output: stage1_output.clone(),
        previous_management_plan,
        account: build_account_context(trading_state),
    }
}

pub fn build_stage2c_prompt_input(
    input: &ModelInvocationInput,
    summary: &StrategicIndicatorSummary,
    stage1_output: &Stage1Output,
    exposure_state: &str,
    active_order: WorkflowPendingOrder,
    previous_pending_order_management_plan: Option<PendingOrderManagementPlan>,
    trading_state: &TradingStateSnapshot,
) -> Stage2CPromptInput {
    Stage2CPromptInput {
        task: "Manage the live pending order from the dedicated Stage2 context to maximize returns"
            .to_string(),
        exposure_state: exposure_state.to_string(),
        active_orders: vec![active_order],
        strategic_context_frozen: build_strategic_context_frozen(summary, stage1_output),
        entry_location_context_15m: build_entry_location_context_15m(input, summary, stage1_output),
        continuity_confirmation_context_5m: build_continuity_confirmation_context_5m(
            input, summary,
        ),
        state_guardrail_snapshot: build_state_guardrail_snapshot(summary),
        driver_guardrail_snapshot: build_driver_guardrail_snapshot(summary),
        options_guardrail_snapshot: build_options_guardrail_snapshot(summary, stage1_output),
        stage1_output: stage1_output.clone(),
        previous_pending_order_management_plan,
        account: build_account_context(trading_state),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_kline_history_5m, build_stage2a_prompt_input, build_stage2b_prompt_input,
        build_stage2c_prompt_input, stage2b_active_positions_for_current_path,
    };
    use crate::execution::binance::TradingStateSnapshot;
    use crate::llm::input::ManagementSnapshotForLlm;
    use crate::llm::input::ModelInvocationInput;
    use crate::workflow::code_layer::build_indicator_summary;
    use crate::workflow::schema::{
        CurrentPath, EntrySnapshot, MapSummary, OpportunityAssessment, PendingOrderManagementPlan,
        PositionManagementPlan, PriceZone, ReevaluationTrigger, Stage1Meta, Stage1Output,
        TrackedZone, WorkflowPendingOrder, WorkflowPosition,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn aggregate_kline_history_5m_builds_complete_bars_from_1m() {
        let bars = (0..10)
            .map(|minute| {
                let open_minute = format!("2026-03-30T06:{minute:02}:00Z");
                let close_minute = format!("2026-03-30T06:{:02}:00Z", minute + 1);
                json!({
                    "open_time": open_minute,
                    "close_time": close_minute,
                    "open": 100.0 + minute as f64,
                    "high": 101.0 + minute as f64,
                    "low": 99.5 + minute as f64,
                    "close": 100.5 + minute as f64,
                    "volume_base": 10.0,
                    "volume_quote": 1000.0,
                    "is_closed": true,
                })
            })
            .collect::<Vec<_>>();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: Utc::now(),
            window_code: "1m".to_string(),
            indicator_count: 1,
            source_routing_key: "test".to_string(),
            source_published_at: None,
            received_at: Utc::now(),
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "1m": {
                                "markets": {
                                    "futures": {
                                        "bars": bars
                                    }
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            trading_state: None,
            management_snapshot: None,
        };

        let aggregated = aggregate_kline_history_5m(&input, 6);
        let bars = aggregated.as_array().expect("bars");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].get("open"), Some(&json!(100.0)));
        assert_eq!(bars[0].get("close"), Some(&json!(104.5)));
        assert_eq!(bars[1].get("open"), Some(&json!(105.0)));
        assert_eq!(bars[1].get("close"), Some(&json!(109.5)));
    }

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: DateTime::parse_from_rfc3339("2026-03-30T06:15:00Z")
                    .expect("stage1_ts")
                    .with_timezone(&Utc),
            },
            monitoring_status: "path_live".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: MapSummary::default(),
            opportunity_assessment: OpportunityAssessment::default(),
            current_script: None,
            driver_attribution: None,
            current_path: Some(CurrentPath {
                id: "path_long".to_string(),
                side: "LONG".to_string(),
                thesis: "trend continuation".to_string(),
                risk_grade: "A".to_string(),
                activation_anchor_id: Some("zone_activation".to_string()),
                strategic_activation_level: PriceZone {
                    low: 100.0,
                    high: 102.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                first_path_target_anchor_id: Some("zone_target_1".to_string()),
                first_path_target: PriceZone {
                    low: 104.0,
                    high: 106.0,
                    timeframe: Some("1d".to_string()),
                    label: None,
                    reason: None,
                },
                next_path_target_anchor_id: Some("zone_target_2".to_string()),
                next_path_target: PriceZone {
                    low: 108.0,
                    high: 110.0,
                    timeframe: Some("3d".to_string()),
                    label: None,
                    reason: None,
                },
                failure_anchor_id: Some("zone_failure".to_string()),
                failure_level: PriceZone {
                    low: 96.0,
                    high: 97.0,
                    timeframe: Some("4h".to_string()),
                    label: None,
                    reason: None,
                },
                failure_switch: None,
                setup_type: "pullback".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                tracked_zones: vec![
                    TrackedZone {
                        zone_id: "zone_activation".to_string(),
                        timeframe: "4h".to_string(),
                        role: "activation".to_string(),
                        low: 100.0,
                        high: 102.0,
                        reason: None,
                    },
                    TrackedZone {
                        zone_id: "zone_target_1".to_string(),
                        timeframe: "1d".to_string(),
                        role: "target".to_string(),
                        low: 104.0,
                        high: 106.0,
                        reason: None,
                    },
                    TrackedZone {
                        zone_id: "zone_target_2".to_string(),
                        timeframe: "3d".to_string(),
                        role: "target".to_string(),
                        low: 108.0,
                        high: 110.0,
                        reason: None,
                    },
                    TrackedZone {
                        zone_id: "zone_failure".to_string(),
                        timeframe: "4h".to_string(),
                        role: "failure".to_string(),
                        low: 96.0,
                        high: 97.0,
                        reason: None,
                    },
                ],
            }),
        }
    }

    fn sample_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 750.0,
        }
    }

    fn sample_stage2_input(management: bool) -> ModelInvocationInput {
        let bars_1m = (0..10)
            .map(|minute| {
                let open_minute = format!("2026-03-30T06:{minute:02}:00Z");
                let close_minute = format!("2026-03-30T06:{:02}:00Z", minute + 1);
                json!({
                    "open_time": open_minute,
                    "close_time": close_minute,
                    "open": 100.0 + minute as f64,
                    "high": 100.6 + minute as f64,
                    "low": 99.7 + minute as f64,
                    "close": 100.2 + minute as f64,
                    "volume_base": 10.0,
                    "volume_quote": 1000.0,
                    "is_closed": true,
                })
            })
            .collect::<Vec<_>>();

        ModelInvocationInput {
            symbol: "TESTUSDT".to_string(),
            ts_bucket: DateTime::parse_from_rfc3339("2026-03-30T06:15:00Z")
                .expect("ts_bucket")
                .with_timezone(&Utc),
            window_code: "1m".to_string(),
            indicator_count: 12,
            source_routing_key: "bundle.1m.TESTUSDT".to_string(),
            source_published_at: None,
            received_at: Utc::now(),
            indicators: json!({
                "price_volume_structure": {
                    "payload": {
                        "by_window": {
                            "15m": {"poc_price": 104.0, "vah": 105.0, "val": 103.0, "value_area_levels": [{"price": 104.0, "volume": 40.0}]},
                            "4h": {"poc_price": 101.0, "vah": 102.0, "val": 99.5, "value_area_levels": [{"price": 101.0, "volume": 200.0}]}
                        }
                    }
                },
                "liquidation_density": {
                    "payload": {
                        "by_window": {
                            "15m": {"clusters": [{"price": 104.5, "notional_usd": 100000.0}]},
                            "4h": {"clusters": [{"price": 101.5, "notional_usd": 500000.0}]}
                        }
                    }
                },
                "avwap": {
                    "payload": {
                        "anchor_ts": "2026-03-23T00:00:00Z",
                        "lookback": "7d",
                        "avwap_fut": 103.4,
                        "avwap_spot": 103.1,
                        "price_minus_avwap_fut": 6.8,
                        "price_minus_spot_avwap_fut": 7.1,
                        "series_by_window": {
                            "15m": [{"ts": "2026-03-30T06:15:00Z", "avwap_fut": 105.1, "avwap_spot": 104.8}],
                            "4h": [{"ts": "2026-03-30T04:00:00Z", "avwap_fut": 101.2, "avwap_spot": 100.9}],
                            "1d": [{"ts": "2026-03-30T00:00:00Z", "avwap_fut": 104.8, "avwap_spot": 104.3}],
                            "3d": [{"ts": "2026-03-30T00:00:00Z", "avwap_fut": 108.7, "avwap_spot": 108.1}]
                        }
                    }
                },
                "tpo_market_profile": {
                    "payload": {
                        "as_of_ts": "2026-03-30T06:15:00Z",
                        "by_session": {
                            "4h": {"poc": 101.0, "vah": 102.0, "val": 99.0},
                            "1d": {"poc": 104.0, "vah": 106.0, "val": 100.0}
                        }
                    }
                },
                "rvwap_sigma_bands": {
                    "payload": {
                        "by_window": {
                            "15m": {"mid": 104.0, "sigma_1_up": 105.0},
                            "4h": {"mid": 101.0, "sigma_1_up": 103.0}
                        }
                    }
                },
                "ema_trend_regime": {
                    "payload": {
                        "as_of_ts": "2026-03-30T06:15:00Z",
                        "ema_100_htf": {
                            "4h": {"value": 100.5},
                            "1d": {"value": 98.0}
                        },
                        "ema_200_htf": {
                            "4h": {"value": 99.0},
                            "1d": {"value": 95.0}
                        },
                        "trend_regime_by_tf": {
                            "4h": "bullish_supportive",
                            "1d": "bullish_supportive"
                        },
                        "output_sampling": {
                            "4h": {
                                "trend_regime": "bullish_supportive",
                                "by_tf": {"4h": "bullish_supportive"}
                            }
                        }
                    }
                },
                "open_interest": {
                    "payload": {
                        "by_window": {
                            "5m": {"oi": 1000.0},
                            "15m": {"oi": 1010.0},
                            "4h": {"oi": 1200.0},
                            "1d": {"oi": 1500.0},
                            "3d": {"oi": 1800.0}
                        }
                    }
                },
                "long_short_ratios": {
                    "payload": {
                        "by_window": {
                            "5m": {"long_account_ratio": 0.55},
                            "15m": {"long_account_ratio": 0.56},
                            "4h": {"long_account_ratio": 0.58},
                            "1d": {"long_account_ratio": 0.6},
                            "3d": {"long_account_ratio": 0.61}
                        }
                    }
                },
                "options_surface": {
                    "payload": {
                        "as_of_ts": "2026-03-30T06:15:00Z",
                        "by_window": {
                            "15m": {"is_ready": true, "atm_strike_front": 101.0, "skew_state": "put_skew", "term_structure_state": "flat"},
                            "4h": {"is_ready": true, "atm_strike_front": 101.0, "atm_iv_regime": "elevated", "skew_state": "put_skew", "term_structure_state": "flat"},
                            "1d": {"is_ready": true, "atm_strike_front": 105.0, "atm_iv_regime": "elevated", "skew_state": "put_skew", "term_structure_state": "backwardation"}
                        }
                    }
                },
                "cvd_pack": {
                    "payload": {
                        "by_window": {
                            "5m": {"delta_fut": 10.0},
                            "15m": {"delta_fut": 20.0},
                            "4h": {"delta_fut": 50.0},
                            "1d": {"delta_fut": 80.0}
                        }
                    }
                },
                "divergence": {
                    "payload": {
                        "divergence_type": "bullish",
                        "likely_driver": "spot_cvd",
                        "spot_lead_score": 0.8,
                        "recent_7d": {
                            "event_count": 2,
                            "events": [
                                {"confirm_ts": "2026-03-30T05:45:00Z", "type": "bullish_divergence", "price": 104.0},
                                {"confirm_ts": "2026-03-30T06:00:00Z", "type": "bullish_divergence", "price": 105.0}
                            ]
                        },
                        "latest_7d": {"confirm_ts": "2026-03-30T06:00:00Z", "type": "bullish_divergence", "price": 105.0}
                    }
                },
                "vpin": {
                    "payload": {
                        "by_window": {
                            "5m": {"vpin_fut": 0.4},
                            "15m": {"vpin_fut": 0.42},
                            "4h": {"vpin_fut": 0.48},
                            "1d": {"vpin_fut": 0.5},
                            "3d": {"vpin_fut": 0.52}
                        }
                    }
                },
                "footprint": {
                    "payload": {
                        "stacked_buy": true,
                        "stacked_sell": false,
                        "ua_top": 106.0,
                        "ua_bottom": 103.0,
                        "unfinished_auction": false,
                        "window_delta": 1200.0,
                        "by_window": {
                            "15m": {
                                "window_delta": 1200.0,
                                "buy_stacks": [{"price": 105.0, "stack_len": 3}],
                                "sell_stacks": []
                            }
                        }
                    }
                },
                "orderbook_depth": {
                    "payload": {
                        "by_window": {
                            "5m": {"best_bid": 109.1, "best_ask": 109.3}
                        }
                    }
                },
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "1m": {
                                "markets": {
                                    "futures": {
                                        "bars": bars_1m
                                    }
                                }
                            },
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [{
                                            "open_time": "2026-03-30T06:00:00Z",
                                            "close_time": "2026-03-30T06:15:00Z",
                                            "open": 104.0,
                                            "high": 109.5,
                                            "low": 103.8,
                                            "close": 109.2,
                                            "is_closed": true
                                        }]
                                    }
                                }
                            }
                        }
                    }
                }
            }),
            missing_indicator_codes: vec![],
            trading_state: None,
            management_snapshot: management.then(|| ManagementSnapshotForLlm {
                context_state: "in_position".to_string(),
                has_active_positions: true,
                has_open_orders: false,
                active_position_count: 1,
                open_order_count: 0,
                positions: Vec::new(),
                pending_order: None,
                last_management_reason: None,
                position_context: None,
            }),
        }
    }

    fn sample_stage2b_position() -> WorkflowPosition {
        WorkflowPosition {
            context_key: "TESTUSDT:LONG:path_long".to_string(),
            position_side: "LONG".to_string(),
            direction: "LONG".to_string(),
            quantity: 1.0,
            leverage: 3,
            entry_price: 101.0,
            mark_price: 109.2,
            unrealized_pnl: 8.2,
            current_tp_price: Some(106.0),
            current_sl_price: Some(97.0),
            entry_snapshot: None,
        }
    }

    fn sample_stage2c_order() -> WorkflowPendingOrder {
        WorkflowPendingOrder {
            context_key: "TESTUSDT:LONG:path_long".to_string(),
            order_id: 42,
            side: "LONG".to_string(),
            position_side: "LONG".to_string(),
            order_type: "LIMIT".to_string(),
            status: "NEW".to_string(),
            quantity: 1.0,
            executed_quantity: 0.0,
            price: 101.0,
            stop_price: 0.0,
            post_fill_bracket_template: None,
            entry_snapshot: None,
        }
    }

    #[test]
    fn stage2b_active_positions_fall_back_to_latest_same_side_snapshot_when_path_switched() {
        let stage1_output = sample_stage1_output();
        let trading_state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![crate::execution::binance::ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 101.0,
                mark_price: 103.5,
                unrealized_pnl: 2.5,
                leverage: 3,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 750.0,
        };
        let older_snapshot = EntrySnapshot {
            symbol: "TESTUSDT".to_string(),
            context_key: "TESTUSDT:LONG:path_older".to_string(),
            path_id: "path_older".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("pullback_acceptance".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 96.0,
            take_profit_1: 106.0,
            take_profit_2: 109.0,
            allowed_stop_loss_levels: vec![96.0],
            allowed_take_profit_levels: vec![106.0, 109.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: Vec::new(),
            created_at: DateTime::parse_from_rfc3339("2026-03-30T05:00:00Z")
                .expect("older created")
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2026-03-30T05:10:00Z")
                .expect("older updated")
                .with_timezone(&Utc),
        };
        let newer_snapshot = EntrySnapshot {
            symbol: "TESTUSDT".to_string(),
            context_key: "TESTUSDT:LONG:path_previous".to_string(),
            path_id: "path_previous".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("pullback_acceptance".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 97.0,
            take_profit_1: 107.0,
            take_profit_2: 110.0,
            allowed_stop_loss_levels: vec![97.0],
            allowed_take_profit_levels: vec![107.0, 110.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: Vec::new(),
            created_at: DateTime::parse_from_rfc3339("2026-03-31T05:00:00Z")
                .expect("newer created")
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2026-03-31T05:10:00Z")
                .expect("newer updated")
                .with_timezone(&Utc),
        };
        let entry_snapshots = HashMap::from([
            (older_snapshot.context_key.clone(), older_snapshot),
            (newer_snapshot.context_key.clone(), newer_snapshot),
        ]);

        let positions =
            stage2b_active_positions_for_current_path(&stage1_output, &trading_state, &entry_snapshots);

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].context_key, "TESTUSDT:LONG:path_previous");
        assert_eq!(
            positions[0]
                .entry_snapshot
                .as_ref()
                .expect("fallback snapshot")
                .path_id,
            "path_previous"
        );
    }

    #[test]
    fn stage2b_active_positions_use_live_exit_orders_for_current_tp_and_sl() {
        let stage1_output = sample_stage1_output();
        let trading_state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![crate::execution::binance::ActivePositionSnapshot {
                position_side: "BOTH".to_string(),
                position_amt: 1.0,
                entry_price: 101.0,
                mark_price: 103.5,
                unrealized_pnl: 2.5,
                leverage: 3,
            }],
            open_orders: vec![
                crate::execution::binance::OpenOrderSnapshot {
                    order_id: 41,
                    side: "SELL".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "TAKE_PROFIT_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 1.0,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 106.5,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
                crate::execution::binance::OpenOrderSnapshot {
                    order_id: 42,
                    side: "SELL".to_string(),
                    position_side: "BOTH".to_string(),
                    order_type: "STOP_MARKET".to_string(),
                    status: "NEW".to_string(),
                    orig_qty: 1.0,
                    executed_qty: 0.0,
                    price: 0.0,
                    stop_price: 97.5,
                    close_position: true,
                    reduce_only: true,
                    is_algo_order: true,
                },
            ],
            total_wallet_balance: 1000.0,
            available_balance: 750.0,
        };
        let entry_snapshot = EntrySnapshot {
            symbol: "TESTUSDT".to_string(),
            context_key: "TESTUSDT:LONG:path_current".to_string(),
            path_id: "path_current".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("pullback_acceptance".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 96.0,
            take_profit_1: 108.0,
            take_profit_2: 111.0,
            allowed_stop_loss_levels: vec![96.0],
            allowed_take_profit_levels: vec![108.0, 111.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: Vec::new(),
            created_at: DateTime::parse_from_rfc3339("2026-04-01T01:00:00Z")
                .expect("snapshot created")
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2026-04-01T01:05:00Z")
                .expect("snapshot updated")
                .with_timezone(&Utc),
        };
        let entry_snapshots =
            HashMap::from([(entry_snapshot.context_key.clone(), entry_snapshot.clone())]);

        let positions =
            stage2b_active_positions_for_current_path(&stage1_output, &trading_state, &entry_snapshots);

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].context_key, entry_snapshot.context_key);
        assert_eq!(positions[0].current_tp_price, Some(106.5));
        assert_eq!(positions[0].current_sl_price, Some(97.5));
        assert_eq!(
            positions[0]
                .entry_snapshot
                .as_ref()
                .expect("snapshot")
                .take_profit_1,
            108.0
        );
        assert_eq!(
            positions[0]
                .entry_snapshot
                .as_ref()
                .expect("snapshot")
                .stop_loss,
            96.0
        );
    }

    #[test]
    fn stage2b_active_positions_clear_current_tp_and_sl_when_live_exit_orders_are_missing() {
        let stage1_output = sample_stage1_output();
        let trading_state = TradingStateSnapshot {
            symbol: "TESTUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![crate::execution::binance::ActivePositionSnapshot {
                position_side: "BOTH".to_string(),
                position_amt: 1.0,
                entry_price: 101.0,
                mark_price: 103.5,
                unrealized_pnl: 2.5,
                leverage: 3,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 750.0,
        };
        let entry_snapshot = EntrySnapshot {
            symbol: "TESTUSDT".to_string(),
            context_key: "TESTUSDT:LONG:path_current".to_string(),
            path_id: "path_current".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("pullback_acceptance".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: Some(0.2),
            stop_loss: 96.0,
            take_profit_1: 108.0,
            take_profit_2: 111.0,
            allowed_stop_loss_levels: vec![96.0],
            allowed_take_profit_levels: vec![108.0, 111.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: Vec::new(),
            created_at: DateTime::parse_from_rfc3339("2026-04-01T01:00:00Z")
                .expect("snapshot created")
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2026-04-01T01:05:00Z")
                .expect("snapshot updated")
                .with_timezone(&Utc),
        };
        let entry_snapshots =
            HashMap::from([(entry_snapshot.context_key.clone(), entry_snapshot.clone())]);

        let positions =
            stage2b_active_positions_for_current_path(&stage1_output, &trading_state, &entry_snapshots);

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].context_key, entry_snapshot.context_key);
        assert_eq!(positions[0].current_tp_price, None);
        assert_eq!(positions[0].current_sl_price, None);
    }

    #[test]
    fn stage2a_prompt_input_contract_uses_abc_layers_and_anchor_mapped_avwap() {
        let input = sample_stage2_input(false);
        let stage1_output = sample_stage1_output();
        let tracked_zones = stage1_output
            .current_path
            .as_ref()
            .expect("current_path")
            .tracked_zones
            .clone();
        let summary = build_indicator_summary(&input, &tracked_zones).expect("indicator summary");
        let prompt =
            build_stage2a_prompt_input(&input, &summary, &stage1_output, &sample_trading_state());
        let encoded = serde_json::to_value(&prompt).expect("encode prompt");

        for key in [
            "strategic_context_frozen",
            "entry_location_context_15m",
            "continuity_confirmation_context_5m",
            "stage1_output",
            "account",
        ] {
            assert!(encoded.get(key).is_some(), "missing key {key}");
        }
        for legacy_key in [
            "latest_15m_trigger_facts",
            "path_runtime_state",
            "candidate_event",
            "previous_tactical_plan",
            "tactical_position_slice",
            "hard_invalidation",
        ] {
            assert!(
                encoded.get(legacy_key).is_none(),
                "legacy key leaked: {legacy_key}"
            );
        }

        let selected_anchors = encoded["strategic_context_frozen"]["selected_avwap_anchors"]
            .as_array()
            .expect("selected anchors");
        let next_target_anchor = selected_anchors
            .iter()
            .find(|item| item["anchor_role"] == "next_path_target")
            .expect("next path target anchor");
        assert_eq!(next_target_anchor["mapped_reference_window"], json!("3d"));
        assert_eq!(
            next_target_anchor["mapped_avwap_reference"]["avwap_fut"],
            json!(108.7)
        );
        assert_eq!(
            encoded["strategic_context_frozen"]["ema_trend_regime_4h_1d"]["trend_regime_by_tf"]
                ["4h"],
            json!("bullish_supportive")
        );
        let mapped_distance = encoded["entry_location_context_15m"]["avwap_anchor_distances"]
            ["selected_anchor_distances"][2]["distance_to_mapped_avwap_fut"]
            .as_f64()
            .expect("mapped avwap distance");
        assert!((mapped_distance - 0.5).abs() < 1e-9);
    }

    #[test]
    fn stage2b_and_stage2c_prompt_contracts_keep_management_context_without_legacy_stage2a_fields()
    {
        let input = sample_stage2_input(true);
        let stage1_output = sample_stage1_output();
        let tracked_zones = stage1_output
            .current_path
            .as_ref()
            .expect("current_path")
            .tracked_zones
            .clone();
        let summary = build_indicator_summary(&input, &tracked_zones).expect("indicator summary");
        let trading_state = sample_trading_state();

        let stage2b_prompt = build_stage2b_prompt_input(
            &input,
            &summary,
            &stage1_output,
            sample_stage2b_position(),
            Some(PositionManagementPlan {
                path_id: "path_long".to_string(),
                exposure_state: "in_position".to_string(),
                path_live_assessment: "live".to_string(),
                path_assessment_reason: Some("carry forward".to_string()),
                actions: Vec::new(),
                management_note: "hold".to_string(),
            }),
            &trading_state,
        );
        let stage2b_encoded = serde_json::to_value(&stage2b_prompt).expect("encode stage2b");
        assert!(stage2b_encoded.get("active_positions").is_some());
        assert!(stage2b_encoded.get("previous_management_plan").is_some());
        assert!(stage2b_encoded.get("strategic_context_frozen").is_some());
        assert!(stage2b_encoded.get("entry_location_context_15m").is_some());
        assert!(stage2b_encoded
            .get("continuity_confirmation_context_5m")
            .is_some());
        assert!(stage2b_encoded.get("previous_tactical_plan").is_none());
        assert_eq!(
            stage2b_encoded["strategic_context_frozen"]["ema_trend_regime_4h_1d"]
                ["trend_regime_by_tf"]["1d"],
            json!("bullish_supportive")
        );

        let stage2c_prompt = build_stage2c_prompt_input(
            &input,
            &summary,
            &stage1_output,
            "pending_entry_only",
            sample_stage2c_order(),
            Some(PendingOrderManagementPlan {
                path_id: "path_long".to_string(),
                exposure_state: "pending_entry_only".to_string(),
                path_live_assessment: "live".to_string(),
                path_assessment_reason: Some("carry forward".to_string()),
                actions: Vec::new(),
                management_note: "keep".to_string(),
            }),
            &trading_state,
        );
        let stage2c_encoded = serde_json::to_value(&stage2c_prompt).expect("encode stage2c");
        assert!(stage2c_encoded.get("active_orders").is_some());
        assert!(stage2c_encoded
            .get("previous_pending_order_management_plan")
            .is_some());
        assert!(stage2c_encoded.get("strategic_context_frozen").is_some());
        assert!(stage2c_encoded.get("entry_location_context_15m").is_some());
        assert!(stage2c_encoded
            .get("continuity_confirmation_context_5m")
            .is_some());
        assert!(stage2c_encoded.get("candidate_event").is_none());
        assert_eq!(
            stage2c_encoded["entry_location_context_15m"]["avwap_anchor_distances"]
                ["selected_anchor_distances"][0]["mapped_reference_window"],
            json!("4h")
        );
    }
}
