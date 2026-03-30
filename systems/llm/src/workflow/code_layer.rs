use crate::llm::{
    filter::{code_layer_entry, code_layer_management, core_shared},
    input::ModelInvocationInput,
};
use crate::workflow::schema::{
    AuctionContext, RecentBar, StrategicIndicatorSummary, StrategicSummaryMeta, TrackedZone,
    ZoneState,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

const EVENT_TS_KEYS: &[&str] = &[
    "confirmed_at",
    "confirm_ts",
    "event_time",
    "detected_at",
    "timestamp",
    "ts",
    "close_time",
];
const EVENT_PRICE_KEYS: &[&str] = &[
    "confirmed_price",
    "pivot_price",
    "trigger_price",
    "price",
    "level",
    "close",
    "mark_price",
];

fn raw_indicator_payload(indicators: &Value, key: &str) -> Value {
    indicators
        .get(key)
        .and_then(|value| value.get("payload"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn find_first_matching_value<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(found) = map.get(*key) {
                    return Some(found);
                }
            }
            map.values()
                .find_map(|child| find_first_matching_value(child, keys))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_first_matching_value(child, keys)),
        _ => None,
    }
}

fn enrich_event_contract(mut map: Map<String, Value>, original: &Value) -> Map<String, Value> {
    if !map.contains_key("confirmed_at") {
        if let Some(found) = find_first_matching_value(original, EVENT_TS_KEYS) {
            map.insert("confirmed_at".to_string(), found.clone());
        }
    }
    if !map.contains_key("confirmed_price") {
        if let Some(found) = find_first_matching_value(original, EVENT_PRICE_KEYS) {
            map.insert("confirmed_price".to_string(), found.clone());
        }
    }
    map
}

fn normalize_indicator_payload(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut normalized = Map::new();
            for (key, child) in map {
                normalized.insert(key.clone(), normalize_indicator_payload(child));
            }
            Value::Object(enrich_event_contract(normalized, value))
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(normalize_indicator_payload)
                .collect::<Vec<_>>(),
        ),
        _ => value.clone(),
    }
}

fn indicator_payload(indicators: &Value, key: &str) -> Value {
    normalize_indicator_payload(&raw_indicator_payload(indicators, key))
}

fn object_get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or(&Value::Null)
}

fn is_effectively_missing(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

fn preferred_indicator_payload(
    filtered_indicators: &Value,
    raw_indicators: &Value,
    key: &str,
) -> Value {
    let filtered = indicator_payload(filtered_indicators, key);
    if is_effectively_missing(&filtered) {
        indicator_payload(raw_indicators, key)
    } else {
        filtered
    }
}

fn wrap_filtered_payload(payload: Value) -> Value {
    json!({ "payload": payload })
}

fn summarize_options_surface_window(window: &Value) -> Value {
    json!({
        "is_ready": window.get("is_ready").cloned().unwrap_or(Value::Bool(false)),
        "front_expiry_ts": window.get("front_expiry_ts").cloned().unwrap_or(Value::Null),
        "second_expiry_ts": window.get("second_expiry_ts").cloned().unwrap_or(Value::Null),
        "atm_iv_front": window.get("atm_iv_front").cloned().unwrap_or(Value::Null),
        "atm_iv_second": window.get("atm_iv_second").cloned().unwrap_or(Value::Null),
        "atm_iv_30d_proxy": window.get("atm_iv_30d_proxy").cloned().unwrap_or(Value::Null),
        "atm_iv_regime": window.get("atm_iv_regime").cloned().unwrap_or(Value::Null),
        "rr_25d_front": window.get("rr_25d_front").cloned().unwrap_or(Value::Null),
        "rr_25d_second": window.get("rr_25d_second").cloned().unwrap_or(Value::Null),
        "atm_iv_front_change": window.get("atm_iv_front_change").cloned().unwrap_or(Value::Null),
        "rr_25d_front_change": window.get("rr_25d_front_change").cloned().unwrap_or(Value::Null),
        "skew_state": window.get("skew_state").cloned().unwrap_or(Value::Null),
        "term_structure_state": window.get("term_structure_state").cloned().unwrap_or(Value::Null)
    })
}

fn options_surface_summary(raw: &Value) -> Value {
    let by_window = object_get(raw, "by_window");
    let strategic_windows = ["4h", "1d", "3d"]
        .into_iter()
        .filter_map(|window| {
            by_window
                .get(window)
                .map(|value| (window.to_string(), summarize_options_surface_window(value)))
        })
        .collect::<Map<_, _>>();
    let tactical_windows = ["15m", "4h", "1d"]
        .into_iter()
        .filter_map(|window| {
            by_window
                .get(window)
                .map(|value| (window.to_string(), summarize_options_surface_window(value)))
        })
        .collect::<Map<_, _>>();

    let ready_windows = |windows: &Map<String, Value>| {
        windows
            .iter()
            .filter_map(|(window, value)| {
                value
                    .get("is_ready")
                    .and_then(Value::as_bool)
                    .filter(|ready| *ready)
                    .map(|_| Value::String(window.clone()))
            })
            .collect::<Vec<_>>()
    };

    json!({
        "as_of_ts": raw.get("as_of_ts").cloned().unwrap_or(Value::Null),
        "strategic_summary": {
            "windows": strategic_windows.clone(),
            "ready_windows": ready_windows(&strategic_windows)
        },
        "tactical_guardrail": {
            "windows": tactical_windows.clone(),
            "ready_windows": ready_windows(&tactical_windows)
        }
    })
}

fn filtered_indicator_set(input: &ModelInvocationInput) -> Result<Value> {
    let source = input.indicators.as_object().cloned().unwrap_or_default();
    let ts_bucket = input.ts_bucket.to_rfc3339();
    let mut filtered = code_layer_entry::filter_indicators(&source, Some(&ts_bucket));

    for (code, keep_last) in [
        ("bullish_absorption", 10usize),
        ("bearish_absorption", 10usize),
        ("bullish_initiation", 17usize),
        ("bearish_initiation", 10usize),
    ] {
        if let Some(indicator) = source.get(code) {
            let payload = indicator.get("payload").cloned().unwrap_or(Value::Null);
            filtered.insert(
                code.to_string(),
                wrap_filtered_payload(core_shared::filter_event_indicator_entry_v3(
                    &payload, keep_last,
                )),
            );
        }
    }

    if let Some(snapshot) = input.management_snapshot.as_ref() {
        let snapshot_value =
            serde_json::to_value(snapshot).context("serialize workflow management snapshot")?;
        let management_filtered = code_layer_management::filter_indicators(
            &source,
            Some(&ts_bucket),
            Some(&snapshot_value),
        );
        for (key, value) in management_filtered {
            filtered.insert(key, value);
        }
    }

    Ok(Value::Object(filtered))
}

fn parse_ts(value: &Value, key: &str) -> Option<DateTime<Utc>> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|ts| ts.with_timezone(&Utc))
}

fn extract_kline_history_market_bars(
    indicators: &Value,
    interval_code: &str,
    market: &str,
) -> Vec<Value> {
    let Some(market_node) = indicators
        .get("kline_history")
        .and_then(|value| value.get("payload"))
        .and_then(|value| value.get("intervals"))
        .and_then(|value| value.get(interval_code))
        .and_then(|value| value.get("markets"))
        .and_then(|value| value.get(market))
    else {
        return Vec::new();
    };

    let bars = market_node
        .get("bars")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !bars.is_empty() {
        return bars;
    }

    market_node
        .get("latest_bar")
        .cloned()
        .map(|bar| vec![bar])
        .unwrap_or_default()
}

fn extract_recent_15m_bars(indicators: &Value) -> Vec<RecentBar> {
    let bars = extract_kline_history_market_bars(indicators, "15m", "futures");

    bars.into_iter()
        .rev()
        .take(8)
        .filter_map(|bar| {
            Some(RecentBar {
                open_time: parse_ts(&bar, "open_time")?,
                close_time: parse_ts(&bar, "close_time")?,
                open: bar.get("open")?.as_f64()?,
                high: bar.get("high")?.as_f64()?,
                low: bar.get("low")?.as_f64()?,
                close: bar.get("close")?.as_f64()?,
                is_closed: bar
                    .get("is_closed")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

fn compute_zone_state(zone: &TrackedZone, bars: &[RecentBar]) -> ZoneState {
    let last_close = bars.last().map(|bar| bar.close);
    let confirmed_at = bars.last().map(|bar| bar.close_time);
    let position_relative = match last_close {
        Some(close) if close > zone.high => "above",
        Some(close) if close < zone.low => "below",
        Some(_) => "inside",
        None => "unknown",
    }
    .to_string();

    let closes_above = bars.iter().rev().take(2).all(|bar| bar.close > zone.high);
    let closes_below = bars.iter().rev().take(2).all(|bar| bar.close < zone.low);
    let acceptance_state = if closes_above {
        "accepted_above"
    } else if closes_below {
        "accepted_below"
    } else {
        "not_confirmed"
    }
    .to_string();

    let failed_above = bars
        .iter()
        .rev()
        .take(4)
        .any(|bar| bar.high > zone.high && bar.close < zone.high);
    let failed_below = bars
        .iter()
        .rev()
        .take(4)
        .any(|bar| bar.low < zone.low && bar.close > zone.low);
    let failed_auction_state = if failed_above {
        "failed_above"
    } else if failed_below {
        "failed_below"
    } else {
        "none"
    }
    .to_string();

    ZoneState {
        zone_id: zone.zone_id.clone(),
        position_relative,
        acceptance_state,
        failed_auction_state,
        last_close,
        confirmed_at,
    }
}

pub fn build_indicator_summary(
    input: &ModelInvocationInput,
    tracked_zones: &[TrackedZone],
) -> Result<StrategicIndicatorSummary> {
    let filtered_indicators = filtered_indicator_set(input)?;
    let options_surface = indicator_payload(&input.indicators, "options_surface");
    let bars = extract_recent_15m_bars(&input.indicators);
    let zone_states = tracked_zones
        .iter()
        .map(|zone| compute_zone_state(zone, &bars))
        .collect();

    let position_layer = json!({
        "price_volume_structure": preferred_indicator_payload(&filtered_indicators, &input.indicators, "price_volume_structure"),
        "rvwap_sigma_bands": preferred_indicator_payload(&filtered_indicators, &input.indicators, "rvwap_sigma_bands"),
        "avwap": preferred_indicator_payload(&filtered_indicators, &input.indicators, "avwap"),
        "tpo_market_profile": preferred_indicator_payload(&filtered_indicators, &input.indicators, "tpo_market_profile"),
        "fvg": preferred_indicator_payload(&filtered_indicators, &input.indicators, "fvg"),
        "liquidation_density": preferred_indicator_payload(&filtered_indicators, &input.indicators, "liquidation_density"),
        "ema_trend_regime": preferred_indicator_payload(&filtered_indicators, &input.indicators, "ema_trend_regime")
    });
    let state_layer = json!({
        "open_interest": indicator_payload(&input.indicators, "open_interest"),
        "long_short_ratios": indicator_payload(&input.indicators, "long_short_ratios"),
        "funding_rate": preferred_indicator_payload(&filtered_indicators, &input.indicators, "funding_rate"),
        "vpin": preferred_indicator_payload(&filtered_indicators, &input.indicators, "vpin")
    });
    let driver_layer = json!({
        "whale_trades": preferred_indicator_payload(&filtered_indicators, &input.indicators, "whale_trades"),
        "cvd_pack": preferred_indicator_payload(&filtered_indicators, &input.indicators, "cvd_pack"),
        "divergence": preferred_indicator_payload(&filtered_indicators, &input.indicators, "divergence")
    });
    let trigger_layer = json!({
        "selling_exhaustion": preferred_indicator_payload(&filtered_indicators, &input.indicators, "selling_exhaustion"),
        "buying_exhaustion": preferred_indicator_payload(&filtered_indicators, &input.indicators, "buying_exhaustion"),
        "footprint": preferred_indicator_payload(&filtered_indicators, &input.indicators, "footprint"),
        "high_volume_pulse": preferred_indicator_payload(&filtered_indicators, &input.indicators, "high_volume_pulse"),
        "orderbook_depth": preferred_indicator_payload(&filtered_indicators, &input.indicators, "orderbook_depth"),
        "absorption": preferred_indicator_payload(&filtered_indicators, &input.indicators, "absorption"),
        "initiation": preferred_indicator_payload(&filtered_indicators, &input.indicators, "initiation"),
        "bullish_initiation": preferred_indicator_payload(&filtered_indicators, &input.indicators, "bullish_initiation"),
        "bearish_initiation": preferred_indicator_payload(&filtered_indicators, &input.indicators, "bearish_initiation"),
        "bullish_absorption": preferred_indicator_payload(&filtered_indicators, &input.indicators, "bullish_absorption"),
        "bearish_absorption": preferred_indicator_payload(&filtered_indicators, &input.indicators, "bearish_absorption"),
        "divergence": preferred_indicator_payload(&filtered_indicators, &input.indicators, "divergence")
    });
    let aux_context = json!({
        "atr_context": preferred_indicator_payload(&filtered_indicators, &input.indicators, "atr_context"),
        "events_summary": preferred_indicator_payload(&filtered_indicators, &input.indicators, "events_summary"),
        "position_evidence": preferred_indicator_payload(&filtered_indicators, &input.indicators, "position_evidence"),
        "kline_history": preferred_indicator_payload(&filtered_indicators, &input.indicators, "kline_history"),
        "options_surface": options_surface_summary(&options_surface),
    });

    Ok(StrategicIndicatorSummary {
        meta: Some(StrategicSummaryMeta {
            symbol: input.symbol.clone(),
            ts_bucket: input.ts_bucket,
            source_routing_key: input.source_routing_key.clone(),
            indicator_count: input.indicator_count,
            missing_indicator_codes: input.missing_indicator_codes.clone(),
        }),
        position_layer,
        state_layer,
        driver_layer,
        trigger_layer,
        structural_refresh_context: crate::workflow::schema::StructuralRefreshContext::default(),
        aux_context,
        auction_context: AuctionContext {
            tracked_zones: tracked_zones.to_vec(),
            zone_states,
            recent_15m_bars: bars,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::build_indicator_summary;
    use crate::llm::input::ModelInvocationInput;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn code_layer_enriches_event_payload_with_confirmed_fields() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 1,
            source_routing_key: "test".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "initiation": {
                    "payload": {
                        "direction": "buy",
                        "event_time": "2026-03-27T12:00:00Z",
                        "price": 2010.5
                    }
                },
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [{
                                            "open_time": "2026-03-27T11:45:00Z",
                                            "close_time": "2026-03-27T12:00:00Z",
                                            "open": 2009.0,
                                            "high": 2012.0,
                                            "low": 2008.0,
                                            "close": 2010.5,
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
            management_snapshot: None,
        };

        let summary = build_indicator_summary(&input, &[]).expect("build indicator summary");
        assert_eq!(
            summary.trigger_layer["initiation"]["confirmed_at"],
            json!("2026-03-27T12:00:00Z")
        );
        assert_eq!(
            summary.trigger_layer["initiation"]["confirmed_price"],
            json!(2010.5)
        );
    }

    #[test]
    fn code_layer_uses_compact_15m_latest_bar_when_bars_are_missing() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 1,
            source_routing_key: "test".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "returned_count": 120,
                                        "latest_bar": {
                                            "open_time": "2026-03-27T11:45:00Z",
                                            "close_time": "2026-03-27T12:00:00Z",
                                            "open": 1998.0,
                                            "high": 2002.0,
                                            "low": 1997.5,
                                            "close": 2000.5,
                                            "is_closed": true
                                        }
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

        let summary = build_indicator_summary(&input, &[]).expect("build indicator summary");
        assert_eq!(summary.auction_context.recent_15m_bars.len(), 1);
        assert_eq!(summary.auction_context.recent_15m_bars[0].close, 2000.5);
    }

    #[test]
    fn code_layer_uses_filtered_indicator_shape_and_keeps_options_surface_only_in_aux() {
        let now = Utc::now();
        let input = ModelInvocationInput {
            symbol: "ETHUSDT".to_string(),
            ts_bucket: now,
            window_code: "15m".to_string(),
            indicator_count: 3,
            source_routing_key: "test".to_string(),
            source_published_at: None,
            received_at: now,
            indicators: json!({
                "price_volume_structure": {
                    "payload": {
                        "poc_price": 2000.0,
                        "value_area_levels": [
                            {"price": 1999.0, "volume": 100.0},
                            {"price": 2001.0, "volume": 120.0}
                        ],
                        "by_window": {
                            "15m": {
                                "poc_price": 2000.0,
                                "value_area_levels": [{"price": 2000.0, "volume": 50.0}],
                                "window_bars_used": 4
                            }
                        }
                    }
                },
                "options_surface": {
                    "payload": {
                        "as_of_ts": "2026-03-27T12:00:00Z",
                        "by_window": {
                            "15m": {
                                "is_ready": true,
                                "skew_state": "put_skew",
                                "term_structure_state": "flat"
                            },
                            "4h": {
                                "is_ready": true,
                                "atm_iv_front": 0.55,
                                "atm_iv_30d_proxy": 0.50,
                                "atm_iv_regime": "elevated",
                                "skew_state": "put_skew",
                                "term_structure_state": "flat"
                            },
                            "1d": {
                                "is_ready": false,
                                "atm_iv_regime": "neutral",
                                "skew_state": "neutral",
                                "term_structure_state": "contango"
                            }
                        }
                    }
                },
                "kline_history": {
                    "payload": {
                        "intervals": {
                            "15m": {
                                "markets": {
                                    "futures": {
                                        "bars": [{
                                            "open_time": "2026-03-27T11:45:00Z",
                                            "close_time": "2026-03-27T12:00:00Z",
                                            "open": 1998.0,
                                            "high": 2002.0,
                                            "low": 1997.5,
                                            "close": 2000.5,
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
            management_snapshot: None,
        };

        let summary = build_indicator_summary(&input, &[]).expect("build indicator summary");
        assert!(summary
            .position_layer
            .pointer("/price_volume_structure/value_area_levels")
            .is_none());
        assert_eq!(
            summary.aux_context["options_surface"]["strategic_summary"]["windows"]["4h"]
                ["atm_iv_regime"],
            json!("elevated")
        );
        assert_eq!(
            summary.aux_context["options_surface"]["tactical_guardrail"]["ready_windows"],
            json!(["15m", "4h"])
        );
        assert!(summary.position_layer.get("options_surface").is_none());
        assert!(summary.aux_context.get("raw_indicators").is_none());
    }
}
