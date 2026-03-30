use crate::app::bootstrap::AppContext;
use crate::app::config::RootConfig;
use crate::app::telegram::{TelegramOperator, TradeSignalNotification};
use crate::app::x::XOperator;
use crate::execution::binance::{
    cancel_workflow_pending_entry_orders, ensure_account_trading_ws_started,
    execute_workflow_execution_intent, execute_workflow_management_action,
    fetch_symbol_trading_state, fetch_symbol_trading_state_for_fast_path, ActivePositionSnapshot,
    ExecutionReport, ManagementExecutionReport, OpenOrderSnapshot,
    TradeExecutionBlockedByCurrentPriceBeyondStopLoss, TradingStateSnapshot,
};
use crate::execution::intent_adapter::{adapt_execution_intent, adapt_management_action};
use crate::llm::input::{
    ManagementSnapshotForLlm, ModelInvocationInput, PositionContextForLlm, PositionSummaryForLlm,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Timelike, Utc};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueuePurgeOptions},
    types::FieldTable,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::borrow::Cow;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use tokio::time::{sleep_until, Duration, Instant, Sleep};
use tracing::{debug, error, info, warn};

const TEMP_INDICATOR_DIR: &str = "systems/llm/temp_indicator";
const TEMP_MODEL_INPUT_DIR: &str = "systems/llm/temp_model_input";
const TEMP_MODEL_OUTPUT_DIR: &str = "systems/llm/temp_model_output";
const LLM_JOURNAL_DIR: &str = "systems/llm/journal";
const LLM_JOURNAL_FILE: &str = "systems/llm/journal/llm_trade_journal.jsonl";
const KLINE_DB_BACKFILL_INTERVALS: [(&str, i64); 2] = [("4h", 240), ("1d", 1440)];
const KLINE_RANGE_CACHE_TTL_MINUTES: i64 = 30;
const KLINE_RANGE_CACHE_MAX_SERIES: usize = 8;

#[derive(Debug, Clone, Copy)]
enum WorkflowStageKind {
    Stage1,
    Stage2,
}

#[derive(Debug, Default, Clone)]
struct WorkflowStageFlights {
    stage1: bool,
    stage2: bool,
}

static WORKFLOW_STAGE_FLIGHTS: OnceLock<StdMutex<HashMap<String, WorkflowStageFlights>>> =
    OnceLock::new();
static STARTUP_STAGE1_REFRESHED_SYMBOLS: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();

fn workflow_stage_flights() -> &'static StdMutex<HashMap<String, WorkflowStageFlights>> {
    WORKFLOW_STAGE_FLIGHTS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn startup_stage1_refreshed_symbols() -> &'static StdMutex<HashSet<String>> {
    STARTUP_STAGE1_REFRESHED_SYMBOLS.get_or_init(|| StdMutex::new(HashSet::new()))
}

fn startup_stage1_refresh_due(symbol: &str) -> bool {
    startup_stage1_refreshed_symbols()
        .lock()
        .map(|symbols| !symbols.contains(&symbol.to_ascii_uppercase()))
        .unwrap_or(true)
}

fn mark_startup_stage1_refresh_consumed(symbol: &str) {
    if let Ok(mut symbols) = startup_stage1_refreshed_symbols().lock() {
        symbols.insert(symbol.to_ascii_uppercase());
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn reset_startup_stage1_refresh_for_symbol(symbol: &str) {
    if let Ok(mut symbols) = startup_stage1_refreshed_symbols().lock() {
        symbols.remove(&symbol.to_ascii_uppercase());
    }
}

struct WorkflowStageFlightGuard {
    symbol: String,
    stage: WorkflowStageKind,
}

impl Drop for WorkflowStageFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut flights) = workflow_stage_flights().lock() {
            if let Some(entry) = flights.get_mut(&self.symbol) {
                match self.stage {
                    WorkflowStageKind::Stage1 => entry.stage1 = false,
                    WorkflowStageKind::Stage2 => entry.stage2 = false,
                }
                if !entry.stage1 && !entry.stage2 {
                    flights.remove(&self.symbol);
                }
            }
        }
    }
}

fn try_acquire_workflow_stage(
    symbol: &str,
    stage: WorkflowStageKind,
) -> Option<WorkflowStageFlightGuard> {
    let mut flights = workflow_stage_flights().lock().ok()?;
    let entry = flights.entry(symbol.to_string()).or_default();
    let occupied = match stage {
        WorkflowStageKind::Stage1 => &mut entry.stage1,
        WorkflowStageKind::Stage2 => &mut entry.stage2,
    };
    if *occupied {
        return None;
    }
    *occupied = true;
    Some(WorkflowStageFlightGuard {
        symbol: symbol.to_string(),
        stage,
    })
}

fn workflow_stage_inflight(symbol: &str, stage: WorkflowStageKind) -> bool {
    workflow_stage_flights()
        .lock()
        .ok()
        .and_then(|flights| flights.get(symbol).cloned())
        .map(|entry| match stage {
            WorkflowStageKind::Stage1 => entry.stage1,
            WorkflowStageKind::Stage2 => entry.stage2,
        })
        .unwrap_or(false)
}

#[derive(Debug, Default)]
struct Stage1RefreshAttempt {
    refreshed: bool,
    inflight_suppressed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MinuteBundleEnvelope {
    msg_type: String,
    routing_key: String,
    symbol: String,
    ts_bucket: DateTime<Utc>,
    window_code: String,
    indicator_count: usize,
    published_at: Option<DateTime<Utc>>,
    indicators: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FastMarketEnvelope {
    msg_type: String,
    routing_key: String,
    market: String,
    symbol: String,
    #[serde(default)]
    source_kind: Option<String>,
    #[serde(default)]
    backfill_in_progress: Option<bool>,
    event_ts: DateTime<Utc>,
    #[serde(default)]
    published_at: Option<DateTime<Utc>>,
    data: Value,
}

#[derive(Debug, Clone)]
struct LatestBundle {
    raw: MinuteBundleEnvelope,
    indicators: Value,
    missing_indicator_codes: Vec<String>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FastPriceSource {
    MarkPrice,
    Trade1s,
    Kline1m,
}

impl FastPriceSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::MarkPrice => "mark_price",
            Self::Trade1s => "trade_1s",
            Self::Kline1m => "kline_1m",
        }
    }
}

#[derive(Debug, Clone)]
struct FastPriceEvent {
    symbol: String,
    event_ts: DateTime<Utc>,
    price: f64,
    source: FastPriceSource,
    routing_key: String,
}

#[derive(Debug, Clone, Default)]
struct FastWatcherPlanState {
    plan_version: String,
    context_key: String,
    last_event_ts: Option<DateTime<Utc>>,
    activation_seen_at: Option<DateTime<Utc>>,
    advanced_beyond_entry_after_activation: bool,
    breakout_started_at: Option<DateTime<Utc>>,
    breakout_extreme_price: Option<f64>,
    invalidation_probe_seen_at: Option<DateTime<Utc>>,
    recovery_started_at: Option<DateTime<Utc>>,
    fired: bool,
}

fn update_pending_invoke_bundle(
    pending_invoke_bundle: &mut Option<LatestBundle>,
    incoming_bundle: LatestBundle,
) -> bool {
    let should_replace = pending_invoke_bundle
        .as_ref()
        .map(|pending| incoming_bundle.raw.ts_bucket >= pending.raw.ts_bucket)
        .unwrap_or(true);
    if should_replace {
        *pending_invoke_bundle = Some(incoming_bundle);
        true
    } else {
        false
    }
}

fn decode_minute_bundle_body<'a>(
    raw: &'a [u8],
    content_encoding: Option<&str>,
) -> Result<Cow<'a, [u8]>> {
    let encoding = content_encoding.map(str::trim).filter(|v| !v.is_empty());
    match encoding {
        None => Ok(Cow::Borrowed(raw)),
        Some(value) if value.eq_ignore_ascii_case("identity") => Ok(Cow::Borrowed(raw)),
        Some(value)
            if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") =>
        {
            let mut decoder = GzDecoder::new(raw);
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("gunzip minute bundle payload")?;
            Ok(Cow::Owned(decoded))
        }
        Some(other) => Err(anyhow!(
            "unsupported minute bundle content_encoding={}",
            other
        )),
    }
}

fn decode_minute_bundle_envelope(
    raw: &[u8],
    content_encoding: Option<&str>,
) -> Result<MinuteBundleEnvelope> {
    let decoded = decode_minute_bundle_body(raw, content_encoding)?;
    serde_json::from_slice(decoded.as_ref()).context("parse minute bundle as json")
}

fn decode_fast_market_envelope(
    raw: &[u8],
    content_encoding: Option<&str>,
) -> Result<FastMarketEnvelope> {
    let decoded = decode_minute_bundle_body(raw, content_encoding)?;
    serde_json::from_slice(decoded.as_ref()).context("parse fast market event as json")
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|item| item as f64))
        .or_else(|| value.as_u64().map(|item| item as f64))
        .or_else(|| value.as_str().and_then(|item| item.parse::<f64>().ok()))
}

pub async fn run(ctx: AppContext) -> Result<()> {
    ensure_temp_indicator_dir().await?;
    ensure_temp_model_input_dir().await?;
    ensure_llm_journal_dir().await?;
    ensure_account_trading_ws_started(
        &ctx.http_client,
        &ctx.config.api.binance,
        &ctx.config.llm.execution,
    );

    if ctx.config.llm.purge_queue_on_start {
        let purged = ctx
            .mq_consume_channel
            .queue_purge(&ctx.consume_queue_name, QueuePurgeOptions::default())
            .await
            .with_context(|| format!("purge llm consume queue {}", ctx.consume_queue_name))?;
        debug!(
            queue = %ctx.consume_queue_name,
            purged = purged,
            "llm startup queue purge completed"
        );
    } else {
        debug!(
            queue = %ctx.consume_queue_name,
            "llm startup queue purge disabled (llm.purge_queue_on_start=false)"
        );
    }

    let fast_purged = ctx
        .mq_fast_consume_channel
        .queue_purge(&ctx.fast_consume_queue_name, QueuePurgeOptions::default())
        .await
        .with_context(|| {
            format!(
                "purge llm fast consume queue {}",
                ctx.fast_consume_queue_name
            )
        })?;
    debug!(
        queue = %ctx.fast_consume_queue_name,
        purged = fast_purged,
        "llm fast watcher startup queue purge completed"
    );

    ctx.mq_consume_channel
        .basic_qos(500, BasicQosOptions::default())
        .await
        .context("set llm queue qos")?;
    ctx.mq_fast_consume_channel
        .basic_qos(1_000, BasicQosOptions::default())
        .await
        .context("set llm fast queue qos")?;

    let consumer_tag = format!(
        "llm_{}_{}",
        ctx.consume_queue_name.replace('.', "_"),
        &ctx.producer_instance_id
    );
    let mut consumer = ctx
        .mq_consume_channel
        .basic_consume(
            &ctx.consume_queue_name,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("consume queue {}", ctx.consume_queue_name))?;
    let fast_consumer_tag = format!(
        "llm_fast_{}_{}",
        ctx.fast_consume_queue_name.replace('.', "_"),
        &ctx.producer_instance_id
    );
    let mut fast_consumer = ctx
        .mq_fast_consume_channel
        .basic_consume(
            &ctx.fast_consume_queue_name,
            &fast_consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("consume queue {}", ctx.fast_consume_queue_name))?;

    let mut pending_invoke_bundle: Option<LatestBundle> = None;
    let mut last_invoked_ts_bucket: Option<DateTime<Utc>> = None;
    let mut fast_watcher_state: Option<FastWatcherPlanState> = None;
    let disabled_deadline = Instant::now() + Duration::from_secs(365 * 24 * 60 * 60);
    let mut settle_timer: Pin<Box<Sleep>> = Box::pin(sleep_until(disabled_deadline));

    debug!(
        queue = %ctx.consume_queue_name,
        symbol = %ctx.config.llm.symbol,
        request_enabled = ctx.config.llm.request_enabled,
        active_provider = %ctx.config.active_default_model(),
        prompt_template = %ctx.config.llm.prompt_template,
        purge_queue_on_start = ctx.config.llm.purge_queue_on_start,
        bundle_settle_ms = ctx.config.llm.bundle_settle_ms,
        "llm runtime started"
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                debug!("shutdown signal received, stopping llm runtime");
                break;
            }
            _ = &mut settle_timer => {
                queue_latest_bundle_invoke(
                    &ctx,
                    &pending_invoke_bundle,
                    &mut last_invoked_ts_bucket,
                    "scheduled_bundle",
                );
                pending_invoke_bundle = None;
                settle_timer.as_mut().reset(disabled_deadline);
            }
            maybe_delivery = consumer.next() => {
                let Some(delivery_result) = maybe_delivery else {
                    warn!("llm mq consumer stream ended");
                    break;
                };
                match delivery_result {
                    Ok(delivery) => {
                        let now = Utc::now();
                        let content_encoding = delivery
                            .properties
                            .content_encoding()
                            .as_ref()
                            .map(|value| value.as_str().to_string());
                        let parse_result = decode_minute_bundle_envelope(
                            &delivery.data,
                            content_encoding.as_deref(),
                        );
                        match parse_result {
                            Ok(bundle) => {
                                if bundle.msg_type != "ind.minute_bundle" || bundle.window_code != "1m" {
                                    if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                                        warn!(error = %err, "llm ack non-target message failed");
                                    }
                                    continue;
                                }
                                if !bundle.symbol.eq_ignore_ascii_case(&ctx.config.llm.symbol) {
                                    if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                                        warn!(error = %err, "llm ack non-symbol message failed");
                                    }
                                    continue;
                                }

                                let consume_staleness_secs = now
                                    .signed_duration_since(bundle.ts_bucket)
                                    .num_seconds();
                                let max_consume_stale =
                                    ctx.config.llm.bundle_consume_stale_secs as i64;
                                if consume_staleness_secs > max_consume_stale {
                                    debug!(
                                        symbol = %bundle.symbol,
                                        ts_bucket = %bundle.ts_bucket,
                                        staleness_secs = consume_staleness_secs,
                                        max_consume_stale_secs = max_consume_stale,
                                        "llm consumer discarding stale bundle (backfill/replay data)"
                                    );
                                    if let Err(err) =
                                        delivery.ack(BasicAckOptions::default()).await
                                    {
                                        warn!(error = %err, "llm ack stale bundle failed");
                                    }
                                    continue;
                                }

                                let missing = Vec::new();

                                if let Err(err) = persist_bundle_to_disk(
                                    &bundle,
                                    &delivery.data,
                                    content_encoding.as_deref(),
                                    ctx.config.llm.temp_cache_retention_minutes(),
                                )
                                .await
                                {
                                    warn!(
                                        symbol = %bundle.symbol,
                                        ts_bucket = %bundle.ts_bucket,
                                        error = %err,
                                        "persist minute bundle to temp_indicator failed"
                                    );
                                }

                                let current_bundle = LatestBundle {
                                    raw: bundle.clone(),
                                    indicators: bundle.indicators.clone(),
                                    missing_indicator_codes: missing.clone(),
                                    received_at: now,
                                };

                                debug!(
                                    symbol = %bundle.symbol,
                                    ts_bucket = %bundle.ts_bucket,
                                    indicator_count = bundle.indicator_count,
                                    missing_count = missing.len(),
                                    routing_key = %bundle.routing_key,
                                    "llm received minute indicator bundle"
                                );

                                let incoming_ts_bucket = current_bundle.raw.ts_bucket;
                                let pending_ts_bucket = pending_invoke_bundle
                                    .as_ref()
                                    .map(|pending| pending.raw.ts_bucket);
                                if update_pending_invoke_bundle(
                                    &mut pending_invoke_bundle,
                                    current_bundle,
                                ) {
                                    settle_timer.as_mut().reset(
                                        Instant::now()
                                            + Duration::from_millis(
                                                ctx.config.llm.bundle_settle_ms,
                                            ),
                                    );
                                } else if let Some(pending_ts_bucket) = pending_ts_bucket {
                                    warn!(
                                        symbol = %bundle.symbol,
                                        incoming_ts_bucket = %incoming_ts_bucket,
                                        pending_ts_bucket = %pending_ts_bucket,
                                        "llm ignored late out-of-order minute bundle because a newer pending bundle is already queued"
                                    );
                                }
                            }
                            Err(err) => {
                                warn!(
                                    error = %err,
                                    payload_len = delivery.data.len(),
                                    content_encoding = content_encoding.as_deref().unwrap_or("identity"),
                                    "llm decode minute bundle failed"
                                );
                            }
                        }

                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            warn!(error = %err, "llm ack failed");
                        }
                    }
                    Err(err) => {
                        error!(error = %err, "llm consumer stream error");
                    }
                }
            }
            maybe_delivery = fast_consumer.next() => {
                let Some(delivery_result) = maybe_delivery else {
                    warn!("llm fast mq consumer stream ended");
                    break;
                };
                match delivery_result {
                    Ok(delivery) => {
                        let content_encoding = delivery
                            .properties
                            .content_encoding()
                            .as_ref()
                            .map(|value| value.as_str().to_string());
                        match decode_fast_market_envelope(&delivery.data, content_encoding.as_deref()) {
                            Ok(envelope) => {
                                if let Some(event) =
                                    extract_fast_price_event(&envelope, &ctx.config.llm.symbol)
                                {
                                    if let Err(err) =
                                        handle_fast_market_event(&ctx, &mut fast_watcher_state, event)
                                            .await
                                    {
                                        warn!(error = %err, "llm fast watcher event handling failed");
                                    }
                                }
                            }
                            Err(err) => {
                                warn!(
                                    error = %err,
                                    payload_len = delivery.data.len(),
                                    content_encoding = content_encoding.as_deref().unwrap_or("identity"),
                                    "llm decode fast market event failed"
                                );
                            }
                        }

                        if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                            warn!(error = %err, "llm fast ack failed");
                        }
                    }
                    Err(err) => {
                        error!(error = %err, "llm fast consumer stream error");
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MissingKlineBarRequest {
    market: String,
    interval_code: String,
    open_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KlineHistoryRangeRequest {
    market: String,
    interval_code: String,
    start_open_time: DateTime<Utc>,
    end_open_time: DateTime<Utc>,
}

#[derive(Default)]
struct KlineHistoryPatchStats {
    bars_patched: usize,
    divergence_events_patched: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KlineRangeCacheSeriesKey {
    symbol: String,
    market: String,
    interval_code: String,
}

#[derive(Debug, Clone)]
struct KlineRangeCacheEntry {
    start_open_time: DateTime<Utc>,
    end_open_time: DateTime<Utc>,
    bars: Vec<Value>,
    last_accessed_at: DateTime<Utc>,
}

static KLINE_RANGE_QUERY_CACHE: OnceLock<
    StdMutex<HashMap<KlineRangeCacheSeriesKey, KlineRangeCacheEntry>>,
> = OnceLock::new();

fn kline_range_query_cache(
) -> &'static StdMutex<HashMap<KlineRangeCacheSeriesKey, KlineRangeCacheEntry>> {
    KLINE_RANGE_QUERY_CACHE.get_or_init(|| StdMutex::new(HashMap::new()))
}

async fn hydrate_missing_kline_history_from_db(
    pool: &PgPool,
    input: &mut ModelInvocationInput,
) -> Result<KlineHistoryPatchStats> {
    let requests = collect_missing_kline_bar_requests(&input.indicators);
    let mut stats = KlineHistoryPatchStats::default();

    let mut replacements = Vec::new();
    for request in &requests {
        if let Some(bar) = fetch_kline_bar_from_db(
            pool,
            &input.symbol,
            &request.market,
            &request.interval_code,
            request.open_time,
        )
        .await?
        {
            replacements.push((request.clone(), bar));
        }
    }

    stats.bars_patched = apply_backfilled_kline_bars(&mut input.indicators, &replacements);

    let mut divergence_backfill_bars =
        extract_kline_history_bars(&input.indicators, "futures", "1m");

    if let Some(range_request) = collect_divergence_kline_coverage_request(&input.indicators) {
        let additional_bars = fetch_kline_bars_range_from_db(
            pool,
            &input.symbol,
            &range_request.market,
            &range_request.interval_code,
            range_request.start_open_time,
            range_request.end_open_time,
        )
        .await?;
        stats.bars_patched += prepend_backfilled_kline_bars(
            &mut input.indicators,
            &range_request.market,
            &range_request.interval_code,
            &additional_bars,
        );
        divergence_backfill_bars.extend(additional_bars);
    }

    stats.divergence_events_patched = backfill_divergence_event_prices_from_bars(
        &mut input.indicators,
        &divergence_backfill_bars,
    );

    Ok(stats)
}

fn collect_missing_kline_bar_requests(indicators: &Value) -> Vec<MissingKlineBarRequest> {
    let mut requests = BTreeSet::new();

    let Some(intervals) = indicators
        .get("kline_history")
        .and_then(|indicator| indicator.get("payload"))
        .and_then(|payload| payload.get("intervals"))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };

    for (interval_code, _) in KLINE_DB_BACKFILL_INTERVALS {
        let Some(markets) = intervals
            .get(interval_code)
            .and_then(|interval| interval.get("markets"))
            .and_then(Value::as_object)
        else {
            continue;
        };

        for market in ["futures", "spot"] {
            let Some(bars) = markets
                .get(market)
                .and_then(|market_value| market_value.get("bars"))
                .and_then(Value::as_array)
            else {
                continue;
            };

            for bar in bars {
                let Some(open_time) = empty_kline_bar_open_time(bar) else {
                    continue;
                };
                requests.insert(MissingKlineBarRequest {
                    market: market.to_string(),
                    interval_code: interval_code.to_string(),
                    open_time,
                });
            }
        }
    }

    requests.into_iter().collect()
}

fn empty_kline_bar_open_time(bar: &Value) -> Option<DateTime<Utc>> {
    let object = bar.as_object()?;
    let all_prices_empty = ["open", "high", "low", "close"]
        .iter()
        .all(|field| object.get(*field).map(Value::is_null).unwrap_or(true));
    if !all_prices_empty {
        return None;
    }
    object
        .get("open_time")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc)
}

async fn fetch_kline_bar_from_db(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    open_time: DateTime<Utc>,
) -> Result<Option<Value>> {
    let row = sqlx::query(
        r#"
        SELECT
            open_time,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            COALESCE(volume_base, 0.0) AS volume_base,
            COALESCE(quote_volume, 0.0) AS volume_quote,
            is_closed
        FROM md.kline_bar
        WHERE market = $1::cfg.market_type
          AND symbol = $2
          AND interval_code = $3
          AND open_time = $4
        LIMIT 1
        "#,
    )
    .bind(market)
    .bind(symbol.to_uppercase())
    .bind(interval_code)
    .bind(open_time)
    .fetch_optional(pool)
    .await
    .context("query missing kline bar from db")?;

    let Some(row) = row else {
        return Ok(None);
    };

    let open_time: DateTime<Utc> = row.get("open_time");
    let close_time: DateTime<Utc> = row.get("close_time");
    let expected_minutes = interval_minutes(interval_code);
    Ok(Some(json!({
        "open_time": open_time.to_rfc3339(),
        "close_time": close_time.to_rfc3339(),
        "open": row.get::<f64, _>("open_price"),
        "high": row.get::<f64, _>("high_price"),
        "low": row.get::<f64, _>("low_price"),
        "close": row.get::<f64, _>("close_price"),
        "volume_base": row.get::<f64, _>("volume_base"),
        "volume_quote": row.get::<f64, _>("volume_quote"),
        "is_closed": row.get::<bool, _>("is_closed"),
        "minutes_covered": expected_minutes,
        "expected_minutes": expected_minutes,
    })))
}

async fn fetch_kline_bars_range_from_db_uncached(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    start_open_time: DateTime<Utc>,
    end_open_time: DateTime<Utc>,
) -> Result<Vec<Value>> {
    if end_open_time < start_open_time {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            open_time,
            close_time,
            open_price,
            high_price,
            low_price,
            close_price,
            COALESCE(volume_base, 0.0) AS volume_base,
            COALESCE(quote_volume, 0.0) AS volume_quote,
            is_closed
        FROM md.kline_bar
        WHERE market = $1::cfg.market_type
          AND symbol = $2
          AND interval_code = $3
          AND open_time >= $4
          AND open_time <= $5
        ORDER BY open_time ASC
        "#,
    )
    .bind(market)
    .bind(symbol.to_uppercase())
    .bind(interval_code)
    .bind(start_open_time)
    .bind(end_open_time)
    .fetch_all(pool)
    .await
    .context("query kline bar range from db")?;

    let expected_minutes = interval_minutes(interval_code);
    Ok(rows
        .into_iter()
        .map(|row| {
            let open_time: DateTime<Utc> = row.get("open_time");
            let close_time: DateTime<Utc> = row.get("close_time");
            json!({
                "open_time": open_time.to_rfc3339(),
                "close_time": close_time.to_rfc3339(),
                "open": row.get::<f64, _>("open_price"),
                "high": row.get::<f64, _>("high_price"),
                "low": row.get::<f64, _>("low_price"),
                "close": row.get::<f64, _>("close_price"),
                "volume_base": row.get::<f64, _>("volume_base"),
                "volume_quote": row.get::<f64, _>("volume_quote"),
                "is_closed": row.get::<bool, _>("is_closed"),
                "minutes_covered": expected_minutes,
                "expected_minutes": expected_minutes,
            })
        })
        .collect())
}

fn kline_cache_series_key(
    symbol: &str,
    market: &str,
    interval_code: &str,
) -> KlineRangeCacheSeriesKey {
    KlineRangeCacheSeriesKey {
        symbol: symbol.to_ascii_uppercase(),
        market: market.to_string(),
        interval_code: interval_code.to_string(),
    }
}

fn prune_kline_range_query_cache(
    cache: &mut HashMap<KlineRangeCacheSeriesKey, KlineRangeCacheEntry>,
    now: DateTime<Utc>,
) {
    let ttl_cutoff = now - ChronoDuration::minutes(KLINE_RANGE_CACHE_TTL_MINUTES);
    cache.retain(|_, entry| entry.last_accessed_at >= ttl_cutoff);

    while cache.len() > KLINE_RANGE_CACHE_MAX_SERIES {
        let Some(evict_key) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&evict_key);
    }
}

fn bar_open_time(bar: &Value) -> Option<DateTime<Utc>> {
    bar.get("open_time")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339_utc)
}

fn slice_cached_kline_bars(
    bars: &[Value],
    start_open_time: DateTime<Utc>,
    end_open_time: DateTime<Utc>,
) -> Vec<Value> {
    bars.iter()
        .filter(|bar| {
            bar_open_time(bar)
                .map(|open_time| open_time >= start_open_time && open_time <= end_open_time)
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

async fn fetch_kline_bars_range_from_db(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    start_open_time: DateTime<Utc>,
    end_open_time: DateTime<Utc>,
) -> Result<Vec<Value>> {
    if end_open_time < start_open_time {
        return Ok(Vec::new());
    }

    let series_key = kline_cache_series_key(symbol, market, interval_code);
    let now = Utc::now();

    {
        let mut cache = kline_range_query_cache()
            .lock()
            .expect("kline range query cache poisoned");
        prune_kline_range_query_cache(&mut cache, now);

        if let Some(entry) = cache.get_mut(&series_key) {
            if start_open_time >= entry.start_open_time && end_open_time <= entry.end_open_time {
                entry.last_accessed_at = now;
                return Ok(slice_cached_kline_bars(
                    &entry.bars,
                    start_open_time,
                    end_open_time,
                ));
            }
        }
    }

    let interval_step = ChronoDuration::minutes(interval_minutes(interval_code));
    let maybe_extension = {
        let mut cache = kline_range_query_cache()
            .lock()
            .expect("kline range query cache poisoned");
        prune_kline_range_query_cache(&mut cache, now);
        cache.get_mut(&series_key).and_then(|entry| {
            if start_open_time == entry.start_open_time && end_open_time > entry.end_open_time {
                let fetch_start = entry.end_open_time + interval_step;
                if fetch_start <= end_open_time {
                    entry.last_accessed_at = now;
                    return Some((entry.clone(), fetch_start, end_open_time, true));
                }
            }

            if end_open_time == entry.end_open_time && start_open_time < entry.start_open_time {
                let fetch_end = entry.start_open_time - interval_step;
                if start_open_time <= fetch_end {
                    entry.last_accessed_at = now;
                    return Some((entry.clone(), start_open_time, fetch_end, false));
                }
            }

            None
        })
    };

    if let Some((entry, fetch_start, fetch_end, append_right)) = maybe_extension {
        let delta_bars = fetch_kline_bars_range_from_db_uncached(
            pool,
            symbol,
            market,
            interval_code,
            fetch_start,
            fetch_end,
        )
        .await?;

        let merged_bars = if append_right {
            let mut bars = entry.bars.clone();
            bars.extend(delta_bars);
            bars
        } else {
            let mut bars = delta_bars;
            bars.extend(entry.bars.clone());
            bars
        };

        let new_entry = KlineRangeCacheEntry {
            start_open_time: start_open_time.min(entry.start_open_time),
            end_open_time: end_open_time.max(entry.end_open_time),
            bars: merged_bars.clone(),
            last_accessed_at: now,
        };

        let mut cache = kline_range_query_cache()
            .lock()
            .expect("kline range query cache poisoned");
        cache.insert(series_key, new_entry);
        prune_kline_range_query_cache(&mut cache, now);
        return Ok(merged_bars);
    }

    let bars = fetch_kline_bars_range_from_db_uncached(
        pool,
        symbol,
        market,
        interval_code,
        start_open_time,
        end_open_time,
    )
    .await?;

    let mut cache = kline_range_query_cache()
        .lock()
        .expect("kline range query cache poisoned");
    cache.insert(
        series_key,
        KlineRangeCacheEntry {
            start_open_time,
            end_open_time,
            bars: bars.clone(),
            last_accessed_at: now,
        },
    );
    prune_kline_range_query_cache(&mut cache, now);

    Ok(bars)
}

fn apply_backfilled_kline_bars(
    indicators: &mut Value,
    replacements: &[(MissingKlineBarRequest, Value)],
) -> usize {
    if replacements.is_empty() {
        return 0;
    }

    let Some(intervals) = indicators
        .get_mut("kline_history")
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("intervals"))
        .and_then(Value::as_object_mut)
    else {
        return 0;
    };

    let mut patched = 0usize;
    for (request, replacement) in replacements {
        let Some(bars) = intervals
            .get_mut(&request.interval_code)
            .and_then(Value::as_object_mut)
            .and_then(|interval| interval.get_mut("markets"))
            .and_then(Value::as_object_mut)
            .and_then(|markets| markets.get_mut(&request.market))
            .and_then(Value::as_object_mut)
            .and_then(|market| market.get_mut("bars"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };

        for bar in bars.iter_mut() {
            let Some(open_time) = empty_kline_bar_open_time(bar) else {
                continue;
            };
            if open_time == request.open_time {
                *bar = replacement.clone();
                patched += 1;
                break;
            }
        }
    }

    patched
}

fn prepend_backfilled_kline_bars(
    indicators: &mut Value,
    market: &str,
    interval_code: &str,
    replacements: &[Value],
) -> usize {
    if replacements.is_empty() {
        return 0;
    }

    let Some(market_node) = indicators
        .get_mut("kline_history")
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("intervals"))
        .and_then(Value::as_object_mut)
        .and_then(|intervals| intervals.get_mut(interval_code))
        .and_then(Value::as_object_mut)
        .and_then(|interval| interval.get_mut("markets"))
        .and_then(Value::as_object_mut)
        .and_then(|markets| markets.get_mut(market))
        .and_then(Value::as_object_mut)
    else {
        return 0;
    };

    let (inserted, merged_len) = {
        let Some(existing_bars) = market_node.get_mut("bars").and_then(Value::as_array_mut) else {
            return 0;
        };

        let existing_open_times = existing_bars
            .iter()
            .filter_map(|bar| {
                bar.get("open_time")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<HashSet<_>>();

        let mut merged = replacements
            .iter()
            .filter(|bar| {
                bar.get("open_time")
                    .and_then(Value::as_str)
                    .map(|open_time| !existing_open_times.contains(open_time))
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();
        let inserted = merged.len();
        if inserted == 0 {
            return 0;
        }

        merged.extend(existing_bars.drain(..));
        *existing_bars = merged;
        (inserted, existing_bars.len())
    };

    market_node.insert("returned_count".to_string(), json!(merged_len));
    inserted
}

fn extract_kline_history_bars(indicators: &Value, market: &str, interval_code: &str) -> Vec<Value> {
    let market_pointer =
        format!("/kline_history/payload/intervals/{interval_code}/markets/{market}");
    let Some(market_node) = indicators.pointer(&market_pointer) else {
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

fn backfill_divergence_event_prices_from_bars(
    indicators: &mut Value,
    one_minute_bars: &[Value],
) -> usize {
    if one_minute_bars.is_empty() {
        return 0;
    }

    let Some(payload) = indicators
        .get_mut("divergence")
        .and_then(Value::as_object_mut)
        .and_then(|indicator| indicator.get_mut("payload"))
        .and_then(Value::as_object_mut)
    else {
        return 0;
    };

    if let Some(events) = payload
        .get_mut("recent_7d")
        .and_then(Value::as_object_mut)
        .and_then(|recent| recent.get_mut("events"))
        .and_then(Value::as_array_mut)
    {
        return backfill_divergence_event_array_from_bars(events, one_minute_bars);
    }

    payload
        .get_mut("events")
        .and_then(Value::as_array_mut)
        .map(|events| backfill_divergence_event_array_from_bars(events, one_minute_bars))
        .unwrap_or(0)
}

fn backfill_divergence_event_array_from_bars(
    events: &mut [Value],
    one_minute_bars: &[Value],
) -> usize {
    let mut patched = 0usize;

    for event in events.iter_mut().filter_map(Value::as_object_mut) {
        let has_all_prices = event.get("pivot_price").and_then(Value::as_f64).is_some()
            && event.get("price_low").and_then(Value::as_f64).is_some()
            && event.get("price_high").and_then(Value::as_f64).is_some();
        if has_all_prices {
            continue;
        }

        let Some(event_start) = event
            .get("event_start_ts")
            .or_else(|| event.get("start_ts"))
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_utc)
        else {
            continue;
        };
        let Some(event_end) = event
            .get("event_end_ts")
            .or_else(|| event.get("end_ts"))
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_utc)
        else {
            continue;
        };

        let relevant_bars = one_minute_bars
            .iter()
            .filter_map(Value::as_object)
            .filter(|bar| {
                bar.get("open_time")
                    .and_then(Value::as_str)
                    .and_then(parse_rfc3339_utc)
                    .map(|open_time| open_time >= event_start && open_time <= event_end)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        if relevant_bars.is_empty() {
            continue;
        }

        let derived_low = relevant_bars
            .iter()
            .filter_map(|bar| bar.get("low").and_then(Value::as_f64))
            .min_by(|left, right| left.partial_cmp(right).unwrap_or(CmpOrdering::Equal));
        let derived_high = relevant_bars
            .iter()
            .filter_map(|bar| bar.get("high").and_then(Value::as_f64))
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(CmpOrdering::Equal));
        let derived_pivot = match event.get("pivot_side").and_then(Value::as_str) {
            Some("low") => derived_low,
            Some("high") => derived_high,
            _ => event
                .get("pivot_price")
                .and_then(Value::as_f64)
                .or(derived_low)
                .or(derived_high),
        };

        let mut event_patched = false;
        if event.get("pivot_price").and_then(Value::as_f64).is_none() {
            if let Some(value) = derived_pivot {
                event.insert("pivot_price".to_string(), json!(value));
                event_patched = true;
            }
        }
        if event.get("price_low").and_then(Value::as_f64).is_none() {
            if let Some(value) = derived_low.or(derived_pivot) {
                event.insert("price_low".to_string(), json!(value));
                event_patched = true;
            }
        }
        if event.get("price_high").and_then(Value::as_f64).is_none() {
            if let Some(value) = derived_high.or(derived_pivot) {
                event.insert("price_high".to_string(), json!(value));
                event_patched = true;
            }
        }

        if event_patched {
            patched += 1;
        }
    }

    patched
}

fn collect_divergence_kline_coverage_request(
    indicators: &Value,
) -> Option<KlineHistoryRangeRequest> {
    let earliest_existing_open = indicators
        .pointer("/kline_history/payload/intervals/1m/markets/futures/bars")
        .and_then(Value::as_array)
        .and_then(|bars| {
            bars.iter()
                .filter_map(|bar| bar.get("open_time").and_then(Value::as_str))
                .filter_map(parse_rfc3339_utc)
                .min()
        })?;

    let divergence_events = indicators
        .pointer("/divergence/payload/recent_7d/events")
        .and_then(Value::as_array)
        .or_else(|| {
            indicators
                .pointer("/divergence/payload/events")
                .and_then(Value::as_array)
        })?;

    let earliest_required_open = divergence_events
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|event| {
            event
                .get("event_start_ts")
                .or_else(|| event.get("start_ts"))
                .and_then(Value::as_str)
                .and_then(parse_rfc3339_utc)
        })
        .min()?;

    if earliest_required_open >= earliest_existing_open {
        return None;
    }

    let end_open_time = earliest_existing_open - ChronoDuration::minutes(1);
    if end_open_time < earliest_required_open {
        return None;
    }

    Some(KlineHistoryRangeRequest {
        market: "futures".to_string(),
        interval_code: "1m".to_string(),
        start_open_time: earliest_required_open,
        end_open_time,
    })
}

fn parse_rfc3339_utc(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn interval_minutes(interval_code: &str) -> i64 {
    match interval_code {
        "15m" => 15,
        "1h" => 60,
        "4h" => 240,
        "1d" => 1440,
        "3d" => 4320,
        _ => 1,
    }
}

async fn patch_input_kline_history_from_db(
    pool: &PgPool,
    input: &mut ModelInvocationInput,
    trigger: &str,
) -> Result<()> {
    let stats = hydrate_missing_kline_history_from_db(pool, input).await?;
    let _ = (trigger, stats);
    Ok(())
}

fn build_persist_only_input(bundle: &LatestBundle) -> ModelInvocationInput {
    ModelInvocationInput {
        symbol: bundle.raw.symbol.clone(),
        ts_bucket: bundle.raw.ts_bucket,
        window_code: bundle.raw.window_code.clone(),
        indicator_count: bundle.raw.indicator_count,
        source_routing_key: bundle.raw.routing_key.clone(),
        source_published_at: bundle.raw.published_at,
        received_at: bundle.received_at,
        indicators: bundle.indicators.clone(),
        missing_indicator_codes: bundle.missing_indicator_codes.clone(),
        trading_state: None,
        management_snapshot: None,
    }
}

fn queue_latest_bundle_invoke(
    ctx: &AppContext,
    latest_bundle: &Option<LatestBundle>,
    last_invoked_ts_bucket: &mut Option<DateTime<Utc>>,
    trigger: &str,
) {
    let Some(bundle) = latest_bundle.clone() else {
        return;
    };
    if last_invoked_ts_bucket
        .map(|ts| ts >= bundle.raw.ts_bucket)
        .unwrap_or(false)
    {
        debug!(
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = trigger,
            "llm invoke skipped: latest bundle unchanged"
        );
        return;
    }
    let staleness_secs = Utc::now()
        .signed_duration_since(bundle.raw.ts_bucket)
        .num_seconds();
    let max_stale = ctx.config.llm.bundle_stale_secs as i64;
    if staleness_secs > max_stale {
        warn!(
            ts_bucket = %bundle.raw.ts_bucket,
            staleness_secs = staleness_secs,
            max_stale_secs = max_stale,
            trigger = trigger,
            "llm invoke skipped: indicator bundle is stale (backfill/replay data)"
        );
        return;
    }
    *last_invoked_ts_bucket = Some(bundle.raw.ts_bucket);
    let config = Arc::clone(&ctx.config);
    let db_pool = ctx.db_pool.clone();
    let http_client = ctx.http_client.clone();
    let loopback_http_client = ctx.loopback_http_client.clone();
    let print_response = ctx.config.llm.print_response;
    let trigger = Arc::<str>::from(trigger.to_string());
    tokio::spawn(async move {
        invoke_bundle_models(
            config,
            db_pool,
            http_client,
            loopback_http_client,
            print_response,
            bundle,
            trigger,
        )
        .await;
    });
}

async fn invoke_bundle_models(
    config: Arc<RootConfig>,
    db_pool: PgPool,
    http_client: Client,
    loopback_http_client: Client,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
) {
    if !config.llm.workflow.enabled {
        debug!(
            symbol = %bundle.raw.symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = %trigger,
            "workflow invoke skipped because llm.workflow.enabled=false"
        );
        return;
    }
    if let Err(err) = invoke_workflow_bundle_models(
        Arc::clone(&config),
        db_pool,
        http_client,
        loopback_http_client,
        print_response,
        bundle,
        trigger,
    )
    .await
    {
        error!(error = %err, "workflow invoke failed");
    }
}

fn workflow_stage1_refresh_reason(
    config: &RootConfig,
    bundle: &LatestBundle,
    workflow_state: &crate::workflow::state::WorkflowState,
    stage1_output: Option<&crate::workflow::schema::Stage1Output>,
) -> Option<String> {
    if let Some(reason) = workflow_state.pending_stage1_refresh_reason.as_ref() {
        return Some(reason.clone());
    }
    if startup_stage1_refresh_due(&bundle.raw.symbol) {
        return Some("startup_force_stage1".to_string());
    }
    if stage1_output.is_none() {
        return Some("startup_missing_stage1".to_string());
    }
    let hour = bundle.raw.ts_bucket.hour() as u8;
    let minute = bundle.raw.ts_bucket.minute() as u8;
    if minute == 0 && config.llm.workflow.stage1_refresh_hours.contains(&hour) {
        return Some("scheduled_2h".to_string());
    }
    None
}

fn consume_pending_stage1_refresh_reason(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    refresh_reason: &str,
) -> bool {
    if workflow_state.pending_stage1_refresh_reason.as_deref() == Some(refresh_reason) {
        workflow_state.pending_stage1_refresh_reason = None;
        return true;
    }
    false
}

fn workflow_stage2_review_due(
    config: &RootConfig,
    bundle: &LatestBundle,
    stage1_output: Option<&crate::workflow::schema::Stage1Output>,
    stage1_refresh_blocking: bool,
) -> bool {
    if stage1_refresh_blocking {
        return false;
    }
    let Some(stage1_output) = stage1_output else {
        return false;
    };
    if stage1_output.monitoring_status != "active" || stage1_output.current_path.is_none() {
        return false;
    }
    let minute = bundle.raw.ts_bucket.minute() as u8;
    config.llm.workflow.stage2_review_minutes.contains(&minute)
}

fn default_workflow_state(symbol: &str) -> crate::workflow::state::WorkflowState {
    crate::workflow::state::WorkflowState {
        symbol: symbol.to_string(),
        ..crate::workflow::state::WorkflowState::default()
    }
}

fn floor_to_15m_window_start(ts: DateTime<Utc>) -> DateTime<Utc> {
    let epoch = ts.timestamp();
    let floored = epoch - epoch.rem_euclid(15 * 60);
    DateTime::<Utc>::from_timestamp(floored, 0).unwrap_or(ts)
}

fn reset_watcher_window_if_needed(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    ts: DateTime<Utc>,
) -> Option<crate::workflow::schema::TacticalEntryPlan> {
    let window_start = floor_to_15m_window_start(ts);
    if workflow_state.active_15m_window_start == Some(window_start) {
        return None;
    }
    let expired_tactical_plan = workflow_state.approved_tactical_plan.clone();
    workflow_state.active_15m_window_start = Some(window_start);
    workflow_state.filled_stopout_attempts = 0;
    workflow_state.last_filled_context_key = None;
    workflow_state.approved_tactical_plan = None;
    workflow_state.approved_tactical_plan_updated_at = None;
    workflow_state.pending_entry_bracket_template_override = None;
    expired_tactical_plan
}

fn clear_approved_tactical_plan(workflow_state: &mut crate::workflow::state::WorkflowState) {
    workflow_state.approved_tactical_plan = None;
    workflow_state.approved_tactical_plan_updated_at = None;
    workflow_state.last_filled_context_key = None;
    workflow_state.filled_stopout_attempts = 0;
    workflow_state.pending_entry_bracket_template_override = None;
}

#[derive(Debug, Clone, Copy)]
struct SelectedEntryPlan<'a> {
    plan: &'a crate::workflow::schema::EntryPlan,
    trigger_price: f64,
}

fn workflow_entry_context_key(symbol: &str, side: &str, path_id: &str) -> String {
    format!(
        "{}:{}:{}",
        symbol.to_ascii_uppercase(),
        side.to_ascii_uppercase(),
        path_id
    )
}

fn extract_fast_price_event(
    envelope: &FastMarketEnvelope,
    target_symbol: &str,
) -> Option<FastPriceEvent> {
    if !envelope.symbol.eq_ignore_ascii_case(target_symbol) {
        return None;
    }
    if envelope.backfill_in_progress.unwrap_or(false) {
        return None;
    }
    let (price, source) = match envelope.msg_type.as_str() {
        "md.mark_price" => (
            value_as_f64(envelope.data.get("mark_price")?)?,
            FastPriceSource::MarkPrice,
        ),
        "md.agg.trade.1s" => (
            value_as_f64(envelope.data.get("last_price")?)?,
            FastPriceSource::Trade1s,
        ),
        "md.kline" => (
            value_as_f64(envelope.data.get("close_price")?)?,
            FastPriceSource::Kline1m,
        ),
        _ => return None,
    };
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    Some(FastPriceEvent {
        symbol: envelope.symbol.to_ascii_uppercase(),
        event_ts: envelope.event_ts,
        price,
        source,
        routing_key: envelope.routing_key.clone(),
    })
}

fn fast_plan_version(
    tactical_plan: &crate::workflow::schema::TacticalEntryPlan,
    updated_at: Option<DateTime<Utc>>,
) -> String {
    let updated_at_ms = updated_at
        .map(|ts| ts.timestamp_millis())
        .unwrap_or_default();
    format!("{}:{updated_at_ms}", tactical_plan.path_id)
}

fn sync_fast_watcher_plan_state(
    fast_state: &mut Option<FastWatcherPlanState>,
    plan_version: &str,
    context_key: &str,
) {
    let should_reset = fast_state
        .as_ref()
        .map(|state| state.plan_version != plan_version || state.context_key != context_key)
        .unwrap_or(true);
    if should_reset {
        *fast_state = Some(FastWatcherPlanState {
            plan_version: plan_version.to_string(),
            context_key: context_key.to_string(),
            ..FastWatcherPlanState::default()
        });
    }
}

fn favorable_beyond_zone(
    side: &str,
    price: f64,
    zone: &crate::workflow::schema::PriceZone,
) -> bool {
    match side {
        "LONG" => price > zone.high,
        "SHORT" => price < zone.low,
        _ => false,
    }
}

fn inside_or_beyond_activation(plan: &crate::workflow::schema::EntryPlan, price: f64) -> bool {
    plan.entry_activation_level.contains(price)
        || favorable_beyond_zone(&plan.side, price, &plan.entry_activation_level)
}

fn breakout_crossed(plan: &crate::workflow::schema::EntryPlan, price: f64) -> bool {
    favorable_beyond_zone(&plan.side, price, &plan.entry_zone)
}

fn breakout_excursion_bps(plan: &crate::workflow::schema::EntryPlan, extreme_price: f64) -> f64 {
    match plan.side.as_str() {
        "LONG" if plan.entry_zone.high > 0.0 => {
            ((extreme_price / plan.entry_zone.high) - 1.0) * 10_000.0
        }
        "SHORT" if extreme_price > 0.0 => ((plan.entry_zone.low / extreme_price) - 1.0) * 10_000.0,
        _ => 0.0,
    }
}

fn fast_confirm_duration_ms(confirm_bars: u8) -> i64 {
    confirm_bars.saturating_sub(1) as i64 * 1_000
}

fn fast_watcher_entry_ready(
    state: &mut FastWatcherPlanState,
    plan: &crate::workflow::schema::EntryPlan,
    event: &FastPriceEvent,
    watcher_cfg: &crate::app::config::WorkflowWatcherConfig,
) -> bool {
    if state.fired {
        return false;
    }
    if state
        .last_event_ts
        .map(|last_ts| event.event_ts < last_ts)
        .unwrap_or(false)
    {
        return false;
    }
    state.last_event_ts = Some(event.event_ts);

    if plan.entry_profile == "failed_auction_reentry" {
        if plan.entry_invalidation_level.contains(event.price) {
            state.invalidation_probe_seen_at = Some(event.event_ts);
            state.recovery_started_at = None;
        }
        let probe_window_ms = watcher_cfg
            .price_predicates
            .failed_auction_reentry_confirmed
            .probe_lookback_bars as i64
            * 1_000;
        let probe_recent = state
            .invalidation_probe_seen_at
            .map(|ts| {
                event.event_ts.signed_duration_since(ts).num_milliseconds() <= probe_window_ms
            })
            .unwrap_or(false);
        if !probe_recent {
            state.invalidation_probe_seen_at = None;
            state.recovery_started_at = None;
            return false;
        }
        let recovered =
            plan.entry_zone.contains(event.price) || inside_or_beyond_activation(plan, event.price);
        if !recovered {
            state.recovery_started_at = None;
            return false;
        }
        let recovery_started_at = *state.recovery_started_at.get_or_insert(event.event_ts);
        let recovery_ms = fast_confirm_duration_ms(
            watcher_cfg
                .price_predicates
                .failed_auction_reentry_confirmed
                .reaccept_confirm_bars,
        );
        return event
            .event_ts
            .signed_duration_since(recovery_started_at)
            .num_milliseconds()
            >= recovery_ms;
    }

    match plan.intent_mode.as_str() {
        "immediate" => plan.entry_zone.contains(event.price),
        "pullback" => {
            if inside_or_beyond_activation(plan, event.price) {
                state.activation_seen_at.get_or_insert(event.event_ts);
            }
            if state.activation_seen_at.is_some() && breakout_crossed(plan, event.price) {
                state.advanced_beyond_entry_after_activation = true;
            }
            state.activation_seen_at.is_some()
                && state.advanced_beyond_entry_after_activation
                && plan.entry_zone.contains(event.price)
        }
        "breakout" => {
            if !breakout_crossed(plan, event.price) {
                state.breakout_started_at = None;
                state.breakout_extreme_price = None;
                return false;
            }
            let breakout_started_at = *state.breakout_started_at.get_or_insert(event.event_ts);
            state.breakout_extreme_price =
                Some(match (plan.side.as_str(), state.breakout_extreme_price) {
                    ("LONG", Some(previous)) => previous.max(event.price),
                    ("SHORT", Some(previous)) => previous.min(event.price),
                    _ => event.price,
                });
            let dwell_ms = fast_confirm_duration_ms(
                watcher_cfg.price_predicates.breakout_confirmed.confirm_bars,
            );
            let dwell_ready = event
                .event_ts
                .signed_duration_since(breakout_started_at)
                .num_milliseconds()
                >= dwell_ms;
            let excursion_ready = state
                .breakout_extreme_price
                .map(|price| breakout_excursion_bps(plan, price))
                .unwrap_or_default()
                >= watcher_cfg
                    .price_predicates
                    .breakout_confirmed
                    .min_break_bps;
            dwell_ready || excursion_ready
        }
        _ => false,
    }
}

fn select_fast_entry_plan<'a>(
    symbol: &str,
    tactical_plan: &'a crate::workflow::schema::TacticalEntryPlan,
    workflow_state: &crate::workflow::state::WorkflowState,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    trigger_price: f64,
    entry_ready: bool,
    hard_invalidation_hit: bool,
    max_filled_stopout_attempts: u8,
) -> Option<SelectedEntryPlan<'a>> {
    if !entry_ready {
        return None;
    }
    if hard_invalidation_hit {
        return None;
    }
    let side = tactical_plan.entry_plan.side.as_str();
    if has_active_position_for_side(trading_state, side) {
        return None;
    }
    if live_entry_order_count_for_side(trading_state, side) > 0 {
        return None;
    }
    if workflow_state.filled_stopout_attempts >= max_filled_stopout_attempts {
        return None;
    }
    let candidate = &tactical_plan.entry_plan;
    let context_key = workflow_entry_context_key(symbol, &candidate.side, &tactical_plan.path_id);
    if entry_snapshots.contains_key(&context_key) {
        return None;
    }
    Some(SelectedEntryPlan {
        plan: candidate,
        trigger_price,
    })
}

fn execution_intent_from_entry_plan(
    symbol: &str,
    path_id: &str,
    plan: &crate::workflow::schema::EntryPlan,
    current_path: &crate::workflow::schema::CurrentPath,
    bracket_override: Option<&crate::workflow::schema::PostFillBracketTemplate>,
    trigger_price: f64,
    ttl_minutes: u64,
    quantity_override: Option<f64>,
) -> crate::workflow::schema::ExecutionIntent {
    let take_profit_1 = bracket_override
        .map(|item| item.take_profit_1)
        .unwrap_or_else(|| current_path.first_path_target.midpoint());
    let take_profit_2 = bracket_override
        .map(|item| item.take_profit_2)
        .unwrap_or_else(|| current_path.next_path_target.midpoint());
    let stop_loss = bracket_override
        .map(|item| item.stop_loss)
        .unwrap_or(plan.stop_loss);
    crate::workflow::schema::ExecutionIntent {
        side: plan.side.clone(),
        entry_profile: Some(plan.entry_profile.clone()),
        intent_mode: plan.intent_mode.clone(),
        entry_activation_level: Some(plan.entry_activation_level.clone()),
        entry_zone: plan.entry_zone.clone(),
        entry_invalidation_level: Some(plan.entry_invalidation_level.clone()),
        trigger_price: Some(trigger_price),
        stop_loss,
        take_profit_1,
        take_profit_2,
        ttl_minutes,
        max_drift_pct: plan.max_drift_pct,
        path_id: path_id.to_string(),
        entry_snapshot: crate::workflow::schema::EntrySnapshotRef {
            context_key: workflow_entry_context_key(symbol, &plan.side, path_id),
            path_id: path_id.to_string(),
        },
        reason: Some(plan.entry_note.clone()),
        quantity_override,
    }
}

fn matching_tactical_entry_plan<'a>(
    workflow_state: &'a crate::workflow::state::WorkflowState,
    path_id: &str,
) -> Option<&'a crate::workflow::schema::EntryPlan> {
    workflow_state
        .approved_tactical_plan
        .as_ref()
        .filter(|plan| plan.path_id == path_id)
        .map(|plan| &plan.entry_plan)
}

fn build_fallback_entry_snapshot(
    symbol: &str,
    context_key: &str,
    path_id: &str,
    current_path: &crate::workflow::schema::CurrentPath,
    fallback_plan: Option<&crate::workflow::schema::EntryPlan>,
    bracket_override: Option<&crate::workflow::schema::PostFillBracketTemplate>,
) -> crate::workflow::schema::EntrySnapshot {
    crate::workflow::schema::EntrySnapshot {
        symbol: symbol.to_ascii_uppercase(),
        context_key: context_key.to_string(),
        path_id: path_id.to_string(),
        side: fallback_plan
            .map(|plan| plan.side.clone())
            .unwrap_or_else(|| current_path.side.clone()),
        entry_profile: fallback_plan.map(|plan| plan.entry_profile.clone()),
        intent_mode: fallback_plan.map(|plan| plan.intent_mode.clone()),
        entry_activation_level: fallback_plan.map(|plan| plan.entry_activation_level.clone()),
        entry_zone: fallback_plan.map(|plan| plan.entry_zone.clone()),
        entry_invalidation_level: fallback_plan.map(|plan| plan.entry_invalidation_level.clone()),
        max_drift_pct: fallback_plan.map(|plan| plan.max_drift_pct),
        stop_loss: bracket_override
            .map(|item| item.stop_loss)
            .unwrap_or_else(|| {
                fallback_plan
                    .map(|plan| plan.stop_loss)
                    .unwrap_or_else(|| current_path.failure_level.midpoint())
            }),
        take_profit_1: bracket_override
            .map(|item| item.take_profit_1)
            .unwrap_or_else(|| current_path.first_path_target.midpoint()),
        take_profit_2: bracket_override
            .map(|item| item.take_profit_2)
            .unwrap_or_else(|| current_path.next_path_target.midpoint()),
        allowed_stop_loss_levels: vec![],
        allowed_take_profit_levels: vec![],
        tp1_realized: false,
        applied_driver_deterioration_signals: Vec::new(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn snapshot_for_management_context(
    symbol: &str,
    current_path: &crate::workflow::schema::CurrentPath,
    workflow_state: &crate::workflow::state::WorkflowState,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    context_key: &str,
    path_id: &str,
) -> crate::workflow::schema::EntrySnapshot {
    entry_snapshots
        .get(context_key)
        .cloned()
        .unwrap_or_else(|| {
            build_fallback_entry_snapshot(
                symbol,
                context_key,
                path_id,
                current_path,
                matching_tactical_entry_plan(workflow_state, path_id),
                workflow_state
                    .pending_entry_bracket_template_override
                    .as_ref(),
            )
        })
}

fn entry_plan_from_snapshot_template(
    snapshot: &crate::workflow::schema::EntrySnapshot,
    fallback_plan: Option<&crate::workflow::schema::EntryPlan>,
) -> Result<crate::workflow::schema::EntryPlan> {
    let entry_profile = snapshot
        .entry_profile
        .clone()
        .or_else(|| fallback_plan.map(|plan| plan.entry_profile.clone()))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing entry_profile for {}",
                snapshot.context_key
            )
        })?;
    let intent_mode = snapshot
        .intent_mode
        .clone()
        .or_else(|| fallback_plan.map(|plan| plan.intent_mode.clone()))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing intent_mode for {}",
                snapshot.context_key
            )
        })?;
    let entry_activation_level = snapshot
        .entry_activation_level
        .clone()
        .or_else(|| fallback_plan.map(|plan| plan.entry_activation_level.clone()))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing entry_activation_level for {}",
                snapshot.context_key
            )
        })?;
    let entry_zone = snapshot
        .entry_zone
        .clone()
        .or_else(|| fallback_plan.map(|plan| plan.entry_zone.clone()))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing entry_zone for {}",
                snapshot.context_key
            )
        })?;
    let entry_invalidation_level = snapshot
        .entry_invalidation_level
        .clone()
        .or_else(|| fallback_plan.map(|plan| plan.entry_invalidation_level.clone()))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing entry_invalidation_level for {}",
                snapshot.context_key
            )
        })?;
    let max_drift_pct = snapshot
        .max_drift_pct
        .or_else(|| fallback_plan.map(|plan| plan.max_drift_pct))
        .ok_or_else(|| {
            anyhow!(
                "entry template missing max_drift_pct for {}",
                snapshot.context_key
            )
        })?;

    Ok(crate::workflow::schema::EntryPlan {
        side: snapshot.side.clone(),
        entry_profile,
        intent_mode,
        entry_activation_level,
        entry_zone,
        entry_invalidation_level,
        stop_loss: snapshot.stop_loss,
        max_drift_pct,
        entry_note: fallback_plan
            .map(|plan| plan.entry_note.clone())
            .unwrap_or_default(),
    })
}

fn build_stage2c_replace_execution_intent(
    symbol: &str,
    current_path: &crate::workflow::schema::CurrentPath,
    workflow_state: &crate::workflow::state::WorkflowState,
    snapshot: &crate::workflow::schema::EntrySnapshot,
    action: &crate::workflow::schema::PendingOrderManagementAction,
    watch_price: f64,
    ttl_minutes: u64,
) -> Result<(
    crate::workflow::schema::TacticalEntryPlan,
    crate::workflow::schema::PostFillBracketTemplate,
    crate::workflow::schema::ExecutionIntent,
)> {
    let fallback_plan = matching_tactical_entry_plan(workflow_state, &action.path_id);
    let mut next_entry_plan = entry_plan_from_snapshot_template(snapshot, fallback_plan)?;
    if let Some(zone) = action.replacement_entry_zone.clone() {
        next_entry_plan.entry_zone = zone;
    }
    if let Some(level) = action.replacement_entry_invalidation_level.clone() {
        next_entry_plan.entry_invalidation_level = level;
    }
    if let Some(stop_loss) = action.replacement_stop_loss {
        next_entry_plan.stop_loss = stop_loss;
    }

    let mut bracket_template = workflow_state
        .pending_entry_bracket_template_override
        .clone()
        .unwrap_or(crate::workflow::schema::PostFillBracketTemplate {
            take_profit_1: snapshot.take_profit_1,
            take_profit_2: snapshot.take_profit_2,
            stop_loss: snapshot.stop_loss,
        });
    if let Some(stop_loss) = action.replacement_stop_loss {
        bracket_template.stop_loss = stop_loss;
    }

    let tactical_plan = crate::workflow::schema::TacticalEntryPlan {
        path_id: action.path_id.clone(),
        entry_plan: next_entry_plan.clone(),
    };
    let mut intent = execution_intent_from_entry_plan(
        symbol,
        &action.path_id,
        &next_entry_plan,
        current_path,
        Some(&bracket_template),
        watch_price,
        ttl_minutes,
        None,
    );
    intent.reason = Some(action.reason.clone());

    Ok((tactical_plan, bracket_template, intent))
}

fn append_unique_level(levels: &mut Vec<f64>, level: f64) {
    if !level.is_finite() {
        return;
    }
    if levels
        .iter()
        .any(|existing| (*existing - level).abs() < f64::EPSILON)
    {
        return;
    }
    levels.push(level);
}

fn patch_entry_snapshot_levels(
    snapshot: &crate::workflow::schema::EntrySnapshot,
    new_stop_loss: Option<f64>,
    take_profit_1: Option<f64>,
    take_profit_2: Option<f64>,
) -> crate::workflow::schema::EntrySnapshot {
    let mut next = snapshot.clone();
    if let Some(level) = new_stop_loss {
        next.stop_loss = level;
        append_unique_level(&mut next.allowed_stop_loss_levels, level);
    }
    if let Some(level) = take_profit_1 {
        next.take_profit_1 = level;
        append_unique_level(&mut next.allowed_take_profit_levels, level);
    }
    if let Some(level) = take_profit_2 {
        next.take_profit_2 = level;
        append_unique_level(&mut next.allowed_take_profit_levels, level);
    }
    next.updated_at = Utc::now();
    next
}

fn has_pending_position_management_actions(
    plan: &crate::workflow::schema::PositionManagementPlan,
) -> bool {
    plan.actions
        .iter()
        // Treat legacy placeholder holds as inert so old persisted plans stay harmless.
        .any(|action| action.action_type != "hold")
}

fn has_pending_order_management_actions(
    plan: &crate::workflow::schema::PendingOrderManagementPlan,
) -> bool {
    plan.actions
        .iter()
        // Treat legacy placeholder keep_order actions as inert.
        .any(|action| action.action_type != "keep_order")
}

fn position_management_plan_for_context(
    plan: Option<crate::workflow::schema::PositionManagementPlan>,
    path_id: &str,
    context_key: &str,
) -> Option<crate::workflow::schema::PositionManagementPlan> {
    let mut plan = plan?;
    if plan.path_id != path_id {
        return None;
    }
    plan.actions
        .retain(|action| action.context_key == context_key && action.action_type != "hold");
    if !has_pending_position_management_actions(&plan) {
        None
    } else {
        Some(plan)
    }
}

fn pending_order_management_plan_for_context(
    plan: Option<crate::workflow::schema::PendingOrderManagementPlan>,
    path_id: &str,
    context_key: &str,
) -> Option<crate::workflow::schema::PendingOrderManagementPlan> {
    let mut plan = plan?;
    if plan.path_id != path_id {
        return None;
    }
    plan.actions
        .retain(|action| action.context_key == context_key && action.action_type != "keep_order");
    if !has_pending_order_management_actions(&plan) {
        None
    } else {
        Some(plan)
    }
}

fn clear_position_management_plans(workflow_state: &mut crate::workflow::state::WorkflowState) {
    workflow_state.approved_position_management_plans.clear();
    workflow_state
        .approved_position_management_plans_updated_at
        .clear();
}

fn clear_pending_order_management_plans(
    workflow_state: &mut crate::workflow::state::WorkflowState,
) {
    workflow_state
        .approved_pending_order_management_plans
        .clear();
    workflow_state
        .approved_pending_order_management_plans_updated_at
        .clear();
}

fn replace_position_management_plans(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    plans: BTreeMap<String, crate::workflow::schema::PositionManagementPlan>,
) {
    let now = Utc::now();
    workflow_state.approved_position_management_plans = plans
        .into_iter()
        .filter(|(_, plan)| has_pending_position_management_actions(plan))
        .collect();
    workflow_state.approved_position_management_plans_updated_at = workflow_state
        .approved_position_management_plans
        .keys()
        .cloned()
        .map(|context_key| (context_key, now))
        .collect();
}

fn replace_pending_order_management_plans(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    plans: BTreeMap<String, crate::workflow::schema::PendingOrderManagementPlan>,
) {
    let now = Utc::now();
    workflow_state.approved_pending_order_management_plans = plans
        .into_iter()
        .filter(|(_, plan)| has_pending_order_management_actions(plan))
        .collect();
    workflow_state.approved_pending_order_management_plans_updated_at = workflow_state
        .approved_pending_order_management_plans
        .keys()
        .cloned()
        .map(|context_key| (context_key, now))
        .collect();
}

fn remove_position_management_plan(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    context_key: &str,
) {
    workflow_state
        .approved_position_management_plans
        .remove(context_key);
    workflow_state
        .approved_position_management_plans_updated_at
        .remove(context_key);
}

fn remove_pending_order_management_plan(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    context_key: &str,
) {
    workflow_state
        .approved_pending_order_management_plans
        .remove(context_key);
    workflow_state
        .approved_pending_order_management_plans_updated_at
        .remove(context_key);
}

fn upsert_position_management_plan(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    context_key: &str,
    plan: crate::workflow::schema::PositionManagementPlan,
) {
    workflow_state
        .approved_position_management_plans
        .insert(context_key.to_string(), plan);
    workflow_state
        .approved_position_management_plans_updated_at
        .insert(context_key.to_string(), Utc::now());
}

fn upsert_pending_order_management_plan(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    context_key: &str,
    plan: crate::workflow::schema::PendingOrderManagementPlan,
) {
    workflow_state
        .approved_pending_order_management_plans
        .insert(context_key.to_string(), plan);
    workflow_state
        .approved_pending_order_management_plans_updated_at
        .insert(context_key.to_string(), Utc::now());
}

fn remove_position_management_action(
    plan: &crate::workflow::schema::PositionManagementPlan,
    index: usize,
) -> Option<crate::workflow::schema::PositionManagementPlan> {
    let mut next = plan.clone();
    if index < next.actions.len() {
        next.actions.remove(index);
    }
    if has_pending_position_management_actions(&next) {
        Some(next)
    } else {
        None
    }
}

fn remove_pending_order_management_action(
    plan: &crate::workflow::schema::PendingOrderManagementPlan,
    index: usize,
) -> Option<crate::workflow::schema::PendingOrderManagementPlan> {
    let mut next = plan.clone();
    if index < next.actions.len() {
        next.actions.remove(index);
    }
    if has_pending_order_management_actions(&next) {
        Some(next)
    } else {
        None
    }
}

fn first_triggered_position_management_action_index(
    plan: &crate::workflow::schema::PositionManagementPlan,
    facts: &WatcherPriceFacts,
) -> Option<usize> {
    plan.actions.iter().position(|action| {
        action.action_type != "hold"
            && action
                .trigger_condition
                .as_ref()
                .is_some_and(|trigger| price_trigger_condition_met(trigger, facts.current_price))
    })
}

fn first_triggered_pending_order_action_index(
    plan: &crate::workflow::schema::PendingOrderManagementPlan,
    facts: &WatcherPriceFacts,
) -> Option<usize> {
    plan.actions.iter().position(|action| {
        action.action_type != "keep_order"
            && action
                .trigger_condition
                .as_ref()
                .is_some_and(|trigger| price_trigger_condition_met(trigger, facts.current_price))
    })
}

fn management_action_from_position_management_action(
    action: &crate::workflow::schema::PositionManagementAction,
) -> Result<crate::workflow::schema::ManagementAction> {
    let action_type = match action.action_type.as_str() {
        "reduce" => "REDUCE_POSITION",
        "exit_full" => "FLATTEN_POSITION",
        "move_stop" => "MOVE_STOP",
        "update_take_profit" => "UPDATE_TAKE_PROFIT",
        other => return Err(anyhow!("unsupported stage2b action_type {}", other)),
    };
    Ok(crate::workflow::schema::ManagementAction {
        action_type: action_type.to_string(),
        context_key: action.context_key.clone(),
        path_id: action.path_id.clone(),
        execution_price: action.execution_price,
        reduce_ratio: action.reduce_ratio,
        new_stop_loss: action.new_stop_loss,
        take_profit_1: action.take_profit_1,
        take_profit_2: action.take_profit_2,
        reason: Some(action.reason.clone()),
    })
}

fn watcher_reference_price(indicators: &Value, fallback_price: f64) -> f64 {
    latest_closed_1m_price(indicators).unwrap_or(fallback_price)
}

fn select_entry_plan<'a>(
    symbol: &str,
    tactical_plan: &'a crate::workflow::schema::TacticalEntryPlan,
    workflow_state: &crate::workflow::state::WorkflowState,
    latest_price: f64,
    hard_invalidation_hit: bool,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    indicators: &Value,
    watcher_cfg: &crate::app::config::WorkflowWatcherConfig,
) -> Option<SelectedEntryPlan<'a>> {
    if hard_invalidation_hit {
        return None;
    }
    let side = tactical_plan.entry_plan.side.as_str();
    if has_active_position_for_side(trading_state, side) {
        return None;
    }
    if workflow_state.filled_stopout_attempts >= watcher_cfg.max_filled_stopout_attempts {
        return None;
    }
    let candidate = &tactical_plan.entry_plan;
    let watch_price = watcher_reference_price(indicators, latest_price);
    let watch_facts = build_watcher_price_facts(indicators, watch_price);
    if !watcher_entry_ready(candidate, &watch_facts, watcher_cfg) {
        return None;
    }
    let context_key = workflow_entry_context_key(symbol, &candidate.side, &tactical_plan.path_id);
    if entry_snapshots.contains_key(&context_key) {
        return None;
    }
    Some(SelectedEntryPlan {
        plan: candidate,
        trigger_price: watch_price,
    })
}

async fn handle_fast_market_event(
    ctx: &AppContext,
    fast_state: &mut Option<FastWatcherPlanState>,
    event: FastPriceEvent,
) -> Result<()> {
    let symbol = event.symbol.to_ascii_uppercase();
    let state_dir = ctx.config.llm.workflow.state_dir.clone();
    let mut workflow_state =
        crate::workflow::persistence::load_workflow_state(&state_dir, &symbol)?
            .unwrap_or_else(|| default_workflow_state(&symbol));
    workflow_state.symbol = symbol.clone();

    let Some(stage1_output) =
        crate::workflow::persistence::load_stage1_output(&state_dir, &symbol)?
    else {
        *fast_state = None;
        return Ok(());
    };
    if stage1_output.monitoring_status != "active" {
        *fast_state = None;
        return Ok(());
    }
    let Some(current_path) = stage1_output.current_path.as_ref() else {
        *fast_state = None;
        return Ok(());
    };
    if workflow_state.pending_stage1_refresh_reason.is_some() {
        *fast_state = None;
        return Ok(());
    }
    let Some(tactical_plan) = workflow_state.approved_tactical_plan.as_ref() else {
        *fast_state = None;
        return Ok(());
    };
    if tactical_plan.path_id != current_path.id {
        *fast_state = None;
        return Ok(());
    }
    if ctx.config.llm.workflow.watcher.entry_attempt_window == "same_15m_window"
        && workflow_state
            .active_15m_window_start
            .is_some_and(|start| start != floor_to_15m_window_start(event.event_ts))
    {
        *fast_state = None;
        return Ok(());
    }

    let context_key = workflow_entry_context_key(
        &symbol,
        &tactical_plan.entry_plan.side,
        &tactical_plan.path_id,
    );
    let plan_version = fast_plan_version(
        tactical_plan,
        workflow_state.approved_tactical_plan_updated_at,
    );
    sync_fast_watcher_plan_state(fast_state, &plan_version, &context_key);

    let hard_invalidation_hit =
        crate::workflow::stage2::failure_level_breached(&stage1_output, event.price)?;
    if hard_invalidation_hit {
        *fast_state = None;
        return Ok(());
    }

    let fast_plan_state = fast_state
        .as_mut()
        .ok_or_else(|| anyhow!("fast watcher state unavailable"))?;

    let mut entry_snapshots =
        crate::workflow::persistence::load_entry_snapshots_for_symbol(&state_dir, &symbol)?
            .into_iter()
            .map(|snapshot| (snapshot.context_key.clone(), snapshot))
            .collect::<HashMap<_, _>>();
    if entry_snapshots.contains_key(&context_key) {
        fast_plan_state.fired = true;
        return Ok(());
    }

    let entry_ready = fast_watcher_entry_ready(
        fast_plan_state,
        &tactical_plan.entry_plan,
        &event,
        &ctx.config.llm.workflow.watcher,
    );
    if !entry_ready {
        return Ok(());
    }

    let trading_state = fetch_symbol_trading_state_for_fast_path(
        &ctx.http_client,
        &ctx.config.api.binance,
        &ctx.config.llm.execution,
        &symbol,
    )
    .await?;
    let Some(selected_entry_plan) = select_fast_entry_plan(
        &symbol,
        tactical_plan,
        &workflow_state,
        &trading_state,
        &entry_snapshots,
        event.price,
        entry_ready,
        hard_invalidation_hit,
        ctx.config.llm.workflow.watcher.max_filled_stopout_attempts,
    ) else {
        return Ok(());
    };

    let intent = execution_intent_from_entry_plan(
        &symbol,
        &tactical_plan.path_id,
        selected_entry_plan.plan,
        current_path,
        workflow_state
            .pending_entry_bracket_template_override
            .as_ref(),
        selected_entry_plan.trigger_price,
        ctx.config.llm.workflow.watcher.entry_ttl_minutes,
        None,
    );

    match adapt_execution_intent(&intent) {
        Ok(adapted_intent) => match execute_workflow_execution_intent(
            &ctx.http_client,
            &ctx.config.api.binance,
            &ctx.config.llm.execution,
            &symbol,
            &adapted_intent,
        )
        .await
        {
            Ok(report) => {
                append_workflow_journal_event(
                    "workflow_execution_report",
                    &symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "path_id": intent.path_id,
                        "context_key": intent.entry_snapshot.context_key,
                        "price_source": event.source.as_str(),
                        "routing_key": event.routing_key,
                        "trigger_price": event.price,
                        "report": {
                            "decision": report.decision,
                            "quantity": report.quantity,
                            "leverage": report.leverage,
                            "position_side": report.position_side,
                            "maker_entry_price": report.maker_entry_price,
                            "take_profit": report.actual_take_profit,
                            "stop_loss": report.actual_stop_loss,
                            "risk_reward_ratio": report.actual_risk_reward_ratio,
                            "dry_run": report.dry_run,
                        }
                    }),
                );
                fast_plan_state.fired = true;
                if !report.dry_run {
                    let snapshot = crate::workflow::management::snapshot_from_execution_intent(
                        &symbol,
                        &intent,
                        current_path,
                        Utc::now(),
                    );
                    crate::workflow::persistence::save_entry_snapshot(&state_dir, &snapshot)?;
                    entry_snapshots.insert(snapshot.context_key.clone(), snapshot);
                    workflow_state.last_filled_context_key =
                        Some(intent.entry_snapshot.context_key.clone());
                    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
                }

                let signal = build_execution_trade_signal(
                    event.event_ts,
                    "watcher_fast_consumer",
                    &symbol,
                    "workflow_watcher_fast",
                    &trading_state,
                    &intent,
                    Some(&report),
                    intent.reason.as_deref(),
                );
                let telegram_operator = TelegramOperator::from_config(&ctx.config.api.telegram);
                let x_operator = XOperator::from_config(&ctx.config.api.x);
                send_trade_signal_notifications(
                    telegram_operator.as_ref(),
                    x_operator.as_ref(),
                    &ctx.config.llm.telegram_signal_decisions,
                    &ctx.config.llm.x_signal_decisions,
                    &ctx.http_client,
                    &signal,
                )
                .await;
            }
            Err(err) => {
                let blocked = err
                    .downcast_ref::<TradeExecutionBlockedByCurrentPriceBeyondStopLoss>()
                    .map(|item| {
                        json!({
                            "decision": item.decision.as_str(),
                            "current_reference_price": item.current_reference_price,
                            "current_price_source": item.current_price_source,
                            "entry_price": item.entry_price,
                            "stop_loss": item.stop_loss,
                            "best_bid_price": item.best_bid_price,
                            "best_ask_price": item.best_ask_price,
                        })
                    });
                append_workflow_journal_event(
                    "workflow_execution_error",
                    &symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "path_id": intent.path_id,
                        "context_key": intent.entry_snapshot.context_key,
                        "price_source": event.source.as_str(),
                        "routing_key": event.routing_key,
                        "trigger_price": event.price,
                        "error": format!("{err:#}"),
                        "blocked": blocked,
                    }),
                );
            }
        },
        Err(err) => {
            append_workflow_journal_event(
                "workflow_execution_error",
                &symbol,
                event.event_ts,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "path_id": intent.path_id,
                    "context_key": intent.entry_snapshot.context_key,
                    "price_source": event.source.as_str(),
                    "routing_key": event.routing_key,
                    "trigger_price": event.price,
                    "error": format!("{err:#}"),
                    "phase": "intent_adapter",
                }),
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct WatcherPriceFacts {
    current_price: f64,
    recent_bars: Vec<WatcherBar>,
}

#[derive(Debug, Clone, Copy)]
struct WatcherBar {
    close: f64,
    high: f64,
    low: f64,
}

fn latest_closed_1m_price(indicators: &Value) -> Option<f64> {
    extract_kline_history_bars(indicators, "futures", "1m")
        .into_iter()
        .rev()
        .find(|bar| {
            bar.get("is_closed")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .and_then(|bar| bar.get("close").and_then(Value::as_f64))
}

fn build_watcher_price_facts(indicators: &Value, fallback_price: f64) -> WatcherPriceFacts {
    let closed_bars = extract_kline_history_bars(indicators, "futures", "1m")
        .into_iter()
        .filter(|bar| {
            bar.get("is_closed")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .filter_map(|bar| {
            Some(WatcherBar {
                close: bar.get("close").and_then(Value::as_f64)?,
                high: bar.get("high").and_then(Value::as_f64)?,
                low: bar.get("low").and_then(Value::as_f64)?,
            })
        })
        .collect::<Vec<_>>();

    WatcherPriceFacts {
        current_price: closed_bars
            .last()
            .map(|bar| bar.close)
            .unwrap_or(fallback_price),
        recent_bars: closed_bars,
    }
}

fn recent_bars<'a>(facts: &'a WatcherPriceFacts, count: u8) -> Option<&'a [WatcherBar]> {
    let count = count as usize;
    if count == 0 || facts.recent_bars.len() < count {
        return None;
    }
    Some(&facts.recent_bars[facts.recent_bars.len() - count..])
}

fn price_above_on_close(
    facts: &WatcherPriceFacts,
    level: f64,
    predicate: &crate::app::config::ClosePredicateConfig,
) -> bool {
    let threshold = level * (1.0 + predicate.min_close_bps / 10_000.0);
    recent_bars(facts, predicate.confirm_bars)
        .map(|bars| bars.iter().all(|bar| bar.close >= threshold))
        .unwrap_or(false)
}

fn price_below_on_close(
    facts: &WatcherPriceFacts,
    level: f64,
    predicate: &crate::app::config::ClosePredicateConfig,
) -> bool {
    let threshold = level * (1.0 - predicate.min_close_bps / 10_000.0);
    recent_bars(facts, predicate.confirm_bars)
        .map(|bars| bars.iter().all(|bar| bar.close <= threshold))
        .unwrap_or(false)
}

fn entry_reclaim_confirmed(
    side: &str,
    level: &crate::workflow::schema::PriceZone,
    facts: &WatcherPriceFacts,
    predicate: &crate::app::config::EntryReclaimPredicateConfig,
) -> bool {
    match side {
        "LONG" => recent_bars(facts, predicate.confirm_bars)
            .map(|bars| {
                bars.iter().all(|bar| {
                    if predicate.allow_equal {
                        bar.close >= level.high
                    } else {
                        bar.close > level.high
                    }
                })
            })
            .unwrap_or(false),
        "SHORT" => recent_bars(facts, predicate.confirm_bars)
            .map(|bars| {
                bars.iter().all(|bar| {
                    if predicate.allow_equal {
                        bar.close <= level.low
                    } else {
                        bar.close < level.low
                    }
                })
            })
            .unwrap_or(false),
        _ => false,
    }
}

fn entry_hold_confirmed(
    side: &str,
    level: &crate::workflow::schema::PriceZone,
    facts: &WatcherPriceFacts,
    predicate: &crate::app::config::EntryHoldPredicateConfig,
) -> bool {
    let tolerance = predicate.retest_tolerance_bps / 10_000.0;
    match side {
        "LONG" => recent_bars(facts, predicate.hold_bars)
            .map(|bars| {
                let hold_floor = level.high * (1.0 - tolerance);
                bars.iter().all(|bar| bar.close >= hold_floor)
            })
            .unwrap_or(false),
        "SHORT" => recent_bars(facts, predicate.hold_bars)
            .map(|bars| {
                let hold_ceiling = level.low * (1.0 + tolerance);
                bars.iter().all(|bar| bar.close <= hold_ceiling)
            })
            .unwrap_or(false),
        _ => false,
    }
}

fn pullback_acceptance_confirmed(
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    predicate: &crate::app::config::PullbackAcceptancePredicateConfig,
) -> bool {
    let overshoot = predicate.max_overshoot_bps / 10_000.0;
    recent_bars(facts, predicate.confirm_bars)
        .map(|bars| match plan.side.as_str() {
            "LONG" => {
                let touched = !predicate.require_touch_entry_zone
                    || bars.iter().any(|bar| bar.low <= plan.entry_zone.high);
                let floor = plan.entry_zone.low * (1.0 - overshoot);
                touched && bars.iter().all(|bar| bar.close >= floor)
            }
            "SHORT" => {
                let touched = !predicate.require_touch_entry_zone
                    || bars.iter().any(|bar| bar.high >= plan.entry_zone.low);
                let ceiling = plan.entry_zone.high * (1.0 + overshoot);
                touched && bars.iter().all(|bar| bar.close <= ceiling)
            }
            _ => false,
        })
        .unwrap_or(false)
}

fn breakout_confirmed(
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    predicate: &crate::app::config::BreakoutPredicateConfig,
) -> bool {
    let threshold = predicate.min_break_bps / 10_000.0;
    recent_bars(facts, predicate.confirm_bars)
        .map(|bars| match plan.side.as_str() {
            "LONG" => {
                let breakout_level = plan.entry_zone.high * (1.0 + threshold);
                bars.iter().all(|bar| bar.close >= breakout_level)
            }
            "SHORT" => {
                let breakout_level = plan.entry_zone.low * (1.0 - threshold);
                bars.iter().all(|bar| bar.close <= breakout_level)
            }
            _ => false,
        })
        .unwrap_or(false)
}

fn failed_auction_reentry_confirmed(
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    predicate: &crate::app::config::FailedAuctionReentryPredicateConfig,
    pullback_predicate: &crate::app::config::PullbackAcceptancePredicateConfig,
) -> bool {
    let invalidation_probe = if predicate.require_probe_invalidation {
        recent_bars(facts, predicate.probe_lookback_bars)
            .map(|bars| match plan.side.as_str() {
                "LONG" => bars
                    .iter()
                    .any(|bar| bar.low <= plan.entry_invalidation_level.high),
                "SHORT" => bars
                    .iter()
                    .any(|bar| bar.high >= plan.entry_invalidation_level.low),
                _ => false,
            })
            .unwrap_or(false)
    } else {
        true
    };
    if !invalidation_probe {
        return false;
    }
    let reaccept_predicate = crate::app::config::PullbackAcceptancePredicateConfig {
        confirm_bars: predicate.reaccept_confirm_bars,
        ..pullback_predicate.clone()
    };
    pullback_acceptance_confirmed(plan, facts, &reaccept_predicate)
}

fn evaluate_watcher_predicate(
    name: &str,
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    watcher_cfg: &crate::app::config::WorkflowWatcherConfig,
) -> bool {
    match name {
        "price_above_on_close" => price_above_on_close(
            facts,
            plan.entry_activation_level.high,
            &watcher_cfg.price_predicates.price_above_on_close,
        ),
        "price_below_on_close" => price_below_on_close(
            facts,
            plan.entry_activation_level.low,
            &watcher_cfg.price_predicates.price_below_on_close,
        ),
        "entry_reclaim_confirmed" => entry_reclaim_confirmed(
            &plan.side,
            &plan.entry_activation_level,
            facts,
            &watcher_cfg.price_predicates.entry_reclaim_confirmed,
        ),
        "entry_hold_confirmed" => entry_hold_confirmed(
            &plan.side,
            &plan.entry_activation_level,
            facts,
            &watcher_cfg.price_predicates.entry_hold_confirmed,
        ),
        "breakout_confirmed" => breakout_confirmed(
            plan,
            facts,
            &watcher_cfg.price_predicates.breakout_confirmed,
        ),
        "pullback_acceptance_confirmed" => pullback_acceptance_confirmed(
            plan,
            facts,
            &watcher_cfg.price_predicates.pullback_acceptance_confirmed,
        ),
        "failed_auction_reentry_confirmed" => failed_auction_reentry_confirmed(
            plan,
            facts,
            &watcher_cfg
                .price_predicates
                .failed_auction_reentry_confirmed,
            &watcher_cfg.price_predicates.pullback_acceptance_confirmed,
        ),
        _ => false,
    }
}

fn price_trigger_condition_met(
    trigger: &crate::workflow::schema::PriceTriggerCondition,
    current_price: f64,
) -> bool {
    match trigger.trigger_type.as_str() {
        "price_above" => current_price >= trigger.trigger_price,
        "price_below" => current_price <= trigger.trigger_price,
        _ => false,
    }
}

fn all_required_predicates(
    names: &[String],
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    watcher_cfg: &crate::app::config::WorkflowWatcherConfig,
) -> bool {
    names
        .iter()
        .all(|name| evaluate_watcher_predicate(name, plan, facts, watcher_cfg))
}

fn watcher_entry_ready(
    plan: &crate::workflow::schema::EntryPlan,
    facts: &WatcherPriceFacts,
    watcher_cfg: &crate::app::config::WorkflowWatcherConfig,
) -> bool {
    let price_within_or_beyond_entry = plan.entry_activation_level.contains(facts.current_price)
        || plan.entry_zone.contains(facts.current_price)
        || match plan.side.as_str() {
            "LONG" => facts.current_price >= plan.entry_zone.high,
            "SHORT" => facts.current_price <= plan.entry_zone.low,
            _ => false,
        };
    if !price_within_or_beyond_entry {
        return false;
    }
    let profile_rule = match plan.entry_profile.as_str() {
        "reclaim_then_hold" => {
            &watcher_cfg
                .entry_profile_rules
                .reclaim_then_hold
                .required_predicates
        }
        "pullback_acceptance" => {
            &watcher_cfg
                .entry_profile_rules
                .pullback_acceptance
                .required_predicates
        }
        "failed_auction_reentry" => {
            &watcher_cfg
                .entry_profile_rules
                .failed_auction_reentry
                .required_predicates
        }
        _ => return false,
    };
    if !all_required_predicates(profile_rule, plan, facts, watcher_cfg) {
        return false;
    }

    let intent_rule = match plan.intent_mode.as_str() {
        "immediate" => &watcher_cfg.intent_mode_rules.immediate,
        "pullback" => &watcher_cfg.intent_mode_rules.pullback,
        "breakout" => &watcher_cfg.intent_mode_rules.breakout,
        _ => return false,
    };
    if !all_required_predicates(&intent_rule.required_predicates, plan, facts, watcher_cfg) {
        return false;
    }
    if intent_rule.require_price_inside_entry_zone && !plan.entry_zone.contains(facts.current_price)
    {
        return false;
    }
    if intent_rule.disallow_breakout_chase {
        match plan.side.as_str() {
            "LONG" if facts.current_price > plan.entry_zone.high => return false,
            "SHORT" if facts.current_price < plan.entry_zone.low => return false,
            _ => {}
        }
    }
    true
}

fn stop_loss_hit(plan: &crate::workflow::schema::EntryPlan, latest_price: f64) -> bool {
    match plan.side.as_str() {
        "LONG" => latest_price <= plan.stop_loss,
        "SHORT" => latest_price >= plan.stop_loss,
        _ => false,
    }
}

fn maybe_record_stopout_and_cleanup(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    approved_tactical_plan: Option<&crate::workflow::schema::TacticalEntryPlan>,
    symbol: &str,
    trading_state: &TradingStateSnapshot,
    latest_price: f64,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    state_dir: &str,
) -> Result<()> {
    let Some(tactical_plan) = approved_tactical_plan else {
        return Ok(());
    };
    let Some(last_context_key) = workflow_state.last_filled_context_key.clone() else {
        return Ok(());
    };
    let watched_plan = &tactical_plan.entry_plan;
    let expected_context_key =
        workflow_entry_context_key(symbol, &watched_plan.side, &tactical_plan.path_id);
    if expected_context_key != last_context_key {
        return Ok(());
    }
    if has_active_position_for_side(trading_state, &watched_plan.side) {
        return Ok(());
    }
    if !stop_loss_hit(watched_plan, latest_price) {
        return Ok(());
    }

    workflow_state.filled_stopout_attempts = workflow_state
        .filled_stopout_attempts
        .saturating_add(1)
        .min(255);
    workflow_state.last_filled_context_key = None;
    if entry_snapshots.remove(&last_context_key).is_some() {
        crate::workflow::persistence::delete_entry_snapshot(state_dir, symbol, &last_context_key)?;
    }
    Ok(())
}

async fn persist_workflow_prompt_input_to_disk(
    bundle: &MinuteBundleEnvelope,
    stage: &str,
    value: &Value,
    retention_minutes: u64,
) -> Result<PathBuf> {
    ensure_temp_model_output_dir().await?;
    let path = llm_stage_prompt_output_path(bundle, "workflow", "input", stage);
    let payload = json!({
        "ts_bucket": bundle.ts_bucket,
        "symbol": bundle.symbol,
        "stage": stage,
        "captured_at": Utc::now().to_rfc3339(),
        "prompt_input": value,
    });
    write_pretty_json_file(&path, &payload)?;
    let removed = prune_expired_temp_model_output_files(
        Path::new(TEMP_MODEL_OUTPUT_DIR),
        bundle.ts_bucket,
        retention_minutes_i64(retention_minutes),
    )?;
    if removed > 0 {
        debug!(
            ts_bucket = %bundle.ts_bucket,
            removed,
            retention_minutes = retention_minutes,
            stage = stage,
            "pruned expired workflow temp_model_output cache"
        );
    }
    Ok(path)
}

fn append_workflow_journal_event(
    event_type: &str,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
    payload: Value,
) {
    let event = json!({
        "event_type": event_type,
        "event_ts": Utc::now().to_rfc3339(),
        "symbol": symbol,
        "ts_bucket": ts_bucket.to_rfc3339(),
        "payload": payload,
    });
    if let Err(err) = append_journal_event(event) {
        warn!(error = %err, event_type = event_type, "append workflow journal failed");
    }
}

async fn maybe_refresh_stage1(
    config: &RootConfig,
    http_client: &Client,
    loopback_http_client: &Client,
    print_response: bool,
    bundle: &LatestBundle,
    trigger: &str,
    symbol: &str,
    state_dir: &str,
    retention_minutes: u64,
    input: &ModelInvocationInput,
    workflow_state: &mut crate::workflow::state::WorkflowState,
    stage1_output: &mut Option<crate::workflow::schema::Stage1Output>,
    tracked_zones: &mut Vec<crate::workflow::schema::TrackedZone>,
    refresh_reason: Option<String>,
) -> Result<Stage1RefreshAttempt> {
    let Some(refresh_reason) = refresh_reason else {
        return Ok(Stage1RefreshAttempt::default());
    };

    let Some(stage1_guard) = try_acquire_workflow_stage(symbol, WorkflowStageKind::Stage1) else {
        append_workflow_journal_event(
            "workflow_stage1_suppressed",
            symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": trigger,
                "refresh_reason": refresh_reason,
                "reason": "stage1_inflight",
            }),
        );
        return Ok(Stage1RefreshAttempt {
            refreshed: false,
            inflight_suppressed: true,
        });
    };

    if refresh_reason == "startup_force_stage1" {
        mark_startup_stage1_refresh_consumed(symbol);
    }

    if consume_pending_stage1_refresh_reason(workflow_state, &refresh_reason) {
        crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    }

    let indicator_summary =
        crate::workflow::code_layer::build_indicator_summary(input, tracked_zones)?;
    let prompt_input = crate::workflow::stage1::build_stage1_prompt_input(
        indicator_summary,
        stage1_output.clone(),
        refresh_reason.clone(),
    );
    let prompt_input_value =
        serde_json::to_value(&prompt_input).context("serialize workflow stage1 prompt input")?;
    if config.llm.workflow.persist_prompt_inputs {
        let path = persist_workflow_prompt_input_to_disk(
            &bundle.raw,
            "workflow_stage1",
            &prompt_input_value,
            retention_minutes,
        )
        .await?;
        debug!(
            symbol = %symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            path = %path.display(),
            "persisted workflow stage1 prompt input"
        );
    }

    if !config.llm.request_enabled {
        info!(
            symbol = %symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            refresh_reason = %refresh_reason,
            "workflow stage1 skipped because llm.request_enabled=false"
        );
        drop(stage1_guard);
        return Ok(Stage1RefreshAttempt::default());
    }

    info!(
        symbol = %symbol,
        ts_bucket = %bundle.raw.ts_bucket,
        trigger = trigger,
        refresh_reason = %refresh_reason,
        had_cached_stage1 = stage1_output.is_some(),
        "invoking workflow stage1 models"
    );

    let outputs = crate::llm::workflow_provider::invoke_stage1_models(
        http_client,
        loopback_http_client,
        config,
        &prompt_input_value,
        symbol,
    )
    .await;

    let mut parsed_stage1: Option<crate::workflow::schema::Stage1Output> = None;
    for out in outputs {
        let payload = json!({
            "trigger": trigger,
            "refresh_reason": refresh_reason,
            "model_name": out.model_name,
            "provider": out.provider,
            "model_id": out.model,
            "latency_ms": out.latency_ms,
            "raw_response_text": out.raw_response_text,
            "parsed_value": out.parsed_value,
            "error": out.error,
        });
        append_workflow_journal_event(
            "workflow_stage1_response",
            symbol,
            bundle.raw.ts_bucket,
            payload.clone(),
        );
        if print_response {
            println!(
                "WORKFLOW_STAGE1_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                bundle.raw.ts_bucket,
                trigger,
                symbol,
                render_pretty_json_value(&payload)
            );
        }

        if parsed_stage1.is_some() {
            continue;
        }
        let Some(value) = payload.get("parsed_value").cloned() else {
            continue;
        };
        match crate::workflow::parser::parse_stage1_output(value) {
            Ok(parsed) => parsed_stage1 = Some(parsed),
            Err(err) => {
                append_workflow_journal_event(
                    "workflow_stage1_parse_error",
                    symbol,
                    bundle.raw.ts_bucket,
                    json!({
                        "trigger": trigger,
                        "refresh_reason": refresh_reason,
                        "error": format!("{err:#}"),
                    }),
                );
            }
        }
    }

    let parsed_stage1 =
        parsed_stage1.ok_or_else(|| anyhow!("workflow stage1 produced no valid output"))?;
    *tracked_zones = parsed_stage1
        .current_path
        .as_ref()
        .map(|path| path.tracked_zones.clone())
        .unwrap_or_default();
    workflow_state.last_stage1_ts = Some(parsed_stage1.meta.stage1_ts);
    workflow_state.pending_stage1_refresh_reason = None;
    clear_approved_tactical_plan(workflow_state);
    crate::workflow::persistence::save_stage1_output(state_dir, symbol, &parsed_stage1)?;
    crate::workflow::persistence::save_tracked_zones(state_dir, symbol, tracked_zones)?;
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    *stage1_output = Some(parsed_stage1);
    drop(stage1_guard);

    Ok(Stage1RefreshAttempt {
        refreshed: true,
        inflight_suppressed: false,
    })
}

async fn invoke_workflow_bundle_models(
    config: Arc<RootConfig>,
    db_pool: PgPool,
    http_client: Client,
    loopback_http_client: Client,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
) -> Result<()> {
    let symbol = bundle.raw.symbol.to_ascii_uppercase();
    let state_dir = config.llm.workflow.state_dir.clone();
    let retention_minutes = config.llm.temp_cache_retention_minutes();

    let mut input = build_persist_only_input(&bundle);
    patch_input_kline_history_from_db(
        &db_pool,
        &mut input,
        &format!("{}:workflow", trigger.as_ref()),
    )
    .await?;

    let mut workflow_state =
        crate::workflow::persistence::load_workflow_state(&state_dir, &symbol)?
            .unwrap_or_else(|| default_workflow_state(&symbol));
    workflow_state.symbol = symbol.clone();

    let mut stage1_output = crate::workflow::persistence::load_stage1_output(&state_dir, &symbol)?;
    let mut tracked_zones = crate::workflow::persistence::load_tracked_zones(&state_dir, &symbol)?;

    let stage1_refresh_reason =
        workflow_stage1_refresh_reason(&config, &bundle, &workflow_state, stage1_output.as_ref());
    let stage1_attempt = maybe_refresh_stage1(
        &config,
        &http_client,
        &loopback_http_client,
        print_response,
        &bundle,
        trigger.as_ref(),
        &symbol,
        &state_dir,
        retention_minutes,
        &input,
        &mut workflow_state,
        &mut stage1_output,
        &mut tracked_zones,
        stage1_refresh_reason.clone(),
    )
    .await?;
    let mut stage1_refreshed_this_bundle = stage1_attempt.refreshed;

    if stage1_output.is_none() {
        if stage1_attempt.inflight_suppressed
            || workflow_stage_inflight(&symbol, WorkflowStageKind::Stage1)
        {
            append_workflow_journal_event(
                "workflow_stage1_waiting",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "reason": "stage1_inflight",
                }),
            );
            return Ok(());
        }
        append_workflow_journal_event(
            "workflow_stage1_unavailable",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "reason": "no_stage1_output_off_schedule",
            }),
        );
        return Ok(());
    }

    let mut stage1_output =
        stage1_output.ok_or_else(|| anyhow!("workflow stage1 output missing after refresh"))?;

    let trading_state = fetch_symbol_trading_state(
        &http_client,
        &config.api.binance,
        &config.llm.execution,
        &symbol,
    )
    .await?;
    let mut entry_snapshots =
        crate::workflow::persistence::load_entry_snapshots_for_symbol(&state_dir, &symbol)?
            .into_iter()
            .map(|snapshot| (snapshot.context_key.clone(), snapshot))
            .collect::<HashMap<_, _>>();
    input.trading_state = Some(trading_state.clone());
    input.management_snapshot =
        build_workflow_management_snapshot(&trading_state, &symbol, &entry_snapshots);

    if stage1_output.monitoring_status == "no_edge" {
        clear_approved_tactical_plan(&mut workflow_state);
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        if !trading_state.has_active_positions && !trading_state.has_open_orders {
            append_workflow_journal_event(
                "workflow_no_edge_skip",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "reason": stage1_output.no_trade_reason,
                }),
            );
            return Ok(());
        }
    }

    let indicator_summary =
        crate::workflow::code_layer::build_indicator_summary(&input, &tracked_zones)?;
    let previous_tactical_plan_for_review =
        reset_watcher_window_if_needed(&mut workflow_state, bundle.raw.ts_bucket)
            .or_else(|| workflow_state.approved_tactical_plan.clone());
    if let Some(expired_tactical_plan) = previous_tactical_plan_for_review
        .as_ref()
        .filter(|_| workflow_state.approved_tactical_plan.is_none())
    {
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        append_workflow_journal_event(
            "workflow_tactical_plan_cleared",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "path_id": expired_tactical_plan.path_id,
                "reason": "same_15m_window_expired",
            }),
        );
    }

    let mut latest_price = latest_closed_1m_price(&input.indicators)
        .or_else(|| {
            indicator_summary
                .auction_context
                .recent_15m_bars
                .last()
                .map(|bar| bar.close)
        })
        .ok_or_else(|| anyhow!("missing latest workflow price reference"))?;
    let approved_plan_before_review = workflow_state.approved_tactical_plan.clone();
    maybe_record_stopout_and_cleanup(
        &mut workflow_state,
        approved_plan_before_review.as_ref(),
        &symbol,
        &trading_state,
        latest_price,
        &mut entry_snapshots,
        &state_dir,
    )?;
    latest_price = latest_closed_1m_price(&input.indicators)
        .or_else(|| {
            indicator_summary
                .auction_context
                .recent_15m_bars
                .last()
                .map(|bar| bar.close)
        })
        .ok_or_else(|| anyhow!("missing latest workflow price reference"))?;
    let hard_invalidation_hit =
        crate::workflow::stage2::failure_level_breached(&stage1_output, latest_price)?;

    let mut management_signal_report: Option<ManagementExecutionReport> = None;
    let mut management_signal_action: Option<crate::workflow::schema::ManagementAction> = None;
    let mut management_signal_model_name: Option<String> = None;
    let mut execution_signal_report: Option<ExecutionReport> = None;
    let mut execution_signal_intent: Option<crate::workflow::schema::ExecutionIntent> = None;
    let mut execution_signal_model_name: Option<String> = None;
    let mut pending_order_execution_consumed = false;

    let mut stage2a_output: Option<crate::workflow::schema::Stage2AOutput> = None;
    let mut selected_stage2a_model_name: Option<String> = None;
    let mut selected_stage2b_model_names: HashMap<String, String> = HashMap::new();
    let mut selected_stage2c_model_names: HashMap<String, String> = HashMap::new();
    if hard_invalidation_hit {
        let had_approved_workflow_plans = workflow_state.approved_tactical_plan.is_some()
            || !workflow_state.approved_position_management_plans.is_empty()
            || !workflow_state
                .approved_pending_order_management_plans
                .is_empty();
        clear_approved_tactical_plan(&mut workflow_state);
        clear_position_management_plans(&mut workflow_state);
        clear_pending_order_management_plans(&mut workflow_state);
        workflow_state.pending_stage1_refresh_reason = Some("hard_invalidation".to_string());
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        if had_approved_workflow_plans {
            append_workflow_journal_event(
                "workflow_tactical_plan_cleared",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "path_id": stage1_output.current_path.as_ref().map(|path| path.id.clone()),
                    "reason": "hard_invalidation",
                }),
            );
        }
        append_workflow_journal_event(
            "workflow_hard_invalidation_detected",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "path_id": stage1_output.current_path.as_ref().map(|path| path.id.clone()),
                "latest_price": latest_price,
                "failure_level_breached": true,
                "action": "request_stage1_rebuild",
            }),
        );

        let mut stage1_output_slot = Some(stage1_output);
        let hard_invalidation_attempt = maybe_refresh_stage1(
            &config,
            &http_client,
            &loopback_http_client,
            print_response,
            &bundle,
            trigger.as_ref(),
            &symbol,
            &state_dir,
            retention_minutes,
            &input,
            &mut workflow_state,
            &mut stage1_output_slot,
            &mut tracked_zones,
            Some("hard_invalidation".to_string()),
        )
        .await?;
        stage1_refreshed_this_bundle |= hard_invalidation_attempt.refreshed;
        stage1_output = stage1_output_slot
            .ok_or_else(|| anyhow!("workflow stage1 output missing after hard invalidation"))?;
        if !hard_invalidation_attempt.refreshed {
            append_workflow_journal_event(
                "workflow_stage1_waiting",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "reason": "hard_invalidation_stage1_refresh_pending",
                }),
            );
            return Ok(());
        }
        latest_price = latest_closed_1m_price(&input.indicators)
            .or_else(|| {
                indicator_summary
                    .auction_context
                    .recent_15m_bars
                    .last()
                    .map(|bar| bar.close)
            })
            .ok_or_else(|| anyhow!("missing latest workflow price reference"))?;
    }

    let stage1_refresh_blocking = (stage1_refresh_reason.is_some()
        && !stage1_refreshed_this_bundle)
        || workflow_stage_inflight(&symbol, WorkflowStageKind::Stage1);
    let stage2_review_due = workflow_stage2_review_due(
        &config,
        &bundle,
        Some(&stage1_output),
        stage1_refresh_blocking,
    );

    if stage2_review_due {
        if let Some(_stage2_guard) = try_acquire_workflow_stage(&symbol, WorkflowStageKind::Stage2)
        {
            if config.llm.request_enabled {
                let current_path_id = stage1_output
                    .current_path
                    .as_ref()
                    .map(|path| path.id.clone())
                    .unwrap_or_default();
                let path_side = stage1_output
                    .current_path
                    .as_ref()
                    .map(|path| path.side.clone())
                    .unwrap_or_default();
                let active_position_count =
                    active_position_count_for_side(&trading_state, &path_side);
                let live_entry_order_count =
                    live_entry_order_count_for_side(&trading_state, &path_side);
                let quality_allows_new_entry = stage1_quality_allows_new_entry(
                    &stage1_output,
                    &config
                        .llm
                        .workflow
                        .stage1
                        .min_overall_quality_for_new_entry_dispatch,
                );
                let dispatch_flags = workflow_stage2_dispatch_flags(
                    active_position_count,
                    live_entry_order_count,
                    quality_allows_new_entry,
                    &config.llm.workflow.limits,
                );
                let stage2b_contexts =
                    crate::workflow::stage2_input::stage2b_active_positions_for_current_path(
                        &stage1_output,
                        &trading_state,
                        &entry_snapshots,
                    );
                let stage2c_contexts =
                    crate::workflow::stage2_input::stage2c_active_orders_for_current_path(
                        &stage1_output,
                        &trading_state,
                        &entry_snapshots,
                    );
                let should_run_stage2a = dispatch_flags.should_run_stage2a;
                let should_run_stage2b =
                    dispatch_flags.should_run_stage2b && !stage2b_contexts.is_empty();
                let should_run_stage2c =
                    dispatch_flags.should_run_stage2c && !stage2c_contexts.is_empty();
                let stage2c_exposure_state = stage2c_exposure_state_for_counts(
                    active_position_count,
                    live_entry_order_count,
                );

                if should_run_stage2a {
                    let prompt_input = crate::workflow::stage2_input::build_stage2a_prompt_input(
                        &input,
                        &indicator_summary,
                        &stage1_output,
                        &trading_state,
                    );
                    let prompt_value = serde_json::to_value(&prompt_input)
                        .context("serialize workflow stage2a prompt input")?;
                    if config.llm.workflow.persist_prompt_inputs {
                        let _ = persist_workflow_prompt_input_to_disk(
                            &bundle.raw,
                            "workflow_stage2a",
                            &prompt_value,
                            retention_minutes,
                        )
                        .await?;
                    }
                    info!(
                        symbol = %symbol,
                        ts_bucket = %bundle.raw.ts_bucket,
                        trigger = &*trigger,
                        path_id = %current_path_id,
                        "invoking workflow stage2a models"
                    );
                    for out in crate::llm::workflow_provider::invoke_stage2a_models(
                        &http_client,
                        &loopback_http_client,
                        &config,
                        &prompt_value,
                        &symbol,
                    )
                    .await
                    {
                        let payload = json!({
                            "trigger": &*trigger,
                            "stage": "stage2a",
                            "model_name": out.model_name,
                            "provider": out.provider,
                            "model_id": out.model,
                            "latency_ms": out.latency_ms,
                            "raw_response_text": out.raw_response_text,
                            "parsed_value": out.parsed_value,
                            "error": out.error,
                        });
                        append_workflow_journal_event(
                            "workflow_stage2a_response",
                            &symbol,
                            bundle.raw.ts_bucket,
                            payload.clone(),
                        );
                        if print_response {
                            println!(
                                "WORKFLOW_STAGE2A_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                                bundle.raw.ts_bucket,
                                &*trigger,
                                symbol,
                                render_pretty_json_value(&payload)
                            );
                        }
                        if stage2a_output.is_some() {
                            continue;
                        }
                        let Some(value) = payload.get("parsed_value").cloned() else {
                            continue;
                        };
                        match crate::workflow::parser::parse_stage2a_output(value, &stage1_output) {
                            Ok(parsed) => {
                                selected_stage2a_model_name = Some(out.model_name.clone());
                                stage2a_output = Some(parsed);
                            }
                            Err(err) => {
                                append_workflow_journal_event(
                                    "workflow_stage2a_parse_error",
                                    &symbol,
                                    bundle.raw.ts_bucket,
                                    json!({
                                        "trigger": &*trigger,
                                        "error": format!("{err:#}"),
                                    }),
                                );
                            }
                        }
                    }

                    let parsed_stage2a = stage2a_output
                        .clone()
                        .ok_or_else(|| anyhow!("workflow stage2a produced no valid output"))?;
                    match parsed_stage2a.stage2_decision.as_str() {
                        "REQUEST_STAGE1_REEVALUATION" => {
                            workflow_state.pending_stage1_refresh_reason =
                                Some("thesis_invalidated".to_string());
                            clear_approved_tactical_plan(&mut workflow_state);
                            crate::workflow::persistence::save_workflow_state(
                                &state_dir,
                                &workflow_state,
                            )?;
                            append_workflow_journal_event(
                                "workflow_stage1_reevaluation_requested",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "reevaluation_reason": parsed_stage2a.reevaluation_reason,
                                    "path_id": stage1_output.current_path.as_ref().map(|path| path.id.clone()),
                                }),
                            );
                        }
                        "PATH_CONFIRMED" => {
                            workflow_state.approved_tactical_plan =
                                parsed_stage2a.tactical_entry_plan.clone();
                            workflow_state.approved_tactical_plan_updated_at = Some(Utc::now());
                            workflow_state.pending_stage1_refresh_reason = None;
                            crate::workflow::persistence::save_workflow_state(
                                &state_dir,
                                &workflow_state,
                            )?;
                            append_workflow_journal_event(
                                "workflow_tactical_plan_approved",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "tactical_entry_plan": parsed_stage2a.tactical_entry_plan,
                                }),
                            );
                        }
                        other => return Err(anyhow!("unsupported stage2a decision {}", other)),
                    }
                } else if active_position_count == 0 && live_entry_order_count == 0 {
                    clear_approved_tactical_plan(&mut workflow_state);
                    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
                    append_workflow_journal_event(
                        "workflow_stage2a_skipped",
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "quality_allows_new_entry": stage1_quality_allows_new_entry(
                                &stage1_output,
                                &config.llm.workflow.stage1.min_overall_quality_for_new_entry_dispatch,
                            ),
                            "active_position_count": active_position_count,
                            "live_entry_order_count": live_entry_order_count,
                        }),
                    );
                }

                if should_run_stage2b {
                    let mut next_position_plans = BTreeMap::new();
                    for active_position in &stage2b_contexts {
                        let previous_management_plan = position_management_plan_for_context(
                            workflow_state
                                .approved_position_management_plans
                                .get(&active_position.context_key)
                                .cloned(),
                            &current_path_id,
                            &active_position.context_key,
                        );
                        let prompt_input =
                            crate::workflow::stage2_input::build_stage2b_prompt_input(
                                &input,
                                &indicator_summary,
                                &stage1_output,
                                active_position.clone(),
                                previous_management_plan.clone(),
                                &trading_state,
                            );
                        let prompt_value = serde_json::to_value(&prompt_input)
                            .context("serialize workflow stage2b prompt input")?;
                        if config.llm.workflow.persist_prompt_inputs {
                            let _ = persist_workflow_prompt_input_to_disk(
                                &bundle.raw,
                                "workflow_stage2b",
                                &prompt_value,
                                retention_minutes,
                            )
                            .await?;
                        }
                        info!(
                            symbol = %symbol,
                            ts_bucket = %bundle.raw.ts_bucket,
                            trigger = &*trigger,
                            path_id = %current_path_id,
                            context_key = %active_position.context_key,
                            exposure_state = %prompt_input.exposure_state,
                            had_previous_management_plan = previous_management_plan.is_some(),
                            "invoking workflow stage2b models"
                        );
                        let mut stage2b_output_for_context: Option<
                            crate::workflow::schema::Stage2BOutput,
                        > = None;
                        for out in crate::llm::workflow_provider::invoke_stage2b_models(
                            &http_client,
                            &loopback_http_client,
                            &config,
                            &prompt_value,
                            &symbol,
                        )
                        .await
                        {
                            let payload = json!({
                                "trigger": &*trigger,
                                "stage": "stage2b",
                                "context_key": active_position.context_key,
                                "model_name": out.model_name,
                                "provider": out.provider,
                                "model_id": out.model,
                                "latency_ms": out.latency_ms,
                                "raw_response_text": out.raw_response_text,
                                "parsed_value": out.parsed_value,
                                "error": out.error,
                            });
                            append_workflow_journal_event(
                                "workflow_stage2b_response",
                                &symbol,
                                bundle.raw.ts_bucket,
                                payload.clone(),
                            );
                            if print_response {
                                println!(
                                    "WORKFLOW_STAGE2B_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                                    bundle.raw.ts_bucket,
                                    &*trigger,
                                    symbol,
                                    render_pretty_json_value(&payload)
                                );
                            }
                            if stage2b_output_for_context.is_some() {
                                continue;
                            }
                            let Some(value) = payload.get("parsed_value").cloned() else {
                                continue;
                            };
                            match crate::workflow::parser::parse_stage2b_output(
                                value,
                                &stage1_output,
                                &active_position.context_key,
                            ) {
                                Ok(parsed) => {
                                    selected_stage2b_model_names.insert(
                                        active_position.context_key.clone(),
                                        out.model_name.clone(),
                                    );
                                    stage2b_output_for_context = Some(parsed);
                                }
                                Err(err) => {
                                    append_workflow_journal_event(
                                        "workflow_stage2b_parse_error",
                                        &symbol,
                                        bundle.raw.ts_bucket,
                                        json!({
                                            "trigger": &*trigger,
                                            "context_key": active_position.context_key,
                                            "error": format!("{err:#}"),
                                        }),
                                    );
                                }
                            }
                        }
                        if let Some(parsed) = stage2b_output_for_context {
                            next_position_plans.insert(
                                active_position.context_key.clone(),
                                parsed.position_management_plan,
                            );
                        }
                    }
                    replace_position_management_plans(&mut workflow_state, next_position_plans);
                    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
                } else {
                    clear_position_management_plans(&mut workflow_state);
                }

                if should_run_stage2c {
                    let mut next_pending_order_plans = BTreeMap::new();
                    let expected_stage2c_exposure_state = stage2c_exposure_state
                        .expect("Stage2C exposure state must exist when Stage2C is enabled");
                    for active_order in &stage2c_contexts {
                        let previous_pending_order_management_plan =
                            pending_order_management_plan_for_context(
                                workflow_state
                                    .approved_pending_order_management_plans
                                    .get(&active_order.context_key)
                                    .cloned(),
                                &current_path_id,
                                &active_order.context_key,
                            );
                        let prompt_input =
                            crate::workflow::stage2_input::build_stage2c_prompt_input(
                                &input,
                                &indicator_summary,
                                &stage1_output,
                                expected_stage2c_exposure_state,
                                active_order.clone(),
                                previous_pending_order_management_plan.clone(),
                                &trading_state,
                            );
                        let prompt_value = serde_json::to_value(&prompt_input)
                            .context("serialize workflow stage2c prompt input")?;
                        if config.llm.workflow.persist_prompt_inputs {
                            let _ = persist_workflow_prompt_input_to_disk(
                                &bundle.raw,
                                "workflow_stage2c",
                                &prompt_value,
                                retention_minutes,
                            )
                            .await?;
                        }
                        info!(
                            symbol = %symbol,
                            ts_bucket = %bundle.raw.ts_bucket,
                            trigger = &*trigger,
                            path_id = %current_path_id,
                            context_key = %active_order.context_key,
                            order_id = active_order.order_id,
                            exposure_state = %prompt_input.exposure_state,
                            had_previous_pending_order_management_plan =
                                previous_pending_order_management_plan.is_some(),
                            "invoking workflow stage2c models"
                        );
                        let mut stage2c_output_for_context: Option<
                            crate::workflow::schema::Stage2COutput,
                        > = None;
                        for out in crate::llm::workflow_provider::invoke_stage2c_models(
                            &http_client,
                            &loopback_http_client,
                            &config,
                            &prompt_value,
                            &symbol,
                        )
                        .await
                        {
                            let payload = json!({
                                "trigger": &*trigger,
                                "stage": "stage2c",
                                "context_key": active_order.context_key,
                                "order_id": active_order.order_id,
                                "model_name": out.model_name,
                                "provider": out.provider,
                                "model_id": out.model,
                                "latency_ms": out.latency_ms,
                                "raw_response_text": out.raw_response_text,
                                "parsed_value": out.parsed_value,
                                "error": out.error,
                            });
                            append_workflow_journal_event(
                                "workflow_stage2c_response",
                                &symbol,
                                bundle.raw.ts_bucket,
                                payload.clone(),
                            );
                            if print_response {
                                println!(
                                    "WORKFLOW_STAGE2C_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                                    bundle.raw.ts_bucket,
                                    &*trigger,
                                    symbol,
                                    render_pretty_json_value(&payload)
                                );
                            }
                            if stage2c_output_for_context.is_some() {
                                continue;
                            }
                            let Some(value) = payload.get("parsed_value").cloned() else {
                                continue;
                            };
                            match crate::workflow::parser::parse_stage2c_output(
                                value,
                                &stage1_output,
                                expected_stage2c_exposure_state,
                                &active_order.context_key,
                            ) {
                                Ok(parsed) => {
                                    selected_stage2c_model_names.insert(
                                        active_order.context_key.clone(),
                                        out.model_name.clone(),
                                    );
                                    stage2c_output_for_context = Some(parsed);
                                }
                                Err(err) => {
                                    append_workflow_journal_event(
                                        "workflow_stage2c_parse_error",
                                        &symbol,
                                        bundle.raw.ts_bucket,
                                        json!({
                                            "trigger": &*trigger,
                                            "context_key": active_order.context_key,
                                            "order_id": active_order.order_id,
                                            "error": format!("{err:#}"),
                                        }),
                                    );
                                }
                            }
                        }
                        if let Some(parsed) = stage2c_output_for_context {
                            next_pending_order_plans.insert(
                                active_order.context_key.clone(),
                                parsed.pending_order_management_plan,
                            );
                        }
                    }
                    replace_pending_order_management_plans(
                        &mut workflow_state,
                        next_pending_order_plans,
                    );
                    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
                } else {
                    clear_pending_order_management_plans(&mut workflow_state);
                }
                crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
            } else {
                info!(
                    symbol = %symbol,
                    ts_bucket = %bundle.raw.ts_bucket,
                    "workflow stage2 branches skipped because llm.request_enabled=false"
                );
            }
        } else {
            append_workflow_journal_event(
                "workflow_stage2_suppressed",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "reason": "stage2_inflight",
                }),
            );
        }
    }

    let post_invoke_data_age_secs = Utc::now()
        .signed_duration_since(bundle.raw.ts_bucket)
        .num_seconds();
    let max_exec_stale_secs = config.llm.bundle_execution_stale_secs as i64;
    let execution_blocked_due_to_stale =
        config.llm.execution.enabled && post_invoke_data_age_secs > max_exec_stale_secs;
    if execution_blocked_due_to_stale {
        append_workflow_journal_event(
            "workflow_execution_blocked_stale",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "post_invoke_data_age_secs": post_invoke_data_age_secs,
                "max_execution_stale_secs": max_exec_stale_secs,
            }),
        );
    }

    if config.llm.execution.enabled && !execution_blocked_due_to_stale {
        if let Some(current_path) = stage1_output.current_path.as_ref() {
            let watch_price = watcher_reference_price(&input.indicators, latest_price);
            let watch_facts = build_watcher_price_facts(&input.indicators, watch_price);

            let position_plan_context_keys = workflow_state
                .approved_position_management_plans
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for plan_context_key in position_plan_context_keys {
                if execution_signal_intent.is_some() || management_signal_action.is_some() {
                    break;
                }
                let Some(plan) = workflow_state
                    .approved_position_management_plans
                    .get(&plan_context_key)
                    .cloned()
                else {
                    continue;
                };
                if plan.path_id != current_path.id {
                    remove_position_management_plan(&mut workflow_state, &plan_context_key);
                    continue;
                }
                let Some(action_index) =
                    first_triggered_position_management_action_index(&plan, &watch_facts)
                else {
                    continue;
                };
                let action = plan.actions[action_index].clone();
                let snapshot = snapshot_for_management_context(
                    &symbol,
                    current_path,
                    &workflow_state,
                    &entry_snapshots,
                    &action.context_key,
                    &action.path_id,
                );
                match action.action_type.as_str() {
                    "add" => {
                        let fallback_plan =
                            matching_tactical_entry_plan(&workflow_state, &action.path_id);
                        match (
                            entry_plan_from_snapshot_template(&snapshot, fallback_plan),
                            find_active_position_for_side(&trading_state, &snapshot.side),
                        ) {
                            (Ok(entry_template), Some(active_position)) => {
                                let bracket_template =
                                    crate::workflow::schema::PostFillBracketTemplate {
                                        take_profit_1: snapshot.take_profit_1,
                                        take_profit_2: snapshot.take_profit_2,
                                        stop_loss: snapshot.stop_loss,
                                    };
                                let mut intent = execution_intent_from_entry_plan(
                                    &symbol,
                                    &action.path_id,
                                    &entry_template,
                                    current_path,
                                    Some(&bracket_template),
                                    watch_price,
                                    config.llm.workflow.watcher.entry_ttl_minutes,
                                    action
                                        .add_ratio
                                        .map(|ratio| active_position.position_amt.abs() * ratio),
                                );
                                intent.reason = Some(action.reason.clone());
                                match adapt_execution_intent(&intent) {
                                    Ok(adapted_intent) => {
                                        match execute_workflow_execution_intent(
                                            &http_client,
                                            &config.api.binance,
                                            &config.llm.execution,
                                            &symbol,
                                            &adapted_intent,
                                        )
                                        .await
                                        {
                                            Ok(report) => {
                                                append_workflow_journal_event(
                                                    "workflow_stage2b_add_execution_report",
                                                    &symbol,
                                                    bundle.raw.ts_bucket,
                                                    json!({
                                                        "trigger": &*trigger,
                                                        "action": &action,
                                                        "report": {
                                                            "decision": report.decision,
                                                            "quantity": report.quantity,
                                                            "leverage": report.leverage,
                                                            "position_side": report.position_side,
                                                            "maker_entry_price": report.maker_entry_price,
                                                            "take_profit": report.actual_take_profit,
                                                            "stop_loss": report.actual_stop_loss,
                                                            "risk_reward_ratio": report.actual_risk_reward_ratio,
                                                            "dry_run": report.dry_run,
                                                        },
                                                    }),
                                                );
                                                if !report.dry_run {
                                                    let mut next_snapshot = snapshot.clone();
                                                    next_snapshot.updated_at = Utc::now();
                                                    crate::workflow::persistence::save_entry_snapshot(
                                                            &state_dir,
                                                            &next_snapshot,
                                                        )?;
                                                    entry_snapshots.insert(
                                                        next_snapshot.context_key.clone(),
                                                        next_snapshot,
                                                    );
                                                    workflow_state.last_filled_context_key =
                                                        Some(snapshot.context_key.clone());
                                                    if let Some(next_plan) =
                                                        remove_position_management_action(
                                                            &plan,
                                                            action_index,
                                                        )
                                                    {
                                                        upsert_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                            next_plan,
                                                        );
                                                    } else {
                                                        remove_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                        );
                                                    }
                                                }
                                                execution_signal_intent = Some(intent);
                                                execution_signal_report = Some(report);
                                                execution_signal_model_name =
                                                    selected_stage2b_model_names
                                                        .get(&action.context_key)
                                                        .cloned()
                                                        .or_else(|| {
                                                            Some(
                                                                "workflow_stage2b_watcher"
                                                                    .to_string(),
                                                            )
                                                        });
                                            }
                                            Err(err) => {
                                                append_workflow_journal_event(
                                                    "workflow_stage2b_add_execution_error",
                                                    &symbol,
                                                    bundle.raw.ts_bucket,
                                                    json!({
                                                        "trigger": &*trigger,
                                                        "action": &action,
                                                        "error": format!("{err:#}"),
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        append_workflow_journal_event(
                                            "workflow_stage2b_add_execution_error",
                                            &symbol,
                                            bundle.raw.ts_bucket,
                                            json!({
                                                "trigger": &*trigger,
                                                "action": &action,
                                                "error": format!("{err:#}"),
                                                "phase": "intent_adapter",
                                            }),
                                        );
                                    }
                                }
                            }
                            (Err(err), _) => {
                                append_workflow_journal_event(
                                    "workflow_stage2b_add_execution_error",
                                    &symbol,
                                    bundle.raw.ts_bucket,
                                    json!({
                                        "trigger": &*trigger,
                                        "action": &action,
                                        "error": format!("{err:#}"),
                                        "phase": "entry_template",
                                    }),
                                );
                            }
                            (_, None) => {
                                append_workflow_journal_event(
                                    "workflow_stage2b_add_execution_error",
                                    &symbol,
                                    bundle.raw.ts_bucket,
                                    json!({
                                        "trigger": &*trigger,
                                        "action": &action,
                                        "error": "no active position available for add execution",
                                    }),
                                );
                            }
                        }
                    }
                    "reduce" | "exit_full" | "move_stop" | "update_take_profit" => {
                        match management_action_from_position_management_action(&action).and_then(
                            |management_action| {
                                let adapted =
                                    adapt_management_action(&management_action, &snapshot)?;
                                Ok((management_action, adapted))
                            },
                        ) {
                            Ok((management_action, adapted_action)) => {
                                match execute_workflow_management_action(
                                    &http_client,
                                    &config.api.binance,
                                    &config.llm.execution,
                                    &symbol,
                                    &snapshot,
                                    &adapted_action,
                                )
                                .await
                                {
                                    Ok(report) => {
                                        append_workflow_journal_event(
                                            "workflow_stage2b_management_execution_report",
                                            &symbol,
                                            bundle.raw.ts_bucket,
                                            json!({
                                                "trigger": &*trigger,
                                                "action": &action,
                                                "report": {
                                                    "action": report.action,
                                                    "dry_run": report.dry_run,
                                                    "position_count": report.position_count,
                                                    "open_order_count": report.open_order_count,
                                                    "canceled_open_orders": report.canceled_open_orders,
                                                    "reduce_order_ids": report.reduce_order_ids,
                                                    "close_order_ids": report.close_order_ids,
                                                    "modify_take_profit_order_ids": report.modify_take_profit_order_ids,
                                                    "modify_stop_loss_order_ids": report.modify_stop_loss_order_ids,
                                                    "realized_pnl_usdt": report.realized_pnl_usdt,
                                                },
                                            }),
                                        );
                                        if !report.dry_run {
                                            match action.action_type.as_str() {
                                                "exit_full" => {
                                                    entry_snapshots.remove(&snapshot.context_key);
                                                    crate::workflow::persistence::delete_entry_snapshot(
                                                            &state_dir,
                                                            &symbol,
                                                            &snapshot.context_key,
                                                        )?;
                                                    clear_approved_tactical_plan(
                                                        &mut workflow_state,
                                                    );
                                                    remove_position_management_plan(
                                                        &mut workflow_state,
                                                        &plan_context_key,
                                                    );
                                                }
                                                "move_stop" => {
                                                    let next_snapshot = patch_entry_snapshot_levels(
                                                        &snapshot,
                                                        action.new_stop_loss,
                                                        None,
                                                        None,
                                                    );
                                                    crate::workflow::persistence::save_entry_snapshot(
                                                            &state_dir,
                                                            &next_snapshot,
                                                        )?;
                                                    entry_snapshots.insert(
                                                        next_snapshot.context_key.clone(),
                                                        next_snapshot,
                                                    );
                                                    if let Some(next_plan) =
                                                        remove_position_management_action(
                                                            &plan,
                                                            action_index,
                                                        )
                                                    {
                                                        upsert_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                            next_plan,
                                                        );
                                                    } else {
                                                        remove_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                        );
                                                    }
                                                }
                                                "update_take_profit" => {
                                                    let next_snapshot = patch_entry_snapshot_levels(
                                                        &snapshot,
                                                        None,
                                                        action.take_profit_1,
                                                        action.take_profit_2,
                                                    );
                                                    crate::workflow::persistence::save_entry_snapshot(
                                                            &state_dir,
                                                            &next_snapshot,
                                                        )?;
                                                    entry_snapshots.insert(
                                                        next_snapshot.context_key.clone(),
                                                        next_snapshot,
                                                    );
                                                    if let Some(next_plan) =
                                                        remove_position_management_action(
                                                            &plan,
                                                            action_index,
                                                        )
                                                    {
                                                        upsert_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                            next_plan,
                                                        );
                                                    } else {
                                                        remove_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                        );
                                                    }
                                                }
                                                _ => {
                                                    let mut next_snapshot = snapshot.clone();
                                                    next_snapshot.updated_at = Utc::now();
                                                    crate::workflow::persistence::save_entry_snapshot(
                                                            &state_dir,
                                                            &next_snapshot,
                                                        )?;
                                                    entry_snapshots.insert(
                                                        next_snapshot.context_key.clone(),
                                                        next_snapshot,
                                                    );
                                                    if let Some(next_plan) =
                                                        remove_position_management_action(
                                                            &plan,
                                                            action_index,
                                                        )
                                                    {
                                                        upsert_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                            next_plan,
                                                        );
                                                    } else {
                                                        remove_position_management_plan(
                                                            &mut workflow_state,
                                                            &plan_context_key,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        management_signal_action = Some(management_action);
                                        management_signal_report = Some(report);
                                        management_signal_model_name = selected_stage2b_model_names
                                            .get(&action.context_key)
                                            .cloned()
                                            .or_else(|| {
                                                Some("workflow_stage2b_watcher".to_string())
                                            });
                                    }
                                    Err(err) => {
                                        append_workflow_journal_event(
                                            "workflow_stage2b_management_execution_error",
                                            &symbol,
                                            bundle.raw.ts_bucket,
                                            json!({
                                                "trigger": &*trigger,
                                                "action": &action,
                                                "error": format!("{err:#}"),
                                            }),
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                append_workflow_journal_event(
                                    "workflow_stage2b_management_execution_error",
                                    &symbol,
                                    bundle.raw.ts_bucket,
                                    json!({
                                        "trigger": &*trigger,
                                        "action": &action,
                                        "error": format!("{err:#}"),
                                        "phase": "adapter",
                                    }),
                                );
                            }
                        }
                    }
                    other => {
                        append_workflow_journal_event(
                            "workflow_stage2b_management_execution_error",
                            &symbol,
                            bundle.raw.ts_bucket,
                            json!({
                                "trigger": &*trigger,
                                "action": &action,
                                "error": format!("unsupported stage2b watcher action {}", other),
                            }),
                        );
                    }
                }
            }

            if execution_signal_intent.is_none() && management_signal_action.is_none() {
                let pending_plan_context_keys = workflow_state
                    .approved_pending_order_management_plans
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                for plan_context_key in pending_plan_context_keys {
                    if execution_signal_intent.is_some()
                        || management_signal_action.is_some()
                        || pending_order_execution_consumed
                    {
                        break;
                    }
                    let Some(plan) = workflow_state
                        .approved_pending_order_management_plans
                        .get(&plan_context_key)
                        .cloned()
                    else {
                        continue;
                    };
                    if plan.path_id != current_path.id {
                        remove_pending_order_management_plan(
                            &mut workflow_state,
                            &plan_context_key,
                        );
                        continue;
                    }
                    let Some(action_index) =
                        first_triggered_pending_order_action_index(&plan, &watch_facts)
                    else {
                        continue;
                    };
                    let action = plan.actions[action_index].clone();
                    let snapshot = snapshot_for_management_context(
                        &symbol,
                        current_path,
                        &workflow_state,
                        &entry_snapshots,
                        &action.context_key,
                        &action.path_id,
                    );
                    let has_live_position =
                        has_active_position_for_side(&trading_state, &snapshot.side);
                    match action.action_type.as_str() {
                        "cancel_pending_order" => {
                            match cancel_workflow_pending_entry_orders(
                                &http_client,
                                &config.api.binance,
                                &config.llm.execution,
                                &symbol,
                                &snapshot.side,
                            )
                            .await
                            {
                                Ok(canceled_order_ids) => {
                                    pending_order_execution_consumed = true;
                                    append_workflow_journal_event(
                                        "workflow_stage2c_pending_order_execution_report",
                                        &symbol,
                                        bundle.raw.ts_bucket,
                                        json!({
                                            "trigger": &*trigger,
                                            "action": &action,
                                            "canceled_order_ids": canceled_order_ids,
                                            "dry_run": config.llm.execution.dry_run,
                                        }),
                                    );
                                    if !config.llm.execution.dry_run {
                                        if !has_live_position {
                                            entry_snapshots.remove(&snapshot.context_key);
                                            crate::workflow::persistence::delete_entry_snapshot(
                                                &state_dir,
                                                &symbol,
                                                &snapshot.context_key,
                                            )?;
                                            if workflow_state.last_filled_context_key.as_deref()
                                                == Some(snapshot.context_key.as_str())
                                            {
                                                workflow_state.last_filled_context_key = None;
                                            }
                                        }
                                        if workflow_state
                                            .approved_tactical_plan
                                            .as_ref()
                                            .is_some_and(|item| item.path_id == action.path_id)
                                        {
                                            clear_approved_tactical_plan(&mut workflow_state);
                                        }
                                        workflow_state.pending_entry_bracket_template_override =
                                            None;
                                        if let Some(next_plan) =
                                            remove_pending_order_management_action(
                                                &plan,
                                                action_index,
                                            )
                                        {
                                            upsert_pending_order_management_plan(
                                                &mut workflow_state,
                                                &plan_context_key,
                                                next_plan,
                                            );
                                        } else {
                                            remove_pending_order_management_plan(
                                                &mut workflow_state,
                                                &plan_context_key,
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    append_workflow_journal_event(
                                        "workflow_stage2c_pending_order_execution_error",
                                        &symbol,
                                        bundle.raw.ts_bucket,
                                        json!({
                                            "trigger": &*trigger,
                                            "action": &action,
                                            "error": format!("{err:#}"),
                                        }),
                                    );
                                }
                            }
                        }
                        "replace_entry" => {
                            match build_stage2c_replace_execution_intent(
                                &symbol,
                                current_path,
                                &workflow_state,
                                &snapshot,
                                &action,
                                watch_price,
                                config.llm.workflow.watcher.entry_ttl_minutes,
                            ) {
                                Ok((next_tactical_plan, bracket_template, intent)) => {
                                    match adapt_execution_intent(&intent) {
                                        Ok(adapted_intent) => {
                                            match cancel_workflow_pending_entry_orders(
                                                &http_client,
                                                &config.api.binance,
                                                &config.llm.execution,
                                                &symbol,
                                                &snapshot.side,
                                            )
                                            .await
                                            {
                                                Ok(canceled_order_ids) => {
                                                    pending_order_execution_consumed = true;
                                                    append_workflow_journal_event(
                                                        "workflow_stage2c_pending_order_execution_report",
                                                        &symbol,
                                                        bundle.raw.ts_bucket,
                                                        json!({
                                                            "trigger": &*trigger,
                                                            "action": &action,
                                                            "canceled_order_ids": canceled_order_ids,
                                                            "dry_run": config.llm.execution.dry_run,
                                                        }),
                                                    );
                                                    match execute_workflow_execution_intent(
                                                        &http_client,
                                                        &config.api.binance,
                                                        &config.llm.execution,
                                                        &symbol,
                                                        &adapted_intent,
                                                    )
                                                    .await
                                                    {
                                                        Ok(report) => {
                                                            append_workflow_journal_event(
                                                                "workflow_stage2c_replace_execution_report",
                                                                &symbol,
                                                                bundle.raw.ts_bucket,
                                                                json!({
                                                                    "trigger": &*trigger,
                                                                    "action": &action,
                                                                    "report": {
                                                                        "decision": report.decision,
                                                                        "quantity": report.quantity,
                                                                        "leverage": report.leverage,
                                                                        "position_side": report.position_side,
                                                                        "maker_entry_price": report.maker_entry_price,
                                                                        "take_profit": report.actual_take_profit,
                                                                        "stop_loss": report.actual_stop_loss,
                                                                        "risk_reward_ratio": report.actual_risk_reward_ratio,
                                                                        "dry_run": report.dry_run,
                                                                    },
                                                                }),
                                                            );
                                                            if !report.dry_run {
                                                                workflow_state
                                                                    .approved_tactical_plan =
                                                                    Some(next_tactical_plan);
                                                                workflow_state
                                                                    .approved_tactical_plan_updated_at =
                                                                    Some(Utc::now());
                                                                workflow_state
                                                                    .pending_entry_bracket_template_override =
                                                                    Some(bracket_template);
                                                                let next_snapshot =
                                                                    crate::workflow::management::snapshot_from_execution_intent(
                                                                        &symbol,
                                                                        &intent,
                                                                        current_path,
                                                                        Utc::now(),
                                                                    );
                                                                crate::workflow::persistence::save_entry_snapshot(
                                                                    &state_dir,
                                                                    &next_snapshot,
                                                                )?;
                                                                entry_snapshots.insert(
                                                                    next_snapshot
                                                                        .context_key
                                                                        .clone(),
                                                                    next_snapshot,
                                                                );
                                                                workflow_state
                                                                    .last_filled_context_key = Some(
                                                                    intent
                                                                        .entry_snapshot
                                                                        .context_key
                                                                        .clone(),
                                                                );
                                                                if let Some(next_plan) =
                                                                    remove_pending_order_management_action(
                                                                        &plan,
                                                                        action_index,
                                                                    )
                                                                {
                                                                    upsert_pending_order_management_plan(
                                                                        &mut workflow_state,
                                                                        &plan_context_key,
                                                                        next_plan,
                                                                    );
                                                                } else {
                                                                    remove_pending_order_management_plan(
                                                                        &mut workflow_state,
                                                                        &plan_context_key,
                                                                    );
                                                                }
                                                            }
                                                            execution_signal_intent = Some(intent);
                                                            execution_signal_report = Some(report);
                                                            execution_signal_model_name =
                                                                selected_stage2c_model_names
                                                                    .get(&action.context_key)
                                                                    .cloned()
                                                                    .or_else(|| {
                                                                        Some(
                                                                            "workflow_stage2c_watcher"
                                                                                .to_string(),
                                                                        )
                                                                    });
                                                        }
                                                        Err(err) => {
                                                            append_workflow_journal_event(
                                                                "workflow_stage2c_pending_order_execution_error",
                                                                &symbol,
                                                                bundle.raw.ts_bucket,
                                                                json!({
                                                                    "trigger": &*trigger,
                                                                    "action": &action,
                                                                    "error": format!("{err:#}"),
                                                                    "phase": "replace_execution",
                                                                }),
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(err) => {
                                                    append_workflow_journal_event(
                                                        "workflow_stage2c_pending_order_execution_error",
                                                        &symbol,
                                                        bundle.raw.ts_bucket,
                                                        json!({
                                                            "trigger": &*trigger,
                                                            "action": &action,
                                                            "error": format!("{err:#}"),
                                                        }),
                                                    );
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            append_workflow_journal_event(
                                                "workflow_stage2c_pending_order_execution_error",
                                                &symbol,
                                                bundle.raw.ts_bucket,
                                                json!({
                                                    "trigger": &*trigger,
                                                    "action": &action,
                                                    "error": format!("{err:#}"),
                                                    "phase": "intent_adapter",
                                                }),
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    append_workflow_journal_event(
                                        "workflow_stage2c_pending_order_execution_error",
                                        &symbol,
                                        bundle.raw.ts_bucket,
                                        json!({
                                            "trigger": &*trigger,
                                            "action": &action,
                                            "error": format!("{err:#}"),
                                            "phase": "replace_template",
                                        }),
                                    );
                                }
                            }
                        }
                        "update_post_fill_bracket_template" => {
                            pending_order_execution_consumed = true;
                            append_workflow_journal_event(
                                "workflow_stage2c_pending_order_execution_report",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "action": &action,
                                    "dry_run": config.llm.execution.dry_run,
                                }),
                            );
                            if !config.llm.execution.dry_run {
                                if let Some(template) = action.post_fill_bracket_template.clone() {
                                    workflow_state.pending_entry_bracket_template_override =
                                        Some(template.clone());
                                    if !has_live_position {
                                        let next_snapshot = patch_entry_snapshot_levels(
                                            &snapshot,
                                            Some(template.stop_loss),
                                            Some(template.take_profit_1),
                                            Some(template.take_profit_2),
                                        );
                                        crate::workflow::persistence::save_entry_snapshot(
                                            &state_dir,
                                            &next_snapshot,
                                        )?;
                                        entry_snapshots.insert(
                                            next_snapshot.context_key.clone(),
                                            next_snapshot,
                                        );
                                    }
                                }
                                if let Some(next_plan) =
                                    remove_pending_order_management_action(&plan, action_index)
                                {
                                    upsert_pending_order_management_plan(
                                        &mut workflow_state,
                                        &plan_context_key,
                                        next_plan,
                                    );
                                } else {
                                    remove_pending_order_management_plan(
                                        &mut workflow_state,
                                        &plan_context_key,
                                    );
                                }
                            }
                        }
                        other => {
                            append_workflow_journal_event(
                                "workflow_stage2c_pending_order_execution_error",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "action": &action,
                                    "error": format!("unsupported stage2c watcher action {}", other),
                                }),
                            );
                        }
                    }
                }
            }
        }
    }

    let mut trade_signals = Vec::new();
    let stage2a_signal_model_name = selected_stage2a_model_name
        .clone()
        .unwrap_or_else(|| "workflow_stage2a".to_string());

    if let Some(output) = stage2a_output.as_ref() {
        if output.stage2_decision == "REQUEST_STAGE1_REEVALUATION" {
            trade_signals.push(build_stage2_reevaluation_trade_signal(
                bundle.raw.ts_bucket,
                trigger.as_ref(),
                &symbol,
                &stage2a_signal_model_name,
                output
                    .reevaluation_reason
                    .as_deref()
                    .unwrap_or("path_invalidated"),
            ));
        }
    }
    if let Some(tactical_plan) = workflow_state.approved_tactical_plan.as_ref() {
        if management_signal_action.is_none()
            && !pending_order_execution_consumed
            && execution_signal_intent.is_none()
            && config.llm.execution.enabled
            && !execution_blocked_due_to_stale
        {
            if let Some(entry_plan) = select_entry_plan(
                &symbol,
                tactical_plan,
                &workflow_state,
                latest_price,
                hard_invalidation_hit,
                &trading_state,
                &entry_snapshots,
                &input.indicators,
                &config.llm.workflow.watcher,
            ) {
                let current_path = stage1_output.current_path.as_ref().ok_or_else(|| {
                    anyhow!("workflow stage1 current_path missing during entry execution")
                })?;
                let intent = execution_intent_from_entry_plan(
                    &symbol,
                    &tactical_plan.path_id,
                    entry_plan.plan,
                    current_path,
                    workflow_state
                        .pending_entry_bracket_template_override
                        .as_ref(),
                    entry_plan.trigger_price,
                    config.llm.workflow.watcher.entry_ttl_minutes,
                    None,
                );
                match adapt_execution_intent(&intent) {
                    Ok(adapted_intent) => match execute_workflow_execution_intent(
                        &http_client,
                        &config.api.binance,
                        &config.llm.execution,
                        &symbol,
                        &adapted_intent,
                    )
                    .await
                    {
                        Ok(report) => {
                            append_workflow_journal_event(
                                "workflow_execution_report",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "path_id": intent.path_id,
                                    "context_key": intent.entry_snapshot.context_key,
                                    "report": {
                                        "decision": report.decision,
                                        "quantity": report.quantity,
                                        "leverage": report.leverage,
                                        "position_side": report.position_side,
                                        "maker_entry_price": report.maker_entry_price,
                                        "take_profit": report.actual_take_profit,
                                        "stop_loss": report.actual_stop_loss,
                                        "risk_reward_ratio": report.actual_risk_reward_ratio,
                                        "dry_run": report.dry_run,
                                    }
                                }),
                            );
                            if !report.dry_run {
                                let snapshot = crate::workflow::management::snapshot_from_execution_intent(
                                    &symbol,
                                    &intent,
                                    stage1_output
                                        .current_path
                                        .as_ref()
                                        .ok_or_else(|| anyhow!("workflow stage1 current_path missing during snapshot persistence"))?,
                                    Utc::now(),
                                );
                                crate::workflow::persistence::save_entry_snapshot(
                                    &state_dir, &snapshot,
                                )?;
                                entry_snapshots.insert(snapshot.context_key.clone(), snapshot);
                                workflow_state.last_filled_context_key =
                                    Some(intent.entry_snapshot.context_key.clone());
                                crate::workflow::persistence::save_workflow_state(
                                    &state_dir,
                                    &workflow_state,
                                )?;
                            }
                            execution_signal_intent = Some(intent);
                            execution_signal_report = Some(report);
                            execution_signal_model_name = selected_stage2a_model_name
                                .clone()
                                .or_else(|| Some("workflow_stage2a_watcher".to_string()));
                        }
                        Err(err) => {
                            let blocked = err
                                .downcast_ref::<TradeExecutionBlockedByCurrentPriceBeyondStopLoss>()
                                .map(|item| {
                                    json!({
                                        "decision": item.decision.as_str(),
                                        "current_reference_price": item.current_reference_price,
                                        "current_price_source": item.current_price_source,
                                        "entry_price": item.entry_price,
                                        "stop_loss": item.stop_loss,
                                        "best_bid_price": item.best_bid_price,
                                        "best_ask_price": item.best_ask_price,
                                    })
                                });
                            append_workflow_journal_event(
                                "workflow_execution_error",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "path_id": intent.path_id,
                                    "context_key": intent.entry_snapshot.context_key,
                                    "error": format!("{err:#}"),
                                    "blocked": blocked,
                                }),
                            );
                        }
                    },
                    Err(err) => {
                        append_workflow_journal_event(
                            "workflow_execution_error",
                            &symbol,
                            bundle.raw.ts_bucket,
                            json!({
                                "trigger": &*trigger,
                                "path_id": intent.path_id,
                                "context_key": intent.entry_snapshot.context_key,
                                "error": format!("{err:#}"),
                                "phase": "intent_adapter",
                            }),
                        );
                    }
                }
            } else {
                append_workflow_journal_event(
                    "workflow_execution_skipped",
                    &symbol,
                    bundle.raw.ts_bucket,
                    json!({
                        "trigger": &*trigger,
                        "execution_enabled": config.llm.execution.enabled,
                        "execution_blocked_due_to_stale": execution_blocked_due_to_stale,
                        "filled_stopout_attempts": workflow_state.filled_stopout_attempts,
                        "path_id": tactical_plan.path_id,
                    }),
                );
            }
        }
    }

    if let (Some(intent), Some(report)) = (
        execution_signal_intent.as_ref(),
        execution_signal_report.as_ref(),
    ) {
        trade_signals.push(build_execution_trade_signal(
            bundle.raw.ts_bucket,
            trigger.as_ref(),
            &symbol,
            execution_signal_model_name
                .as_deref()
                .unwrap_or(stage2a_signal_model_name.as_str()),
            &trading_state,
            intent,
            Some(report),
            intent.reason.as_deref(),
        ));
    }

    if let (Some(action), Some(report)) = (
        management_signal_action.as_ref(),
        management_signal_report.as_ref(),
    ) {
        trade_signals.push(build_management_trade_signal(
            bundle.raw.ts_bucket,
            trigger.as_ref(),
            &symbol,
            management_signal_model_name
                .as_deref()
                .unwrap_or("workflow_stage2b_watcher"),
            action,
            report,
        ));
    }

    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;

    let telegram_operator = TelegramOperator::from_config(&config.api.telegram);
    let x_operator = XOperator::from_config(&config.api.x);
    for signal in &trade_signals {
        send_trade_signal_notifications(
            telegram_operator.as_ref(),
            x_operator.as_ref(),
            &config.llm.telegram_signal_decisions,
            &config.llm.x_signal_decisions,
            &http_client,
            signal,
        )
        .await;
    }

    Ok(())
}

fn build_stage2_reevaluation_trade_signal(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    reason: &str,
) -> TradeSignalNotification {
    TradeSignalNotification {
        ts_bucket,
        trigger: trigger.to_string(),
        symbol: symbol.to_string(),
        model_name: model_name.to_string(),
        decision: "NO_TRADE".to_string(),
        context_key: None,
        path_id: None,
        entry_price: None,
        leverage: None,
        risk_reward_ratio: None,
        take_profit_1: None,
        take_profit_2: None,
        stop_loss: None,
        reason: reason.to_string(),
    }
}

fn build_execution_trade_signal(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    trading_state: &TradingStateSnapshot,
    intent: &crate::workflow::schema::ExecutionIntent,
    execution_report: Option<&ExecutionReport>,
    secondary_reason: Option<&str>,
) -> TradeSignalNotification {
    let decision = if has_active_position_for_side(trading_state, &intent.side) {
        "ADD".to_string()
    } else {
        intent.side.clone()
    };
    let entry_price = execution_report
        .map(|report| report.maker_entry_price)
        .or(intent.trigger_price)
        .or(Some(intent.entry_zone.midpoint()));
    let take_profit_1 = execution_report
        .map(|report| report.actual_take_profit)
        .or(Some(intent.take_profit_1));
    let take_profit_2 = Some(intent.take_profit_2);
    let stop_loss = execution_report
        .map(|report| report.actual_stop_loss)
        .or(Some(intent.stop_loss));
    let leverage = execution_report.map(|report| report.leverage as f64);
    let risk_reward_ratio = execution_report
        .map(|report| report.actual_risk_reward_ratio)
        .or_else(|| compute_signal_rr(entry_price, stop_loss, take_profit_1));

    TradeSignalNotification {
        ts_bucket,
        trigger: trigger.to_string(),
        symbol: symbol.to_string(),
        model_name: model_name.to_string(),
        decision,
        context_key: Some(intent.entry_snapshot.context_key.clone()),
        path_id: Some(intent.path_id.clone()),
        entry_price,
        leverage,
        risk_reward_ratio,
        take_profit_1,
        take_profit_2,
        stop_loss,
        reason: workflow_signal_reason(
            secondary_reason.unwrap_or("workflow_execution"),
            intent.reason.as_deref(),
        ),
    }
}

fn build_management_trade_signal(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    action: &crate::workflow::schema::ManagementAction,
    report: &ManagementExecutionReport,
) -> TradeSignalNotification {
    let decision = match action.action_type.as_str() {
        "REDUCE_POSITION" => "REDUCE",
        "FLATTEN_POSITION" => "CLOSE",
        "MOVE_STOP" | "UPDATE_TAKE_PROFIT" => "MODIFY_TPSL",
        _ => "HOLD",
    };

    TradeSignalNotification {
        ts_bucket,
        trigger: trigger.to_string(),
        symbol: symbol.to_string(),
        model_name: model_name.to_string(),
        decision: decision.to_string(),
        context_key: Some(action.context_key.clone()),
        path_id: Some(action.path_id.clone()),
        entry_price: None,
        leverage: None,
        risk_reward_ratio: None,
        take_profit_1: action.take_profit_1,
        take_profit_2: action.take_profit_2,
        stop_loss: action.new_stop_loss,
        reason: workflow_signal_reason(
            action.reason.as_deref().unwrap_or("workflow_management"),
            Some(report.action),
        ),
    }
}

fn workflow_signal_reason(primary: &str, secondary: Option<&str>) -> String {
    let primary = primary.trim();
    let secondary = secondary.unwrap_or("").trim();
    if secondary.is_empty() {
        return primary.to_string();
    }
    if primary.eq_ignore_ascii_case(secondary) {
        return primary.to_string();
    }
    format!("{primary} | {secondary}")
}

fn compute_signal_rr(
    entry_price: Option<f64>,
    stop_loss: Option<f64>,
    take_profit_1: Option<f64>,
) -> Option<f64> {
    let entry_price = entry_price?;
    let stop_loss = stop_loss?;
    let take_profit_1 = take_profit_1?;
    let risk = (entry_price - stop_loss).abs();
    let reward = (take_profit_1 - entry_price).abs();
    if risk <= f64::EPSILON {
        None
    } else {
        Some(reward / risk)
    }
}

fn find_entry_snapshot_for_side<'a>(
    entry_snapshots: &'a HashMap<String, crate::workflow::schema::EntrySnapshot>,
    symbol: &str,
    side: &str,
) -> Option<&'a crate::workflow::schema::EntrySnapshot> {
    entry_snapshots.values().find(|snapshot| {
        snapshot.symbol.eq_ignore_ascii_case(symbol) && snapshot.side.eq_ignore_ascii_case(side)
    })
}

fn build_workflow_management_snapshot(
    trading_state: &TradingStateSnapshot,
    symbol: &str,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
) -> Option<ManagementSnapshotForLlm> {
    if !trading_state.has_active_positions && !trading_state.has_open_orders {
        return None;
    }

    let positions = trading_state
        .active_positions
        .iter()
        .map(|position| {
            let side = active_position_side(position).unwrap_or("LONG");
            let snapshot = find_entry_snapshot_for_side(entry_snapshots, symbol, side);
            PositionSummaryForLlm {
                position_side: position.position_side.clone(),
                direction: side.to_string(),
                quantity: position.position_amt.abs(),
                leverage: position.leverage,
                entry_price: position.entry_price,
                mark_price: position.mark_price,
                unrealized_pnl: position.unrealized_pnl,
                pnl_by_latest_price: position.unrealized_pnl,
                current_tp_price: snapshot.map(|item| item.take_profit_1),
                current_sl_price: snapshot.map(|item| item.stop_loss),
            }
        })
        .collect::<Vec<_>>();

    let position_context = trading_state.active_positions.first().map(|position| {
        let side = active_position_side(position).unwrap_or("LONG");
        let snapshot = find_entry_snapshot_for_side(entry_snapshots, symbol, side);
        PositionContextForLlm {
            original_qty: position.position_amt.abs(),
            current_qty: position.position_amt.abs(),
            current_pct_of_original: 1.0,
            effective_leverage: Some(position.leverage),
            effective_entry_price: Some(position.entry_price),
            effective_take_profit: snapshot.map(|item| item.take_profit_1),
            effective_stop_loss: snapshot.map(|item| item.stop_loss),
            reduction_history: Vec::new(),
            times_reduced_at_current_level: 0,
            last_management_action: None,
            last_management_reason: None,
            entry_context: None,
        }
    });

    Some(ManagementSnapshotForLlm {
        context_state: if trading_state.has_active_positions {
            "active_positions".to_string()
        } else {
            "open_orders".to_string()
        },
        has_active_positions: trading_state.has_active_positions,
        has_open_orders: trading_state.has_open_orders,
        active_position_count: trading_state.active_positions.len(),
        open_order_count: trading_state.open_orders.len(),
        positions,
        pending_order: None,
        last_management_reason: None,
        position_context,
    })
}

fn has_active_position_for_side(trading_state: &TradingStateSnapshot, side: &str) -> bool {
    find_active_position_for_side(trading_state, side).is_some()
}

fn entry_order_side(order: &OpenOrderSnapshot) -> Option<&'static str> {
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

fn live_entry_order_count_for_side(trading_state: &TradingStateSnapshot, side: &str) -> usize {
    trading_state
        .open_orders
        .iter()
        .filter(|order| {
            entry_order_side(order).is_some_and(|value| value.eq_ignore_ascii_case(side))
        })
        .count()
}

fn active_position_count_for_side(trading_state: &TradingStateSnapshot, side: &str) -> usize {
    trading_state
        .active_positions
        .iter()
        .filter(|position| {
            active_position_side(position).is_some_and(|value| value.eq_ignore_ascii_case(side))
        })
        .count()
}

fn quality_rank(value: &str) -> u8 {
    match value.trim().to_ascii_lowercase().as_str() {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn stage1_quality_allows_new_entry(
    stage1_output: &crate::workflow::schema::Stage1Output,
    min_quality: &str,
) -> bool {
    let current = stage1_output
        .opportunity_assessment
        .overall_quality
        .as_deref()
        .unwrap_or("low");
    quality_rank(current) >= quality_rank(min_quality)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkflowStage2DispatchFlags {
    should_run_stage2a: bool,
    should_run_stage2b: bool,
    should_run_stage2c: bool,
}

fn stage2c_exposure_state_for_counts(
    active_position_count: usize,
    live_entry_order_count: usize,
) -> Option<&'static str> {
    if live_entry_order_count == 0 {
        None
    } else if active_position_count > 0 {
        Some("in_position_with_live_entry_orders")
    } else {
        Some("flat_with_live_entry_orders")
    }
}

fn workflow_stage2_dispatch_flags(
    active_position_count: usize,
    live_entry_order_count: usize,
    quality_allows_new_entry: bool,
    limits: &crate::app::config::WorkflowLimitsConfig,
) -> WorkflowStage2DispatchFlags {
    let direction_limit_reached = active_position_count >= limits.max_live_positions_per_direction
        || live_entry_order_count >= limits.max_live_entry_orders_per_direction;
    WorkflowStage2DispatchFlags {
        should_run_stage2a: active_position_count == 0
            && live_entry_order_count == 0
            && quality_allows_new_entry
            && !direction_limit_reached,
        should_run_stage2b: active_position_count > 0,
        should_run_stage2c: live_entry_order_count > 0,
    }
}

fn find_active_position_for_side<'a>(
    trading_state: &'a TradingStateSnapshot,
    side: &str,
) -> Option<&'a ActivePositionSnapshot> {
    trading_state.active_positions.iter().find(|position| {
        active_position_side(position).is_some_and(|value| value.eq_ignore_ascii_case(side))
    })
}

fn active_position_side(position: &ActivePositionSnapshot) -> Option<&'static str> {
    if position.position_side.eq_ignore_ascii_case("LONG") {
        Some("LONG")
    } else if position.position_side.eq_ignore_ascii_case("SHORT") {
        Some("SHORT")
    } else if position.position_amt > 0.0 {
        Some("LONG")
    } else if position.position_amt < 0.0 {
        Some("SHORT")
    } else {
        None
    }
}

async fn send_trade_signal_notifications(
    telegram_operator: Option<&TelegramOperator>,
    x_operator: Option<&XOperator>,
    telegram_allowed_decisions: &[String],
    x_allowed_decisions: &[String],
    http_client: &Client,
    signal: &TradeSignalNotification,
) {
    let telegram = send_telegram_signal(
        telegram_operator,
        telegram_allowed_decisions,
        http_client,
        signal,
    );
    let x = send_x_signal(x_operator, x_allowed_decisions, http_client, signal);
    let _ = tokio::join!(telegram, x);
}

async fn send_telegram_signal(
    telegram_operator: Option<&TelegramOperator>,
    allowed_decisions: &[String],
    http_client: &Client,
    signal: &TradeSignalNotification,
) {
    if !decision_is_signal_allowed(&signal.decision, allowed_decisions) {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            allowed_decisions = ?allowed_decisions,
            "telegram signal skipped: decision is not enabled by llm.telegram_signal_decisions"
        );
        return;
    }
    let Some(operator) = telegram_operator else {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            "telegram signal skipped: telegram not configured"
        );
        return;
    };
    match operator.send_trade_signal(http_client, signal).await {
        Ok(()) => {
            println!(
                "LLM_TELEGRAM_SIGNAL ts_bucket={} trigger={} symbol={} model={} decision={} rr={} tp1={} tp2={} sl={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                signal
                    .risk_reward_ratio
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit_1
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit_2
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .stop_loss
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        Err(err) => {
            warn!(
                symbol = %signal.symbol,
                model_name = %signal.model_name,
                decision = %signal.decision,
                error = %err,
                "send telegram trade signal failed"
            );
            println!(
                "LLM_TELEGRAM_SIGNAL_ERROR ts_bucket={} trigger={} symbol={} model={} decision={} error={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                err.to_string().replace('\n', " "),
            );
        }
    }
}

async fn send_x_signal(
    x_operator: Option<&XOperator>,
    allowed_decisions: &[String],
    http_client: &Client,
    signal: &TradeSignalNotification,
) {
    if !decision_is_signal_allowed(&signal.decision, allowed_decisions) {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            allowed_decisions = ?allowed_decisions,
            "x signal skipped: decision is not enabled by llm.x_signal_decisions"
        );
        return;
    }
    let Some(operator) = x_operator else {
        debug!(
            symbol = %signal.symbol,
            decision = %signal.decision,
            "x signal skipped: x not configured"
        );
        return;
    };
    match operator.send_trade_signal(http_client, signal).await {
        Ok(()) => {
            println!(
                "LLM_X_SIGNAL ts_bucket={} trigger={} symbol={} model={} decision={} rr={} tp1={} tp2={} sl={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                signal
                    .risk_reward_ratio
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit_1
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .take_profit_2
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                signal
                    .stop_loss
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        Err(err) => {
            warn!(
                symbol = %signal.symbol,
                model_name = %signal.model_name,
                decision = %signal.decision,
                error = %err,
                "send x trade signal failed"
            );
            println!(
                "LLM_X_SIGNAL_ERROR ts_bucket={} trigger={} symbol={} model={} decision={} error={}",
                signal.ts_bucket,
                signal.trigger,
                signal.symbol,
                signal.model_name,
                signal.decision,
                err.to_string().replace('\n', " "),
            );
        }
    }
}

fn decision_is_signal_allowed(decision: &str, allowed_decisions: &[String]) -> bool {
    allowed_decisions
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(decision.trim()))
}

async fn ensure_temp_indicator_dir() -> Result<()> {
    fs::create_dir_all(TEMP_INDICATOR_DIR)
        .with_context(|| format!("create {}", TEMP_INDICATOR_DIR))?;
    Ok(())
}

async fn ensure_temp_model_input_dir() -> Result<()> {
    fs::create_dir_all(TEMP_MODEL_INPUT_DIR)
        .with_context(|| format!("create {}", TEMP_MODEL_INPUT_DIR))?;
    Ok(())
}

async fn ensure_temp_model_output_dir() -> Result<()> {
    fs::create_dir_all(TEMP_MODEL_OUTPUT_DIR)
        .with_context(|| format!("create {}", TEMP_MODEL_OUTPUT_DIR))?;
    Ok(())
}

async fn ensure_llm_journal_dir() -> Result<()> {
    fs::create_dir_all(LLM_JOURNAL_DIR).with_context(|| format!("create {}", LLM_JOURNAL_DIR))?;
    Ok(())
}

fn append_journal_event(event: Value) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(LLM_JOURNAL_FILE)
        .with_context(|| format!("open {}", LLM_JOURNAL_FILE))?;
    let mut line = serde_json::to_vec(&event).context("serialize journal event")?;
    line.push(b'\n');
    file.write_all(&line)
        .with_context(|| format!("write {}", LLM_JOURNAL_FILE))?;
    file.flush()
        .with_context(|| format!("flush {}", LLM_JOURNAL_FILE))?;
    Ok(())
}

fn render_pretty_json_value(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn retention_minutes_i64(retention_minutes: u64) -> i64 {
    retention_minutes.min(i64::MAX as u64) as i64
}

fn minute_bundle_path(bundle: &MinuteBundleEnvelope) -> PathBuf {
    let bucket_ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ");
    let symbol = sanitize_filename_component(&bundle.symbol);
    Path::new(TEMP_INDICATOR_DIR).join(format!("{bucket_ts}_{symbol}.json"))
}

fn write_pretty_json_file(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(value).context("serialize pretty json")?;
    fs::write(path, payload).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

async fn persist_bundle_to_disk(
    bundle: &MinuteBundleEnvelope,
    raw: &[u8],
    content_encoding: Option<&str>,
    retention_minutes: u64,
) -> Result<()> {
    ensure_temp_indicator_dir().await?;

    let decoded = decode_minute_bundle_body(raw, content_encoding)?;
    let raw_json: Value =
        serde_json::from_slice(decoded.as_ref()).context("parse minute bundle as json")?;
    write_pretty_json_file(&minute_bundle_path(bundle), &raw_json)
        .context("write raw minute bundle")?;
    let removed = prune_expired_temp_indicator_files(
        Path::new(TEMP_INDICATOR_DIR),
        bundle.ts_bucket,
        retention_minutes_i64(retention_minutes),
    )
    .context("prune expired temp_indicator cache")?;
    if removed > 0 {
        debug!(
            ts_bucket = %bundle.ts_bucket,
            removed,
            retention_minutes = retention_minutes,
            "pruned expired temp_indicator cache"
        );
    }

    Ok(())
}

fn prune_expired_temp_indicator_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    prune_expired_timestamped_json_files(dir, current_ts_bucket, retention_minutes)
}

fn prune_expired_temp_model_output_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    prune_expired_timestamped_json_files(dir, current_ts_bucket, retention_minutes)
}

fn prune_expired_timestamped_json_files(
    dir: &Path,
    current_ts_bucket: DateTime<Utc>,
    retention_minutes: i64,
) -> Result<usize> {
    let cutoff = current_ts_bucket - ChronoDuration::minutes(retention_minutes);
    let mut removed = 0usize;

    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("iterate {}", dir.display()))?;
        let path = entry.path();
        let keep = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.eq_ignore_ascii_case(".gitignore"))
            .unwrap_or(false);
        if keep {
            continue;
        }

        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        if !file_type.is_file() {
            continue;
        }

        let Some(file_ts_bucket) = temp_indicator_ts_bucket_from_path(&path) else {
            continue;
        };
        if file_ts_bucket < cutoff {
            fs::remove_file(&path).with_context(|| format!("remove file {}", path.display()))?;
            removed += 1;
        }
    }

    Ok(removed)
}

fn temp_indicator_ts_bucket_from_path(path: &Path) -> Option<DateTime<Utc>> {
    let file_name = path.file_name()?.to_str()?;
    let ts = file_name.split('_').next()?;
    let naive = NaiveDateTime::parse_from_str(ts, "%Y%m%dT%H%M%SZ").ok()?;
    Some(DateTime::from_naive_utc_and_offset(naive, Utc))
}

fn llm_stage_prompt_output_path(
    bundle: &MinuteBundleEnvelope,
    provider: &str,
    model_name: &str,
    stage: &str,
) -> PathBuf {
    let bucket_ts = bundle.ts_bucket.format("%Y%m%dT%H%M%SZ").to_string();
    let invoke_ts = Utc::now()
        .format("%Y%m%dT%H%M%S%.3fZ")
        .to_string()
        .replace('.', "");
    let symbol = sanitize_filename_component(&bundle.symbol);
    let provider = sanitize_filename_component(provider);
    let model_name = sanitize_filename_component(model_name);
    let stage = sanitize_filename_component(stage);
    Path::new(TEMP_MODEL_OUTPUT_DIR).join(format!(
        "{}_{}_{}_{}_{}_{}_prompt_input_{}.json",
        bucket_ts, symbol, "workflow", provider, model_name, stage, invoke_ts
    ))
}

fn sanitize_filename_component(raw: &str) -> String {
    let cleaned = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::schema::{
        CurrentPath, EntryPlan, EntrySnapshot, PendingOrderManagementAction,
        PendingOrderManagementPlan, PositionManagementAction, PositionManagementPlan,
        PriceTriggerCondition, PriceZone, ReevaluationTrigger, Stage1Meta, Stage1Output,
        TacticalEntryPlan,
    };
    use crate::workflow::state::WorkflowState;
    use chrono::Duration as ChronoDuration;
    use flate2::{write::GzEncoder, Compression};
    use std::collections::HashMap;
    use std::fs;

    fn sample_price_zone(low: f64, high: f64, timeframe: &str) -> PriceZone {
        PriceZone {
            low,
            high,
            timeframe: Some(timeframe.to_string()),
            label: None,
            reason: None,
        }
    }

    fn sample_fast_entry_plan(intent_mode: &str, entry_profile: &str) -> EntryPlan {
        EntryPlan {
            side: "LONG".to_string(),
            entry_profile: entry_profile.to_string(),
            intent_mode: intent_mode.to_string(),
            entry_activation_level: sample_price_zone(100.0, 101.2, "15m"),
            entry_zone: sample_price_zone(100.0, 101.0, "15m"),
            entry_invalidation_level: sample_price_zone(98.5, 99.0, "15m"),
            stop_loss: 98.4,
            max_drift_pct: 0.2,
            entry_note: String::new(),
        }
    }

    fn sample_fast_price_event(ts: &str, price: f64) -> FastPriceEvent {
        FastPriceEvent {
            symbol: "ETHUSDT".to_string(),
            event_ts: DateTime::parse_from_rfc3339(ts)
                .expect("fast ts")
                .with_timezone(&Utc),
            price,
            source: FastPriceSource::MarkPrice,
            routing_key: "md.futures.mark_price.ethusdt".to_string(),
        }
    }

    fn sample_fast_watcher_config() -> crate::app::config::WorkflowWatcherConfig {
        let mut watcher_cfg = crate::app::config::WorkflowWatcherConfig::default();
        watcher_cfg
            .price_predicates
            .price_above_on_close
            .confirm_bars = 2;
        watcher_cfg
            .price_predicates
            .price_above_on_close
            .min_close_bps = 1.0;
        watcher_cfg
            .price_predicates
            .price_below_on_close
            .confirm_bars = 2;
        watcher_cfg
            .price_predicates
            .price_below_on_close
            .min_close_bps = 1.0;
        watcher_cfg
            .price_predicates
            .entry_reclaim_confirmed
            .confirm_bars = 2;
        watcher_cfg.price_predicates.entry_hold_confirmed.hold_bars = 2;
        watcher_cfg
            .price_predicates
            .entry_hold_confirmed
            .retest_tolerance_bps = 10.0;
        watcher_cfg.price_predicates.breakout_confirmed.confirm_bars = 2;
        watcher_cfg
            .price_predicates
            .breakout_confirmed
            .min_break_bps = 3.0;
        watcher_cfg
            .price_predicates
            .pullback_acceptance_confirmed
            .confirm_bars = 2;
        watcher_cfg
            .price_predicates
            .pullback_acceptance_confirmed
            .max_overshoot_bps = 10.0;
        watcher_cfg
            .price_predicates
            .failed_auction_reentry_confirmed
            .probe_lookback_bars = 2;
        watcher_cfg
            .price_predicates
            .failed_auction_reentry_confirmed
            .reaccept_confirm_bars = 2;
        watcher_cfg
    }

    fn sample_stage1_output() -> Stage1Output {
        Stage1Output {
            meta: Stage1Meta {
                stage1_ts: Utc::now(),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: Vec::new(),
            map_summary: crate::workflow::schema::MapSummary {
                location_3d: json!({}),
                location_1d: json!({}),
                location_4h: json!({}),
                price_location_class: "value_edge".to_string(),
                key_levels: json!({}),
            },
            opportunity_assessment: crate::workflow::schema::OpportunityAssessment {
                overall_quality: Some("high".to_string()),
                ..Default::default()
            },
            current_script: Some("continuation".to_string()),
            driver_attribution: None,
            current_path: Some(CurrentPath {
                id: "path_a".to_string(),
                side: "LONG".to_string(),
                thesis: "continuation".to_string(),
                risk_grade: "aligned_trend".to_string(),
                activation_anchor_id: None,
                strategic_activation_level: sample_price_zone(100.0, 101.0, "4h"),
                first_path_target_anchor_id: None,
                first_path_target: sample_price_zone(104.0, 104.0, "4h"),
                next_path_target_anchor_id: None,
                next_path_target: sample_price_zone(107.0, 107.0, "1d"),
                failure_anchor_id: None,
                failure_level: sample_price_zone(98.0, 98.0, "4h"),
                failure_switch: Some("value_return".to_string()),
                setup_type: "A_continuation".to_string(),
                reevaluation_trigger: ReevaluationTrigger::default(),
                tracked_zones: Vec::new(),
            }),
        }
    }

    fn sample_minute_bundle_json() -> Value {
        json!({
            "msg_type": "ind.minute_bundle",
            "routing_key": "bundle.1m.btcusdt",
            "symbol": "BTCUSDT",
            "ts_bucket": "2026-03-28T04:15:00Z",
            "window_code": "1m",
            "indicator_count": 2,
            "published_at": "2026-03-28T04:15:08Z",
            "indicators": {
                "footprint": {
                    "window_code": "1m",
                    "payload": {"levels": [1, 2, 3]}
                }
            }
        })
    }

    fn sample_tactical_plan() -> TacticalEntryPlan {
        TacticalEntryPlan {
            path_id: "path_a".to_string(),
            entry_plan: EntryPlan {
                side: "LONG".to_string(),
                entry_profile: "reclaim_then_hold".to_string(),
                intent_mode: "breakout".to_string(),
                entry_activation_level: sample_price_zone(100.0, 101.0, "15m"),
                entry_zone: sample_price_zone(101.0, 102.0, "15m"),
                entry_invalidation_level: sample_price_zone(98.0, 99.0, "15m"),
                stop_loss: 98.8,
                max_drift_pct: 0.2,
                entry_note: "entry".to_string(),
            },
        }
    }

    #[test]
    fn workflow_stage2_dispatch_flags_block_stage2a_when_same_side_exposure_exists() {
        let limits = crate::app::config::WorkflowLimitsConfig::default();

        let pending_only = workflow_stage2_dispatch_flags(0, 1, true, &limits);
        assert!(!pending_only.should_run_stage2a);
        assert!(!pending_only.should_run_stage2b);
        assert!(pending_only.should_run_stage2c);

        let position_only = workflow_stage2_dispatch_flags(1, 0, true, &limits);
        assert!(!position_only.should_run_stage2a);
        assert!(position_only.should_run_stage2b);
        assert!(!position_only.should_run_stage2c);
    }

    #[test]
    fn workflow_stage2_dispatch_flags_run_stage2b_and_stage2c_for_coexisting_state() {
        let limits = crate::app::config::WorkflowLimitsConfig::default();
        let dispatch = workflow_stage2_dispatch_flags(1, 1, true, &limits);

        assert!(!dispatch.should_run_stage2a);
        assert!(dispatch.should_run_stage2b);
        assert!(dispatch.should_run_stage2c);
        assert_eq!(
            stage2c_exposure_state_for_counts(1, 1),
            Some("in_position_with_live_entry_orders")
        );
    }

    #[test]
    fn stage1_quality_gate_honors_configured_threshold() {
        let mut stage1_output = sample_stage1_output();
        stage1_output.opportunity_assessment.overall_quality = Some("medium".to_string());

        assert!(stage1_quality_allows_new_entry(&stage1_output, "medium"));
        assert!(stage1_quality_allows_new_entry(&stage1_output, "low"));
        assert!(!stage1_quality_allows_new_entry(&stage1_output, "high"));
    }

    fn sample_flat_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: false,
            has_active_positions: false,
            has_open_orders: false,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 0.0,
            available_balance: 0.0,
        }
    }

    #[test]
    fn consume_pending_stage1_refresh_reason_is_one_shot() {
        let mut state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            ..WorkflowState::default()
        };

        assert!(!consume_pending_stage1_refresh_reason(
            &mut state,
            "scheduled_2h"
        ));
        assert_eq!(
            state.pending_stage1_refresh_reason.as_deref(),
            Some("thesis_invalidated")
        );
        assert!(consume_pending_stage1_refresh_reason(
            &mut state,
            "thesis_invalidated"
        ));
        assert!(state.pending_stage1_refresh_reason.is_none());
    }

    #[test]
    fn remove_position_management_action_drops_empty_tail() {
        let plan = PositionManagementPlan {
            path_id: "path_a".to_string(),
            exposure_state: "in_position".to_string(),
            path_live_assessment: "live".to_string(),
            path_assessment_reason: None,
            actions: vec![PositionManagementAction {
                action_type: "add".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                trigger_condition: Some(PriceTriggerCondition {
                    trigger_type: "price_above".to_string(),
                    trigger_price: 101.0,
                }),
                execution_price: None,
                add_ratio: Some(0.25),
                reuse_current_entry_template: Some(true),
                reduce_ratio: None,
                new_stop_loss: None,
                reuse_current_bracket_template: None,
                take_profit_1: None,
                take_profit_2: None,
                reason: "add".to_string(),
            }],
            management_note: "note".to_string(),
        };
        assert!(remove_position_management_action(&plan, 0).is_none());
    }

    #[test]
    fn remove_pending_order_management_action_drops_empty_tail() {
        let plan = PendingOrderManagementPlan {
            path_id: "path_a".to_string(),
            exposure_state: "flat_with_live_entry_orders".to_string(),
            path_live_assessment: "live".to_string(),
            path_assessment_reason: None,
            actions: vec![PendingOrderManagementAction {
                action_type: "replace_entry".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                trigger_condition: Some(PriceTriggerCondition {
                    trigger_type: "price_above".to_string(),
                    trigger_price: 101.0,
                }),
                execution_price: None,
                replacement_entry_zone: Some(sample_price_zone(102.0, 103.0, "15m")),
                replacement_entry_invalidation_level: Some(sample_price_zone(99.0, 99.0, "15m")),
                replacement_stop_loss: Some(98.5),
                reuse_current_entry_template: Some(true),
                post_fill_bracket_template: None,
                reason: "replace".to_string(),
            }],
            management_note: "note".to_string(),
        };
        assert!(remove_pending_order_management_action(&plan, 0).is_none());
    }

    #[test]
    fn first_triggered_position_management_action_matches_immediate_price_rule() {
        let facts = WatcherPriceFacts {
            current_price: 102.0,
            recent_bars: vec![
                WatcherBar {
                    close: 101.3,
                    high: 101.4,
                    low: 100.9,
                },
                WatcherBar {
                    close: 101.7,
                    high: 101.8,
                    low: 101.2,
                },
                WatcherBar {
                    close: 102.0,
                    high: 102.1,
                    low: 101.6,
                },
            ],
        };
        let plan = PositionManagementPlan {
            path_id: "path_a".to_string(),
            exposure_state: "in_position".to_string(),
            path_live_assessment: "live".to_string(),
            path_assessment_reason: None,
            actions: vec![PositionManagementAction {
                action_type: "move_stop".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                trigger_condition: Some(PriceTriggerCondition {
                    trigger_type: "price_above".to_string(),
                    trigger_price: 101.0,
                }),
                execution_price: None,
                add_ratio: None,
                reuse_current_entry_template: None,
                reduce_ratio: None,
                new_stop_loss: Some(100.5),
                reuse_current_bracket_template: Some(true),
                take_profit_1: None,
                take_profit_2: None,
                reason: "tighten".to_string(),
            }],
            management_note: "note".to_string(),
        };

        assert_eq!(
            first_triggered_position_management_action_index(&plan, &facts),
            Some(0)
        );
    }

    #[test]
    fn first_triggered_pending_order_action_matches_immediate_price_rule() {
        let facts = WatcherPriceFacts {
            current_price: 99.0,
            recent_bars: vec![
                WatcherBar {
                    close: 99.6,
                    high: 100.1,
                    low: 99.4,
                },
                WatcherBar {
                    close: 99.2,
                    high: 99.5,
                    low: 99.0,
                },
                WatcherBar {
                    close: 99.0,
                    high: 99.2,
                    low: 98.8,
                },
            ],
        };
        let plan = PendingOrderManagementPlan {
            path_id: "path_a".to_string(),
            exposure_state: "flat_with_live_entry_orders".to_string(),
            path_live_assessment: "degraded".to_string(),
            path_assessment_reason: None,
            actions: vec![PendingOrderManagementAction {
                action_type: "cancel_pending_order".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                trigger_condition: Some(PriceTriggerCondition {
                    trigger_type: "price_below".to_string(),
                    trigger_price: 100.0,
                }),
                execution_price: None,
                replacement_entry_zone: None,
                replacement_entry_invalidation_level: None,
                replacement_stop_loss: None,
                reuse_current_entry_template: None,
                post_fill_bracket_template: None,
                reason: "cancel".to_string(),
            }],
            management_note: "note".to_string(),
        };

        assert_eq!(
            first_triggered_pending_order_action_index(&plan, &facts),
            Some(0)
        );
    }

    #[test]
    fn execution_intent_from_entry_plan_uses_configured_ttl() {
        let stage1_output = sample_stage1_output();
        let current_path = stage1_output.current_path.as_ref().expect("path");
        let tactical_plan = sample_tactical_plan();
        let intent = execution_intent_from_entry_plan(
            "ETHUSDT",
            &tactical_plan.path_id,
            &tactical_plan.entry_plan,
            current_path,
            None,
            101.5,
            22,
            None,
        );

        assert_eq!(intent.ttl_minutes, 22);
        assert_eq!(intent.take_profit_1, 104.0);
        assert_eq!(intent.take_profit_2, 107.0);
    }

    #[test]
    fn maybe_record_stopout_and_cleanup_removes_snapshot_after_stop_is_hit() {
        let tactical_plan = sample_tactical_plan();
        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(tactical_plan.clone()),
            last_filled_context_key: Some("ETHUSDT:LONG:path_a".to_string()),
            ..WorkflowState::default()
        };
        let approved_tactical_plan = workflow_state.approved_tactical_plan.clone();
        let trading_state = sample_flat_trading_state();
        let mut entry_snapshots = HashMap::from([(
            "ETHUSDT:LONG:path_a".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                side: "LONG".to_string(),
                entry_profile: Some("reclaim_then_hold".to_string()),
                intent_mode: Some("breakout".to_string()),
                entry_activation_level: Some(sample_price_zone(100.0, 101.0, "15m")),
                entry_zone: Some(sample_price_zone(101.0, 102.0, "15m")),
                entry_invalidation_level: Some(sample_price_zone(98.0, 99.0, "15m")),
                max_drift_pct: Some(0.2),
                stop_loss: 98.8,
                take_profit_1: 104.0,
                take_profit_2: 107.0,
                allowed_stop_loss_levels: vec![98.8],
                allowed_take_profit_levels: vec![104.0, 107.0],
                tp1_realized: false,
                applied_driver_deterioration_signals: vec![],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        )]);
        let state_dir = format!("/tmp/workflow_runtime_stopout_{}", uuid::Uuid::new_v4());
        std::fs::create_dir_all(&state_dir).expect("create state dir");

        maybe_record_stopout_and_cleanup(
            &mut workflow_state,
            approved_tactical_plan.as_ref(),
            "ETHUSDT",
            &trading_state,
            98.5,
            &mut entry_snapshots,
            &state_dir,
        )
        .expect("cleanup");

        assert_eq!(workflow_state.filled_stopout_attempts, 1);
        assert!(workflow_state.last_filled_context_key.is_none());
        assert!(entry_snapshots.is_empty());
        let _ = std::fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn build_workflow_management_snapshot_uses_entry_snapshot_levels() {
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 0.25,
                entry_price: 2010.0,
                mark_price: 2025.0,
                unrealized_pnl: 3.75,
                leverage: 6,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 800.0,
        };
        let mut entry_snapshots = HashMap::new();
        entry_snapshots.insert(
            "ETHUSDT:LONG:path_a".to_string(),
            EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                side: "LONG".to_string(),
                entry_profile: Some("reclaim_then_hold".to_string()),
                intent_mode: Some("breakout".to_string()),
                entry_activation_level: Some(sample_price_zone(100.0, 101.0, "15m")),
                entry_zone: Some(sample_price_zone(101.0, 102.0, "15m")),
                entry_invalidation_level: Some(sample_price_zone(98.0, 99.0, "15m")),
                max_drift_pct: Some(0.2),
                stop_loss: 1980.0,
                take_profit_1: 2040.0,
                take_profit_2: 2080.0,
                allowed_stop_loss_levels: vec![1980.0, 2010.0],
                allowed_take_profit_levels: vec![2040.0, 2080.0],
                tp1_realized: false,
                applied_driver_deterioration_signals: vec![],
                created_at: Utc::now() - ChronoDuration::minutes(5),
                updated_at: Utc::now(),
            },
        );

        let snapshot =
            build_workflow_management_snapshot(&trading_state, "ETHUSDT", &entry_snapshots)
                .expect("management snapshot");

        assert_eq!(snapshot.context_state, "active_positions");
        assert_eq!(snapshot.active_position_count, 1);
        assert_eq!(snapshot.positions[0].direction, "LONG");
        assert_eq!(snapshot.positions[0].current_tp_price, Some(2040.0));
        assert_eq!(snapshot.positions[0].current_sl_price, Some(1980.0));
    }

    #[test]
    fn snapshot_for_management_context_falls_back_to_current_path_template() {
        let stage1_output = sample_stage1_output();
        let current_path = stage1_output.current_path.as_ref().expect("path");
        let workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            ..WorkflowState::default()
        };
        let snapshot = snapshot_for_management_context(
            "ETHUSDT",
            current_path,
            &workflow_state,
            &HashMap::new(),
            "ETHUSDT:LONG:path_a",
            "path_a",
        );

        assert_eq!(snapshot.context_key, "ETHUSDT:LONG:path_a");
        assert_eq!(snapshot.side, "LONG");
        assert_eq!(snapshot.take_profit_1, 104.0);
        assert_eq!(snapshot.take_profit_2, 107.0);
        assert_eq!(snapshot.entry_profile.as_deref(), Some("reclaim_then_hold"));
        assert_eq!(snapshot.intent_mode.as_deref(), Some("breakout"));
    }

    #[test]
    fn stage2c_replace_execution_intent_reuses_entry_template_and_current_bracket() {
        let stage1_output = sample_stage1_output();
        let current_path = stage1_output.current_path.as_ref().expect("path");
        let workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            pending_entry_bracket_template_override: Some(
                crate::workflow::schema::PostFillBracketTemplate {
                    take_profit_1: 105.0,
                    take_profit_2: 108.0,
                    stop_loss: 98.8,
                },
            ),
            ..WorkflowState::default()
        };
        let snapshot = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("breakout".to_string()),
            entry_activation_level: Some(sample_price_zone(100.0, 101.0, "15m")),
            entry_zone: Some(sample_price_zone(101.0, 102.0, "15m")),
            entry_invalidation_level: Some(sample_price_zone(98.0, 99.0, "15m")),
            max_drift_pct: Some(0.2),
            stop_loss: 98.8,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            allowed_stop_loss_levels: vec![98.8],
            allowed_take_profit_levels: vec![104.0, 107.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let action = PendingOrderManagementAction {
            action_type: "replace_entry".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            trigger_condition: Some(PriceTriggerCondition {
                trigger_type: "price_above".to_string(),
                trigger_price: 101.0,
            }),
            execution_price: None,
            replacement_entry_zone: Some(sample_price_zone(102.0, 103.0, "15m")),
            replacement_entry_invalidation_level: Some(sample_price_zone(99.5, 100.0, "15m")),
            replacement_stop_loss: Some(99.4),
            reuse_current_entry_template: Some(true),
            post_fill_bracket_template: None,
            reason: "replace".to_string(),
        };

        let (tactical_plan, bracket_template, intent) = build_stage2c_replace_execution_intent(
            "ETHUSDT",
            current_path,
            &workflow_state,
            &snapshot,
            &action,
            102.4,
            15,
        )
        .expect("replace intent");

        assert_eq!(tactical_plan.entry_plan.entry_profile, "reclaim_then_hold");
        assert_eq!(tactical_plan.entry_plan.intent_mode, "breakout");
        assert_eq!(tactical_plan.entry_plan.entry_zone.low, 102.0);
        assert_eq!(tactical_plan.entry_plan.entry_zone.high, 103.0);
        assert_eq!(tactical_plan.entry_plan.entry_invalidation_level.low, 99.5);
        assert_eq!(tactical_plan.entry_plan.stop_loss, 99.4);
        assert_eq!(bracket_template.take_profit_1, 105.0);
        assert_eq!(bracket_template.take_profit_2, 108.0);
        assert_eq!(bracket_template.stop_loss, 99.4);
        assert_eq!(intent.entry_zone.low, 102.0);
        assert_eq!(intent.stop_loss, 99.4);
        assert_eq!(intent.take_profit_1, 105.0);
        assert_eq!(intent.take_profit_2, 108.0);
        assert_eq!(intent.reason.as_deref(), Some("replace"));
    }

    #[test]
    fn decode_minute_bundle_envelope_accepts_plain_json() {
        let raw = serde_json::to_vec(&sample_minute_bundle_json()).expect("serialize bundle");
        let decoded = decode_minute_bundle_envelope(&raw, None).expect("decode plain bundle");
        assert_eq!(decoded.msg_type, "ind.minute_bundle");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.window_code, "1m");
    }

    #[test]
    fn decode_minute_bundle_envelope_accepts_gzip_json() {
        let raw = serde_json::to_vec(&sample_minute_bundle_json()).expect("serialize bundle");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&raw).expect("gzip write");
        let compressed = encoder.finish().expect("gzip finish");

        let decoded =
            decode_minute_bundle_envelope(&compressed, Some("gzip")).expect("decode gzip bundle");
        assert_eq!(decoded.msg_type, "ind.minute_bundle");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.window_code, "1m");
    }

    #[test]
    fn persist_bundle_to_disk_accepts_gzip_json() {
        let raw_json = sample_minute_bundle_json();
        let raw = serde_json::to_vec(&raw_json).expect("serialize bundle");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&raw).expect("gzip write");
        let compressed = encoder.finish().expect("gzip finish");
        let bundle: MinuteBundleEnvelope =
            serde_json::from_value(raw_json).expect("deserialize envelope");
        let path = minute_bundle_path(&bundle);
        if path.exists() {
            fs::remove_file(&path).expect("cleanup existing minute bundle path");
        }

        let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        runtime
            .block_on(persist_bundle_to_disk(
                &bundle,
                &compressed,
                Some("gzip"),
                10,
            ))
            .expect("persist gzip bundle");

        let persisted = fs::read_to_string(&path).expect("read persisted bundle");
        let persisted_json: Value =
            serde_json::from_str(&persisted).expect("parse persisted minute bundle");
        assert_eq!(
            persisted_json.get("msg_type").and_then(Value::as_str),
            Some("ind.minute_bundle")
        );
        assert_eq!(
            persisted_json.get("symbol").and_then(Value::as_str),
            Some("BTCUSDT")
        );

        fs::remove_file(&path).expect("cleanup persisted minute bundle");
    }

    #[test]
    fn fast_watcher_immediate_triggers_inside_entry_zone() {
        let watcher_cfg = sample_fast_watcher_config();
        let plan = sample_fast_entry_plan("immediate", "pullback_acceptance");
        let mut state = FastWatcherPlanState::default();

        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:00Z", 99.8),
            &watcher_cfg,
        ));
        assert!(fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:01Z", 100.4),
            &watcher_cfg,
        ));
    }

    #[test]
    fn fast_watcher_pullback_requires_activation_then_reentry() {
        let watcher_cfg = sample_fast_watcher_config();
        let plan = sample_fast_entry_plan("pullback", "pullback_acceptance");
        let mut state = FastWatcherPlanState::default();

        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:00Z", 100.4),
            &watcher_cfg,
        ));
        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:01Z", 101.1),
            &watcher_cfg,
        ));
        assert!(fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:02Z", 100.8),
            &watcher_cfg,
        ));
    }

    #[test]
    fn fast_watcher_breakout_triggers_on_dwell_or_excursion() {
        let watcher_cfg = sample_fast_watcher_config();
        let plan = sample_fast_entry_plan("breakout", "reclaim_then_hold");

        let mut dwell_state = FastWatcherPlanState::default();
        assert!(!fast_watcher_entry_ready(
            &mut dwell_state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:00Z", 101.01),
            &watcher_cfg,
        ));
        assert!(fast_watcher_entry_ready(
            &mut dwell_state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:01Z", 101.02),
            &watcher_cfg,
        ));

        let mut excursion_state = FastWatcherPlanState::default();
        assert!(fast_watcher_entry_ready(
            &mut excursion_state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:00Z", 101.05),
            &watcher_cfg,
        ));
    }

    #[test]
    fn fast_watcher_failed_auction_reentry_requires_probe_then_recovery() {
        let watcher_cfg = sample_fast_watcher_config();
        let plan = sample_fast_entry_plan("pullback", "failed_auction_reentry");
        let mut state = FastWatcherPlanState::default();

        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:00Z", 100.6),
            &watcher_cfg,
        ));
        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:01Z", 98.7),
            &watcher_cfg,
        ));
        assert!(!fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:02Z", 100.8),
            &watcher_cfg,
        ));
        assert!(fast_watcher_entry_ready(
            &mut state,
            &plan,
            &sample_fast_price_event("2026-03-30T09:35:03Z", 100.9),
            &watcher_cfg,
        ));
    }

    /*
    use super::*;
    use crate::app::config::load_config;
    use crate::execution::binance::TradingStateSnapshot;
    use crate::workflow::schema::{
        AttemptPolicy, MapSummary, PathRuntimeState, PriceZone, Stage1Meta,
        Stage1Output, TacticalEntryPlan, TacticalEntrySnapshot,
    };
    use crate::workflow::state::WorkflowState;
    use flate2::{write::GzEncoder, Compression};

    fn workflow_test_config() -> RootConfig {
        load_config("/data/config/config.yaml").expect("load workflow test config")
    }

    fn empty_kline_bar(open_time: &str, close_time: &str) -> Value {
        json!({
            "open_time": open_time,
            "close_time": close_time,
            "open": Value::Null,
            "high": Value::Null,
            "low": Value::Null,
            "close": Value::Null,
            "volume_base": 0.0,
            "volume_quote": 0.0,
            "is_closed": true,
            "minutes_covered": 240,
            "expected_minutes": 240
        })
    }

    fn sample_kline_bar(open_time: &str, close_time: &str, close: f64) -> Value {
        json!({
            "open_time": open_time,
            "close_time": close_time,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume_base": 1.0,
            "volume_quote": 1.0,
            "is_closed": true,
            "minutes_covered": 1,
            "expected_minutes": 1
        })
    }

    fn sample_minute_bundle_json() -> Value {
        json!({
            "msg_type": "ind.minute_bundle",
            "routing_key": "bundle.1m.btcusdt",
            "symbol": "BTCUSDT",
            "ts_bucket": "2026-03-28T04:15:00Z",
            "window_code": "1m",
            "indicator_count": 2,
            "published_at": "2026-03-28T04:15:08Z",
            "indicators": {
                "footprint": {
                    "window_code": "1m",
                    "payload": {"levels": [1, 2, 3]}
                }
            }
        })
    }

    #[test]
    fn decode_minute_bundle_envelope_accepts_plain_json() {
        let raw = serde_json::to_vec(&sample_minute_bundle_json()).expect("serialize bundle");
        let decoded = decode_minute_bundle_envelope(&raw, None).expect("decode plain bundle");
        assert_eq!(decoded.msg_type, "ind.minute_bundle");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.window_code, "1m");
    }

    #[test]
    fn decode_minute_bundle_envelope_accepts_gzip_json() {
        let raw = serde_json::to_vec(&sample_minute_bundle_json()).expect("serialize bundle");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&raw).expect("gzip write");
        let compressed = encoder.finish().expect("gzip finish");

        let decoded =
            decode_minute_bundle_envelope(&compressed, Some("gzip")).expect("decode gzip bundle");
        assert_eq!(decoded.msg_type, "ind.minute_bundle");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.window_code, "1m");
    }

    #[test]
    fn decode_minute_bundle_envelope_rejects_unknown_content_encoding() {
        let raw = serde_json::to_vec(&sample_minute_bundle_json()).expect("serialize bundle");
        let err = decode_minute_bundle_envelope(&raw, Some("br")).expect_err("reject encoding");
        assert!(err
            .to_string()
            .contains("unsupported minute bundle content_encoding"));
    }

    #[test]
    fn persist_bundle_to_disk_accepts_gzip_json() {
        let raw_json = sample_minute_bundle_json();
        let raw = serde_json::to_vec(&raw_json).expect("serialize bundle");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&raw).expect("gzip write");
        let compressed = encoder.finish().expect("gzip finish");
        let bundle: MinuteBundleEnvelope =
            serde_json::from_value(raw_json).expect("deserialize envelope");
        let path = minute_bundle_path(&bundle);
        if path.exists() {
            fs::remove_file(&path).expect("cleanup existing minute bundle path");
        }

        let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        runtime
            .block_on(persist_bundle_to_disk(&bundle, &compressed, Some("gzip"), 10))
            .expect("persist gzip bundle");

        let persisted = fs::read_to_string(&path).expect("read persisted bundle");
        let persisted_json: Value =
            serde_json::from_str(&persisted).expect("parse persisted minute bundle");
        assert_eq!(
            persisted_json.get("msg_type").and_then(Value::as_str),
            Some("ind.minute_bundle")
        );
        assert_eq!(
            persisted_json.get("symbol").and_then(Value::as_str),
            Some("BTCUSDT")
        );

        fs::remove_file(&path).expect("cleanup persisted minute bundle");
    }

    fn empty_map_summary() -> MapSummary {
        MapSummary {
            regime_3d: json!({}),
            location_1d: json!({}),
            location_4h: json!({}),
            price_location_class: "value_edge".to_string(),
            key_levels: json!({}),
        }
    }

    fn sample_tactical_plan() -> TacticalEntryPlan {
        TacticalEntryPlan {
            path_id: "path_a".to_string(),
            primary_entry_plan: crate::workflow::schema::EntryPlan {
                side: "LONG".to_string(),
                entry_profile: "reclaim_then_hold".to_string(),
                intent_mode: "breakout".to_string(),
                entry_activation_level: PriceZone {
                    low: 100.0,
                    high: 101.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                entry_zone: PriceZone {
                    low: 101.0,
                    high: 102.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                entry_invalidation_level: PriceZone {
                    low: 98.0,
                    high: 99.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                stop_loss: 98.8,
                take_profit_1: 104.0,
                take_profit_2: 106.0,
                ttl_minutes: 15,
                max_drift_pct: 0.2,
                entry_snapshot: TacticalEntrySnapshot {
                    context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                    path_id: "path_a".to_string(),
                    plan_role: "primary".to_string(),
                },
                entry_note: String::new(),
            },
            secondary_entry_plan: crate::workflow::schema::EntryPlan {
                side: "LONG".to_string(),
                entry_profile: "failed_auction_reentry".to_string(),
                intent_mode: "pullback".to_string(),
                entry_activation_level: PriceZone {
                    low: 99.0,
                    high: 100.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                entry_zone: PriceZone {
                    low: 99.0,
                    high: 100.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                entry_invalidation_level: PriceZone {
                    low: 97.8,
                    high: 98.5,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                stop_loss: 98.2,
                take_profit_1: 104.0,
                take_profit_2: 106.0,
                ttl_minutes: 15,
                max_drift_pct: 0.25,
                entry_snapshot: TacticalEntrySnapshot {
                    context_key: "ETHUSDT:LONG:path_a:secondary".to_string(),
                    path_id: "path_a".to_string(),
                    plan_role: "secondary".to_string(),
                },
                entry_note: String::new(),
            },
            attempt_policy: AttemptPolicy {
                max_filled_stopout_attempts: 2,
                count_unfilled_attempts: false,
                time_window: "same_15m_window".to_string(),
            },
        }
    }

    fn sample_flat_trading_state() -> TradingStateSnapshot {
        TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: false,
            has_active_positions: false,
            has_open_orders: false,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 0.0,
            available_balance: 0.0,
        }
    }

    fn sample_watcher_indicators(bars: &[(f64, f64, f64)]) -> Value {
        let bars = bars
            .iter()
            .enumerate()
            .map(|(idx, (close, high, low))| {
                let minute = idx.to_string().chars().last().unwrap_or('0');
                json!({
                    "open_time": format!("2026-03-28T05:0{}:00Z", minute),
                    "close_time": format!("2026-03-28T05:0{}:59Z", minute),
                    "open": close,
                    "high": high,
                    "low": low,
                    "close": close,
                    "is_closed": true
                })
            })
            .collect::<Vec<_>>();
        json!({
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

    fn sample_minute_bundle_json() -> Value {
        json!({
            "msg_type": "ind.minute_bundle",
            "routing_key": "bundle.1m.btcusdt",
            "symbol": "BTCUSDT",
            "ts_bucket": "2026-03-28T04:15:00Z",
            "window_code": "1m",
            "indicator_count": 2,
            "published_at": "2026-03-28T04:15:08Z",
            "indicators": {
                "footprint": {
                    "window_code": "1m",
                    "payload": {"levels": [1, 2, 3]}
                }
            }
        })
    }
                    }
                }
            }
        })
    }

    #[test]
    fn collect_missing_kline_bar_requests_finds_empty_scan_bars() {
        let indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        empty_kline_bar("2026-03-08T16:00:00+00:00", "2026-03-08T20:00:00+00:00")
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        });

        let requests = collect_missing_kline_bar_requests(&indicators);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].market, "futures");
        assert_eq!(requests[0].interval_code, "4h");
        assert_eq!(
            requests[0].open_time,
            DateTime::parse_from_rfc3339("2026-03-08T16:00:00+00:00")
                .expect("parse open time")
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn latest_closed_1m_price_falls_back_to_compact_latest_bar() {
        let indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "1m": {
                            "markets": {
                                "futures": {
                                    "returned_count": 1024,
                                    "latest_bar": {
                                        "open_time": "2026-03-30T09:07:00Z",
                                        "close_time": "2026-03-30T09:08:00Z",
                                        "open": 2061.67,
                                        "high": 2062.30,
                                        "low": 2061.31,
                                        "close": 2061.96,
                                        "is_closed": true
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        assert_eq!(latest_closed_1m_price(&indicators), Some(2061.96));

        let bars = extract_kline_history_bars(&indicators, "futures", "1m");
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].get("close"), Some(&json!(2061.96)));
    }

    #[test]
    fn apply_backfilled_kline_bars_replaces_empty_bar_in_place() {
        let mut indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "4h": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        empty_kline_bar("2026-03-08T16:00:00+00:00", "2026-03-08T20:00:00+00:00")
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        });
        let request = MissingKlineBarRequest {
            market: "futures".to_string(),
            interval_code: "4h".to_string(),
            open_time: DateTime::parse_from_rfc3339("2026-03-08T16:00:00+00:00")
                .expect("parse open time")
                .with_timezone(&Utc),
        };
        let replacement = json!({
            "open_time": "2026-03-08T16:00:00+00:00",
            "close_time": "2026-03-08T20:00:00+00:00",
            "open": 1942.01,
            "high": 1969.19,
            "low": 1926.73,
            "close": 1963.20,
            "volume_base": 583922.748,
            "volume_quote": 1.0,
            "is_closed": true,
            "minutes_covered": 240,
            "expected_minutes": 240
        });

        let patched = apply_backfilled_kline_bars(&mut indicators, &[(request, replacement)]);
        assert_eq!(patched, 1);
        assert_eq!(
            indicators.pointer("/kline_history/payload/intervals/4h/markets/futures/bars/0/open"),
            Some(&json!(1942.01))
        );
    }

    #[test]
    fn interval_minutes_supports_expected_timeframes() {
        assert_eq!(interval_minutes("1m"), 1);
        assert_eq!(interval_minutes("15m"), 15);
        assert_eq!(interval_minutes("1h"), 60);
        assert_eq!(interval_minutes("4h"), 240);
        assert_eq!(interval_minutes("1d"), 1440);
        assert_eq!(interval_minutes("3d"), 4320);
    }

    #[test]
    fn slice_cached_kline_bars_returns_requested_subset() {
        let bars = vec![
            sample_kline_bar(
                "2026-03-18T11:00:00+00:00",
                "2026-03-18T11:01:00+00:00",
                2301.0,
            ),
            sample_kline_bar(
                "2026-03-18T11:01:00+00:00",
                "2026-03-18T11:02:00+00:00",
                2302.0,
            ),
            sample_kline_bar(
                "2026-03-18T11:02:00+00:00",
                "2026-03-18T11:03:00+00:00",
                2303.0,
            ),
        ];
        let subset = slice_cached_kline_bars(
            &bars,
            parse_rfc3339_utc("2026-03-18T11:01:00+00:00").expect("parse start"),
            parse_rfc3339_utc("2026-03-18T11:02:00+00:00").expect("parse end"),
        );

        assert_eq!(subset.len(), 2);
        assert_eq!(subset[0].get("close"), Some(&json!(2302.0)));
        assert_eq!(subset[1].get("close"), Some(&json!(2303.0)));
    }

    #[test]
    fn workflow_stage1_refresh_reason_prefers_pending_reason() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_PENDING";
        reset_startup_stage1_refresh_for_symbol(symbol);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket: Utc::now(),
                window_code: "15m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: Utc::now(),
        };
        let state = WorkflowState {
            symbol: symbol.to_string(),
            pending_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            last_stage1_ts: None,
            ..WorkflowState::default()
        };
        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, None).as_deref(),
            Some("thesis_invalidated")
        );
    }

    #[test]
    fn update_pending_invoke_bundle_keeps_newer_ts_bucket_when_older_arrives() {
        let newer_ts = parse_rfc3339_utc("2026-03-30T03:45:00Z").expect("parse newer ts");
        let older_ts = parse_rfc3339_utc("2026-03-30T03:44:00Z").expect("parse older ts");
        let symbol = "ETHUSDT";
        let mut pending = Some(LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket: newer_ts,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: newer_ts,
        });
        let older_bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket: older_ts,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: older_ts,
        };

        assert!(!update_pending_invoke_bundle(&mut pending, older_bundle));
        assert_eq!(
            pending.as_ref().map(|bundle| bundle.raw.ts_bucket),
            Some(newer_ts)
        );
    }

    #[test]
    fn update_pending_invoke_bundle_replaces_pending_when_newer_arrives() {
        let older_ts = parse_rfc3339_utc("2026-03-30T03:44:00Z").expect("parse older ts");
        let newer_ts = parse_rfc3339_utc("2026-03-30T03:45:00Z").expect("parse newer ts");
        let symbol = "ETHUSDT";
        let mut pending = Some(LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket: older_ts,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: older_ts,
        });
        let newer_bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket: newer_ts,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: newer_ts,
        };

        assert!(update_pending_invoke_bundle(&mut pending, newer_bundle));
        assert_eq!(
            pending.as_ref().map(|bundle| bundle.raw.ts_bucket),
            Some(newer_ts)
        );
    }

    #[test]
    fn consume_pending_stage1_refresh_reason_is_one_shot() {
        let mut state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            ..WorkflowState::default()
        };

        assert!(!consume_pending_stage1_refresh_reason(
            &mut state,
            "scheduled_2h"
        ));
        assert_eq!(
            state.pending_stage1_refresh_reason.as_deref(),
            Some("thesis_invalidated")
        );
        assert!(consume_pending_stage1_refresh_reason(
            &mut state,
            "thesis_invalidated"
        ));
        assert!(state.pending_stage1_refresh_reason.is_none());
        assert!(!consume_pending_stage1_refresh_reason(
            &mut state,
            "thesis_invalidated"
        ));
    }

    #[test]
    fn workflow_stage1_refresh_reason_triggers_on_scheduled_boundary() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_SCHEDULED";
        reset_startup_stage1_refresh_for_symbol(symbol);
        mark_startup_stage1_refresh_consumed(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T04:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let state = WorkflowState {
            symbol: symbol.to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::hours(2)),
            ..WorkflowState::default()
        };
        assert_eq!(
            workflow_stage1_refresh_reason(
                &config,
                &bundle,
                &state,
                Some(&sample_stage1_output()),
            )
            .as_deref(),
            Some("scheduled_2h")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_forces_once_on_startup_even_with_existing_stage1() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_STARTUP_FORCE";
        reset_startup_stage1_refresh_for_symbol(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:30:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let state = WorkflowState {
            symbol: symbol.to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::minutes(30)),
            ..WorkflowState::default()
        };
        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, Some(&sample_stage1_output()))
                .as_deref(),
            Some("startup_force_stage1")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_triggers_missing_stage1_after_startup_force_is_consumed() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_STARTUP_MISSING";
        reset_startup_stage1_refresh_for_symbol(symbol);
        mark_startup_stage1_refresh_consumed(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let state = WorkflowState {
            symbol: symbol.to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::minutes(15)),
            ..WorkflowState::default()
        };
        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, None).as_deref(),
            Some("startup_missing_stage1")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_is_none_off_schedule_with_existing_stage1_after_startup_force()
    {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_OFFSCHEDULE";
        reset_startup_stage1_refresh_for_symbol(symbol);
        mark_startup_stage1_refresh_consumed(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: symbol.to_string(),
                ts_bucket,
                window_code: "15m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let state = WorkflowState {
            symbol: symbol.to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::minutes(15)),
            ..WorkflowState::default()
        };
        assert!(workflow_stage1_refresh_reason(
            &config,
            &bundle,
            &state,
            Some(&sample_stage1_output()),
        )
        .is_none());
    }

    #[test]
    fn workflow_stage2_review_due_runs_only_on_configured_15m_boundary_for_active_path() {
        let config = workflow_test_config();
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: ts_bucket - ChronoDuration::minutes(15),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: empty_map_summary(),
            opportunity_assessment: crate::workflow::schema::OpportunityAssessment::default(),
            script_rejections: vec![],
            current_script: Some("script".to_string()),
            driver_attribution: None,
            current_path: Some(crate::workflow::schema::CurrentPath {
                id: "path_a".to_string(),
                side: "LONG".to_string(),
                thesis: "thesis".to_string(),
                risk_grade: "aligned_trend".to_string(),
                activation_anchor_id: None,
                strategic_activation_level: crate::workflow::schema::PriceZone {
                    low: 100.0,
                    high: 101.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                first_path_target_anchor_id: None,
                first_path_target: crate::workflow::schema::PriceZone {
                    low: 103.0,
                    high: 104.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                next_path_target_anchor_id: None,
                next_path_target: crate::workflow::schema::PriceZone {
                    low: 105.0,
                    high: 106.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_anchor_id: None,
                failure_level: crate::workflow::schema::PriceZone {
                    low: 98.0,
                    high: 99.0,
                    timeframe: None,
                    label: None,
                    reason: None,
                },
                failure_switch: Some("reevaluate_short".to_string()),
                setup_type: "continuation".to_string(),
                reevaluation_trigger: crate::workflow::schema::ReevaluationTrigger::default(),
                management_plan: crate::workflow::schema::ManagementPlan {
                    take_profit_1_basis: "first_path_target".to_string(),
                    take_profit_2_basis: "next_path_target".to_string(),
                    take_profit_1_level: 103.0,
                    take_profit_2_level: 105.0,
                    stop_migration_rules: vec![],
                    reduce_on_driver_deterioration: vec![],
                    exit_full_on_driver_deterioration: vec![],
                },
                tracked_zones: vec![],
            }),
        };

        assert!(workflow_stage2_review_due(
            &config,
            &bundle,
            Some(&stage1_output),
            false,
        ));
        assert!(!workflow_stage2_review_due(
            &config,
            &bundle,
            Some(&stage1_output),
            true,
        ));
    }

    #[test]
    fn workflow_stage2_review_due_is_false_for_no_edge() {
        let config = workflow_test_config();
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
                ts_bucket,
                window_code: "1m".to_string(),
                indicator_count: 0,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({}),
            missing_indicator_codes: vec![],
            received_at: ts_bucket,
        };
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: ts_bucket,
            },
            monitoring_status: "no_edge".to_string(),
            no_trade_reason: Some("conflict_no_edge".to_string()),
            refresh_hints: vec![],
            map_summary: empty_map_summary(),
            opportunity_assessment: crate::workflow::schema::OpportunityAssessment::default(),
            script_rejections: vec![],
            current_script: None,
            driver_attribution: None,
            current_path: None,
        };
        assert!(!workflow_stage2_review_due(
            &config,
            &bundle,
            Some(&stage1_output),
            false,
        ));
    }

    #[test]
    fn watcher_entry_ready_requires_reclaim_then_hold_confirmation() {
        let watcher_cfg = workflow_test_config().llm.workflow.watcher;
        let plan = crate::workflow::schema::EntryPlan {
            side: "LONG".to_string(),
            entry_profile: "reclaim_then_hold".to_string(),
            intent_mode: "breakout".to_string(),
            entry_activation_level: crate::workflow::schema::PriceZone {
                low: 100.0,
                high: 101.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            entry_zone: crate::workflow::schema::PriceZone {
                low: 101.0,
                high: 102.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            entry_invalidation_level: crate::workflow::schema::PriceZone {
                low: 98.0,
                high: 99.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            stop_loss: 98.8,
            take_profit_1: 104.0,
            take_profit_2: 106.0,
            ttl_minutes: 15,
            max_drift_pct: 0.2,
            entry_snapshot: crate::workflow::schema::TacticalEntrySnapshot {
                context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                path_id: "path_a".to_string(),
                plan_role: "primary".to_string(),
            },
            entry_note: String::new(),
        };

        let weak_facts = WatcherPriceFacts {
            current_price: 100.6,
            recent_bars: vec![
                WatcherBar {
                    close: 100.2,
                    high: 100.4,
                    low: 99.9,
                },
                WatcherBar {
                    close: 100.4,
                    high: 100.6,
                    low: 100.0,
                },
                WatcherBar {
                    close: 100.6,
                    high: 100.8,
                    low: 100.1,
                },
            ],
        };
        assert!(!watcher_entry_ready(&plan, &weak_facts, &watcher_cfg));

        let confirmed_facts = WatcherPriceFacts {
            current_price: 102.4,
            recent_bars: vec![
                WatcherBar {
                    close: 102.15,
                    high: 102.3,
                    low: 100.8,
                },
                WatcherBar {
                    close: 102.2,
                    high: 102.4,
                    low: 100.7,
                },
                WatcherBar {
                    close: 102.3,
                    high: 102.5,
                    low: 100.9,
                },
                WatcherBar {
                    close: 102.4,
                    high: 102.6,
                    low: 101.0,
                },
            ],
        };
        assert!(watcher_entry_ready(&plan, &confirmed_facts, &watcher_cfg));
    }

    #[test]
    fn entry_hold_confirmation_uses_reclaimed_edge_not_activation_floor() {
        let predicate = crate::app::config::EntryHoldPredicateConfig {
            hold_bars: 3,
            retest_tolerance_bps: 0.0,
        };
        let level = PriceZone {
            low: 100.0,
            high: 101.0,
            timeframe: None,
            label: None,
            reason: None,
        };
        let facts = WatcherPriceFacts {
            current_price: 101.2,
            recent_bars: vec![
                WatcherBar {
                    close: 100.4,
                    high: 100.6,
                    low: 100.2,
                },
                WatcherBar {
                    close: 101.1,
                    high: 101.2,
                    low: 100.9,
                },
                WatcherBar {
                    close: 101.2,
                    high: 101.3,
                    low: 101.0,
                },
            ],
        };

        assert!(!entry_hold_confirmed("LONG", &level, &facts, &predicate));
    }

    #[test]
    fn reset_watcher_window_if_needed_resets_attempts_on_new_window() {
        let ts = DateTime::parse_from_rfc3339("2026-03-28T05:17:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let tactical_plan = sample_tactical_plan();
        let mut state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            active_15m_window_start: Some(
                DateTime::parse_from_rfc3339("2026-03-28T05:00:00Z")
                    .expect("window")
                    .with_timezone(&Utc),
            ),
            approved_tactical_plan: Some(tactical_plan.clone()),
            approved_tactical_plan_updated_at: Some(ts - ChronoDuration::minutes(5)),
            filled_stopout_attempts: 2,
            last_filled_context_key: Some("ETHUSDT:LONG:path_a:primary".to_string()),
            last_executed_plan_role: Some("secondary".to_string()),
            ..WorkflowState::default()
        };

        let expired_plan = reset_watcher_window_if_needed(&mut state, ts);

        assert_eq!(
            state.active_15m_window_start,
            Some(
                DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
                    .expect("window")
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(expired_plan, Some(tactical_plan));
        assert!(state.approved_tactical_plan.is_none());
        assert!(state.approved_tactical_plan_updated_at.is_none());
        assert_eq!(state.filled_stopout_attempts, 0);
        assert!(state.last_filled_context_key.is_none());
        assert!(state.last_executed_plan_role.is_none());
    }

    #[test]
    fn select_entry_plan_keeps_stage2_approved_plan_executable_until_hard_invalidation() {
        let tactical_plan = sample_tactical_plan();
        let workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            ..WorkflowState::default()
        };
        let soft_only_runtime_state = PathRuntimeState {
            path_id: "path_a".to_string(),
            monitoring_status: "active".to_string(),
            latest_price: 115.0,
            failure_level_breached: false,
            active_entry_context_keys: Vec::new(),
            notes: Vec::new(),
        };
        let indicators = sample_watcher_indicators(&[
            (102.15, 102.3, 100.8),
            (102.2, 102.4, 100.7),
            (102.3, 102.5, 100.9),
            (102.4, 102.6, 101.0),
        ]);
        let watcher_cfg = workflow_test_config().llm.workflow.watcher;

        let selected = select_entry_plan(
            "ETHUSDT",
            &tactical_plan,
            &workflow_state,
            &soft_only_runtime_state,
            false,
            &sample_flat_trading_state(),
            &std::collections::HashMap::new(),
            &indicators,
            &watcher_cfg,
        );
        assert_eq!(
            selected
                .as_ref()
                .map(|selected| selected.plan.entry_snapshot.plan_role.as_str()),
            Some("primary")
        );
        assert_eq!(
            selected.as_ref().map(|selected| selected.trigger_price),
            Some(102.4)
        );

        assert!(select_entry_plan(
            "ETHUSDT",
            &tactical_plan,
            &workflow_state,
            &soft_only_runtime_state,
            true,
            &sample_flat_trading_state(),
            &std::collections::HashMap::new(),
            &indicators,
            &watcher_cfg,
        )
        .is_none());
    }

    #[test]
    fn build_persist_only_input_preserves_full_raw_indicator_bundle() {
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "TESTUSDT".to_string(),
                ts_bucket: Utc::now(),
                window_code: "1m".to_string(),
                indicator_count: 3,
                published_at: None,
                indicators: json!({}),
            },
            indicators: json!({
                "absorption": {"payload": {"recent_7d": {"events": []}}},
                "fvg": {"payload": {"by_window": {"15m": {}, "4h": {}}}},
                "cvd_pack": {"payload": {"by_window": {"15m": {"series": []}}}}
            }),
            missing_indicator_codes: vec![],
            received_at: Utc::now(),
        };

        let input = build_persist_only_input(&bundle);

        let mut keys = input
            .indicators
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "absorption".to_string(),
                "cvd_pack".to_string(),
                "fvg".to_string()
            ]
        );
        assert_eq!(
            input
                .indicators
                .pointer("/fvg/payload/by_window/15m")
                .map(|_| true),
            Some(true)
        );
        assert!(input.missing_indicator_codes.is_empty());
        assert!(input.trading_state.is_none());
        assert!(input.management_snapshot.is_none());
    }

    #[test]
    fn collect_divergence_kline_coverage_request_detects_gap_before_existing_1m_history() {
        let indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "1m": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open_time": "2026-03-18T00:10:00Z", "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5, "is_closed": true},
                                        {"open_time": "2026-03-18T00:11:00Z", "open": 100.5, "high": 101.2, "low": 100.0, "close": 101.0, "is_closed": true}
                                    ],
                                    "returned_count": 2
                                }
                            }
                        }
                    }
                }
            },
            "divergence": {
                "payload": {
                    "recent_7d": {
                        "events": [
                            {"event_start_ts": "2026-03-18T00:02:00Z", "event_end_ts": "2026-03-18T00:05:00Z"},
                            {"event_start_ts": "2026-03-18T00:08:00Z", "event_end_ts": "2026-03-18T00:09:00Z"}
                        ]
                    }
                }
            }
        });

        let request =
            collect_divergence_kline_coverage_request(&indicators).expect("range request");

        assert_eq!(request.market, "futures");
        assert_eq!(request.interval_code, "1m");
        assert_eq!(
            request.start_open_time,
            DateTime::parse_from_rfc3339("2026-03-18T00:02:00Z")
                .expect("parse start")
                .with_timezone(&Utc)
        );
        assert_eq!(
            request.end_open_time,
            DateTime::parse_from_rfc3339("2026-03-18T00:09:00Z")
                .expect("parse end")
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn prepend_backfilled_kline_bars_prepends_unique_older_bars_and_updates_count() {
        let mut indicators = json!({
            "kline_history": {
                "payload": {
                    "intervals": {
                        "1m": {
                            "markets": {
                                "futures": {
                                    "bars": [
                                        {"open_time": "2026-03-18T00:10:00Z", "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5, "is_closed": true},
                                        {"open_time": "2026-03-18T00:11:00Z", "open": 100.5, "high": 101.2, "low": 100.0, "close": 101.0, "is_closed": true}
                                    ],
                                    "returned_count": 2
                                }
                            }
                        }
                    }
                }
            }
        });
        let replacements = vec![
            json!({"open_time": "2026-03-18T00:08:00Z", "close_time": "2026-03-18T00:08:59Z", "open": 99.0, "high": 99.5, "low": 98.8, "close": 99.2, "volume_base": 1.0, "volume_quote": 2.0, "is_closed": true, "minutes_covered": 1, "expected_minutes": 1}),
            json!({"open_time": "2026-03-18T00:09:00Z", "close_time": "2026-03-18T00:09:59Z", "open": 99.2, "high": 100.0, "low": 99.1, "close": 100.0, "volume_base": 1.0, "volume_quote": 2.0, "is_closed": true, "minutes_covered": 1, "expected_minutes": 1}),
            json!({"open_time": "2026-03-18T00:10:00Z", "close_time": "2026-03-18T00:10:59Z", "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5, "volume_base": 1.0, "volume_quote": 2.0, "is_closed": true, "minutes_covered": 1, "expected_minutes": 1}),
        ];

        let inserted =
            prepend_backfilled_kline_bars(&mut indicators, "futures", "1m", &replacements);

        assert_eq!(inserted, 2);
        assert_eq!(
            indicators
                .pointer("/kline_history/payload/intervals/1m/markets/futures/returned_count"),
            Some(&json!(4))
        );

        let bars = indicators
            .pointer("/kline_history/payload/intervals/1m/markets/futures/bars")
            .and_then(Value::as_array)
            .expect("bars array");
        let open_times = bars
            .iter()
            .filter_map(|bar| bar.get("open_time").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            open_times,
            vec![
                "2026-03-18T00:08:00Z",
                "2026-03-18T00:09:00Z",
                "2026-03-18T00:10:00Z",
                "2026-03-18T00:11:00Z"
            ]
        );
    }

    #[test]
    fn backfill_divergence_event_prices_from_bars_derives_missing_price_fields() {
        let mut indicators = json!({
            "divergence": {
                "payload": {
                    "recent_7d": {
                        "events": [
                            {
                                "type": "hidden_bullish_divergence",
                                "event_start_ts": "2026-03-18T00:02:00Z",
                                "event_end_ts": "2026-03-18T00:05:00Z",
                                "pivot_side": "low"
                            },
                            {
                                "type": "hidden_bearish_divergence",
                                "event_start_ts": "2026-03-18T00:07:00Z",
                                "event_end_ts": "2026-03-18T00:09:00Z",
                                "pivot_side": "high"
                            }
                        ]
                    }
                }
            }
        });
        let bars = vec![
            json!({"open_time": "2026-03-18T00:02:00Z", "low": 99.5, "high": 100.2}),
            json!({"open_time": "2026-03-18T00:03:00Z", "low": 98.8, "high": 100.6}),
            json!({"open_time": "2026-03-18T00:05:00Z", "low": 99.1, "high": 101.0}),
            json!({"open_time": "2026-03-18T00:07:00Z", "low": 100.5, "high": 101.8}),
            json!({"open_time": "2026-03-18T00:09:00Z", "low": 100.7, "high": 102.4}),
        ];

        let patched = backfill_divergence_event_prices_from_bars(&mut indicators, &bars);
        assert_eq!(patched, 2);

        let events = indicators
            .pointer("/divergence/payload/recent_7d/events")
            .and_then(Value::as_array)
            .expect("events array");
        assert_eq!(
            events[0].get("pivot_price").and_then(Value::as_f64),
            Some(98.8)
        );
        assert_eq!(
            events[0].get("price_low").and_then(Value::as_f64),
            Some(98.8)
        );
        assert_eq!(
            events[0].get("price_high").and_then(Value::as_f64),
            Some(101.0)
        );
        assert_eq!(
            events[1].get("pivot_price").and_then(Value::as_f64),
            Some(102.4)
        );
        assert_eq!(
            events[1].get("price_low").and_then(Value::as_f64),
            Some(100.5)
        );
        assert_eq!(
            events[1].get("price_high").and_then(Value::as_f64),
            Some(102.4)
        );
    }

    #[test]
    fn prune_temp_indicator_dir_removes_files_older_than_configured_minutes() {
        let dir = std::env::temp_dir().join(format!("llm-temp-indicator-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create temp indicator dir");
        fs::write(dir.join(".gitignore"), "").expect("write .gitignore");
        fs::write(dir.join("20260307T105900Z_TESTUSDT.json"), "{}").expect("write old file");
        fs::write(dir.join("20260307T110000Z_TESTUSDT.json"), "{}").expect("write edge file");
        fs::write(dir.join("20260307T111500Z_TESTUSDT.json"), "{}").expect("write fresh file");
        fs::write(dir.join("not_a_bundle.json"), "{}").expect("write invalid file");

        let removed = prune_expired_temp_indicator_files(
            &dir,
            DateTime::parse_from_rfc3339("2026-03-07T11:30:00Z")
                .expect("parse current ts")
                .with_timezone(&Utc),
            30,
        )
        .expect("prune temp indicator dir");

        assert_eq!(removed, 1);
        assert!(!dir.join("20260307T105900Z_TESTUSDT.json").exists());
        assert!(dir.join("20260307T110000Z_TESTUSDT.json").exists());
        assert!(dir.join("20260307T111500Z_TESTUSDT.json").exists());
        assert!(dir.join("not_a_bundle.json").exists());
        assert!(dir.join(".gitignore").exists());

        fs::remove_dir_all(&dir).expect("cleanup temp indicator dir");
    }

    #[test]
    fn render_pretty_json_value_formats_stage_trace_objects() {
        let rendered = render_pretty_json_value(&json!({
            "stage": "workflow_stage1",
            "parsed_output": {
                "monitoring_status": "active"
            }
        }));
        assert!(rendered.contains('\n'));
        assert!(rendered.contains("\"stage\": \"workflow_stage1\""));
        assert!(rendered.contains("\"monitoring_status\": \"active\""));
    }

    #[test]
    fn build_execution_trade_signal_marks_same_side_execution_as_add() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let intent = crate::workflow::schema::ExecutionIntent {
            side: "LONG".to_string(),
            intent_mode: "immediate".to_string(),
            entry_zone: crate::workflow::schema::PriceZone {
                low: 1999.0,
                high: 2001.0,
                timeframe: Some("15m".to_string()),
                label: Some("entry".to_string()),
                reason: None,
            },
            trigger_price: Some(2000.0),
            stop_loss: 1980.0,
            take_profit_1: 2040.0,
            take_profit_2: 2080.0,
            ttl_minutes: 15,
            max_drift_pct: 0.2,
            path_id: "path_a".to_string(),
            entry_snapshot: crate::workflow::schema::EntrySnapshotRef {
                context_key: "ETHUSDT:LONG:path_a:primary".to_string(),
                path_id: "path_a".to_string(),
            },
            reason: Some("driver aligned".to_string()),
        };
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 1900.0,
                mark_price: 2000.0,
                unrealized_pnl: 100.0,
                leverage: 5,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 500.0,
        };
        let signal = build_execution_trade_signal(
            ts_bucket,
            "schedule",
            "ETHUSDT",
            "custom_llm",
            &trading_state,
            &intent,
            None,
            Some("path confirmed"),
        );

        assert_eq!(signal.decision, "ADD");
        assert_eq!(signal.entry_price, Some(2000.0));
        assert_eq!(signal.take_profit_1, Some(2040.0));
        assert_eq!(signal.take_profit_2, Some(2080.0));
        assert_eq!(signal.stop_loss, Some(1980.0));
        assert_eq!(signal.risk_reward_ratio, Some(2.0));
    }

    #[test]
    fn build_stage2_reevaluation_trade_signal_maps_to_no_trade() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let signal = build_stage2_reevaluation_trade_signal(
            ts_bucket,
            "schedule",
            "ETHUSDT",
            "custom_llm",
            "soft_invalidation_triplet",
        );

        assert_eq!(signal.decision, "NO_TRADE");
        assert_eq!(signal.reason, "soft_invalidation_triplet");
        assert!(signal.entry_price.is_none());
    }

    #[test]
    fn workflow_management_snapshot_uses_live_position_and_entry_snapshot_contract() {
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.25,
                entry_price: 2000.0,
                mark_price: 2015.0,
                unrealized_pnl: 18.75,
                leverage: 8,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 500.0,
        };
        let mut entry_snapshots = HashMap::new();
        entry_snapshots.insert(
            "ETHUSDT:LONG:path_a".to_string(),
            crate::workflow::schema::EntrySnapshot {
                symbol: "ETHUSDT".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                side: "LONG".to_string(),
                stop_loss: 1980.0,
                take_profit_1: 2040.0,
                take_profit_2: 2080.0,
                allowed_stop_loss_levels: vec![1980.0, 2010.0],
                allowed_take_profit_levels: vec![2040.0, 2080.0],
                tp1_realized: false,
                applied_driver_deterioration_signals: vec![],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        );

        let snapshot =
            build_workflow_management_snapshot(&trading_state, "ETHUSDT", &entry_snapshots)
                .expect("management snapshot");

        assert_eq!(snapshot.context_state, "active_positions");
        assert_eq!(snapshot.active_position_count, 1);
        assert_eq!(snapshot.positions[0].direction, "LONG");
        assert_eq!(snapshot.positions[0].current_tp_price, Some(2040.0));
        assert_eq!(snapshot.positions[0].current_sl_price, Some(1980.0));
        assert_eq!(
            snapshot
                .position_context
                .as_ref()
                .and_then(|item| item.effective_take_profit),
            Some(2040.0)
        );
        assert_eq!(
            snapshot
                .position_context
                .as_ref()
                .and_then(|item| item.effective_stop_loss),
            Some(1980.0)
        );
    }
    */
}
