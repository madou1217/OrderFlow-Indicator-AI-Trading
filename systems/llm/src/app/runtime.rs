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
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueuePurgeOptions},
    types::FieldTable,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::borrow::Cow;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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
const FAST_EVENT_BUFFER_RETENTION_SECS: i64 = 15 * 60;

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
static FAST_PRICE_EVENT_BUFFER: OnceLock<StdMutex<HashMap<String, VecDeque<FastPriceEvent>>>> =
    OnceLock::new();

fn workflow_stage_flights() -> &'static StdMutex<HashMap<String, WorkflowStageFlights>> {
    WORKFLOW_STAGE_FLIGHTS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn startup_stage1_refreshed_symbols() -> &'static StdMutex<HashSet<String>> {
    STARTUP_STAGE1_REFRESHED_SYMBOLS.get_or_init(|| StdMutex::new(HashSet::new()))
}

fn fast_price_event_buffer() -> &'static StdMutex<HashMap<String, VecDeque<FastPriceEvent>>> {
    FAST_PRICE_EVENT_BUFFER.get_or_init(|| StdMutex::new(HashMap::new()))
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
    ready_logged: bool,
    last_dispatch_block_reason: Option<String>,
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
                        process_fast_market_delivery(&ctx, &mut fast_watcher_state, delivery)
                            .await;
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
    let print_response = ctx.config.llm.print_response;
    let trigger = Arc::<str>::from(trigger.to_string());
    let invoke_ctx = ctx.clone();
    tokio::spawn(async move {
        invoke_bundle_models(invoke_ctx, print_response, bundle, trigger).await;
    });
}

async fn invoke_bundle_models(
    ctx: AppContext,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
) {
    let config = Arc::clone(&ctx.config);
    if !config.llm.workflow.enabled {
        debug!(
            symbol = %bundle.raw.symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            trigger = %trigger,
            "workflow invoke skipped because llm.workflow.enabled=false"
        );
        return;
    }
    if let Err(err) = invoke_workflow_bundle_models(ctx, print_response, bundle, trigger).await {
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
    if stage1_no_edge_retry_due(config, bundle, workflow_state, stage1_output) {
        return Some("scheduled_no_edge_retry".to_string());
    }
    None
}

fn stage1_no_edge_retry_due(
    config: &RootConfig,
    bundle: &LatestBundle,
    workflow_state: &crate::workflow::state::WorkflowState,
    stage1_output: Option<&crate::workflow::schema::Stage1Output>,
) -> bool {
    let Some(stage1_output) = stage1_output else {
        return false;
    };
    if stage1_output.monitoring_status != "no_edge" {
        return false;
    }
    let retry_minutes = &config.llm.workflow.stage1_no_edge_retry_minutes;
    if retry_minutes.is_empty() {
        return false;
    }
    let current_ts_bucket = bundle.raw.ts_bucket;
    let current_minute = current_ts_bucket.minute() as u8;
    if !retry_minutes.contains(&current_minute) {
        return false;
    }
    if workflow_state.last_stage1_refresh_reason.as_deref() != Some("scheduled_2h") {
        return false;
    }
    let Some(last_source_ts_bucket) = workflow_state.last_stage1_source_ts_bucket else {
        return false;
    };
    if last_source_ts_bucket >= current_ts_bucket {
        return false;
    }
    if last_source_ts_bucket.date_naive() != current_ts_bucket.date_naive()
        || last_source_ts_bucket.hour() != current_ts_bucket.hour()
        || last_source_ts_bucket.minute() != 0
    {
        return false;
    }
    true
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

fn startup_stage1_immediate_stage2a_due(
    stage1_refresh_reason: Option<&str>,
    stage1_refreshed_this_bundle: bool,
    stage1_output: Option<&crate::workflow::schema::Stage1Output>,
    trading_state: &TradingStateSnapshot,
) -> bool {
    if !stage1_refreshed_this_bundle || stage1_refresh_reason != Some("startup_force_stage1") {
        return false;
    }
    let Some(stage1_output) = stage1_output else {
        return false;
    };
    if stage1_output.monitoring_status != "active" || stage1_output.current_path.is_none() {
        return false;
    }
    !trading_state.has_active_positions && !trading_state.has_open_orders
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
    workflow_state.approved_tactical_plan_source_ts_bucket = None;
    workflow_state.approved_tactical_plan_replayed_at = None;
    workflow_state.pending_entry_bracket_template_override = None;
    expired_tactical_plan
}

fn clear_approved_tactical_plan(workflow_state: &mut crate::workflow::state::WorkflowState) {
    workflow_state.approved_tactical_plan = None;
    workflow_state.approved_tactical_plan_updated_at = None;
    workflow_state.approved_tactical_plan_source_ts_bucket = None;
    workflow_state.approved_tactical_plan_replayed_at = None;
    workflow_state.last_filled_context_key = None;
    workflow_state.filled_stopout_attempts = 0;
    workflow_state.pending_entry_bracket_template_override = None;
}

fn set_approved_tactical_plan(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    tactical_plan: crate::workflow::schema::TacticalEntryPlan,
    source_ts_bucket: DateTime<Utc>,
    replay_pending: bool,
) {
    let updated_at = Utc::now();
    workflow_state.approved_tactical_plan = Some(tactical_plan);
    workflow_state.approved_tactical_plan_updated_at = Some(updated_at);
    workflow_state.approved_tactical_plan_source_ts_bucket = Some(source_ts_bucket);
    workflow_state.approved_tactical_plan_replayed_at = if replay_pending {
        None
    } else {
        Some(updated_at)
    };
}

fn fast_event_matches(left: &FastPriceEvent, right: &FastPriceEvent) -> bool {
    left.event_ts == right.event_ts
        && left.source == right.source
        && left.routing_key == right.routing_key
        && (left.price - right.price).abs() <= f64::EPSILON
}

fn tactical_plan_replay_pending(workflow_state: &crate::workflow::state::WorkflowState) -> bool {
    workflow_state.approved_tactical_plan_replayed_at.is_none()
}

fn first_stop_touch_event_in_replay<'a>(
    entry_plan: &crate::workflow::schema::EntryPlan,
    replay_events: &'a [FastPriceEvent],
) -> Option<&'a FastPriceEvent> {
    replay_events
        .iter()
        .find(|replay_event| stop_loss_hit(entry_plan, replay_event.price))
}

fn tactical_plan_replay_cutoff(
    workflow_state: &crate::workflow::state::WorkflowState,
    fallback_cutoff: DateTime<Utc>,
) -> DateTime<Utc> {
    workflow_state
        .approved_tactical_plan_updated_at
        .unwrap_or(fallback_cutoff)
}

fn mark_tactical_plan_replay_completed(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    symbol: &str,
    path_id: &str,
    context_key: &str,
    replay_end: DateTime<Utc>,
    replay_count: usize,
    state_dir: &str,
) -> Result<()> {
    if workflow_state.approved_tactical_plan_replayed_at.is_some() {
        return Ok(());
    }
    let completed_at = Utc::now();
    workflow_state.approved_tactical_plan_replayed_at = Some(completed_at);
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    append_workflow_journal_event(
        "workflow_tactical_plan_replay_completed",
        symbol,
        replay_end,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": path_id,
            "context_key": context_key,
            "replay_count": replay_count,
            "completed_at": completed_at,
        }),
    );
    Ok(())
}

fn invalidate_tactical_plan_on_stop_touch(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    tactical_plan: &crate::workflow::schema::TacticalEntryPlan,
    symbol: &str,
    state_dir: &str,
    event: &FastPriceEvent,
    reason: &str,
) -> Result<bool> {
    if !stop_loss_hit(&tactical_plan.entry_plan, event.price) {
        return Ok(false);
    }
    clear_approved_tactical_plan(workflow_state);
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    append_workflow_journal_event(
        "workflow_tactical_plan_invalidated",
        symbol,
        event.event_ts,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": &tactical_plan.path_id,
            "context_key": workflow_entry_context_key(
                symbol,
                &tactical_plan.entry_plan.side,
                &tactical_plan.path_id,
            ),
            "reason": reason,
            "price_source": event.source.as_str(),
            "routing_key": &event.routing_key,
            "trigger_price": event.price,
            "stop_loss": tactical_plan.entry_plan.stop_loss,
            "side": &tactical_plan.entry_plan.side,
        }),
    );
    Ok(true)
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

fn activation_or_entry_zone<'a>(
    plan: &'a crate::workflow::schema::EntryPlan,
) -> &'a crate::workflow::schema::PriceZone {
    plan.entry_activation_level
        .as_ref()
        .unwrap_or(&plan.entry_zone)
}

fn entry_plan_reason(plan: &crate::workflow::schema::EntryPlan) -> String {
    format!(
        "entry_reason: {}; invalidation_reason: {}; stop_loss_reason: {}",
        plan.entry_reason, plan.invalidation_reason, plan.stop_loss_reason
    )
}

fn entry_plan_log_payload(
    path_id: &str,
    context_key: &str,
    plan: &crate::workflow::schema::EntryPlan,
) -> Value {
    json!({
        "path_id": path_id,
        "context_key": context_key,
        "side": &plan.side,
        "entry_profile": &plan.entry_profile,
        "intent_mode": &plan.intent_mode,
        "entry_activation_level": &plan.entry_activation_level,
        "entry_zone": &plan.entry_zone,
        "entry_invalidation_level": &plan.entry_invalidation_level,
        "stop_loss": plan.stop_loss,
        "leverage": plan.leverage,
        "max_drift_pct": plan.max_drift_pct,
        "entry_reason": &plan.entry_reason,
        "invalidation_reason": &plan.invalidation_reason,
        "stop_loss_reason": &plan.stop_loss_reason,
    })
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

fn record_fast_price_event_in_buffer(event: &FastPriceEvent) {
    let retention = ChronoDuration::seconds(FAST_EVENT_BUFFER_RETENTION_SECS);
    let min_ts = event.event_ts - retention;
    if let Ok(mut guard) = fast_price_event_buffer().lock() {
        let queue = guard
            .entry(event.symbol.to_ascii_uppercase())
            .or_insert_with(VecDeque::new);
        queue.push_back(event.clone());
        while queue
            .front()
            .map(|item| item.event_ts < min_ts)
            .unwrap_or(false)
        {
            queue.pop_front();
        }
    }
}

fn buffered_fast_price_events_in_range(
    symbol: &str,
    start_inclusive: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
) -> Vec<FastPriceEvent> {
    fast_price_event_buffer()
        .lock()
        .ok()
        .and_then(|guard| guard.get(&symbol.to_ascii_uppercase()).cloned())
        .map(|queue| {
            queue
                .into_iter()
                .filter(|event| event.event_ts >= start_inclusive && event.event_ts < end_exclusive)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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
) -> bool {
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
    should_reset
}

fn fast_entry_selection_block_reason(
    symbol: &str,
    tactical_plan: &crate::workflow::schema::TacticalEntryPlan,
    workflow_state: &crate::workflow::state::WorkflowState,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    max_filled_stopout_attempts: u8,
) -> Option<&'static str> {
    let side = tactical_plan.entry_plan.side.as_str();
    if has_active_position_for_side(trading_state, side) {
        return Some("active_position_exists");
    }
    if live_entry_order_count_for_side(trading_state, side) > 0 {
        return Some("live_entry_order_exists");
    }
    if workflow_state.filled_stopout_attempts >= max_filled_stopout_attempts {
        return Some("max_filled_stopout_attempts_reached");
    }
    let context_key = workflow_entry_context_key(symbol, side, &tactical_plan.path_id);
    if entry_snapshots.contains_key(&context_key) {
        return Some("entry_snapshot_exists");
    }
    None
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

fn pullback_dispatch_price_ok(plan: &crate::workflow::schema::EntryPlan, price: f64) -> bool {
    match plan.side.as_str() {
        // Pullback entries post a passive order back into the entry_zone, so
        // dispatch is still valid while price is inside the zone or trading on
        // the favorable side above it. Once price trades through the far side
        // of the zone, the pullback is no longer clean enough to arm.
        "LONG" => price >= plan.entry_zone.low,
        "SHORT" => price <= plan.entry_zone.high,
        _ => false,
    }
}

fn inside_or_beyond_activation(plan: &crate::workflow::schema::EntryPlan, price: f64) -> bool {
    let activation_zone = activation_or_entry_zone(plan);
    activation_zone.contains(price) || favorable_beyond_zone(&plan.side, price, activation_zone)
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
            state.activation_seen_at.is_some() && pullback_dispatch_price_ok(plan, event.price)
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
    max_filled_stopout_attempts: u8,
) -> Option<SelectedEntryPlan<'a>> {
    if !entry_ready {
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

async fn process_fast_market_event_for_plan(
    ctx: &AppContext,
    workflow_state: &mut crate::workflow::state::WorkflowState,
    current_path: &crate::workflow::schema::CurrentPath,
    tactical_plan: &crate::workflow::schema::TacticalEntryPlan,
    symbol: &str,
    state_dir: &str,
    fast_plan_state: &mut FastWatcherPlanState,
    context_key: &str,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    trading_state: &TradingStateSnapshot,
    event: &FastPriceEvent,
) -> Result<()> {
    if entry_snapshots.contains_key(context_key) {
        fast_plan_state.fired = true;
        return Ok(());
    }

    let entry_ready = fast_watcher_entry_ready(
        fast_plan_state,
        &tactical_plan.entry_plan,
        event,
        &ctx.config.llm.workflow.watcher,
    );
    if !entry_ready {
        fast_plan_state.ready_logged = false;
        fast_plan_state.last_dispatch_block_reason = None;
        return Ok(());
    }
    if !fast_plan_state.ready_logged {
        info!(
            symbol = %symbol,
            trigger = "watcher_fast_consumer",
            path_id = %tactical_plan.path_id,
            context_key = %context_key,
            price_source = event.source.as_str(),
            routing_key = %event.routing_key,
            trigger_price = event.price,
            side = %tactical_plan.entry_plan.side,
            entry_profile = %tactical_plan.entry_plan.entry_profile,
            intent_mode = %tactical_plan.entry_plan.intent_mode,
            entry_zone_low = tactical_plan.entry_plan.entry_zone.low,
            entry_zone_high = tactical_plan.entry_plan.entry_zone.high,
            invalidation_low = tactical_plan.entry_plan.entry_invalidation_level.low,
            invalidation_high = tactical_plan.entry_plan.entry_invalidation_level.high,
            stop_loss = tactical_plan.entry_plan.stop_loss,
            "workflow watcher entry ready"
        );
        append_workflow_journal_event(
            "workflow_entry_ready",
            symbol,
            event.event_ts,
            json!({
                "trigger": "watcher_fast_consumer",
                "path_id": &tactical_plan.path_id,
                "context_key": context_key,
                "price_source": event.source.as_str(),
                "routing_key": &event.routing_key,
                "trigger_price": event.price,
                "entry_plan": entry_plan_log_payload(
                    &tactical_plan.path_id,
                    context_key,
                    &tactical_plan.entry_plan,
                ),
            }),
        );
        fast_plan_state.ready_logged = true;
    }

    if let Some(reason) = fast_entry_selection_block_reason(
        symbol,
        tactical_plan,
        workflow_state,
        trading_state,
        entry_snapshots,
        ctx.config.llm.workflow.watcher.max_filled_stopout_attempts,
    ) {
        if fast_plan_state.last_dispatch_block_reason.as_deref() != Some(reason) {
            info!(
                symbol = %symbol,
                trigger = "watcher_fast_consumer",
                path_id = %tactical_plan.path_id,
                context_key = %context_key,
                block_reason = reason,
                trigger_price = event.price,
                "workflow watcher entry ready but dispatch blocked"
            );
            append_workflow_journal_event(
                "workflow_entry_blocked",
                symbol,
                event.event_ts,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "path_id": &tactical_plan.path_id,
                    "context_key": context_key,
                    "block_reason": reason,
                    "trigger_price": event.price,
                    "price_source": event.source.as_str(),
                    "routing_key": &event.routing_key,
                    "filled_stopout_attempts": workflow_state.filled_stopout_attempts,
                }),
            );
        }
        fast_plan_state.last_dispatch_block_reason = Some(reason.to_string());
        return Ok(());
    }
    fast_plan_state.last_dispatch_block_reason = None;
    let Some(selected_entry_plan) = select_fast_entry_plan(
        symbol,
        tactical_plan,
        workflow_state,
        trading_state,
        entry_snapshots,
        event.price,
        entry_ready,
        ctx.config.llm.workflow.watcher.max_filled_stopout_attempts,
    ) else {
        return Ok(());
    };

    let intent = execution_intent_from_entry_plan(
        symbol,
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
    info!(
        symbol = %symbol,
        trigger = "watcher_fast_consumer",
        path_id = %intent.path_id,
        context_key = %intent.entry_snapshot.context_key,
        price_source = event.source.as_str(),
        routing_key = %event.routing_key,
        trigger_price = event.price,
        side = %intent.side,
        intent_mode = %intent.intent_mode,
        stop_loss = intent.stop_loss,
        take_profit_1 = intent.take_profit_1,
        take_profit_2 = intent.take_profit_2,
        "workflow watcher dispatching entry intent"
    );
    append_workflow_journal_event(
        "workflow_entry_dispatch",
        symbol,
        event.event_ts,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": &intent.path_id,
            "context_key": &intent.entry_snapshot.context_key,
            "trigger_price": event.price,
            "price_source": event.source.as_str(),
            "routing_key": &event.routing_key,
            "entry_plan": entry_plan_log_payload(
                &tactical_plan.path_id,
                context_key,
                selected_entry_plan.plan,
            ),
            "execution_intent": {
                "side": &intent.side,
                "intent_mode": &intent.intent_mode,
                "stop_loss": intent.stop_loss,
                "take_profit_1": intent.take_profit_1,
                "take_profit_2": intent.take_profit_2,
                "ttl_minutes": intent.ttl_minutes,
            }
        }),
    );

    match adapt_execution_intent(&intent) {
        Ok(adapted_intent) => match execute_workflow_execution_intent(
            &ctx.http_client,
            &ctx.config.api.binance,
            &ctx.config.llm.execution,
            symbol,
            &adapted_intent,
        )
        .await
        {
            Ok(report) => {
                append_workflow_journal_event(
                    "workflow_execution_report",
                    symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "path_id": &intent.path_id,
                        "context_key": &intent.entry_snapshot.context_key,
                        "price_source": event.source.as_str(),
                        "routing_key": &event.routing_key,
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
                        symbol,
                        &intent,
                        current_path,
                        Utc::now(),
                    );
                    crate::workflow::persistence::save_entry_snapshot(state_dir, &snapshot)?;
                    entry_snapshots.insert(snapshot.context_key.clone(), snapshot);
                    workflow_state.last_filled_context_key =
                        Some(intent.entry_snapshot.context_key.clone());
                    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
                }

                let signal = build_execution_trade_signal(
                    event.event_ts,
                    "watcher_fast_consumer",
                    symbol,
                    "workflow_watcher_fast",
                    trading_state,
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
                    symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "path_id": &intent.path_id,
                        "context_key": &intent.entry_snapshot.context_key,
                        "price_source": event.source.as_str(),
                        "routing_key": &event.routing_key,
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
                symbol,
                event.event_ts,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "path_id": &intent.path_id,
                    "context_key": &intent.entry_snapshot.context_key,
                    "price_source": event.source.as_str(),
                    "routing_key": &event.routing_key,
                    "trigger_price": event.price,
                    "error": format!("{err:#}"),
                    "phase": "intent_adapter",
                }),
            );
        }
    }

    Ok(())
}

fn fast_management_snapshot_for_context(
    symbol: &str,
    current_path: Option<&crate::workflow::schema::CurrentPath>,
    workflow_state: &crate::workflow::state::WorkflowState,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    context_key: &str,
    path_id: &str,
) -> Option<crate::workflow::schema::EntrySnapshot> {
    entry_snapshots.get(context_key).cloned().or_else(|| {
        current_path.and_then(|path| {
            snapshot_for_management_context(
                symbol,
                path,
                workflow_state,
                entry_snapshots,
                context_key,
                path_id,
            )
        })
    })
}

async fn process_fast_position_management_actions(
    ctx: &AppContext,
    workflow_state: &mut crate::workflow::state::WorkflowState,
    current_path: Option<&crate::workflow::schema::CurrentPath>,
    symbol: &str,
    state_dir: &str,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    trading_state: &TradingStateSnapshot,
    watch_facts: &WatcherPriceFacts,
    event: &FastPriceEvent,
) -> Result<bool> {
    let mut state_dirty = false;
    let plan_context_keys = workflow_state
        .approved_position_management_plans
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plan_context_key in plan_context_keys {
        let Some(plan) = workflow_state
            .approved_position_management_plans
            .get(&plan_context_key)
            .cloned()
        else {
            continue;
        };
        let Some(action_index) =
            first_triggered_position_management_action_index(&plan, watch_facts)
        else {
            continue;
        };
        let action = plan.actions[action_index].clone();
        let Some(snapshot) = fast_management_snapshot_for_context(
            symbol,
            current_path,
            workflow_state,
            entry_snapshots,
            &action.context_key,
            &action.path_id,
        ) else {
            info!(
                symbol = %symbol,
                trigger = "watcher_fast_consumer",
                context_key = %action.context_key,
                path_id = %action.path_id,
                action_type = %action.action_type,
                trigger_price = watch_facts.current_price,
                "workflow watcher position management action skipped because snapshot is missing"
            );
            append_workflow_journal_event(
                "workflow_stage2b_management_skipped",
                symbol,
                event.event_ts,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "context_key": &action.context_key,
                    "path_id": &action.path_id,
                    "action": &action,
                    "trigger_price": watch_facts.current_price,
                    "price_source": event.source.as_str(),
                    "routing_key": &event.routing_key,
                    "reason": "snapshot_missing",
                }),
            );
            if reconcile_missing_position_management_snapshot(
                &ctx.http_client,
                &ctx.config.api.binance,
                &ctx.config.llm.execution,
                symbol,
                state_dir,
                workflow_state,
                entry_snapshots,
                &action.context_key,
                &action.path_id,
                "watcher_fast_consumer",
                event.event_ts,
            )
            .await?
            {
                state_dirty = true;
            }
            continue;
        };
        info!(
            symbol = %symbol,
            trigger = "watcher_fast_consumer",
            path_id = %action.path_id,
            context_key = %action.context_key,
            action_type = %action.action_type,
            trigger_price = watch_facts.current_price,
            execution_price = ?action.execution_price,
            price_source = event.source.as_str(),
            routing_key = %event.routing_key,
            "workflow watcher position management action ready"
        );
        append_workflow_journal_event(
            "workflow_stage2b_management_ready",
            symbol,
            event.event_ts,
            json!({
                "trigger": "watcher_fast_consumer",
                "context_key": &action.context_key,
                "path_id": &action.path_id,
                "action": &action,
                "trigger_price": watch_facts.current_price,
                "price_source": event.source.as_str(),
                "routing_key": &event.routing_key,
            }),
        );
        match action.action_type.as_str() {
            "add" => {
                let Some(current_path) = current_path else {
                    append_workflow_journal_event(
                        "workflow_stage2b_add_execution_error",
                        symbol,
                        event.event_ts,
                        json!({
                            "trigger": "watcher_fast_consumer",
                            "action": &action,
                            "error": "missing active current_path for add execution",
                        }),
                    );
                    if state_dirty {
                        crate::workflow::persistence::save_workflow_state(
                            state_dir,
                            workflow_state,
                        )?;
                    }
                    return Ok(true);
                };
                let fallback_plan = matching_tactical_entry_plan(workflow_state, &action.path_id);
                match (
                    entry_plan_from_snapshot_template(&snapshot, fallback_plan),
                    find_active_position_for_side(trading_state, &snapshot.side),
                ) {
                    (Ok(entry_template), Some(active_position)) => {
                        let bracket_template = crate::workflow::schema::PostFillBracketTemplate {
                            take_profit_1: snapshot.take_profit_1,
                            take_profit_2: snapshot.take_profit_2,
                            stop_loss: snapshot.stop_loss,
                        };
                        let mut intent = execution_intent_from_entry_plan(
                            symbol,
                            &action.path_id,
                            &entry_template,
                            current_path,
                            Some(&bracket_template),
                            watch_facts.current_price,
                            ctx.config.llm.workflow.watcher.entry_ttl_minutes,
                            action
                                .add_ratio
                                .map(|ratio| active_position.position_amt.abs() * ratio),
                        );
                        intent.reason = Some(action.reason.clone());
                        match adapt_execution_intent(&intent) {
                            Ok(adapted_intent) => {
                                match execute_workflow_execution_intent(
                                    &ctx.http_client,
                                    &ctx.config.api.binance,
                                    &ctx.config.llm.execution,
                                    symbol,
                                    &adapted_intent,
                                )
                                .await
                                {
                                    Ok(report) => {
                                        append_workflow_journal_event(
                                            "workflow_stage2b_add_execution_report",
                                            symbol,
                                            event.event_ts,
                                            json!({
                                                "trigger": "watcher_fast_consumer",
                                                "action": &action,
                                                "trigger_price": watch_facts.current_price,
                                                "price_source": event.source.as_str(),
                                                "routing_key": &event.routing_key,
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
                                                state_dir,
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
                                                    workflow_state,
                                                    &plan_context_key,
                                                    next_plan,
                                                );
                                            } else {
                                                remove_position_management_plan(
                                                    workflow_state,
                                                    &plan_context_key,
                                                );
                                            }
                                            state_dirty = true;
                                        }
                                        let signal = build_execution_trade_signal(
                                            event.event_ts,
                                            "watcher_fast_consumer",
                                            symbol,
                                            "workflow_stage2b_watcher_fast",
                                            trading_state,
                                            &intent,
                                            Some(&report),
                                            intent.reason.as_deref(),
                                        );
                                        let telegram_operator =
                                            TelegramOperator::from_config(&ctx.config.api.telegram);
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
                                        append_workflow_journal_event(
                                            "workflow_stage2b_add_execution_error",
                                            symbol,
                                            event.event_ts,
                                            json!({
                                                "trigger": "watcher_fast_consumer",
                                                "action": &action,
                                                "trigger_price": watch_facts.current_price,
                                                "price_source": event.source.as_str(),
                                                "routing_key": &event.routing_key,
                                                "error": format!("{err:#}"),
                                            }),
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                append_workflow_journal_event(
                                    "workflow_stage2b_add_execution_error",
                                    symbol,
                                    event.event_ts,
                                    json!({
                                        "trigger": "watcher_fast_consumer",
                                        "action": &action,
                                        "trigger_price": watch_facts.current_price,
                                        "price_source": event.source.as_str(),
                                        "routing_key": &event.routing_key,
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
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
                                "error": format!("{err:#}"),
                                "phase": "entry_template",
                            }),
                        );
                    }
                    (_, None) => {
                        append_workflow_journal_event(
                            "workflow_stage2b_add_execution_error",
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
                                "error": "no active position available for add execution",
                            }),
                        );
                    }
                }
            }
            "reduce" | "exit_full" | "move_stop" | "update_take_profit" => {
                match management_action_from_position_management_action(&action).and_then(
                    |management_action| {
                        let adapted = adapt_management_action(&management_action, &snapshot)?;
                        Ok((management_action, adapted))
                    },
                ) {
                    Ok((management_action, adapted_action)) => {
                        match execute_workflow_management_action(
                            &ctx.http_client,
                            &ctx.config.api.binance,
                            &ctx.config.llm.execution,
                            symbol,
                            &snapshot,
                            &adapted_action,
                        )
                        .await
                        {
                            Ok(report) => {
                                append_workflow_journal_event(
                                    "workflow_stage2b_management_execution_report",
                                    symbol,
                                    event.event_ts,
                                    json!({
                                        "trigger": "watcher_fast_consumer",
                                        "action": &action,
                                        "trigger_price": watch_facts.current_price,
                                        "price_source": event.source.as_str(),
                                        "routing_key": &event.routing_key,
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
                                                state_dir,
                                                symbol,
                                                &snapshot.context_key,
                                            )?;
                                            clear_approved_tactical_plan(workflow_state);
                                            remove_position_management_plan(
                                                workflow_state,
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
                                                state_dir,
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
                                                    workflow_state,
                                                    &plan_context_key,
                                                    next_plan,
                                                );
                                            } else {
                                                remove_position_management_plan(
                                                    workflow_state,
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
                                                state_dir,
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
                                                    workflow_state,
                                                    &plan_context_key,
                                                    next_plan,
                                                );
                                            } else {
                                                remove_position_management_plan(
                                                    workflow_state,
                                                    &plan_context_key,
                                                );
                                            }
                                        }
                                        _ => {
                                            let mut next_snapshot = snapshot.clone();
                                            next_snapshot.updated_at = Utc::now();
                                            crate::workflow::persistence::save_entry_snapshot(
                                                state_dir,
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
                                                    workflow_state,
                                                    &plan_context_key,
                                                    next_plan,
                                                );
                                            } else {
                                                remove_position_management_plan(
                                                    workflow_state,
                                                    &plan_context_key,
                                                );
                                            }
                                        }
                                    }
                                    state_dirty = true;
                                }
                                let signal = build_management_trade_signal(
                                    event.event_ts,
                                    "watcher_fast_consumer",
                                    symbol,
                                    "workflow_stage2b_watcher_fast",
                                    trading_state,
                                    &snapshot,
                                    &management_action,
                                    &report,
                                );
                                let telegram_operator =
                                    TelegramOperator::from_config(&ctx.config.api.telegram);
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
                                append_workflow_journal_event(
                                    "workflow_stage2b_management_execution_error",
                                    symbol,
                                    event.event_ts,
                                    json!({
                                        "trigger": "watcher_fast_consumer",
                                        "action": &action,
                                        "trigger_price": watch_facts.current_price,
                                        "price_source": event.source.as_str(),
                                        "routing_key": &event.routing_key,
                                        "error": format!("{err:#}"),
                                    }),
                                );
                            }
                        }
                    }
                    Err(err) => {
                        append_workflow_journal_event(
                            "workflow_stage2b_management_execution_error",
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
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
                    symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "action": &action,
                        "trigger_price": watch_facts.current_price,
                        "price_source": event.source.as_str(),
                        "routing_key": &event.routing_key,
                        "error": format!("unsupported stage2b watcher action {}", other),
                    }),
                );
            }
        }

        if state_dirty {
            crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
        }
        return Ok(true);
    }

    if state_dirty {
        crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    }
    Ok(false)
}

async fn process_fast_pending_order_management_actions(
    ctx: &AppContext,
    workflow_state: &mut crate::workflow::state::WorkflowState,
    current_path: Option<&crate::workflow::schema::CurrentPath>,
    symbol: &str,
    state_dir: &str,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    trading_state: &TradingStateSnapshot,
    watch_facts: &WatcherPriceFacts,
    event: &FastPriceEvent,
) -> Result<bool> {
    let mut state_dirty = false;
    let plan_context_keys = workflow_state
        .approved_pending_order_management_plans
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plan_context_key in plan_context_keys {
        let Some(plan) = workflow_state
            .approved_pending_order_management_plans
            .get(&plan_context_key)
            .cloned()
        else {
            continue;
        };
        if current_path
            .as_ref()
            .is_some_and(|path| plan.path_id != path.id)
        {
            remove_pending_order_management_plan(workflow_state, &plan_context_key);
            state_dirty = true;
            continue;
        }
        let Some(action_index) = first_triggered_pending_order_action_index(&plan, watch_facts)
        else {
            continue;
        };
        let action = plan.actions[action_index].clone();
        let Some(snapshot) = fast_management_snapshot_for_context(
            symbol,
            current_path,
            workflow_state,
            entry_snapshots,
            &action.context_key,
            &action.path_id,
        ) else {
            continue;
        };
        let has_live_position = has_active_position_for_side(trading_state, &snapshot.side);
        info!(
            symbol = %symbol,
            trigger = "watcher_fast_consumer",
            path_id = %action.path_id,
            context_key = %action.context_key,
            action_type = %action.action_type,
            trigger_price = watch_facts.current_price,
            execution_price = ?action.execution_price,
            price_source = event.source.as_str(),
            routing_key = %event.routing_key,
            "workflow watcher pending-order management action ready"
        );
        append_workflow_journal_event(
            "workflow_stage2c_pending_order_ready",
            symbol,
            event.event_ts,
            json!({
                "trigger": "watcher_fast_consumer",
                "context_key": &action.context_key,
                "path_id": &action.path_id,
                "action": &action,
                "trigger_price": watch_facts.current_price,
                "price_source": event.source.as_str(),
                "routing_key": &event.routing_key,
            }),
        );
        match action.action_type.as_str() {
            "cancel_pending_order" => {
                match cancel_workflow_pending_entry_orders(
                    &ctx.http_client,
                    &ctx.config.api.binance,
                    &ctx.config.llm.execution,
                    symbol,
                    &snapshot.side,
                )
                .await
                {
                    Ok(canceled_order_ids) => {
                        append_workflow_journal_event(
                            "workflow_stage2c_pending_order_execution_report",
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
                                "canceled_order_ids": canceled_order_ids,
                                "dry_run": ctx.config.llm.execution.dry_run,
                            }),
                        );
                        if !ctx.config.llm.execution.dry_run {
                            if !has_live_position {
                                entry_snapshots.remove(&snapshot.context_key);
                                crate::workflow::persistence::delete_entry_snapshot(
                                    state_dir,
                                    symbol,
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
                                clear_approved_tactical_plan(workflow_state);
                            }
                            workflow_state.pending_entry_bracket_template_override = None;
                            if let Some(next_plan) =
                                remove_pending_order_management_action(&plan, action_index)
                            {
                                upsert_pending_order_management_plan(
                                    workflow_state,
                                    &plan_context_key,
                                    next_plan,
                                );
                            } else {
                                remove_pending_order_management_plan(
                                    workflow_state,
                                    &plan_context_key,
                                );
                            }
                            state_dirty = true;
                        }
                    }
                    Err(err) => {
                        append_workflow_journal_event(
                            "workflow_stage2c_pending_order_execution_error",
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
                                "error": format!("{err:#}"),
                            }),
                        );
                    }
                }
            }
            "replace_entry" => {
                let Some(current_path) = current_path else {
                    append_workflow_journal_event(
                        "workflow_stage2c_pending_order_execution_error",
                        symbol,
                        event.event_ts,
                        json!({
                            "trigger": "watcher_fast_consumer",
                            "action": &action,
                            "trigger_price": watch_facts.current_price,
                            "price_source": event.source.as_str(),
                            "routing_key": &event.routing_key,
                            "error": "missing active current_path for replace_entry execution",
                        }),
                    );
                    if state_dirty {
                        crate::workflow::persistence::save_workflow_state(
                            state_dir,
                            workflow_state,
                        )?;
                    }
                    return Ok(true);
                };
                match build_stage2c_replace_execution_intent(
                    symbol,
                    current_path,
                    workflow_state,
                    &snapshot,
                    &action,
                    watch_facts.current_price,
                    ctx.config.llm.workflow.watcher.entry_ttl_minutes,
                ) {
                    Ok((next_tactical_plan, bracket_template, intent)) => {
                        match adapt_execution_intent(&intent) {
                            Ok(adapted_intent) => {
                                match cancel_workflow_pending_entry_orders(
                                    &ctx.http_client,
                                    &ctx.config.api.binance,
                                    &ctx.config.llm.execution,
                                    symbol,
                                    &snapshot.side,
                                )
                                .await
                                {
                                    Ok(canceled_order_ids) => {
                                        append_workflow_journal_event(
                                            "workflow_stage2c_pending_order_execution_report",
                                            symbol,
                                            event.event_ts,
                                            json!({
                                                "trigger": "watcher_fast_consumer",
                                                "action": &action,
                                                "trigger_price": watch_facts.current_price,
                                                "price_source": event.source.as_str(),
                                                "routing_key": &event.routing_key,
                                                "canceled_order_ids": canceled_order_ids,
                                                "dry_run": ctx.config.llm.execution.dry_run,
                                            }),
                                        );
                                        match execute_workflow_execution_intent(
                                            &ctx.http_client,
                                            &ctx.config.api.binance,
                                            &ctx.config.llm.execution,
                                            symbol,
                                            &adapted_intent,
                                        )
                                        .await
                                        {
                                            Ok(report) => {
                                                append_workflow_journal_event(
                                                    "workflow_stage2c_replace_execution_report",
                                                    symbol,
                                                    event.event_ts,
                                                    json!({
                                                        "trigger": "watcher_fast_consumer",
                                                        "action": &action,
                                                        "trigger_price": watch_facts.current_price,
                                                        "price_source": event.source.as_str(),
                                                        "routing_key": &event.routing_key,
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
                                                    set_approved_tactical_plan(
                                                        workflow_state,
                                                        next_tactical_plan,
                                                        event.event_ts,
                                                        false,
                                                    );
                                                    workflow_state
                                                        .pending_entry_bracket_template_override =
                                                        Some(bracket_template);
                                                    let next_snapshot =
                                                        crate::workflow::management::snapshot_from_execution_intent(
                                                            symbol,
                                                            &intent,
                                                            current_path,
                                                            Utc::now(),
                                                        );
                                                    crate::workflow::persistence::save_entry_snapshot(
                                                        state_dir,
                                                        &next_snapshot,
                                                    )?;
                                                    entry_snapshots.insert(
                                                        next_snapshot.context_key.clone(),
                                                        next_snapshot,
                                                    );
                                                    workflow_state.last_filled_context_key = Some(
                                                        intent.entry_snapshot.context_key.clone(),
                                                    );
                                                    if let Some(next_plan) =
                                                        remove_pending_order_management_action(
                                                            &plan,
                                                            action_index,
                                                        )
                                                    {
                                                        upsert_pending_order_management_plan(
                                                            workflow_state,
                                                            &plan_context_key,
                                                            next_plan,
                                                        );
                                                    } else {
                                                        remove_pending_order_management_plan(
                                                            workflow_state,
                                                            &plan_context_key,
                                                        );
                                                    }
                                                    state_dirty = true;
                                                }
                                                let signal = build_execution_trade_signal(
                                                    event.event_ts,
                                                    "watcher_fast_consumer",
                                                    symbol,
                                                    "workflow_stage2c_watcher_fast",
                                                    trading_state,
                                                    &intent,
                                                    Some(&report),
                                                    intent.reason.as_deref(),
                                                );
                                                let telegram_operator =
                                                    TelegramOperator::from_config(
                                                        &ctx.config.api.telegram,
                                                    );
                                                let x_operator =
                                                    XOperator::from_config(&ctx.config.api.x);
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
                                                append_workflow_journal_event(
                                                    "workflow_stage2c_pending_order_execution_error",
                                                    symbol,
                                                    event.event_ts,
                                                    json!({
                                                        "trigger": "watcher_fast_consumer",
                                                        "action": &action,
                                                        "trigger_price": watch_facts.current_price,
                                                        "price_source": event.source.as_str(),
                                                        "routing_key": &event.routing_key,
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
                                            symbol,
                                            event.event_ts,
                                            json!({
                                                "trigger": "watcher_fast_consumer",
                                                "action": &action,
                                                "trigger_price": watch_facts.current_price,
                                                "price_source": event.source.as_str(),
                                                "routing_key": &event.routing_key,
                                                "error": format!("{err:#}"),
                                            }),
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                append_workflow_journal_event(
                                    "workflow_stage2c_pending_order_execution_error",
                                    symbol,
                                    event.event_ts,
                                    json!({
                                        "trigger": "watcher_fast_consumer",
                                        "action": &action,
                                        "trigger_price": watch_facts.current_price,
                                        "price_source": event.source.as_str(),
                                        "routing_key": &event.routing_key,
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
                            symbol,
                            event.event_ts,
                            json!({
                                "trigger": "watcher_fast_consumer",
                                "action": &action,
                                "trigger_price": watch_facts.current_price,
                                "price_source": event.source.as_str(),
                                "routing_key": &event.routing_key,
                                "error": format!("{err:#}"),
                                "phase": "replace_template",
                            }),
                        );
                    }
                }
            }
            "update_post_fill_bracket_template" => {
                append_workflow_journal_event(
                    "workflow_stage2c_pending_order_execution_report",
                    symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "action": &action,
                        "trigger_price": watch_facts.current_price,
                        "price_source": event.source.as_str(),
                        "routing_key": &event.routing_key,
                        "dry_run": ctx.config.llm.execution.dry_run,
                    }),
                );
                if !ctx.config.llm.execution.dry_run {
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
                                state_dir,
                                &next_snapshot,
                            )?;
                            entry_snapshots
                                .insert(next_snapshot.context_key.clone(), next_snapshot);
                        }
                    }
                    if let Some(next_plan) =
                        remove_pending_order_management_action(&plan, action_index)
                    {
                        upsert_pending_order_management_plan(
                            workflow_state,
                            &plan_context_key,
                            next_plan,
                        );
                    } else {
                        remove_pending_order_management_plan(workflow_state, &plan_context_key);
                    }
                    state_dirty = true;
                }
            }
            other => {
                append_workflow_journal_event(
                    "workflow_stage2c_pending_order_execution_error",
                    symbol,
                    event.event_ts,
                    json!({
                        "trigger": "watcher_fast_consumer",
                        "action": &action,
                        "trigger_price": watch_facts.current_price,
                        "price_source": event.source.as_str(),
                        "routing_key": &event.routing_key,
                        "error": format!("unsupported stage2c watcher action {}", other),
                    }),
                );
            }
        }

        if state_dirty {
            crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
        }
        return Ok(true);
    }

    if state_dirty {
        crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    }
    Ok(false)
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
        .unwrap_or_else(|| {
            current_path
                .first_path_target
                .directional_target(&plan.side)
        });
    let take_profit_2 = bracket_override
        .map(|item| item.take_profit_2)
        .unwrap_or_else(|| current_path.next_path_target.directional_target(&plan.side));
    let stop_loss = bracket_override
        .map(|item| item.stop_loss)
        .unwrap_or(plan.stop_loss);
    crate::workflow::schema::ExecutionIntent {
        side: plan.side.clone(),
        entry_profile: Some(plan.entry_profile.clone()),
        intent_mode: plan.intent_mode.clone(),
        entry_activation_level: plan.entry_activation_level.clone(),
        entry_zone: plan.entry_zone.clone(),
        entry_invalidation_level: Some(plan.entry_invalidation_level.clone()),
        trigger_price: Some(trigger_price),
        stop_loss,
        take_profit_1,
        take_profit_2,
        ttl_minutes,
        leverage: plan.leverage,
        max_drift_pct: plan.max_drift_pct,
        path_id: path_id.to_string(),
        entry_snapshot: crate::workflow::schema::EntrySnapshotRef {
            context_key: workflow_entry_context_key(symbol, &plan.side, path_id),
            path_id: path_id.to_string(),
        },
        reason: Some(entry_plan_reason(plan)),
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
) -> Option<crate::workflow::schema::EntrySnapshot> {
    let stop_loss = bracket_override
        .map(|item| item.stop_loss)
        .or_else(|| fallback_plan.map(|plan| plan.stop_loss))?;
    Some(crate::workflow::schema::EntrySnapshot {
        symbol: symbol.to_ascii_uppercase(),
        context_key: context_key.to_string(),
        path_id: path_id.to_string(),
        side: fallback_plan
            .map(|plan| plan.side.clone())
            .unwrap_or_else(|| current_path.side.clone()),
        entry_profile: fallback_plan.map(|plan| plan.entry_profile.clone()),
        intent_mode: fallback_plan.map(|plan| plan.intent_mode.clone()),
        entry_activation_level: fallback_plan.and_then(|plan| plan.entry_activation_level.clone()),
        entry_zone: fallback_plan.map(|plan| plan.entry_zone.clone()),
        entry_invalidation_level: fallback_plan.map(|plan| plan.entry_invalidation_level.clone()),
        max_drift_pct: fallback_plan.map(|plan| plan.max_drift_pct),
        leverage: fallback_plan.map(|plan| plan.leverage),
        stop_loss,
        take_profit_1: bracket_override
            .map(|item| item.take_profit_1)
            .unwrap_or_else(|| {
                current_path
                    .first_path_target
                    .directional_target(&current_path.side)
            }),
        take_profit_2: bracket_override
            .map(|item| item.take_profit_2)
            .unwrap_or_else(|| {
                current_path
                    .next_path_target
                    .directional_target(&current_path.side)
            }),
        allowed_stop_loss_levels: vec![],
        allowed_take_profit_levels: vec![],
        tp1_realized: false,
        applied_driver_deterioration_signals: Vec::new(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    })
}

fn snapshot_for_management_context(
    symbol: &str,
    current_path: &crate::workflow::schema::CurrentPath,
    workflow_state: &crate::workflow::state::WorkflowState,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    context_key: &str,
    path_id: &str,
) -> Option<crate::workflow::schema::EntrySnapshot> {
    entry_snapshots.get(context_key).cloned().or_else(|| {
        if path_id != current_path.id {
            return None;
        }
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
        .or_else(|| fallback_plan.and_then(|plan| plan.entry_activation_level.clone()));
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
        .or_else(|| {
            crate::workflow::schema::derive_max_drift_pct(
                &snapshot.side,
                &entry_invalidation_level,
                snapshot.stop_loss,
            )
            .ok()
        })
        .ok_or_else(|| {
            anyhow!(
                "entry template missing max_drift_pct for {}",
                snapshot.context_key
            )
        })?;
    let leverage = snapshot
        .leverage
        .or_else(|| fallback_plan.map(|plan| plan.leverage))
        .unwrap_or_else(crate::workflow::schema::default_stage2a_leverage);

    Ok(crate::workflow::schema::EntryPlan {
        side: snapshot.side.clone(),
        entry_profile,
        intent_mode,
        entry_activation_level,
        entry_zone,
        entry_invalidation_level,
        stop_loss: snapshot.stop_loss,
        leverage,
        max_drift_pct,
        entry_reason: fallback_plan
            .map(|plan| plan.entry_reason.clone())
            .unwrap_or_else(|| "reused_from_snapshot_template".to_string()),
        invalidation_reason: fallback_plan
            .map(|plan| plan.invalidation_reason.clone())
            .unwrap_or_else(|| "reused_from_snapshot_template".to_string()),
        stop_loss_reason: fallback_plan
            .map(|plan| plan.stop_loss_reason.clone())
            .unwrap_or_else(|| "reused_from_snapshot_template".to_string()),
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

fn workflow_side_from_context_key(context_key: &str) -> Option<&'static str> {
    let mut parts = context_key.split(':');
    let _symbol = parts.next()?;
    let side = parts.next()?;
    if side.eq_ignore_ascii_case("LONG") {
        Some("LONG")
    } else if side.eq_ignore_ascii_case("SHORT") {
        Some("SHORT")
    } else if side.eq_ignore_ascii_case("BOTH") {
        Some("BOTH")
    } else {
        None
    }
}

async fn reconcile_missing_position_management_snapshot(
    http_client: &Client,
    api_config: &crate::app::config::BinanceApiConfig,
    exec_config: &crate::app::config::LlmExecutionConfig,
    symbol: &str,
    state_dir: &str,
    workflow_state: &mut crate::workflow::state::WorkflowState,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    context_key: &str,
    path_id: &str,
    trigger: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<bool> {
    let Some(side) = workflow_side_from_context_key(context_key) else {
        append_workflow_journal_event(
            "workflow_stage2b_management_skipped",
            symbol,
            ts_bucket,
            json!({
                "trigger": trigger,
                "context_key": context_key,
                "path_id": path_id,
                "reason": "snapshot_missing_context_side_unknown",
            }),
        );
        return Ok(false);
    };

    let refreshed_state =
        match fetch_symbol_trading_state(http_client, api_config, exec_config, symbol).await {
            Ok(state) => state,
            Err(err) => {
                append_workflow_journal_event(
                    "workflow_stage2b_management_skipped",
                    symbol,
                    ts_bucket,
                    json!({
                        "trigger": trigger,
                        "context_key": context_key,
                        "path_id": path_id,
                        "reason": "snapshot_missing_refresh_failed",
                        "error": format!("{err:#}"),
                    }),
                );
                return Ok(false);
            }
        };

    if has_active_position_for_side(&refreshed_state, side) {
        append_workflow_journal_event(
            "workflow_stage2b_management_skipped",
            symbol,
            ts_bucket,
            json!({
                "trigger": trigger,
                "context_key": context_key,
                "path_id": path_id,
                "reason": "snapshot_missing_refresh_still_live",
                "side": side,
                "refreshed_active_position_count": refreshed_state.active_positions.len(),
                "refreshed_open_order_count": refreshed_state.open_orders.len(),
            }),
        );
        return Ok(false);
    }

    remove_position_management_plan(workflow_state, context_key);
    if entry_snapshots.remove(context_key).is_some() {
        crate::workflow::persistence::delete_entry_snapshot(state_dir, symbol, context_key)?;
    }
    if workflow_state.last_filled_context_key.as_deref() == Some(context_key) {
        workflow_state.last_filled_context_key = None;
    }
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    info!(
        symbol = %symbol,
        trigger = trigger,
        context_key = %context_key,
        path_id = %path_id,
        side = %side,
        refreshed_active_position_count = refreshed_state.active_positions.len(),
        refreshed_open_order_count = refreshed_state.open_orders.len(),
        "workflow stage2b management plan removed after refreshing trading state because position is already flat"
    );
    append_workflow_journal_event(
        "workflow_stage2b_management_plan_removed",
        symbol,
        ts_bucket,
        json!({
            "trigger": trigger,
            "context_key": context_key,
            "path_id": path_id,
            "reason": "snapshot_missing_refresh_confirmed_flat",
            "side": side,
            "refreshed_active_position_count": refreshed_state.active_positions.len(),
            "refreshed_open_order_count": refreshed_state.open_orders.len(),
        }),
    );
    Ok(true)
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

async fn process_fast_market_delivery(
    ctx: &AppContext,
    fast_state: &mut Option<FastWatcherPlanState>,
    delivery: Delivery,
) -> bool {
    let content_encoding = delivery
        .properties
        .content_encoding()
        .as_ref()
        .map(|value| value.as_str().to_string());
    let mut processed_relevant_event = false;

    match decode_fast_market_envelope(&delivery.data, content_encoding.as_deref()) {
        Ok(envelope) => {
            if let Some(event) = extract_fast_price_event(&envelope, &ctx.config.llm.symbol) {
                processed_relevant_event = true;
                record_fast_price_event_in_buffer(&event);
                if let Err(err) = handle_fast_market_event(ctx, fast_state, event).await {
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

    processed_relevant_event
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

    let stage1_output = crate::workflow::persistence::load_stage1_output(&state_dir, &symbol)?;
    let active_current_path = stage1_output
        .as_ref()
        .filter(|stage1| stage1.monitoring_status == "active")
        .and_then(|stage1| stage1.current_path.as_ref())
        .cloned();

    let mut entry_snapshots =
        crate::workflow::persistence::load_entry_snapshots_for_symbol(&state_dir, &symbol)?
            .into_iter()
            .map(|snapshot| (snapshot.context_key.clone(), snapshot))
            .collect::<HashMap<_, _>>();

    let trading_state = fetch_symbol_trading_state_for_fast_path(
        &ctx.http_client,
        &ctx.config.api.binance,
        &ctx.config.llm.execution,
        &symbol,
    )
    .await
    .unwrap_or_else(|err| {
        warn!(
            symbol = %symbol,
            trigger = "watcher_fast_consumer",
            error = %err,
            "workflow watcher failed to load fast trading state"
        );
        TradingStateSnapshot {
            symbol: symbol.clone(),
            has_active_context: false,
            has_active_positions: false,
            has_open_orders: false,
            active_positions: Vec::new(),
            open_orders: Vec::new(),
            total_wallet_balance: 0.0,
            available_balance: 0.0,
        }
    });
    let approved_plan_before_fast_review = workflow_state.approved_tactical_plan.clone();
    if maybe_record_stopout_and_cleanup(
        &mut workflow_state,
        approved_plan_before_fast_review.as_ref(),
        &symbol,
        &trading_state,
        event.price,
        &mut entry_snapshots,
        &state_dir,
    )? {
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
    }
    let watch_facts = fast_watcher_price_facts(event.price);
    let stage1_replay_attempt = maybe_replay_stage1_path_boundary_window(
        &mut workflow_state,
        active_current_path.as_ref(),
        &symbol,
        &trading_state,
        &state_dir,
        &event,
    )?;
    if stage1_replay_attempt.requested_refresh {
        *fast_state = None;
        return Ok(());
    }
    if !stage1_replay_attempt.current_event_was_replayed
        && maybe_request_stage1_refresh_on_path_boundary_touch(
            &mut workflow_state,
            active_current_path.as_ref(),
            &symbol,
            &trading_state,
            &state_dir,
            &event,
            Stage1RefreshOrigin::Live,
        )?
    {
        *fast_state = None;
        return Ok(());
    }

    if process_fast_position_management_actions(
        ctx,
        &mut workflow_state,
        active_current_path.as_ref(),
        &symbol,
        &state_dir,
        &mut entry_snapshots,
        &trading_state,
        &watch_facts,
        &event,
    )
    .await?
    {
        return Ok(());
    }

    if process_fast_pending_order_management_actions(
        ctx,
        &mut workflow_state,
        active_current_path.as_ref(),
        &symbol,
        &state_dir,
        &mut entry_snapshots,
        &trading_state,
        &watch_facts,
        &event,
    )
    .await?
    {
        return Ok(());
    }

    if workflow_state.pending_stage1_refresh_reason.is_some() {
        *fast_state = None;
        return Ok(());
    }
    let Some(current_path) = active_current_path.as_ref() else {
        *fast_state = None;
        return Ok(());
    };
    let Some(tactical_plan) = workflow_state.approved_tactical_plan.as_ref() else {
        *fast_state = None;
        return Ok(());
    };
    let tactical_plan = tactical_plan.clone();
    let replay_pending = tactical_plan_replay_pending(&workflow_state);
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
        &tactical_plan,
        workflow_state.approved_tactical_plan_updated_at,
    );
    let plan_state_reset = sync_fast_watcher_plan_state(fast_state, &plan_version, &context_key);

    let fast_plan_state = fast_state
        .as_mut()
        .ok_or_else(|| anyhow!("fast watcher state unavailable"))?;
    if plan_state_reset {
        info!(
            symbol = %symbol,
            trigger = "watcher_fast_consumer",
            path_id = %tactical_plan.path_id,
            context_key = %context_key,
            side = %tactical_plan.entry_plan.side,
            entry_profile = %tactical_plan.entry_plan.entry_profile,
            intent_mode = %tactical_plan.entry_plan.intent_mode,
            entry_zone_low = tactical_plan.entry_plan.entry_zone.low,
            entry_zone_high = tactical_plan.entry_plan.entry_zone.high,
            invalidation_low = tactical_plan.entry_plan.entry_invalidation_level.low,
            invalidation_high = tactical_plan.entry_plan.entry_invalidation_level.high,
            stop_loss = tactical_plan.entry_plan.stop_loss,
            "workflow watcher armed tactical entry plan"
        );
        append_workflow_journal_event(
            "workflow_tactical_plan_armed",
            &symbol,
            event.event_ts,
            json!({
                "trigger": "watcher_fast_consumer",
                "price_source": event.source.as_str(),
                "routing_key": &event.routing_key,
                "entry_plan": entry_plan_log_payload(
                    &tactical_plan.path_id,
                    &context_key,
                    &tactical_plan.entry_plan,
                ),
            }),
        );
    }

    if invalidate_tactical_plan_on_stop_touch(
        &mut workflow_state,
        &tactical_plan,
        &symbol,
        &state_dir,
        &event,
        "stop_loss_touched_on_fast_event",
    )? {
        *fast_state = None;
        return Ok(());
    }

    if entry_snapshots.contains_key(&context_key) {
        fast_plan_state.fired = true;
        return Ok(());
    }

    let mut current_event_was_replayed = false;
    if replay_pending {
        let replay_cutoff = tactical_plan_replay_cutoff(&workflow_state, event.event_ts);
        if let Some(replay_start) = workflow_state.approved_tactical_plan_source_ts_bucket {
            let replay_events =
                buffered_fast_price_events_in_range(&symbol, replay_start, replay_cutoff);
            let replay_count = replay_events.len();
            current_event_was_replayed = replay_events
                .iter()
                .any(|replay_event| fast_event_matches(replay_event, &event));
            if let Some(stop_touch_event) =
                first_stop_touch_event_in_replay(&tactical_plan.entry_plan, &replay_events)
            {
                if invalidate_tactical_plan_on_stop_touch(
                    &mut workflow_state,
                    &tactical_plan,
                    &symbol,
                    &state_dir,
                    stop_touch_event,
                    "stop_loss_touched_during_replay",
                )? {
                    *fast_state = None;
                    return Ok(());
                }
            }
            info!(
                symbol = %symbol,
                trigger = "watcher_fast_consumer",
                path_id = %tactical_plan.path_id,
                context_key = %context_key,
                replay_start = %replay_start,
                replay_end = %replay_cutoff,
                replay_count = replay_count,
                approval_ts = %replay_cutoff,
                source_ts_bucket = %replay_start,
                "workflow watcher replaying fast market events until tactical plan approval timestamp"
            );
            append_workflow_journal_event(
                "workflow_tactical_plan_replay",
                &symbol,
                replay_cutoff,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "path_id": &tactical_plan.path_id,
                    "context_key": &context_key,
                    "replay_start": replay_start,
                    "replay_end": replay_cutoff,
                    "replay_count": replay_count,
                    "approval_ts": replay_cutoff,
                    "source_ts_bucket": replay_start,
                }),
            );
            for replay_event in replay_events {
                process_fast_market_event_for_plan(
                    ctx,
                    &mut workflow_state,
                    current_path,
                    &tactical_plan,
                    &symbol,
                    &state_dir,
                    fast_plan_state,
                    &context_key,
                    &mut entry_snapshots,
                    &trading_state,
                    &replay_event,
                )
                .await?;
                if fast_plan_state.fired {
                    mark_tactical_plan_replay_completed(
                        &mut workflow_state,
                        &symbol,
                        &tactical_plan.path_id,
                        &context_key,
                        replay_cutoff,
                        replay_count,
                        &state_dir,
                    )?;
                    return Ok(());
                }
            }
            mark_tactical_plan_replay_completed(
                &mut workflow_state,
                &symbol,
                &tactical_plan.path_id,
                &context_key,
                replay_cutoff,
                replay_count,
                &state_dir,
            )?;
        } else {
            info!(
                symbol = %symbol,
                trigger = "watcher_fast_consumer",
                path_id = %tactical_plan.path_id,
                context_key = %context_key,
                approval_ts = %replay_cutoff,
                "workflow watcher replay skipped because tactical plan source timestamp is missing"
            );
            append_workflow_journal_event(
                "workflow_tactical_plan_replay_skipped",
                &symbol,
                replay_cutoff,
                json!({
                    "trigger": "watcher_fast_consumer",
                    "path_id": &tactical_plan.path_id,
                    "context_key": &context_key,
                    "approval_ts": replay_cutoff,
                    "reason": "missing_source_ts_bucket",
                }),
            );
            mark_tactical_plan_replay_completed(
                &mut workflow_state,
                &symbol,
                &tactical_plan.path_id,
                &context_key,
                replay_cutoff,
                0,
                &state_dir,
            )?;
        }
    }

    if current_event_was_replayed {
        return Ok(());
    }

    process_fast_market_event_for_plan(
        ctx,
        &mut workflow_state,
        current_path,
        &tactical_plan,
        &symbol,
        &state_dir,
        fast_plan_state,
        &context_key,
        &mut entry_snapshots,
        &trading_state,
        &event,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
struct WatcherPriceFacts {
    current_price: f64,
}

fn fast_watcher_price_facts(current_price: f64) -> WatcherPriceFacts {
    WatcherPriceFacts { current_price }
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

fn stop_loss_hit(plan: &crate::workflow::schema::EntryPlan, latest_price: f64) -> bool {
    match plan.side.as_str() {
        "LONG" => latest_price <= plan.stop_loss,
        "SHORT" => latest_price >= plan.stop_loss,
        _ => false,
    }
}

fn first_path_target_hit(
    current_path: &crate::workflow::schema::CurrentPath,
    latest_price: f64,
) -> bool {
    let target_price = current_path
        .first_path_target
        .directional_target(&current_path.side);
    match current_path.side.as_str() {
        "LONG" => latest_price >= target_price,
        "SHORT" => latest_price <= target_price,
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage1RefreshOrigin {
    Live,
    Replay,
}

impl Stage1RefreshOrigin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "fast_event",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage1PathBoundary {
    FailureLevel,
    FirstPathTarget,
}

impl Stage1PathBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::FailureLevel => "failure_level",
            Self::FirstPathTarget => "first_path_target",
        }
    }

    fn zone<'a>(
        self,
        current_path: &'a crate::workflow::schema::CurrentPath,
    ) -> &'a crate::workflow::schema::PriceZone {
        match self {
            Self::FailureLevel => &current_path.failure_level,
            Self::FirstPathTarget => &current_path.first_path_target,
        }
    }

    fn trigger_level(self, current_path: &crate::workflow::schema::CurrentPath) -> f64 {
        match self {
            Self::FailureLevel => failure_level_touch_price(current_path),
            Self::FirstPathTarget => current_path
                .first_path_target
                .directional_target(&current_path.side),
        }
    }

    fn refresh_reason(self, origin: Stage1RefreshOrigin) -> &'static str {
        match (self, origin) {
            (Self::FailureLevel, Stage1RefreshOrigin::Live) => "failure_level_touched",
            (Self::FailureLevel, Stage1RefreshOrigin::Replay) => {
                "failure_level_touched_during_replay"
            }
            (Self::FirstPathTarget, Stage1RefreshOrigin::Live) => "first_path_target_touched",
            (Self::FirstPathTarget, Stage1RefreshOrigin::Replay) => {
                "first_path_target_touched_during_replay"
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Stage1ReplayAttempt {
    requested_refresh: bool,
    current_event_was_replayed: bool,
}

fn failure_level_touch_price(current_path: &crate::workflow::schema::CurrentPath) -> f64 {
    match current_path.side.as_str() {
        "LONG" => current_path.failure_level.low,
        "SHORT" => current_path.failure_level.high,
        _ => current_path.failure_level.midpoint(),
    }
}

fn failure_level_touched(
    current_path: &crate::workflow::schema::CurrentPath,
    latest_price: f64,
) -> bool {
    match current_path.side.as_str() {
        "LONG" => latest_price <= current_path.failure_level.low,
        "SHORT" => latest_price >= current_path.failure_level.high,
        _ => false,
    }
}

fn stage1_path_boundary_touched(
    current_path: &crate::workflow::schema::CurrentPath,
    latest_price: f64,
) -> Option<Stage1PathBoundary> {
    if failure_level_touched(current_path, latest_price) {
        Some(Stage1PathBoundary::FailureLevel)
    } else if first_path_target_hit(current_path, latest_price) {
        Some(Stage1PathBoundary::FirstPathTarget)
    } else {
        None
    }
}

fn stage1_replay_pending(workflow_state: &crate::workflow::state::WorkflowState) -> bool {
    workflow_state.last_stage1_completed_at.is_some()
        && workflow_state.last_stage1_replayed_at.is_none()
}

fn stage1_replay_cutoff(
    workflow_state: &crate::workflow::state::WorkflowState,
    fallback_cutoff: DateTime<Utc>,
) -> DateTime<Utc> {
    workflow_state
        .last_stage1_completed_at
        .unwrap_or(fallback_cutoff)
}

fn mark_stage1_replay_completed(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    symbol: &str,
    path_id: Option<&str>,
    replay_start: Option<DateTime<Utc>>,
    replay_end: DateTime<Utc>,
    replay_count: usize,
    state_dir: &str,
) -> Result<()> {
    if workflow_state.last_stage1_replayed_at.is_some() {
        return Ok(());
    }
    let completed_at = Utc::now();
    workflow_state.last_stage1_replayed_at = Some(completed_at);
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    append_workflow_journal_event(
        "workflow_stage1_replay_completed",
        symbol,
        replay_end,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": path_id,
            "replay_start": replay_start,
            "replay_end": replay_end,
            "replay_count": replay_count,
            "completed_at": completed_at,
        }),
    );
    Ok(())
}

fn mark_stage1_replay_skipped(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    symbol: &str,
    path_id: Option<&str>,
    replay_start: Option<DateTime<Utc>>,
    replay_end: DateTime<Utc>,
    reason: &str,
    state_dir: &str,
) -> Result<()> {
    if workflow_state.last_stage1_replayed_at.is_some() {
        return Ok(());
    }
    let completed_at = Utc::now();
    workflow_state.last_stage1_replayed_at = Some(completed_at);
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    append_workflow_journal_event(
        "workflow_stage1_replay_skipped",
        symbol,
        replay_end,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": path_id,
            "replay_start": replay_start,
            "replay_end": replay_end,
            "reason": reason,
            "completed_at": completed_at,
        }),
    );
    Ok(())
}

fn maybe_request_stage1_refresh_on_path_boundary_touch(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    current_path: Option<&crate::workflow::schema::CurrentPath>,
    symbol: &str,
    trading_state: &TradingStateSnapshot,
    state_dir: &str,
    event: &FastPriceEvent,
    origin: Stage1RefreshOrigin,
) -> Result<bool> {
    if workflow_state.pending_stage1_refresh_reason.is_some() {
        return Ok(false);
    }
    let Some(current_path) = current_path else {
        return Ok(false);
    };
    if has_active_position_for_side(trading_state, &current_path.side) {
        return Ok(false);
    }
    if live_entry_order_count_for_side(trading_state, &current_path.side) > 0 {
        return Ok(false);
    }
    let Some(boundary) = stage1_path_boundary_touched(current_path, event.price) else {
        return Ok(false);
    };

    let refresh_reason = boundary.refresh_reason(origin).to_string();
    workflow_state.pending_stage1_refresh_reason = Some(refresh_reason.clone());
    clear_approved_tactical_plan(workflow_state);
    crate::workflow::persistence::save_workflow_state(state_dir, workflow_state)?;
    append_workflow_journal_event(
        "workflow_stage1_refresh_requested",
        symbol,
        event.event_ts,
        json!({
            "trigger": "watcher_fast_consumer",
            "refresh_reason": refresh_reason,
            "refresh_origin": origin.as_str(),
            "path_id": &current_path.id,
            "side": &current_path.side,
            "trigger_price": event.price,
            "price_source": event.source.as_str(),
            "routing_key": &event.routing_key,
            "boundary_type": boundary.name(),
            "boundary_zone": boundary.zone(current_path),
            "boundary_level": boundary.trigger_level(current_path),
        }),
    );
    Ok(true)
}

fn maybe_replay_stage1_path_boundary_window(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    current_path: Option<&crate::workflow::schema::CurrentPath>,
    symbol: &str,
    trading_state: &TradingStateSnapshot,
    state_dir: &str,
    event: &FastPriceEvent,
) -> Result<Stage1ReplayAttempt> {
    if !stage1_replay_pending(workflow_state) {
        return Ok(Stage1ReplayAttempt::default());
    }

    let replay_cutoff = stage1_replay_cutoff(workflow_state, event.event_ts);
    let replay_start = workflow_state.last_stage1_source_ts_bucket;
    let Some(current_path) = current_path else {
        mark_stage1_replay_skipped(
            workflow_state,
            symbol,
            None,
            replay_start,
            replay_cutoff,
            "no_active_path",
            state_dir,
        )?;
        return Ok(Stage1ReplayAttempt::default());
    };

    if has_active_position_for_side(trading_state, &current_path.side)
        || live_entry_order_count_for_side(trading_state, &current_path.side) > 0
    {
        mark_stage1_replay_skipped(
            workflow_state,
            symbol,
            Some(&current_path.id),
            replay_start,
            replay_cutoff,
            "same_side_exposure_exists",
            state_dir,
        )?;
        return Ok(Stage1ReplayAttempt::default());
    }

    let Some(replay_start) = replay_start else {
        mark_stage1_replay_skipped(
            workflow_state,
            symbol,
            Some(&current_path.id),
            None,
            replay_cutoff,
            "missing_source_ts_bucket",
            state_dir,
        )?;
        return Ok(Stage1ReplayAttempt::default());
    };

    let replay_events = buffered_fast_price_events_in_range(symbol, replay_start, replay_cutoff);
    let replay_count = replay_events.len();
    let current_event_was_replayed = replay_events
        .iter()
        .any(|replay_event| fast_event_matches(replay_event, event));

    info!(
        symbol = %symbol,
        trigger = "watcher_fast_consumer",
        path_id = %current_path.id,
        replay_start = %replay_start,
        replay_end = %replay_cutoff,
        replay_count = replay_count,
        "workflow watcher replaying stage1 path boundaries"
    );
    append_workflow_journal_event(
        "workflow_stage1_replay",
        symbol,
        replay_cutoff,
        json!({
            "trigger": "watcher_fast_consumer",
            "path_id": &current_path.id,
            "replay_start": replay_start,
            "replay_end": replay_cutoff,
            "replay_count": replay_count,
        }),
    );

    for replay_event in &replay_events {
        if maybe_request_stage1_refresh_on_path_boundary_touch(
            workflow_state,
            Some(current_path),
            symbol,
            trading_state,
            state_dir,
            replay_event,
            Stage1RefreshOrigin::Replay,
        )? {
            return Ok(Stage1ReplayAttempt {
                requested_refresh: true,
                current_event_was_replayed,
            });
        }
    }

    mark_stage1_replay_completed(
        workflow_state,
        symbol,
        Some(&current_path.id),
        Some(replay_start),
        replay_cutoff,
        replay_count,
        state_dir,
    )?;

    Ok(Stage1ReplayAttempt {
        requested_refresh: false,
        current_event_was_replayed,
    })
}

fn maybe_record_stopout_and_cleanup(
    workflow_state: &mut crate::workflow::state::WorkflowState,
    approved_tactical_plan: Option<&crate::workflow::schema::TacticalEntryPlan>,
    symbol: &str,
    trading_state: &TradingStateSnapshot,
    latest_price: f64,
    entry_snapshots: &mut HashMap<String, crate::workflow::schema::EntrySnapshot>,
    state_dir: &str,
) -> Result<bool> {
    let Some(tactical_plan) = approved_tactical_plan else {
        return Ok(false);
    };
    let Some(last_context_key) = workflow_state.last_filled_context_key.clone() else {
        return Ok(false);
    };
    let watched_plan = &tactical_plan.entry_plan;
    let expected_context_key =
        workflow_entry_context_key(symbol, &watched_plan.side, &tactical_plan.path_id);
    if expected_context_key != last_context_key {
        return Ok(false);
    }
    if has_active_position_for_side(trading_state, &watched_plan.side) {
        return Ok(false);
    }
    if !stop_loss_hit(watched_plan, latest_price) {
        return Ok(false);
    }

    workflow_state.filled_stopout_attempts = workflow_state
        .filled_stopout_attempts
        .saturating_add(1)
        .min(255);
    workflow_state.last_filled_context_key = None;
    if entry_snapshots.remove(&last_context_key).is_some() {
        crate::workflow::persistence::delete_entry_snapshot(state_dir, symbol, &last_context_key)?;
    }
    Ok(true)
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
    let stage1_completed_at = Utc::now();
    workflow_state.last_stage1_ts = Some(parsed_stage1.meta.stage1_ts);
    workflow_state.last_stage1_source_ts_bucket = Some(bundle.raw.ts_bucket);
    workflow_state.last_stage1_refresh_reason = Some(refresh_reason.clone());
    workflow_state.last_stage1_completed_at = Some(stage1_completed_at);
    workflow_state.last_stage1_replayed_at =
        if parsed_stage1.monitoring_status == "active" && parsed_stage1.current_path.is_some() {
            None
        } else {
            Some(stage1_completed_at)
        };
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
    ctx: AppContext,
    print_response: bool,
    bundle: LatestBundle,
    trigger: Arc<str>,
) -> Result<()> {
    let config = Arc::clone(&ctx.config);
    let db_pool = ctx.db_pool.clone();
    let http_client = ctx.http_client.clone();
    let loopback_http_client = ctx.loopback_http_client.clone();
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
    let stage1_refreshed_this_bundle = stage1_attempt.refreshed;

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

    let stage1_output =
        stage1_output.ok_or_else(|| anyhow!("workflow stage1 output missing after refresh"))?;

    let trading_state = fetch_symbol_trading_state(
        &http_client,
        &config.api.binance,
        &config.llm.execution,
        &symbol,
    )
    .await?;
    let entry_snapshots =
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

    let mut stage2a_output: Option<crate::workflow::schema::Stage2AOutput> = None;
    let mut selected_stage2a_model_name: Option<String> = None;
    let stage1_refresh_blocking = (stage1_refresh_reason.is_some()
        && !stage1_refreshed_this_bundle)
        || workflow_stage_inflight(&symbol, WorkflowStageKind::Stage1);
    let startup_immediate_stage2a_due = startup_stage1_immediate_stage2a_due(
        stage1_refresh_reason.as_deref(),
        stage1_refreshed_this_bundle,
        Some(&stage1_output),
        &trading_state,
    );
    let stage2_review_due = startup_immediate_stage2a_due
        || workflow_stage2_review_due(
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
                let should_run_stage2a =
                    startup_immediate_stage2a_due || dispatch_flags.should_run_stage2a;
                let stage2b_dispatch_enabled = dispatch_flags.should_run_stage2b;
                let stage2b_context_count = stage2b_contexts.len();
                let should_run_stage2b = stage2b_dispatch_enabled && stage2b_context_count > 0;
                let should_run_stage2c =
                    dispatch_flags.should_run_stage2c && !stage2c_contexts.is_empty();
                let stage2c_exposure_state = stage2c_exposure_state_for_counts(
                    active_position_count,
                    live_entry_order_count,
                );
                if !should_run_stage2b {
                    let stage2b_skip_reason =
                        match (stage2b_dispatch_enabled, stage2b_context_count > 0) {
                            (false, false) => "no_same_side_live_position_and_no_stage2b_context",
                            (false, true) => "no_same_side_live_position",
                            (true, false) => "no_stage2b_context",
                            (true, true) => "not_skipped",
                        };
                    info!(
                        symbol = %symbol,
                        ts_bucket = %bundle.raw.ts_bucket,
                        trigger = &*trigger,
                        path_id = %current_path_id,
                        path_side = %path_side,
                        active_position_count,
                        live_entry_order_count,
                        stage2b_dispatch_enabled,
                        stage2b_context_count,
                        stage2b_context_keys = ?stage2b_contexts
                            .iter()
                            .map(|position| position.context_key.clone())
                            .collect::<Vec<_>>(),
                        skip_reason = stage2b_skip_reason,
                        "workflow stage2b dispatch skipped"
                    );
                    append_workflow_journal_event(
                        "workflow_stage2b_dispatch_skipped",
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "path_id": current_path_id,
                            "path_side": path_side,
                            "active_position_count": active_position_count,
                            "live_entry_order_count": live_entry_order_count,
                            "dispatch_flag_should_run_stage2b": stage2b_dispatch_enabled,
                            "stage2b_context_count": stage2b_context_count,
                            "stage2b_context_keys": stage2b_contexts
                                .iter()
                                .map(|position| position.context_key.clone())
                                .collect::<Vec<_>>(),
                            "skip_reason": stage2b_skip_reason,
                        }),
                    );
                }

                if startup_immediate_stage2a_due {
                    info!(
                        symbol = %symbol,
                        ts_bucket = %bundle.raw.ts_bucket,
                        trigger = &*trigger,
                        path_id = %current_path_id,
                        "workflow startup stage1 forcing immediate stage2a review"
                    );
                    append_workflow_journal_event(
                        "workflow_stage2a_startup_immediate",
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "path_id": current_path_id,
                            "reason": "startup_force_stage1_active_path_without_live_exposure",
                        }),
                    );
                }

                if should_run_stage2a {
                    let prompt_input = crate::workflow::stage2_input::build_stage2a_prompt_input(
                        &input,
                        &indicator_summary,
                        &stage1_output,
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
                        "PATH_CONFIRMED_WAIT" => {
                            clear_approved_tactical_plan(&mut workflow_state);
                            workflow_state.pending_stage1_refresh_reason = None;
                            crate::workflow::persistence::save_workflow_state(
                                &state_dir,
                                &workflow_state,
                            )?;
                            append_workflow_journal_event(
                                "workflow_stage2a_wait",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "model_name": selected_stage2a_model_name.clone(),
                                    "source_ts_bucket": bundle.raw.ts_bucket,
                                    "path_id": stage1_output.current_path.as_ref().map(|path| path.id.clone()),
                                    "wait_reason": parsed_stage2a.wait_reason,
                                }),
                            );
                            info!(
                                symbol = %symbol,
                                trigger = &*trigger,
                                source_ts_bucket = %bundle.raw.ts_bucket,
                                path_id = ?stage1_output.current_path.as_ref().map(|path| path.id.clone()),
                                wait_reason = ?parsed_stage2a.wait_reason,
                                "workflow stage2a path confirmed but waiting for better execution"
                            );
                        }
                        "PATH_CONFIRMED_ENTRY" => {
                            if let Some(tactical_plan) = parsed_stage2a.tactical_entry_plan.clone()
                            {
                                set_approved_tactical_plan(
                                    &mut workflow_state,
                                    tactical_plan,
                                    bundle.raw.ts_bucket,
                                    true,
                                );
                            } else {
                                clear_approved_tactical_plan(&mut workflow_state);
                            }
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
                                    "model_name": selected_stage2a_model_name.clone(),
                                    "source_ts_bucket": bundle.raw.ts_bucket,
                                    "tactical_entry_plan": parsed_stage2a.tactical_entry_plan.clone(),
                                }),
                            );
                            if let Some(tactical_plan) =
                                workflow_state.approved_tactical_plan.as_ref()
                            {
                                info!(
                                    symbol = %symbol,
                                    trigger = &*trigger,
                                    source_ts_bucket = %bundle.raw.ts_bucket,
                                    path_id = %tactical_plan.path_id,
                                    side = %tactical_plan.entry_plan.side,
                                    entry_profile = %tactical_plan.entry_plan.entry_profile,
                                    intent_mode = %tactical_plan.entry_plan.intent_mode,
                                    entry_zone_low = tactical_plan.entry_plan.entry_zone.low,
                                    entry_zone_high = tactical_plan.entry_plan.entry_zone.high,
                                    invalidation_low = tactical_plan.entry_plan.entry_invalidation_level.low,
                                    invalidation_high = tactical_plan.entry_plan.entry_invalidation_level.high,
                                    stop_loss = tactical_plan.entry_plan.stop_loss,
                                    "workflow tactical plan approved"
                                );
                            }
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
                        let active_position_path_id = active_position
                            .entry_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.path_id.clone())
                            .unwrap_or_else(|| current_path_id.clone());
                        let prompt_input =
                            crate::workflow::stage2_input::build_stage2b_prompt_input(
                                &input,
                                &indicator_summary,
                                &stage1_output,
                                active_position.clone(),
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
                            position_path_id = %active_position_path_id,
                            context_key = %active_position.context_key,
                            exposure_state = %prompt_input.exposure_state,
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
                                &active_position_path_id,
                            ) {
                                Ok(parsed) => {
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
                            info!(
                                symbol = %symbol,
                                ts_bucket = %bundle.raw.ts_bucket,
                                trigger = &*trigger,
                                path_id = %parsed.position_management_plan.path_id,
                                context_key = %active_position.context_key,
                                action_count = parsed.position_management_plan.actions.len(),
                                action_types = ?parsed
                                    .position_management_plan
                                    .actions
                                    .iter()
                                    .map(|action| action.action_type.clone())
                                    .collect::<Vec<_>>(),
                                "workflow stage2b management plan armed"
                            );
                            append_workflow_journal_event(
                                "workflow_stage2b_management_plan_armed",
                                &symbol,
                                bundle.raw.ts_bucket,
                                json!({
                                    "trigger": &*trigger,
                                    "context_key": active_position.context_key,
                                    "path_id": parsed.position_management_plan.path_id,
                                    "action_count": parsed.position_management_plan.actions.len(),
                                    "actions": parsed.position_management_plan.actions,
                                }),
                            );
                            next_position_plans.insert(
                                active_position.context_key.clone(),
                                parsed.position_management_plan,
                            );
                        }
                    }
                    replace_position_management_plans(&mut workflow_state, next_position_plans);
                    crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
                } else {
                    if !workflow_state.approved_position_management_plans.is_empty() {
                        info!(
                            symbol = %symbol,
                            ts_bucket = %bundle.raw.ts_bucket,
                            trigger = &*trigger,
                            path_id = %current_path_id,
                            path_side = %path_side,
                            active_position_count,
                            live_entry_order_count,
                            stage2b_dispatch_enabled,
                            stage2b_context_count,
                            cleared_context_keys = ?workflow_state
                                .approved_position_management_plans
                                .keys()
                                .cloned()
                                .collect::<Vec<_>>(),
                            "workflow stage2b management plans cleared because dispatch is disabled"
                        );
                        append_workflow_journal_event(
                            "workflow_stage2b_management_plans_cleared",
                            &symbol,
                            bundle.raw.ts_bucket,
                            json!({
                                "trigger": &*trigger,
                                "path_id": current_path_id,
                                "path_side": path_side,
                                "active_position_count": active_position_count,
                                "live_entry_order_count": live_entry_order_count,
                                "dispatch_flag_should_run_stage2b": stage2b_dispatch_enabled,
                                "stage2b_context_count": stage2b_context_count,
                                "cleared_context_keys": workflow_state
                                    .approved_position_management_plans
                                    .keys()
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            }),
                        );
                    }
                    clear_position_management_plans(&mut workflow_state);
                }

                if should_run_stage2c {
                    let mut next_pending_order_plans = BTreeMap::new();
                    let expected_stage2c_exposure_state = stage2c_exposure_state
                        .expect("Stage2C exposure state must exist when Stage2C is enabled");
                    for active_order in &stage2c_contexts {
                        let prompt_input =
                            crate::workflow::stage2_input::build_stage2c_prompt_input(
                                &input,
                                &indicator_summary,
                                &stage1_output,
                                expected_stage2c_exposure_state,
                                active_order.clone(),
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
    trading_state: &TradingStateSnapshot,
    snapshot: &crate::workflow::schema::EntrySnapshot,
    action: &crate::workflow::schema::ManagementAction,
    report: &ManagementExecutionReport,
) -> TradeSignalNotification {
    let decision = match action.action_type.as_str() {
        "REDUCE_POSITION" => "REDUCE",
        "FLATTEN_POSITION" => "CLOSE",
        "MOVE_STOP" | "UPDATE_TAKE_PROFIT" => "MODIFY_TPSL",
        _ => "HOLD",
    };
    let position = find_active_position_for_side(trading_state, &snapshot.side);
    let entry_price = position.map(|item| item.entry_price);
    let leverage = position.map(|item| item.leverage as f64);
    let take_profit_1 = action.take_profit_1.or(Some(snapshot.take_profit_1));
    let take_profit_2 = action.take_profit_2.or(Some(snapshot.take_profit_2));
    let stop_loss = action.new_stop_loss.or(Some(snapshot.stop_loss));
    let risk_reward_ratio = compute_signal_rr(entry_price, stop_loss, take_profit_1);

    TradeSignalNotification {
        ts_bucket,
        trigger: trigger.to_string(),
        symbol: symbol.to_string(),
        model_name: model_name.to_string(),
        decision: decision.to_string(),
        context_key: Some(action.context_key.clone()),
        path_id: Some(action.path_id.clone()),
        entry_price,
        leverage,
        risk_reward_ratio,
        take_profit_1,
        take_profit_2,
        stop_loss,
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
            entry_activation_level: Some(sample_price_zone(100.0, 101.2, "15m")),
            entry_zone: sample_price_zone(100.0, 101.0, "15m"),
            entry_invalidation_level: sample_price_zone(98.5, 99.0, "15m"),
            stop_loss: 98.4,
            leverage: 4,
            max_drift_pct: 0.2,
            entry_reason: "entry".to_string(),
            invalidation_reason: "invalidation".to_string(),
            stop_loss_reason: "stop".to_string(),
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

    fn clear_fast_price_event_buffer_for_symbol(symbol: &str) {
        if let Ok(mut guard) = fast_price_event_buffer().lock() {
            guard.remove(&symbol.to_ascii_uppercase());
        }
    }

    fn sample_fast_watcher_config() -> crate::app::config::WorkflowWatcherConfig {
        let mut watcher_cfg = crate::app::config::WorkflowWatcherConfig::default();
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
                entry_activation_level: Some(sample_price_zone(100.0, 101.0, "15m")),
                entry_zone: sample_price_zone(101.0, 102.0, "15m"),
                entry_invalidation_level: sample_price_zone(98.0, 99.0, "15m"),
                stop_loss: 98.8,
                leverage: 5,
                max_drift_pct: 0.2,
                entry_reason: "entry".to_string(),
                invalidation_reason: "invalidation".to_string(),
                stop_loss_reason: "stop".to_string(),
            },
        }
    }

    #[test]
    fn inside_or_beyond_activation_falls_back_to_entry_zone_when_activation_is_missing() {
        let mut plan = sample_fast_entry_plan("pullback", "pullback_acceptance");
        plan.entry_activation_level = None;

        assert!(inside_or_beyond_activation(&plan, 100.5));
        assert!(inside_or_beyond_activation(&plan, 101.2));
        assert!(!inside_or_beyond_activation(&plan, 99.0));
    }

    #[test]
    fn entry_plan_from_snapshot_template_allows_missing_activation_level() {
        let mut fallback_plan = sample_fast_entry_plan("breakout", "reclaim_then_hold");
        fallback_plan.entry_activation_level = None;
        let snapshot = EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("breakout".to_string()),
            entry_activation_level: None,
            entry_zone: Some(sample_price_zone(101.0, 102.0, "15m")),
            entry_invalidation_level: Some(sample_price_zone(98.0, 99.0, "15m")),
            max_drift_pct: Some(0.2),
            leverage: Some(5),
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

        let plan = entry_plan_from_snapshot_template(&snapshot, Some(&fallback_plan))
            .expect("snapshot template should allow missing activation");

        assert!(plan.entry_activation_level.is_none());
        assert_eq!(plan.entry_reason, fallback_plan.entry_reason);
        assert_eq!(plan.invalidation_reason, fallback_plan.invalidation_reason);
        assert_eq!(plan.stop_loss_reason, fallback_plan.stop_loss_reason);
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
        let facts = fast_watcher_price_facts(102.0);
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
    fn first_triggered_position_management_action_matches_fast_price_rule_without_recent_bars() {
        let facts = fast_watcher_price_facts(102.0);
        let plan = PositionManagementPlan {
            path_id: "path_a".to_string(),
            exposure_state: "in_position".to_string(),
            path_live_assessment: "live".to_string(),
            path_assessment_reason: None,
            actions: vec![PositionManagementAction {
                action_type: "reduce".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                trigger_condition: Some(PriceTriggerCondition {
                    trigger_type: "price_below".to_string(),
                    trigger_price: 103.0,
                }),
                execution_price: Some(101.5),
                add_ratio: None,
                reuse_current_entry_template: None,
                reduce_ratio: Some(0.5),
                new_stop_loss: None,
                reuse_current_bracket_template: None,
                take_profit_1: None,
                take_profit_2: None,
                reason: "de-risk".to_string(),
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
        let facts = fast_watcher_price_facts(99.0);
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
    fn first_triggered_pending_order_action_matches_fast_price_rule_without_recent_bars() {
        let facts = fast_watcher_price_facts(99.0);
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
    fn execution_intent_from_entry_plan_uses_directional_target_edges() {
        let current_path = CurrentPath {
            id: "path_a".to_string(),
            side: "LONG".to_string(),
            thesis: "continuation".to_string(),
            risk_grade: "aligned_trend".to_string(),
            activation_anchor_id: None,
            strategic_activation_level: sample_price_zone(100.0, 101.0, "4h"),
            first_path_target_anchor_id: None,
            first_path_target: sample_price_zone(104.0, 106.0, "4h"),
            next_path_target_anchor_id: None,
            next_path_target: sample_price_zone(109.0, 112.0, "1d"),
            failure_anchor_id: None,
            failure_level: sample_price_zone(98.0, 99.0, "4h"),
            failure_switch: Some("alt".to_string()),
            setup_type: "A_continuation".to_string(),
            reevaluation_trigger: ReevaluationTrigger::default(),
            tracked_zones: vec![],
        };
        let long_plan = crate::workflow::schema::EntryPlan {
            side: "LONG".to_string(),
            entry_profile: "reclaim_then_hold".to_string(),
            intent_mode: "pullback".to_string(),
            entry_activation_level: Some(sample_price_zone(100.0, 101.0, "15m")),
            entry_zone: sample_price_zone(100.5, 101.5, "15m"),
            entry_invalidation_level: sample_price_zone(98.0, 99.0, "15m"),
            stop_loss: 97.5,
            leverage: 5,
            max_drift_pct: 0.2,
            entry_reason: "entry".to_string(),
            invalidation_reason: "invalidation".to_string(),
            stop_loss_reason: "stop".to_string(),
        };
        let short_plan = crate::workflow::schema::EntryPlan {
            side: "SHORT".to_string(),
            ..long_plan.clone()
        };

        let long_intent = execution_intent_from_entry_plan(
            "ETHUSDT",
            "path_a",
            &long_plan,
            &current_path,
            None,
            101.2,
            15,
            None,
        );
        let short_intent = execution_intent_from_entry_plan(
            "ETHUSDT",
            "path_a",
            &short_plan,
            &CurrentPath {
                side: "SHORT".to_string(),
                ..current_path.clone()
            },
            None,
            100.8,
            15,
            None,
        );

        assert_eq!(long_intent.take_profit_1, 106.0);
        assert_eq!(long_intent.take_profit_2, 112.0);
        assert_eq!(short_intent.take_profit_1, 104.0);
        assert_eq!(short_intent.take_profit_2, 109.0);
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
                leverage: Some(5),
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
                leverage: Some(5),
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
    fn snapshot_for_management_context_falls_back_to_current_tactical_template() {
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
        )
        .expect("fallback snapshot");

        assert_eq!(snapshot.context_key, "ETHUSDT:LONG:path_a");
        assert_eq!(snapshot.side, "LONG");
        assert_eq!(snapshot.take_profit_1, 104.0);
        assert_eq!(snapshot.take_profit_2, 107.0);
        assert_eq!(snapshot.entry_profile.as_deref(), Some("reclaim_then_hold"));
        assert_eq!(snapshot.intent_mode.as_deref(), Some("breakout"));
    }

    #[test]
    fn entry_plan_from_snapshot_template_derives_missing_max_drift_pct() {
        let snapshot = crate::workflow::schema::EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: Some("pullback".to_string()),
            entry_activation_level: None,
            entry_zone: Some(sample_price_zone(100.0, 101.0, "15m")),
            entry_invalidation_level: Some(sample_price_zone(99.0, 99.5, "15m")),
            max_drift_pct: None,
            leverage: None,
            stop_loss: 98.5,
            take_profit_1: 104.0,
            take_profit_2: 107.0,
            allowed_stop_loss_levels: vec![98.5],
            allowed_take_profit_levels: vec![104.0, 107.0],
            tp1_realized: false,
            applied_driver_deterioration_signals: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let plan = entry_plan_from_snapshot_template(&snapshot, None).expect("entry plan");
        assert_eq!(plan.max_drift_pct, 0.51);
    }

    #[test]
    fn snapshot_for_management_context_does_not_fabricate_stop_from_stage1_failure_level() {
        let stage1_output = sample_stage1_output();
        let current_path = stage1_output.current_path.as_ref().expect("path");
        let workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
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

        assert!(snapshot.is_none());
    }

    #[test]
    fn snapshot_for_management_context_does_not_fallback_across_path_switches() {
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
            "ETHUSDT:LONG:legacy_path",
            "legacy_path",
        );

        assert!(snapshot.is_none());
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
            leverage: Some(5),
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
    fn fast_watcher_pullback_arms_after_activation() {
        let watcher_cfg = sample_fast_watcher_config();
        let plan = sample_fast_entry_plan("pullback", "pullback_acceptance");
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
            &sample_fast_price_event("2026-03-30T09:35:01Z", 101.1),
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

    #[test]
    fn first_path_target_hit_uses_tp1_edge_for_long_and_short_paths() {
        let long_path = sample_stage1_output().current_path.expect("long path");
        assert!(!first_path_target_hit(&long_path, 103.9));
        assert!(first_path_target_hit(&long_path, 104.0));

        let mut short_path = long_path.clone();
        short_path.side = "SHORT".to_string();
        short_path.first_path_target = sample_price_zone(96.0, 97.0, "4h");

        assert!(!first_path_target_hit(&short_path, 96.1));
        assert!(first_path_target_hit(&short_path, 96.0));
    }

    #[test]
    fn failure_level_touch_uses_low_for_long_and_high_for_short_paths() {
        let long_path = sample_stage1_output().current_path.expect("long path");
        assert!(!failure_level_touched(&long_path, 98.1));
        assert!(failure_level_touched(&long_path, 98.0));

        let mut short_path = long_path.clone();
        short_path.side = "SHORT".to_string();
        short_path.failure_level = sample_price_zone(97.0, 98.0, "4h");

        assert!(!failure_level_touched(&short_path, 97.9));
        assert!(failure_level_touched(&short_path, 98.0));
    }

    #[test]
    fn first_path_target_touch_requests_stage1_refresh_when_flat() {
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            approved_tactical_plan_updated_at: Some(Utc::now()),
            filled_stopout_attempts: 2,
            last_filled_context_key: Some("ETHUSDT:LONG:path_a".to_string()),
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let event = sample_fast_price_event("2026-03-30T09:35:00Z", 104.0);

        let requested = maybe_request_stage1_refresh_on_path_boundary_touch(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &sample_flat_trading_state(),
            state_dir.to_str().expect("state dir"),
            &event,
            Stage1RefreshOrigin::Live,
        )
        .expect("request stage1 refresh");

        assert!(requested);
        assert_eq!(
            workflow_state.pending_stage1_refresh_reason.as_deref(),
            Some("first_path_target_touched")
        );
        assert!(workflow_state.approved_tactical_plan.is_none());
        assert_eq!(workflow_state.filled_stopout_attempts, 0);
        assert!(workflow_state.last_filled_context_key.is_none());

        let persisted = crate::workflow::persistence::load_workflow_state(
            state_dir.to_str().expect("state dir"),
            "ETHUSDT",
        )
        .expect("load workflow state")
        .expect("persisted workflow state");
        assert_eq!(
            persisted.pending_stage1_refresh_reason.as_deref(),
            Some("first_path_target_touched")
        );

        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn failure_level_touch_requests_stage1_refresh_when_flat() {
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            approved_tactical_plan_updated_at: Some(Utc::now()),
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let event = sample_fast_price_event("2026-03-30T09:35:00Z", 98.0);

        let requested = maybe_request_stage1_refresh_on_path_boundary_touch(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &sample_flat_trading_state(),
            state_dir.to_str().expect("state dir"),
            &event,
            Stage1RefreshOrigin::Live,
        )
        .expect("request stage1 refresh");

        assert!(requested);
        assert_eq!(
            workflow_state.pending_stage1_refresh_reason.as_deref(),
            Some("failure_level_touched")
        );
        assert!(workflow_state.approved_tactical_plan.is_none());

        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn path_boundary_touch_does_not_request_stage1_refresh_with_live_same_side_position() {
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            approved_tactical_plan_updated_at: Some(Utc::now()),
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let event = sample_fast_price_event("2026-03-30T09:35:00Z", 104.0);
        let mut trading_state = sample_flat_trading_state();
        trading_state.has_active_positions = true;
        trading_state.active_positions.push(ActivePositionSnapshot {
            position_side: "LONG".to_string(),
            position_amt: 1.0,
            entry_price: 100.0,
            mark_price: 104.0,
            unrealized_pnl: 4.0,
            leverage: 3,
        });

        let requested = maybe_request_stage1_refresh_on_path_boundary_touch(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &trading_state,
            state_dir.to_str().expect("state dir"),
            &event,
            Stage1RefreshOrigin::Live,
        )
        .expect("skip stage1 refresh");

        assert!(!requested);
        assert!(workflow_state.pending_stage1_refresh_reason.is_none());
        assert!(workflow_state.approved_tactical_plan.is_some());

        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn stage1_replay_requests_refresh_when_failure_level_was_touched_during_gap() {
        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let replay_start = DateTime::parse_from_rfc3339("2026-03-30T12:00:00Z")
            .expect("replay start")
            .with_timezone(&Utc);
        let replay_end = DateTime::parse_from_rfc3339("2026-03-30T12:06:00Z")
            .expect("replay end")
            .with_timezone(&Utc);
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T12:04:00Z", 98.0));

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            approved_tactical_plan_updated_at: Some(Utc::now()),
            last_stage1_source_ts_bucket: Some(replay_start),
            last_stage1_completed_at: Some(replay_end),
            last_stage1_replayed_at: None,
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let current_event = sample_fast_price_event("2026-03-30T12:06:05Z", 101.0);

        let attempt = maybe_replay_stage1_path_boundary_window(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &sample_flat_trading_state(),
            state_dir.to_str().expect("state dir"),
            &current_event,
        )
        .expect("replay attempt");

        assert!(attempt.requested_refresh);
        assert_eq!(
            workflow_state.pending_stage1_refresh_reason.as_deref(),
            Some("failure_level_touched_during_replay")
        );
        assert!(workflow_state.approved_tactical_plan.is_none());
        assert!(workflow_state.last_stage1_replayed_at.is_none());

        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn stage1_replay_requests_refresh_when_first_path_target_was_touched_during_gap() {
        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let replay_start = DateTime::parse_from_rfc3339("2026-03-30T12:00:00Z")
            .expect("replay start")
            .with_timezone(&Utc);
        let replay_end = DateTime::parse_from_rfc3339("2026-03-30T12:06:00Z")
            .expect("replay end")
            .with_timezone(&Utc);
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T12:04:00Z", 104.0));

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan: Some(sample_tactical_plan()),
            approved_tactical_plan_updated_at: Some(Utc::now()),
            last_stage1_source_ts_bucket: Some(replay_start),
            last_stage1_completed_at: Some(replay_end),
            last_stage1_replayed_at: None,
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let current_event = sample_fast_price_event("2026-03-30T12:06:05Z", 101.0);

        let attempt = maybe_replay_stage1_path_boundary_window(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &sample_flat_trading_state(),
            state_dir.to_str().expect("state dir"),
            &current_event,
        )
        .expect("replay attempt");

        assert!(attempt.requested_refresh);
        assert_eq!(
            workflow_state.pending_stage1_refresh_reason.as_deref(),
            Some("first_path_target_touched_during_replay")
        );
        assert!(workflow_state.approved_tactical_plan.is_none());
        assert!(workflow_state.last_stage1_replayed_at.is_none());

        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn stage1_replay_marks_window_completed_when_no_boundary_was_touched() {
        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        let state_dir =
            std::env::temp_dir().join(format!("llm-workflow-state-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&state_dir).expect("create workflow state dir");

        let replay_start = DateTime::parse_from_rfc3339("2026-03-30T12:00:00Z")
            .expect("replay start")
            .with_timezone(&Utc);
        let replay_end = DateTime::parse_from_rfc3339("2026-03-30T12:06:00Z")
            .expect("replay end")
            .with_timezone(&Utc);
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T12:04:00Z", 101.0));

        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            last_stage1_source_ts_bucket: Some(replay_start),
            last_stage1_completed_at: Some(replay_end),
            last_stage1_replayed_at: None,
            ..WorkflowState::default()
        };
        let current_path = sample_stage1_output().current_path.expect("current path");
        let current_event = sample_fast_price_event("2026-03-30T12:06:05Z", 101.2);

        let attempt = maybe_replay_stage1_path_boundary_window(
            &mut workflow_state,
            Some(&current_path),
            "ETHUSDT",
            &sample_flat_trading_state(),
            state_dir.to_str().expect("state dir"),
            &current_event,
        )
        .expect("replay attempt");

        assert!(!attempt.requested_refresh);
        assert!(workflow_state.pending_stage1_refresh_reason.is_none());
        assert!(workflow_state.last_stage1_replayed_at.is_some());

        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        fs::remove_dir_all(&state_dir).expect("cleanup workflow state dir");
    }

    #[test]
    fn set_approved_tactical_plan_marks_replay_pending_only_for_stage2a_style_approvals() {
        let mut workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            ..WorkflowState::default()
        };
        let source_ts = DateTime::parse_from_rfc3339("2026-03-30T09:35:00Z")
            .expect("source ts")
            .with_timezone(&Utc);

        set_approved_tactical_plan(&mut workflow_state, sample_tactical_plan(), source_ts, true);
        assert!(tactical_plan_replay_pending(&workflow_state));
        assert_eq!(
            workflow_state.approved_tactical_plan_source_ts_bucket,
            Some(source_ts)
        );

        set_approved_tactical_plan(
            &mut workflow_state,
            sample_tactical_plan(),
            source_ts,
            false,
        );
        assert!(!tactical_plan_replay_pending(&workflow_state));
        assert!(workflow_state.approved_tactical_plan_replayed_at.is_some());
    }

    #[test]
    fn first_stop_touch_event_in_replay_detects_late_invalidation() {
        let entry_plan = sample_fast_entry_plan("pullback", "pullback_acceptance");
        let replay_events = vec![
            sample_fast_price_event("2026-03-30T09:35:00Z", 100.8),
            sample_fast_price_event("2026-03-30T09:35:01Z", 98.4),
            sample_fast_price_event("2026-03-30T09:35:02Z", 100.9),
        ];

        let stop_touch_event = first_stop_touch_event_in_replay(&entry_plan, &replay_events)
            .expect("stop touch during replay");

        assert_eq!(
            stop_touch_event.event_ts,
            sample_fast_price_event("2026-03-30T09:35:01Z", 98.4).event_ts
        );
        assert_eq!(stop_touch_event.price, 98.4);
    }

    #[test]
    fn tactical_plan_replay_cutoff_uses_approval_timestamp_instead_of_next_fast_event() {
        let approval_ts = DateTime::parse_from_rfc3339("2026-03-30T09:39:45Z")
            .expect("approval ts")
            .with_timezone(&Utc);
        let next_fast_event_ts = DateTime::parse_from_rfc3339("2026-03-30T09:40:02Z")
            .expect("next fast event ts")
            .with_timezone(&Utc);
        let workflow_state = WorkflowState {
            symbol: "ETHUSDT".to_string(),
            approved_tactical_plan_updated_at: Some(approval_ts),
            ..WorkflowState::default()
        };

        assert_eq!(
            tactical_plan_replay_cutoff(&workflow_state, next_fast_event_ts),
            approval_ts
        );
    }

    #[test]
    fn buffered_fast_price_events_in_range_replays_15m_slice_in_order() {
        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T09:34:59Z", 100.0));
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T09:35:00Z", 100.1));
        record_fast_price_event_in_buffer(&sample_fast_price_event("2026-03-30T09:35:01Z", 100.2));

        let replay = buffered_fast_price_events_in_range(
            "ETHUSDT",
            DateTime::parse_from_rfc3339("2026-03-30T09:35:00Z")
                .expect("range start")
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2026-03-30T09:35:02Z")
                .expect("range end")
                .with_timezone(&Utc),
        );

        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].price, 100.1);
        assert_eq!(replay[1].price, 100.2);
        clear_fast_price_event_buffer_for_symbol("ETHUSDT");
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
    fn workflow_stage1_refresh_reason_retries_no_edge_at_configured_half_hour() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_NO_EDGE_RETRY";
        reset_startup_stage1_refresh_for_symbol(symbol);
        mark_startup_stage1_refresh_consumed(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T04:30:00Z")
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
            last_stage1_source_ts_bucket: Some(ts_bucket - ChronoDuration::minutes(30)),
            last_stage1_refresh_reason: Some("scheduled_2h".to_string()),
            ..WorkflowState::default()
        };
        let mut stage1_output = sample_stage1_output();
        stage1_output.monitoring_status = "no_edge".to_string();
        stage1_output.no_trade_reason = Some("path_not_actionable".to_string());
        stage1_output.current_script = None;
        stage1_output.current_path = None;

        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, Some(&stage1_output))
                .as_deref(),
            Some("scheduled_no_edge_retry")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_does_not_retry_no_edge_after_non_scheduled_refresh() {
        let config = workflow_test_config();
        let symbol = "ETHUSDT_NO_EDGE_NO_RETRY";
        reset_startup_stage1_refresh_for_symbol(symbol);
        mark_startup_stage1_refresh_consumed(symbol);
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T04:30:00Z")
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
            last_stage1_ts: Some(ts_bucket - ChronoDuration::minutes(10)),
            last_stage1_source_ts_bucket: Some(ts_bucket - ChronoDuration::minutes(10)),
            last_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            ..WorkflowState::default()
        };
        let mut stage1_output = sample_stage1_output();
        stage1_output.monitoring_status = "no_edge".to_string();
        stage1_output.no_trade_reason = Some("path_not_actionable".to_string());
        stage1_output.current_script = None;
        stage1_output.current_path = None;

        assert!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, Some(&stage1_output))
                .is_none()
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
    fn startup_stage1_immediate_stage2a_due_when_active_path_and_no_live_exposure() {
        let stage1_output = sample_stage1_output();
        assert!(startup_stage1_immediate_stage2a_due(
            Some("startup_force_stage1"),
            true,
            Some(&stage1_output),
            &sample_flat_trading_state(),
        ));
        assert!(!workflow_stage2_review_due(
            &workflow_test_config(),
            &LatestBundle {
                raw: MinuteBundleEnvelope {
                    msg_type: "bundle".to_string(),
                    routing_key: "test.route".to_string(),
                    symbol: "ETHUSDT".to_string(),
                    ts_bucket: DateTime::parse_from_rfc3339("2026-03-28T05:14:00Z")
                        .expect("ts")
                        .with_timezone(&Utc),
                    window_code: "1m".to_string(),
                    indicator_count: 0,
                    published_at: None,
                    indicators: json!({}),
                },
                indicators: json!({}),
                missing_indicator_codes: vec![],
                received_at: Utc::now(),
            },
            Some(&stage1_output),
            false,
        ));
    }

    #[test]
    fn startup_stage1_immediate_stage2a_due_is_false_when_live_exposure_exists_or_stage1_is_not_active() {
        let active_stage1 = sample_stage1_output();
        let trading_state_with_position = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![],
            open_orders: vec![],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        };
        let trading_state_with_order = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: false,
            has_open_orders: true,
            active_positions: vec![],
            open_orders: vec![],
            total_wallet_balance: 1000.0,
            available_balance: 900.0,
        };
        let mut no_edge_stage1 = sample_stage1_output();
        no_edge_stage1.monitoring_status = "no_edge".to_string();
        no_edge_stage1.current_path = None;

        assert!(!startup_stage1_immediate_stage2a_due(
            Some("startup_force_stage1"),
            true,
            Some(&active_stage1),
            &trading_state_with_position,
        ));
        assert!(!startup_stage1_immediate_stage2a_due(
            Some("startup_force_stage1"),
            true,
            Some(&active_stage1),
            &trading_state_with_order,
        ));
        assert!(!startup_stage1_immediate_stage2a_due(
            Some("startup_force_stage1"),
            true,
            Some(&no_edge_stage1),
            &sample_flat_trading_state(),
        ));
        assert!(!startup_stage1_immediate_stage2a_due(
            Some("scheduled_2h"),
            true,
            Some(&active_stage1),
            &sample_flat_trading_state(),
        ));
        assert!(!startup_stage1_immediate_stage2a_due(
            Some("startup_force_stage1"),
            false,
            Some(&active_stage1),
            &sample_flat_trading_state(),
        ));
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
    fn build_execution_trade_signal_keeps_tp1_tp2_gradient_from_execution_and_intent() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let intent = crate::workflow::schema::ExecutionIntent {
            side: "LONG".to_string(),
            entry_profile: Some("reclaim_then_hold".to_string()),
            intent_mode: "pullback".to_string(),
            entry_activation_level: Some(crate::workflow::schema::PriceZone {
                low: 2129.0,
                high: 2130.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            entry_zone: crate::workflow::schema::PriceZone {
                low: 2131.0,
                high: 2132.0,
                timeframe: None,
                label: None,
                reason: None,
            },
            entry_invalidation_level: Some(crate::workflow::schema::PriceZone {
                low: 2128.0,
                high: 2129.0,
                timeframe: None,
                label: None,
                reason: None,
            }),
            trigger_price: Some(2131.5),
            stop_loss: 2128.78,
            take_profit_1: 2140.08,
            take_profit_2: 2157.99,
            ttl_minutes: 15,
            max_drift_pct: 0.12,
            path_id: "path_a".to_string(),
            entry_snapshot: crate::workflow::schema::EntrySnapshotRef {
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
            },
            reason: Some("driver aligned".to_string()),
            quantity_override: None,
        };
        let report = ExecutionReport {
            decision: "LONG",
            quantity: "0.047".to_string(),
            leverage: 4,
            position_side: "LONG",
            dry_run: false,
            maker_entry_price: 2131.73,
            actual_take_profit: 2140.08,
            actual_stop_loss: 2128.78,
            actual_risk_reward_ratio: 2.84,
        };
        let trading_state = sample_flat_trading_state();

        let signal = build_execution_trade_signal(
            ts_bucket,
            "watcher_fast_consumer",
            "ETHUSDT",
            "workflow_watcher_fast",
            &trading_state,
            &intent,
            Some(&report),
            Some("path confirmed"),
        );

        assert_eq!(signal.take_profit_1, Some(2140.08));
        assert_eq!(signal.take_profit_2, Some(2157.99));
        assert_eq!(signal.risk_reward_ratio, Some(2.84));
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
    fn build_management_trade_signal_uses_live_position_and_snapshot_levels() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: false,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 2000.0,
                mark_price: 2012.0,
                unrealized_pnl: 12.0,
                leverage: 8,
            }],
            open_orders: Vec::new(),
            total_wallet_balance: 1000.0,
            available_balance: 500.0,
        };
        let snapshot = crate::workflow::schema::EntrySnapshot {
            symbol: "ETHUSDT".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            side: "LONG".to_string(),
            entry_profile: None,
            intent_mode: None,
            entry_activation_level: None,
            entry_zone: None,
            entry_invalidation_level: None,
            max_drift_pct: None,
            stop_loss: 1980.0,
            take_profit_1: 2040.0,
            take_profit_2: 2080.0,
            allowed_stop_loss_levels: Vec::new(),
            allowed_take_profit_levels: Vec::new(),
            tp1_realized: false,
            applied_driver_deterioration_signals: Vec::new(),
            created_at: ts_bucket,
            updated_at: ts_bucket,
        };
        let action = crate::workflow::schema::ManagementAction {
            action_type: "MOVE_STOP".to_string(),
            context_key: "ETHUSDT:LONG:path_a".to_string(),
            path_id: "path_a".to_string(),
            execution_price: None,
            reduce_ratio: None,
            new_stop_loss: Some(1990.0),
            take_profit_1: None,
            take_profit_2: None,
            reason: Some("trail the stop".to_string()),
        };
        let report = ManagementExecutionReport {
            action: "move_stop",
            dry_run: false,
            position_count: 1,
            open_order_count: 2,
            canceled_open_orders: true,
            reduce_order_ids: Vec::new(),
            close_order_ids: Vec::new(),
            modify_take_profit_order_ids: Vec::new(),
            modify_stop_loss_order_ids: vec![12345],
            realized_pnl_usdt: 0.0,
        };

        let signal = build_management_trade_signal(
            ts_bucket,
            "watcher_fast_consumer",
            "ETHUSDT",
            "workflow_stage2b_watcher_fast",
            &trading_state,
            &snapshot,
            &action,
            &report,
        );

        assert_eq!(signal.decision, "MODIFY_TPSL");
        assert_eq!(signal.entry_price, Some(2000.0));
        assert_eq!(signal.leverage, Some(8.0));
        assert_eq!(signal.take_profit_1, Some(2040.0));
        assert_eq!(signal.take_profit_2, Some(2080.0));
        assert_eq!(signal.stop_loss, Some(1990.0));
        assert_eq!(signal.risk_reward_ratio, Some(4.0));
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
