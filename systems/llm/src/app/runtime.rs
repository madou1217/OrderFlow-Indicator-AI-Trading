use crate::app::bootstrap::AppContext;
use crate::app::config::RootConfig;
use crate::app::telegram::{TelegramOperator, TradeSignalNotification};
use crate::app::x::XOperator;
use crate::execution::binance::{
    execute_workflow_execution_intent, execute_workflow_management_action,
    fetch_symbol_trading_state, ActivePositionSnapshot, ExecutionReport,
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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use tokio::sync::Mutex;
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

#[derive(Debug, Default)]
struct InvokeThrottleState {
    last_invoke_at: Option<Instant>,
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

#[derive(Debug, Clone)]
struct LatestBundle {
    raw: MinuteBundleEnvelope,
    indicators: Value,
    missing_indicator_codes: Vec<String>,
    received_at: DateTime<Utc>,
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

pub async fn run(ctx: AppContext) -> Result<()> {
    ensure_temp_indicator_dir().await?;
    ensure_temp_model_input_dir().await?;
    ensure_llm_journal_dir().await?;

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

    ctx.mq_consume_channel
        .basic_qos(500, BasicQosOptions::default())
        .await
        .context("set llm queue qos")?;

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

    let mut pending_invoke_bundle: Option<LatestBundle> = None;
    let mut last_invoked_ts_bucket: Option<DateTime<Utc>> = None;
    let invoke_throttle = Arc::new(Mutex::new(InvokeThrottleState::default()));
    let active_provider = ctx.config.active_default_model();
    let schedule_minutes = effective_schedule_minutes(&ctx.config, &active_provider);
    let min_invoke_interval_secs =
        effective_min_invoke_interval_secs(&ctx.config, &active_provider);
    let min_invoke_interval = Duration::from_secs(min_invoke_interval_secs);
    let apply_min_invoke_interval_throttle = schedule_minutes.is_empty();
    let disabled_deadline = Instant::now() + Duration::from_secs(365 * 24 * 60 * 60);
    let mut settle_timer: Pin<Box<Sleep>> = Box::pin(sleep_until(disabled_deadline));

    debug!(
        queue = %ctx.consume_queue_name,
        symbol = %ctx.config.llm.symbol,
        request_enabled = ctx.config.llm.request_enabled,
        active_provider = %active_provider,
        prompt_template = %ctx.config.llm.prompt_template,
        purge_queue_on_start = ctx.config.llm.purge_queue_on_start,
        call_schedule_minutes = ?schedule_minutes,
        bundle_settle_ms = ctx.config.llm.bundle_settle_ms,
        min_invoke_interval_secs = min_invoke_interval_secs,
        apply_min_invoke_interval_throttle = apply_min_invoke_interval_throttle,
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
                    &invoke_throttle,
                    min_invoke_interval,
                    apply_min_invoke_interval_throttle,
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

                                if bundle_matches_call_schedule(&bundle, &schedule_minutes) {
                                    pending_invoke_bundle = Some(current_bundle);
                                    settle_timer.as_mut().reset(
                                        Instant::now() + Duration::from_millis(ctx.config.llm.bundle_settle_ms)
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
        }
    }

    Ok(())
}

fn effective_schedule_minutes(config: &RootConfig, provider: &str) -> Vec<u8> {
    let _ = provider;
    config.llm.workflow.stage2_refresh_minutes.clone()
}

fn effective_min_invoke_interval_secs(config: &RootConfig, provider: &str) -> u64 {
    config
        .llm
        .min_invoke_interval_secs_by_model
        .iter()
        .find_map(|(key, v)| key.eq_ignore_ascii_case(provider).then_some(*v))
        .unwrap_or(config.llm.call_interval_secs.max(1))
}

fn bundle_matches_call_schedule(bundle: &MinuteBundleEnvelope, schedule_minutes: &[u8]) -> bool {
    let minute = bundle.ts_bucket.minute() as u8;
    schedule_minutes.contains(&minute)
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
    indicators
        .pointer(&format!(
            "/kline_history/payload/intervals/{interval_code}/markets/{market}/bars"
        ))
        .and_then(Value::as_array)
        .cloned()
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
    if stats.bars_patched > 0 || stats.divergence_events_patched > 0 {
        info!(
            symbol = %input.symbol,
            ts_bucket = %input.ts_bucket,
            trigger = trigger,
            patched_bars = stats.bars_patched,
            patched_divergence_events = stats.divergence_events_patched,
            "patched kline_history/divergence prices from db before llm invocation"
        );
    }
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
    invoke_throttle: &Arc<Mutex<InvokeThrottleState>>,
    min_invoke_interval: Duration,
    apply_min_invoke_interval_throttle: bool,
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
    let invoke_throttle = Arc::clone(invoke_throttle);
    let trigger = Arc::<str>::from(trigger.to_string());
    let ts_bucket = bundle.raw.ts_bucket;
    tokio::spawn(async move {
        let should_invoke = if apply_min_invoke_interval_throttle {
            let mut gate = invoke_throttle.lock().await;
            let now = Instant::now();
            match gate.last_invoke_at {
                Some(last) => {
                    if now.duration_since(last) < min_invoke_interval {
                        false
                    } else {
                        gate.last_invoke_at = Some(now);
                        true
                    }
                }
                None => {
                    gate.last_invoke_at = Some(now);
                    true
                }
            }
        } else {
            true
        };
        if should_invoke {
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
        } else {
            debug!(
                ts_bucket = %ts_bucket,
                trigger = %trigger,
                min_invoke_interval_secs = min_invoke_interval.as_secs(),
                "llm invoke skipped: min_invoke_interval throttle active"
            );
        }
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
    if stage1_output.is_none() {
        return Some("scheduled_4h".to_string());
    }
    let hour = bundle.raw.ts_bucket.hour() as u8;
    let minute = bundle.raw.ts_bucket.minute() as u8;
    if minute == 0 && config.llm.workflow.stage1_refresh_hours.contains(&hour) {
        return Some("scheduled_4h".to_string());
    }
    None
}

fn workflow_code_allows_execution(eval: &crate::workflow::stage2::Stage2RuntimeEvaluation) -> bool {
    eval.monitoring_status == "active"
        && !eval.no_edge_reentered
        && !eval.failure_level_breached
        && !eval.reevaluation_trigger_hit
        && eval.hard_gate.location_valid
        && eval.hard_gate.trigger_confirmed
        && eval.soft_gate.passed_count >= eval.soft_gate_min_required
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

    let mut workflow_state = crate::workflow::persistence::load_workflow_state(
        &state_dir, &symbol,
    )?
    .unwrap_or(crate::workflow::state::WorkflowState {
        symbol: symbol.clone(),
        pending_stage1_refresh_reason: None,
        last_stage1_ts: None,
    });
    workflow_state.symbol = symbol.clone();

    let mut stage1_output = crate::workflow::persistence::load_stage1_output(&state_dir, &symbol)?;
    let mut tracked_zones = crate::workflow::persistence::load_tracked_zones(&state_dir, &symbol)?;

    let stage1_refresh_reason =
        workflow_stage1_refresh_reason(&config, &bundle, &workflow_state, stage1_output.as_ref());

    if let Some(refresh_reason) = stage1_refresh_reason.clone() {
        let indicator_summary =
            crate::workflow::code_layer::build_indicator_summary(&input, &tracked_zones)?;
        let prompt_input = crate::workflow::stage1::build_stage1_prompt_input(
            indicator_summary,
            stage1_output.clone(),
            refresh_reason.clone(),
        );
        let prompt_input_value = serde_json::to_value(&prompt_input)
            .context("serialize workflow stage1 prompt input")?;
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
            return Ok(());
        }

        let outputs = crate::llm::workflow_provider::invoke_stage1_models(
            &http_client,
            &loopback_http_client,
            &config,
            &prompt_input_value,
            &symbol,
        )
        .await;

        let mut parsed_stage1: Option<crate::workflow::schema::Stage1Output> = None;
        for out in outputs {
            let payload = json!({
                "trigger": &*trigger,
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
                &symbol,
                bundle.raw.ts_bucket,
                payload.clone(),
            );
            if print_response {
                println!(
                    "WORKFLOW_STAGE1_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                    bundle.raw.ts_bucket,
                    &*trigger,
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
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "refresh_reason": refresh_reason,
                            "error": format!("{err:#}"),
                        }),
                    );
                }
            }
        }

        let parsed_stage1 =
            parsed_stage1.ok_or_else(|| anyhow!("workflow stage1 produced no valid output"))?;
        tracked_zones = parsed_stage1
            .current_path
            .as_ref()
            .map(|path| path.tracked_zones.clone())
            .unwrap_or_default();
        workflow_state.last_stage1_ts = Some(parsed_stage1.meta.stage1_ts);
        workflow_state.pending_stage1_refresh_reason = None;
        crate::workflow::persistence::save_stage1_output(&state_dir, &symbol, &parsed_stage1)?;
        crate::workflow::persistence::save_tracked_zones(&state_dir, &symbol, &tracked_zones)?;
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        stage1_output = Some(parsed_stage1);
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

    if stage1_output.monitoring_status == "no_edge"
        && !trading_state.has_active_positions
        && !trading_state.has_open_orders
    {
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

    let indicator_summary =
        crate::workflow::code_layer::build_indicator_summary(&input, &tracked_zones)?;
    let stage2_runtime_eval = crate::workflow::stage2::evaluate_stage2_runtime(
        &indicator_summary,
        &stage1_output,
        &config.llm.workflow.soft_gate_min_pass,
    )?;
    append_workflow_journal_event(
        "workflow_stage2_runtime_eval",
        &symbol,
        bundle.raw.ts_bucket,
        json!({
            "trigger": &*trigger,
            "monitoring_status": stage2_runtime_eval.monitoring_status,
            "latest_price": stage2_runtime_eval.latest_price,
            "no_edge_reentered": stage2_runtime_eval.no_edge_reentered,
            "failure_level_breached": stage2_runtime_eval.failure_level_breached,
            "reevaluation_trigger_hit": stage2_runtime_eval.reevaluation_trigger_hit,
            "activation_level_active": stage2_runtime_eval.activation_level_active,
            "setup_confirmed": stage2_runtime_eval.setup_confirmed,
            "hard_gate": {
                "location_valid": stage2_runtime_eval.hard_gate.location_valid,
                "trigger_confirmed": stage2_runtime_eval.hard_gate.trigger_confirmed,
            },
            "soft_gate": {
                "state_clear": stage2_runtime_eval.soft_gate.state_clear,
                "driver_clear": stage2_runtime_eval.soft_gate.driver_clear,
                "orderflow_real": stage2_runtime_eval.soft_gate.orderflow_real,
                "invalidation_clear": stage2_runtime_eval.soft_gate.invalidation_clear,
                "passed_count": stage2_runtime_eval.soft_gate.passed_count,
                "min_required": stage2_runtime_eval.soft_gate_min_required,
            }
        }),
    );
    let runtime_contract = crate::workflow::stage2::runtime_contract_from_evaluation(
        &indicator_summary,
        &stage1_output,
        &stage2_runtime_eval,
    );
    let mut entry_snapshots = entry_snapshots;
    let signal_entry_snapshots = entry_snapshots.clone();

    let stage2_prompt_input = crate::workflow::stage2::build_stage2_prompt_input(
        indicator_summary,
        stage1_output.clone(),
        runtime_contract.clone(),
        &trading_state,
        &entry_snapshots,
    );
    let stage2_prompt_input_value = serde_json::to_value(&stage2_prompt_input)
        .context("serialize workflow stage2 prompt input")?;
    if config.llm.workflow.persist_prompt_inputs {
        let path = persist_workflow_prompt_input_to_disk(
            &bundle.raw,
            "workflow_stage2",
            &stage2_prompt_input_value,
            retention_minutes,
        )
        .await?;
        debug!(
            symbol = %symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            path = %path.display(),
            "persisted workflow stage2 prompt input"
        );
    }

    if !config.llm.request_enabled {
        info!(
            symbol = %symbol,
            ts_bucket = %bundle.raw.ts_bucket,
            "workflow stage2 skipped because llm.request_enabled=false"
        );
        return Ok(());
    }

    let outputs = crate::llm::workflow_provider::invoke_stage2_models(
        &http_client,
        &loopback_http_client,
        &config,
        &stage2_prompt_input_value,
        &symbol,
    )
    .await;

    let mut stage2_decision: Option<crate::workflow::schema::Stage2Decision> = None;
    let mut selected_stage2_model_name: Option<String> = None;
    for out in outputs {
        let payload = json!({
            "trigger": &*trigger,
            "model_name": out.model_name,
            "provider": out.provider,
            "model_id": out.model,
            "latency_ms": out.latency_ms,
            "raw_response_text": out.raw_response_text,
            "parsed_value": out.parsed_value,
            "error": out.error,
        });
        append_workflow_journal_event(
            "workflow_stage2_response",
            &symbol,
            bundle.raw.ts_bucket,
            payload.clone(),
        );
        if print_response {
            println!(
                "WORKFLOW_STAGE2_RESPONSE ts_bucket={} trigger={} symbol={} payload={}",
                bundle.raw.ts_bucket,
                &*trigger,
                symbol,
                render_pretty_json_value(&payload)
            );
        }

        if stage2_decision.is_some() {
            continue;
        }
        let Some(value) = payload.get("parsed_value").cloned() else {
            continue;
        };
        match crate::workflow::parser::parse_stage2_decision(
            value,
            &symbol,
            &stage1_output,
            &runtime_contract,
            trading_state.has_active_positions,
            &entry_snapshots,
        ) {
            Ok(parsed) => {
                selected_stage2_model_name = Some(out.model_name.clone());
                stage2_decision = Some(parsed);
            }
            Err(err) => {
                append_workflow_journal_event(
                    "workflow_stage2_parse_error",
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

    let stage2_decision =
        stage2_decision.ok_or_else(|| anyhow!("workflow stage2 produced no valid output"))?;

    if stage2_decision.hard_gate.as_ref() != Some(&stage2_runtime_eval.hard_gate)
        || stage2_decision.soft_gate.as_ref() != Some(&stage2_runtime_eval.soft_gate)
    {
        append_workflow_journal_event(
            "workflow_stage2_gate_mismatch",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "runtime_hard_gate": stage2_runtime_eval.hard_gate,
                "runtime_soft_gate": stage2_runtime_eval.soft_gate,
                "model_hard_gate": stage2_decision.hard_gate,
                "model_soft_gate": stage2_decision.soft_gate,
            }),
        );
    }

    if let Some(request) = stage2_decision.request_stage1_reevaluation.as_ref() {
        workflow_state.pending_stage1_refresh_reason = Some(request.refresh_reason.clone());
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        append_workflow_journal_event(
            "workflow_stage1_reevaluation_requested",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "refresh_reason": request.refresh_reason,
                "trigger_source": request.trigger_source,
            }),
        );
    }

    if stage2_runtime_eval.failure_level_breached || stage2_runtime_eval.reevaluation_trigger_hit {
        workflow_state.pending_stage1_refresh_reason = Some("thesis_invalidated".to_string());
        crate::workflow::persistence::save_workflow_state(&state_dir, &workflow_state)?;
        append_workflow_journal_event(
            "workflow_stage2_forced_reevaluation",
            &symbol,
            bundle.raw.ts_bucket,
            json!({
                "trigger": &*trigger,
                "refresh_reason": "thesis_invalidated",
                "failure_level_breached": stage2_runtime_eval.failure_level_breached,
                "reevaluation_trigger_hit": stage2_runtime_eval.reevaluation_trigger_hit,
            }),
        );
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

    let mut execution_signal_report: Option<ExecutionReport> = None;
    if let Some(intent) = stage2_decision.execution_intent.as_ref() {
        let code_allows_execution = workflow_code_allows_execution(&stage2_runtime_eval);
        if config.llm.execution.enabled && !execution_blocked_due_to_stale && code_allows_execution
        {
            let adapted_intent = adapt_execution_intent(intent);
            match adapted_intent {
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
                        execution_signal_report = Some(report.clone());
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
                            intent,
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
                        }
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
                    "code_allows_execution": code_allows_execution,
                    "failure_level_breached": stage2_runtime_eval.failure_level_breached,
                    "reevaluation_trigger_hit": stage2_runtime_eval.reevaluation_trigger_hit,
                    "hard_gate": stage2_runtime_eval.hard_gate,
                    "soft_gate": stage2_runtime_eval.soft_gate,
                    "soft_gate_min_required": stage2_runtime_eval.soft_gate_min_required,
                    "path_id": intent.path_id,
                    "context_key": intent.entry_snapshot.context_key,
                }),
            );
        }
    }

    for action in &stage2_decision.management_actions {
        let snapshot = entry_snapshots.get(&action.context_key).cloned();
        if !config.llm.execution.enabled {
            append_workflow_journal_event(
                "workflow_management_skipped",
                &symbol,
                bundle.raw.ts_bucket,
                json!({
                    "trigger": &*trigger,
                    "execution_enabled": false,
                    "action": action,
                }),
            );
            continue;
        }
        let snapshot = snapshot.ok_or_else(|| {
            anyhow!(
                "workflow management snapshot missing for context_key={}",
                action.context_key
            )
        })?;
        let adapted_action = adapt_management_action(action, &snapshot);
        match adapted_action {
            Ok(adapted_action) => match execute_workflow_management_action(
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
                        "workflow_management_report",
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "context_key": action.context_key,
                            "path_id": action.path_id,
                            "action_type": action.action_type,
                            "report": {
                                "action": report.action,
                                "dry_run": report.dry_run,
                                "position_count": report.position_count,
                                "open_order_count": report.open_order_count,
                                "canceled_open_orders": report.canceled_open_orders,
                                "realized_pnl_usdt": report.realized_pnl_usdt,
                            }
                        }),
                    );
                    if !report.dry_run {
                        match crate::workflow::management::apply_management_action(
                            &snapshot,
                            action,
                            Utc::now(),
                        ) {
                            Some(next_snapshot) => {
                                crate::workflow::persistence::save_entry_snapshot(
                                    &state_dir,
                                    &next_snapshot,
                                )?;
                                entry_snapshots
                                    .insert(next_snapshot.context_key.clone(), next_snapshot);
                            }
                            None => {
                                crate::workflow::persistence::delete_entry_snapshot(
                                    &state_dir,
                                    &symbol,
                                    &action.context_key,
                                )?;
                                entry_snapshots.remove(&action.context_key);
                            }
                        }
                    }
                }
                Err(err) => {
                    append_workflow_journal_event(
                        "workflow_management_error",
                        &symbol,
                        bundle.raw.ts_bucket,
                        json!({
                            "trigger": &*trigger,
                            "context_key": action.context_key,
                            "path_id": action.path_id,
                            "action_type": action.action_type,
                            "error": format!("{err:#}"),
                        }),
                    );
                }
            },
            Err(err) => {
                append_workflow_journal_event(
                    "workflow_management_error",
                    &symbol,
                    bundle.raw.ts_bucket,
                    json!({
                        "trigger": &*trigger,
                        "context_key": action.context_key,
                        "path_id": action.path_id,
                        "action_type": action.action_type,
                        "error": format!("{err:#}"),
                        "phase": "intent_adapter",
                    }),
                );
            }
        }
    }

    let telegram_operator = TelegramOperator::from_config(&config.api.telegram);
    let x_operator = XOperator::from_config(&config.api.x);
    let signal_model_name =
        selected_stage2_model_name.unwrap_or_else(|| "workflow_stage2".to_string());
    let trade_signals = build_workflow_trade_signal_notifications(
        bundle.raw.ts_bucket,
        trigger.as_ref(),
        &symbol,
        &signal_model_name,
        &stage2_decision,
        &trading_state,
        &signal_entry_snapshots,
        execution_signal_report.as_ref(),
    );
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

fn build_workflow_trade_signal_notifications(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    stage2_decision: &crate::workflow::schema::Stage2Decision,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    execution_report: Option<&ExecutionReport>,
) -> Vec<TradeSignalNotification> {
    let mut signals = Vec::new();

    if let Some(intent) = stage2_decision.execution_intent.as_ref() {
        signals.push(build_execution_trade_signal(
            ts_bucket,
            trigger,
            symbol,
            model_name,
            stage2_decision,
            trading_state,
            intent,
            execution_report,
        ));
    }

    for action in &stage2_decision.management_actions {
        signals.push(build_management_trade_signal(
            ts_bucket,
            trigger,
            symbol,
            model_name,
            stage2_decision,
            trading_state,
            entry_snapshots,
            action,
        ));
    }

    if signals.is_empty()
        && stage2_decision.decision == "WAIT"
        && stage2_decision.request_stage1_reevaluation.is_none()
    {
        signals.push(TradeSignalNotification {
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
            reason: stage2_decision.reason.clone(),
        });
    }

    signals
}

fn build_execution_trade_signal(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    stage2_decision: &crate::workflow::schema::Stage2Decision,
    trading_state: &TradingStateSnapshot,
    intent: &crate::workflow::schema::ExecutionIntent,
    execution_report: Option<&ExecutionReport>,
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
        reason: workflow_signal_reason(&stage2_decision.reason, intent.reason.as_deref()),
    }
}

fn build_management_trade_signal(
    ts_bucket: DateTime<Utc>,
    trigger: &str,
    symbol: &str,
    model_name: &str,
    stage2_decision: &crate::workflow::schema::Stage2Decision,
    trading_state: &TradingStateSnapshot,
    entry_snapshots: &HashMap<String, crate::workflow::schema::EntrySnapshot>,
    action: &crate::workflow::schema::ManagementAction,
) -> TradeSignalNotification {
    let decision = match action.action_type.as_str() {
        "HOLD" => "HOLD",
        "REDUCE_POSITION" => "REDUCE",
        "FLATTEN_POSITION" => "CLOSE",
        "MOVE_STOP" | "UPDATE_TAKE_PROFIT" => "MODIFY_TPSL",
        other => other,
    }
    .to_string();
    let snapshot = entry_snapshots.get(&action.context_key);
    let side = snapshot
        .map(|item| item.side.as_str())
        .or_else(|| context_key_side(&action.context_key))
        .unwrap_or("LONG");
    let active_position = find_active_position_for_side(trading_state, side);
    let entry_price = active_position.map(|position| position.entry_price);
    let leverage = active_position.map(|position| position.leverage as f64);
    let take_profit_1 = action
        .take_profit_1
        .or_else(|| snapshot.map(|item| item.take_profit_1));
    let take_profit_2 = action
        .take_profit_2
        .or_else(|| snapshot.map(|item| item.take_profit_2));
    let stop_loss = action
        .new_stop_loss
        .or_else(|| snapshot.map(|item| item.stop_loss));
    let risk_reward_ratio = compute_signal_rr(entry_price, stop_loss, take_profit_1);

    TradeSignalNotification {
        ts_bucket,
        trigger: trigger.to_string(),
        symbol: symbol.to_string(),
        model_name: model_name.to_string(),
        decision,
        context_key: Some(action.context_key.clone()),
        path_id: Some(action.path_id.clone()),
        entry_price,
        leverage,
        risk_reward_ratio,
        take_profit_1,
        take_profit_2,
        stop_loss,
        reason: workflow_signal_reason(&stage2_decision.reason, action.reason.as_deref()),
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

fn context_key_side(context_key: &str) -> Option<&str> {
    let mut parts = context_key.split(':');
    let _symbol = parts.next()?;
    parts.next()
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
    retention_minutes: u64,
) -> Result<()> {
    ensure_temp_indicator_dir().await?;

    let raw_json: Value = serde_json::from_slice(raw).context("parse minute bundle as json")?;
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
    use crate::app::config::load_config;
    use crate::workflow::schema::{Stage1Meta, Stage1Output};
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
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
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
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: Some("thesis_invalidated".to_string()),
            last_stage1_ts: None,
        };
        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, None).as_deref(),
            Some("thesis_invalidated")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_triggers_on_scheduled_boundary() {
        let config = workflow_test_config();
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T04:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let bundle = LatestBundle {
            raw: MinuteBundleEnvelope {
                msg_type: "bundle".to_string(),
                routing_key: "test.route".to_string(),
                symbol: "ETHUSDT".to_string(),
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
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::hours(4)),
        };
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: ts_bucket - ChronoDuration::hours(4),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: None,
            current_script: None,
            driver_attribution: None,
            current_path: None,
        };
        assert_eq!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, Some(&stage1_output))
                .as_deref(),
            Some("scheduled_4h")
        );
    }

    #[test]
    fn workflow_stage1_refresh_reason_is_none_off_schedule_with_existing_stage1() {
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
            symbol: "ETHUSDT".to_string(),
            pending_stage1_refresh_reason: None,
            last_stage1_ts: Some(ts_bucket - ChronoDuration::minutes(15)),
        };
        let stage1_output = Stage1Output {
            meta: Stage1Meta {
                stage1_ts: ts_bucket - ChronoDuration::minutes(15),
            },
            monitoring_status: "active".to_string(),
            no_trade_reason: None,
            refresh_hints: vec![],
            map_summary: None,
            current_script: None,
            driver_attribution: None,
            current_path: None,
        };
        assert!(
            workflow_stage1_refresh_reason(&config, &bundle, &state, Some(&stage1_output))
                .is_none()
        );
    }

    #[test]
    fn workflow_stage2_schedule_follows_15m_refresh_minutes() {
        let config = workflow_test_config();
        let schedule = effective_schedule_minutes(&config, "custom_llm");
        assert_eq!(schedule, vec![0, 15, 30, 45]);

        let matching = MinuteBundleEnvelope {
            msg_type: "bundle".to_string(),
            routing_key: "test.route".to_string(),
            symbol: "ETHUSDT".to_string(),
            ts_bucket: DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            window_code: "15m".to_string(),
            indicator_count: 0,
            published_at: None,
            indicators: json!({}),
        };
        let non_matching = MinuteBundleEnvelope {
            ts_bucket: DateTime::parse_from_rfc3339("2026-03-28T05:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            ..matching.clone()
        };

        assert!(bundle_matches_call_schedule(&matching, &schedule));
        assert!(!bundle_matches_call_schedule(&non_matching, &schedule));
    }

    #[test]
    fn workflow_execution_is_blocked_when_reevaluation_trigger_hits() {
        let eval = crate::workflow::stage2::Stage2RuntimeEvaluation {
            monitoring_status: "active".to_string(),
            latest_price: 2000.0,
            no_edge_reentered: false,
            failure_level_breached: false,
            reevaluation_trigger_hit: true,
            activation_level_active: true,
            setup_confirmed: true,
            hard_gate: crate::workflow::schema::HardGateEvaluation {
                location_valid: true,
                trigger_confirmed: true,
            },
            soft_gate: crate::workflow::schema::SoftGateEvaluation {
                state_clear: true,
                driver_clear: true,
                orderflow_real: true,
                invalidation_clear: true,
                passed_count: 4,
            },
            soft_gate_min_required: 3,
        };

        assert!(!workflow_code_allows_execution(&eval));
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
    fn workflow_trade_signal_notifications_mark_same_side_execution_as_add() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let decision = crate::workflow::schema::Stage2Decision {
            decision: "EXECUTE".to_string(),
            reason: "confirmed".to_string(),
            request_stage1_reevaluation: None,
            execution_intent: Some(crate::workflow::schema::ExecutionIntent {
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
                    context_key: "ETHUSDT:LONG:path_a".to_string(),
                    path_id: "path_a".to_string(),
                },
                reason: Some("driver aligned".to_string()),
            }),
            management_actions: Vec::new(),
            hard_gate: None,
            soft_gate: None,
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

        let signals = build_workflow_trade_signal_notifications(
            ts_bucket,
            "schedule",
            "ETHUSDT",
            "custom_llm",
            &decision,
            &trading_state,
            &HashMap::new(),
            None,
        );

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].decision, "ADD");
        assert_eq!(signals[0].entry_price, Some(2000.0));
        assert_eq!(signals[0].take_profit_1, Some(2040.0));
        assert_eq!(signals[0].take_profit_2, Some(2080.0));
        assert_eq!(signals[0].stop_loss, Some(1980.0));
        assert_eq!(signals[0].risk_reward_ratio, Some(2.0));
    }

    #[test]
    fn workflow_trade_signal_notifications_map_move_stop_to_modify_tpsl() {
        let ts_bucket = DateTime::parse_from_rfc3339("2026-03-28T05:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let decision = crate::workflow::schema::Stage2Decision {
            decision: "WAIT".to_string(),
            reason: "manage open context".to_string(),
            request_stage1_reevaluation: None,
            execution_intent: None,
            management_actions: vec![crate::workflow::schema::ManagementAction {
                action_type: "MOVE_STOP".to_string(),
                context_key: "ETHUSDT:LONG:path_a".to_string(),
                path_id: "path_a".to_string(),
                reduce_ratio: None,
                new_stop_loss: Some(2010.0),
                take_profit_1: None,
                take_profit_2: None,
                reason: Some("lock gains".to_string()),
            }],
            hard_gate: None,
            soft_gate: None,
        };
        let trading_state = TradingStateSnapshot {
            symbol: "ETHUSDT".to_string(),
            has_active_context: true,
            has_active_positions: true,
            has_open_orders: true,
            active_positions: vec![ActivePositionSnapshot {
                position_side: "LONG".to_string(),
                position_amt: 1.0,
                entry_price: 2000.0,
                mark_price: 2020.0,
                unrealized_pnl: 20.0,
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
                created_at: ts_bucket,
                updated_at: ts_bucket,
            },
        );

        let signals = build_workflow_trade_signal_notifications(
            ts_bucket,
            "schedule",
            "ETHUSDT",
            "custom_llm",
            &decision,
            &trading_state,
            &entry_snapshots,
            None,
        );

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].decision, "MODIFY_TPSL");
        assert_eq!(signals[0].entry_price, Some(2000.0));
        assert_eq!(signals[0].stop_loss, Some(2010.0));
        assert_eq!(signals[0].take_profit_1, Some(2040.0));
        assert_eq!(signals[0].take_profit_2, Some(2080.0));
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
}
