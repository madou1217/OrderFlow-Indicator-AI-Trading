use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::BinanceRestClient;
use crate::exchange::binance::rest::long_short_ratio::BinanceLongShortRatioRecord;
use crate::exchange::binance::rest::open_interest_hist::BinanceOpenInterestHistRecord;
use crate::normalize::{funding_rate_normalizer, mark_price_normalizer, NormalizedMdEvent};
use crate::normalize::{long_short_ratio_normalizer, open_interest_normalizer};
use crate::observability::metrics::AppMetrics;
use crate::pipelines::persist_async;
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_writer::OutboxWriter,
};
use crate::state::checkpoints;
use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use serde_json::json;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

const FUNDING_BACKFILL_INTERVAL_SECS: u64 = 60;
const PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS: u64 = 20;
const EXCHANGE_INFO_REFRESH_INTERVAL_SECS: u64 = 86_400;
const OPEN_INTEREST_CURRENT_INTERVAL_SECS: u64 = 60;
const OI_RATIO_STRUCTURE_POLL_INTERVAL_SECS: u64 = 15;
const OI_RATIO_LIVE_READY_GRACE_SECS: i64 = 20;
const OI_RATIO_TOTAL_RETRY_BUDGET_SECS: i64 = 300;
const OI_RATIO_BACKFILL_DAYS: i64 = 30;
const OI_RATIO_FETCH_LIMIT: u16 = 500;

pub async fn run_funding_rate_backfill_loop(
    ctx: Arc<AppContext>,
    rest_client: Arc<BinanceRestClient>,
    db_writer: Arc<MdDbWriter>,
    ops_writer: Arc<OpsDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let symbol = ctx.config.market_data.symbol.as_str();
    let mut last_funding_time: Option<i64> = None;
    let mut last_premium_time: Option<i64> = None;
    let mut spot_exchange_info_bootstrapped = false;
    let mut futures_exchange_info_bootstrapped = false;
    match checkpoints::load_checkpoint_seeds(&ctx.ops_db_pool, "futures", symbol).await {
        Ok(seeds) => {
            for seed in &seeds {
                let Some(last_ts) = seed.last_event_ts else {
                    continue;
                };
                let ts_ms = last_ts.timestamp_millis();
                match seed.stream_name.as_str() {
                    "fapi/v1/fundingRate" => {
                        let current = last_funding_time.unwrap_or(i64::MIN);
                        if ts_ms > current {
                            last_funding_time = Some(ts_ms);
                        }
                    }
                    "fapi/v1/premiumIndex" => {
                        let current = last_premium_time.unwrap_or(i64::MIN);
                        if ts_ms > current {
                            last_premium_time = Some(ts_ms);
                        }
                    }
                    _ => {}
                }
            }
            info!(
                market = "futures",
                symbol = symbol,
                seed_count = seeds.len(),
                last_funding_time = ?last_funding_time,
                last_premium_time = ?last_premium_time,
                "backfill scheduler seeded from checkpoints"
            );
        }
        Err(err) => {
            warn!(error = %err, market = "futures", symbol = symbol, "load checkpoint seeds for scheduler failed");
        }
    }

    let mut funding_ticker = interval(Duration::from_secs(FUNDING_BACKFILL_INTERVAL_SECS));
    funding_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut premium_ticker = interval(Duration::from_secs(PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS));
    premium_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut exchange_info_ticker =
        interval(Duration::from_secs(EXCHANGE_INFO_REFRESH_INTERVAL_SECS));
    exchange_info_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        symbol = symbol,
        funding_interval_secs = FUNDING_BACKFILL_INTERVAL_SECS,
        premium_interval_secs = PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS,
        exchange_info_interval_secs = EXCHANGE_INFO_REFRESH_INTERVAL_SECS,
        "backfill scheduler started"
    );

    loop {
        tokio::select! {
            _ = funding_ticker.tick() => {
                handle_funding_rate(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    symbol,
                    &mut last_funding_time,
                ).await;
            }
            _ = premium_ticker.tick() => {
                handle_premium_index(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    symbol,
                    &mut last_premium_time,
                    ctx.rest_proxy_url.is_some(),
                ).await;
            }
            _ = exchange_info_ticker.tick() => {
                let spot_trigger = if spot_exchange_info_bootstrapped {
                    "reconcile"
                } else {
                    "startup"
                };
                let futures_trigger = if futures_exchange_info_bootstrapped {
                    "reconcile"
                } else {
                    "startup"
                };

                handle_exchange_info(
                    "spot",
                    spot_trigger,
                    &rest_client,
                    &ops_writer,
                    symbol,
                ).await;
                handle_exchange_info(
                    "futures",
                    futures_trigger,
                    &rest_client,
                    &ops_writer,
                    symbol,
                ).await;

                spot_exchange_info_bootstrapped = true;
                futures_exchange_info_bootstrapped = true;
            }
        }
    }
}

pub async fn run_open_interest_ratio_loop(
    ctx: Arc<AppContext>,
    rest_client: Arc<BinanceRestClient>,
    db_writer: Arc<MdDbWriter>,
    ops_writer: Arc<OpsDbWriter>,
    publisher: Arc<MqPublisher>,
    outbox_writer: Arc<OutboxWriter>,
    metrics: Arc<AppMetrics>,
) -> Result<()> {
    let symbol = ctx.config.market_data.symbol.to_ascii_uppercase();
    let mut last_current_oi_ts = load_latest_open_interest_current_ts(&ctx.md_db_pool, &symbol)
        .await
        .unwrap_or(None);
    let mut last_common_bucket = load_latest_common_oi_ratio_bucket(&ctx.md_db_pool, &symbol)
        .await
        .unwrap_or(None);
    let mut current_ticker = interval(Duration::from_secs(OPEN_INTEREST_CURRENT_INTERVAL_SECS));
    current_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut structure_ticker = interval(Duration::from_secs(OI_RATIO_STRUCTURE_POLL_INTERVAL_SECS));
    structure_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    if let Err(err) = backfill_recent_oi_ratio_history(
        &rest_client,
        &db_writer,
        &ops_writer,
        &publisher,
        &outbox_writer,
        &metrics,
        &symbol,
    )
    .await
    {
        warn!(
            error = %err,
            symbol = symbol,
            "startup open interest / ratio backfill failed"
        );
    } else if let Ok(latest) = load_latest_common_oi_ratio_bucket(&ctx.md_db_pool, &symbol).await {
        last_common_bucket = latest;
    }

    info!(
        symbol = symbol,
        current_interval_secs = OPEN_INTEREST_CURRENT_INTERVAL_SECS,
        structure_poll_secs = OI_RATIO_STRUCTURE_POLL_INTERVAL_SECS,
        retry_budget_secs = OI_RATIO_TOTAL_RETRY_BUDGET_SECS,
        "open interest / long short ratio scheduler started"
    );

    loop {
        tokio::select! {
            _ = current_ticker.tick() => {
                handle_current_open_interest(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    &symbol,
                    &mut last_current_oi_ts,
                ).await;
            }
            _ = structure_ticker.tick() => {
                handle_oi_ratio_live_bucket(
                    &rest_client,
                    &db_writer,
                    &ops_writer,
                    &publisher,
                    &outbox_writer,
                    &metrics,
                    &symbol,
                    &mut last_common_bucket,
                ).await;
            }
        }
    }
}

async fn handle_funding_rate(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
    last_funding_time: &mut Option<i64>,
) {
    let row = match rest_client.fetch_latest_funding_rate(symbol).await {
        Ok(row) => row,
        Err(err) => {
            warn!(error = %err, symbol = symbol, "fetch funding rate failed");
            return;
        }
    };

    let Some(record) = row else {
        warn!(symbol = symbol, "funding rate api returned empty result");
        return;
    };

    if *last_funding_time == Some(record.funding_time) {
        return;
    }

    let event = match funding_rate_normalizer::normalize_rest_record(&record, false) {
        Ok(event) => event,
        Err(err) => {
            warn!(
                error = %err,
                symbol = symbol,
                funding_time = record.funding_time,
                "normalize funding rate failed"
            );
            metrics.inc_normalize_error();
            return;
        }
    };

    if let Err(err) = persist_scheduler_event(
        &event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
    )
    .await
    {
        error!(
            error = %err,
            symbol = symbol,
            funding_time = record.funding_time,
            "persist funding rate event failed"
        );
        return;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(symbol),
            "/fapi/v1/fundingRate",
            "funding_history_backfill",
            "reconcile",
            json!({ "limit": 1 }),
            Some(1),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, "write funding backfill job run failed");
    }

    *last_funding_time = Some(record.funding_time);
}

async fn handle_premium_index(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
    last_premium_time: &mut Option<i64>,
    proxy_enabled: bool,
) {
    let premium = match rest_client.fetch_premium_index(symbol).await {
        Ok(v) => v,
        Err(err) => {
            warn!(
                error = %err,
                symbol = symbol,
                proxy_enabled = proxy_enabled,
                interval_secs = PREMIUM_INDEX_BOOTSTRAP_INTERVAL_SECS,
                "fetch premium index failed"
            );
            return;
        }
    };

    let premium_time = premium.time.unwrap_or_default();
    if *last_premium_time == Some(premium_time) {
        return;
    }

    let event = match mark_price_normalizer::normalize_premium_index_rest(
        symbol, &premium, "rest", false,
    ) {
        Ok(event) => event,
        Err(err) => {
            metrics.inc_normalize_error();
            warn!(error = %err, symbol = symbol, "normalize premium index failed");
            return;
        }
    };

    if let Err(err) = persist_scheduler_event(
        &event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
    )
    .await
    {
        error!(error = %err, symbol = symbol, "persist premium index mark price failed");
        return;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(symbol),
            "/fapi/v1/premiumIndex",
            "mark_funding_bootstrap",
            "reconcile",
            json!({}),
            Some(1),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, "write premium index bootstrap job run failed");
    }

    *last_premium_time = Some(premium_time);
}

#[derive(Clone)]
struct RatioBucketSet {
    oi_hist: BinanceOpenInterestHistRecord,
    global_account: BinanceLongShortRatioRecord,
    top_account: BinanceLongShortRatioRecord,
    top_position: BinanceLongShortRatioRecord,
}

async fn handle_current_open_interest(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
    last_current_oi_ts: &mut Option<i64>,
) {
    let deadline = Utc::now() + ChronoDuration::seconds(OI_RATIO_TOTAL_RETRY_BUDGET_SECS);
    let mut backoff_secs = 3u64;
    loop {
        let open_interest = match rest_client.fetch_open_interest(symbol).await {
            Ok(value) => value,
            Err(err) => {
                if Utc::now() >= deadline {
                    warn!(
                        error = %err,
                        symbol = symbol,
                        "fetch current open interest exhausted retry budget"
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_secs(backoff_secs.min(60))).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        };

        if *last_current_oi_ts == Some(open_interest.time) {
            return;
        }

        let mark_price = match rest_client.fetch_premium_index(symbol).await {
            Ok(premium) => premium
                .mark_price
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok()),
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = symbol,
                    "fetch premium index for current open interest failed"
                );
                None
            }
        };

        let event = match open_interest_normalizer::normalize_current_rest(
            "futures",
            symbol,
            &open_interest,
            mark_price,
            false,
        ) {
            Ok(event) => event,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = symbol,
                    "normalize current open interest failed"
                );
                return;
            }
        };

        if let Err(err) = persist_scheduler_event(
            &event,
            db_writer,
            publisher,
            outbox_writer,
            ops_writer,
            metrics,
        )
        .await
        {
            warn!(
                error = %err,
                symbol = symbol,
                "persist current open interest failed"
            );
            return;
        }

        if let Err(err) = ops_writer
            .insert_backfill_job_run(
                Some("futures"),
                Some(symbol),
                "/fapi/v1/openInterest",
                "open_interest_current_1m",
                "reconcile",
                json!({}),
                Some(1),
                "success",
                None,
            )
            .await
        {
            warn!(error = %err, symbol = symbol, "write open interest current job run failed");
        }

        *last_current_oi_ts = Some(open_interest.time);
        return;
    }
}

async fn handle_oi_ratio_live_bucket(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
    last_common_bucket: &mut Option<DateTime<Utc>>,
) {
    let now = Utc::now();
    let boundary_bucket = floor_to_5m(now);
    if boundary_bucket + ChronoDuration::seconds(OI_RATIO_LIVE_READY_GRACE_SECS) > now {
        return;
    }
    let Some(target_bucket) = boundary_bucket.checked_sub_signed(ChronoDuration::minutes(5)) else {
        return;
    };
    if last_common_bucket
        .map(|ts| ts >= target_bucket)
        .unwrap_or(false)
    {
        return;
    }

    let bucket_set = match fetch_live_bucket_set(rest_client, symbol, target_bucket).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return;
        }
        Err(err) => {
            warn!(
                error = %err,
                symbol = symbol,
                target_bucket = %target_bucket,
                "fetch live open interest / ratio bucket failed"
            );
            return;
        }
    };

    if let Err(err) = persist_ratio_bucket_set(
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
        symbol,
        &bucket_set,
        false,
    )
    .await
    {
        warn!(
            error = %err,
            symbol = symbol,
            target_bucket = %target_bucket,
            "persist live open interest / ratio bucket failed"
        );
        return;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(symbol),
            "/futures/data/openInterestHist + longShortRatios",
            "oi_ratio_live_5m",
            "reconcile",
            json!({ "target_bucket": target_bucket.to_rfc3339() }),
            Some(4),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, symbol = symbol, "write oi/ratio live job run failed");
    }

    *last_common_bucket = Some(target_bucket);
}

async fn backfill_recent_oi_ratio_history(
    rest_client: &Arc<BinanceRestClient>,
    db_writer: &Arc<MdDbWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
) -> Result<()> {
    let end = Utc::now();
    let start = end - ChronoDuration::days(OI_RATIO_BACKFILL_DAYS);
    let start_ms = start.timestamp_millis();
    let end_ms = end.timestamp_millis();

    let oi_hist = fetch_open_interest_hist_range(rest_client, symbol, start_ms, end_ms).await?;
    let global_ratio =
        fetch_ratio_range(rest_client, symbol, "global_account", start_ms, end_ms).await?;
    let top_account =
        fetch_ratio_range(rest_client, symbol, "top_account", start_ms, end_ms).await?;
    let top_position =
        fetch_ratio_range(rest_client, symbol, "top_position", start_ms, end_ms).await?;

    let common = align_ratio_buckets(oi_hist, global_ratio, top_account, top_position);
    let row_count = common.len() as i64 * 4;
    for bucket_set in common.values() {
        persist_ratio_bucket_set(
            db_writer,
            publisher,
            outbox_writer,
            ops_writer,
            metrics,
            symbol,
            bucket_set,
            true,
        )
        .await?;
    }

    ops_writer
        .insert_backfill_job_run(
            Some("futures"),
            Some(symbol),
            "/futures/data/openInterestHist + longShortRatios",
            "oi_ratio_history_backfill",
            "startup",
            json!({
                "days": OI_RATIO_BACKFILL_DAYS,
                "start_time_ms": start_ms,
                "end_time_ms": end_ms,
            }),
            Some(row_count),
            "success",
            None,
        )
        .await?;

    Ok(())
}

async fn fetch_open_interest_hist_range(
    rest_client: &Arc<BinanceRestClient>,
    symbol: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Vec<BinanceOpenInterestHistRecord>> {
    let mut cursor_end = end_time_ms;
    let mut out = BTreeMap::<i64, BinanceOpenInterestHistRecord>::new();
    while cursor_end >= start_time_ms {
        let batch = rest_client
            .fetch_open_interest_hist(symbol, "5m", None, Some(cursor_end), OI_RATIO_FETCH_LIMIT)
            .await?;
        if batch.is_empty() {
            break;
        }
        let mut min_ts = i64::MAX;
        for row in batch {
            min_ts = min_ts.min(row.timestamp);
            if row.timestamp < start_time_ms || row.timestamp > end_time_ms {
                continue;
            }
            out.entry(row.timestamp).or_insert(row);
        }
        if min_ts == i64::MAX {
            break;
        }
        if min_ts <= start_time_ms {
            break;
        }
        let Some(next_end) = min_ts.checked_sub(1) else {
            break;
        };
        if next_end >= cursor_end {
            break;
        }
        cursor_end = next_end;
    }
    Ok(out.into_values().collect())
}

async fn fetch_ratio_range(
    rest_client: &Arc<BinanceRestClient>,
    symbol: &str,
    ratio_type: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> Result<Vec<BinanceLongShortRatioRecord>> {
    let mut cursor_end = end_time_ms;
    let mut out = BTreeMap::<i64, BinanceLongShortRatioRecord>::new();
    while cursor_end >= start_time_ms {
        let batch = match ratio_type {
            "global_account" => {
                rest_client
                    .fetch_global_long_short_account_ratio(
                        symbol,
                        "5m",
                        None,
                        Some(cursor_end),
                        OI_RATIO_FETCH_LIMIT,
                    )
                    .await?
            }
            "top_account" => {
                rest_client
                    .fetch_top_long_short_account_ratio(
                        symbol,
                        "5m",
                        None,
                        Some(cursor_end),
                        OI_RATIO_FETCH_LIMIT,
                    )
                    .await?
            }
            "top_position" => {
                rest_client
                    .fetch_top_long_short_position_ratio(
                        symbol,
                        "5m",
                        None,
                        Some(cursor_end),
                        OI_RATIO_FETCH_LIMIT,
                    )
                    .await?
            }
            other => unreachable!("unsupported ratio_type {other}"),
        };
        if batch.is_empty() {
            break;
        }
        let mut min_ts = i64::MAX;
        for row in batch {
            min_ts = min_ts.min(row.timestamp);
            if row.timestamp < start_time_ms || row.timestamp > end_time_ms {
                continue;
            }
            out.entry(row.timestamp).or_insert(row);
        }
        if min_ts == i64::MAX {
            break;
        }
        if min_ts <= start_time_ms {
            break;
        }
        let Some(next_end) = min_ts.checked_sub(1) else {
            break;
        };
        if next_end >= cursor_end {
            break;
        }
        cursor_end = next_end;
    }
    Ok(out.into_values().collect())
}

async fn fetch_live_bucket_set(
    rest_client: &Arc<BinanceRestClient>,
    symbol: &str,
    target_bucket: DateTime<Utc>,
) -> Result<Option<RatioBucketSet>> {
    let target_ms = target_bucket.timestamp_millis();
    let deadline = Utc::now() + ChronoDuration::seconds(OI_RATIO_TOTAL_RETRY_BUDGET_SECS);
    let mut wait_secs = 10u64;

    loop {
        let oi_hist = rest_client
            .fetch_open_interest_hist(symbol, "5m", None, None, 4)
            .await?;
        let global_ratio = rest_client
            .fetch_global_long_short_account_ratio(symbol, "5m", None, None, 4)
            .await?;
        let top_account = rest_client
            .fetch_top_long_short_account_ratio(symbol, "5m", None, None, 4)
            .await?;
        let top_position = rest_client
            .fetch_top_long_short_position_ratio(symbol, "5m", None, None, 4)
            .await?;

        let bundle = live_bucket_set_from_target(
            &oi_hist,
            &global_ratio,
            &top_account,
            &top_position,
            target_ms,
        );
        if bundle.is_some() {
            return Ok(bundle);
        }

        if Utc::now() >= deadline {
            return Ok(None);
        }

        tokio::time::sleep(Duration::from_secs(wait_secs.min(60))).await;
        wait_secs = (wait_secs * 2).min(60);
    }
}

fn live_bucket_set_from_target(
    oi_hist: &[BinanceOpenInterestHistRecord],
    global_ratio: &[BinanceLongShortRatioRecord],
    top_account: &[BinanceLongShortRatioRecord],
    top_position: &[BinanceLongShortRatioRecord],
    target_ms: i64,
) -> Option<RatioBucketSet> {
    Some(RatioBucketSet {
        oi_hist: oi_hist
            .iter()
            .find(|row| row.timestamp == target_ms)?
            .clone(),
        global_account: global_ratio
            .iter()
            .find(|row| row.timestamp == target_ms)?
            .clone(),
        top_account: top_account
            .iter()
            .find(|row| row.timestamp == target_ms)?
            .clone(),
        top_position: top_position
            .iter()
            .find(|row| row.timestamp == target_ms)?
            .clone(),
    })
}

fn align_ratio_buckets(
    oi_hist: Vec<BinanceOpenInterestHistRecord>,
    global_ratio: Vec<BinanceLongShortRatioRecord>,
    top_account: Vec<BinanceLongShortRatioRecord>,
    top_position: Vec<BinanceLongShortRatioRecord>,
) -> BTreeMap<i64, RatioBucketSet> {
    let oi_by_ts = oi_hist
        .into_iter()
        .map(|row| (row.timestamp, row))
        .collect::<BTreeMap<_, _>>();
    let global_by_ts = global_ratio
        .into_iter()
        .map(|row| (row.timestamp, row))
        .collect::<BTreeMap<_, _>>();
    let top_account_by_ts = top_account
        .into_iter()
        .map(|row| (row.timestamp, row))
        .collect::<BTreeMap<_, _>>();
    let top_position_by_ts = top_position
        .into_iter()
        .map(|row| (row.timestamp, row))
        .collect::<BTreeMap<_, _>>();

    let oi_keys = oi_by_ts.keys().copied().collect::<BTreeSet<_>>();
    let global_keys = global_by_ts.keys().copied().collect::<BTreeSet<_>>();
    let top_account_keys = top_account_by_ts.keys().copied().collect::<BTreeSet<_>>();
    let top_position_keys = top_position_by_ts.keys().copied().collect::<BTreeSet<_>>();

    let common = oi_keys
        .intersection(&global_keys)
        .copied()
        .collect::<BTreeSet<_>>();
    let common = common
        .intersection(&top_account_keys)
        .copied()
        .collect::<BTreeSet<_>>();
    let common = common
        .intersection(&top_position_keys)
        .copied()
        .collect::<BTreeSet<_>>();

    common
        .into_iter()
        .map(|ts| {
            (
                ts,
                RatioBucketSet {
                    oi_hist: oi_by_ts.get(&ts).expect("oi ts").clone(),
                    global_account: global_by_ts.get(&ts).expect("global ts").clone(),
                    top_account: top_account_by_ts.get(&ts).expect("top_account ts").clone(),
                    top_position: top_position_by_ts
                        .get(&ts)
                        .expect("top_position ts")
                        .clone(),
                },
            )
        })
        .collect()
}

async fn persist_ratio_bucket_set(
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
    symbol: &str,
    bucket_set: &RatioBucketSet,
    backfill_in_progress: bool,
) -> Result<()> {
    let oi_event = open_interest_normalizer::normalize_hist_5m_rest(
        "futures",
        symbol,
        &bucket_set.oi_hist,
        backfill_in_progress,
    )?;
    persist_scheduler_event(
        &oi_event,
        db_writer,
        publisher,
        outbox_writer,
        ops_writer,
        metrics,
    )
    .await?;

    for (ratio_type, stream_name, record) in [
        (
            "global_account",
            "futures/data/globalLongShortAccountRatio",
            &bucket_set.global_account,
        ),
        (
            "top_account",
            "futures/data/topLongShortAccountRatio",
            &bucket_set.top_account,
        ),
        (
            "top_position",
            "futures/data/topLongShortPositionRatio",
            &bucket_set.top_position,
        ),
    ] {
        let event = long_short_ratio_normalizer::normalize_5m_rest(
            "futures",
            symbol,
            ratio_type,
            stream_name,
            record,
            backfill_in_progress,
        )?;
        persist_scheduler_event(
            &event,
            db_writer,
            publisher,
            outbox_writer,
            ops_writer,
            metrics,
        )
        .await?;
    }

    Ok(())
}

async fn load_latest_open_interest_current_ts(
    pool: &sqlx::PgPool,
    symbol: &str,
) -> Result<Option<i64>> {
    let row = sqlx::query(
        r#"
        SELECT EXTRACT(EPOCH FROM MAX(ts_event)) * 1000 AS ts_ms
        FROM md.open_interest_current_1m
        WHERE market = 'futures'::cfg.market_type
          AND symbol = $1
        "#,
    )
    .bind(symbol)
    .fetch_one(pool)
    .await?;
    Ok(row
        .try_get::<Option<f64>, _>("ts_ms")?
        .map(|value| value as i64))
}

async fn load_latest_common_oi_ratio_bucket(
    pool: &sqlx::PgPool,
    symbol: &str,
) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        WITH latest_oi AS (
            SELECT MAX(ts_bucket) AS ts_bucket
            FROM md.open_interest_hist_5m
            WHERE market = 'futures'::cfg.market_type
              AND symbol = $1
        ),
        latest_global AS (
            SELECT MAX(ts_bucket) AS ts_bucket
            FROM md.long_short_ratio_5m
            WHERE market = 'futures'::cfg.market_type
              AND symbol = $1
              AND ratio_type = 'global_account'
        ),
        latest_top_account AS (
            SELECT MAX(ts_bucket) AS ts_bucket
            FROM md.long_short_ratio_5m
            WHERE market = 'futures'::cfg.market_type
              AND symbol = $1
              AND ratio_type = 'top_account'
        ),
        latest_top_position AS (
            SELECT MAX(ts_bucket) AS ts_bucket
            FROM md.long_short_ratio_5m
            WHERE market = 'futures'::cfg.market_type
              AND symbol = $1
              AND ratio_type = 'top_position'
        )
        SELECT LEAST(
            latest_oi.ts_bucket,
            latest_global.ts_bucket,
            latest_top_account.ts_bucket,
            latest_top_position.ts_bucket
        ) AS ts_bucket
        FROM latest_oi, latest_global, latest_top_account, latest_top_position
        "#,
    )
    .bind(symbol)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<Option<DateTime<Utc>>, _>("ts_bucket")?)
}

fn floor_to_5m(ts: DateTime<Utc>) -> DateTime<Utc> {
    let minute = ts.minute() - (ts.minute() % 5);
    ts.with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .and_then(|value| value.with_minute(minute))
        .unwrap_or(ts)
}

async fn persist_scheduler_event(
    event: &NormalizedMdEvent,
    db_writer: &Arc<MdDbWriter>,
    publisher: &Arc<MqPublisher>,
    outbox_writer: &Arc<OutboxWriter>,
    ops_writer: &Arc<OpsDbWriter>,
    metrics: &Arc<AppMetrics>,
) -> Result<()> {
    for _ in 0..20 {
        if persist_async::try_enqueue_registered_persist_event(event.market.as_str(), event.clone())
            .await?
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    if matches!(
        event.msg_type.as_str(),
        "md.open_interest_current" | "md.open_interest_hist_5m" | "md.long_short_ratio_5m"
    ) {
        persist_async::persist_event(
            event,
            db_writer,
            publisher,
            outbox_writer,
            ops_writer,
            metrics,
        )
        .await
    } else {
        warn!(
            market = event.market,
            symbol = event.symbol,
            msg_type = event.msg_type,
            "registered persist queue unavailable; skip direct scheduler persist to avoid duplicate canonical aggregates"
        );
        Ok(())
    }
}

async fn handle_exchange_info(
    market: &str,
    trigger_type: &str,
    rest_client: &Arc<BinanceRestClient>,
    ops_writer: &Arc<OpsDbWriter>,
    symbol: &str,
) {
    let endpoint = if market == "spot" {
        "/api/v3/exchangeInfo"
    } else {
        "/fapi/v1/exchangeInfo"
    };

    let info = match rest_client.fetch_exchange_info(market, Some(symbol)).await {
        Ok(v) => v,
        Err(err) => {
            warn!(error = %err, market, symbol = symbol, "fetch exchange info failed");
            let err_message = err.to_string();
            if let Err(e) = ops_writer
                .insert_backfill_job_run(
                    Some(market),
                    Some(symbol),
                    endpoint,
                    "bootstrap_metadata",
                    trigger_type,
                    json!({ "symbol": symbol }),
                    Some(0),
                    "failed",
                    Some(err_message.as_str()),
                )
                .await
            {
                warn!(error = %e, market, "write failed exchange info job run failed");
            }
            return;
        }
    };

    let mut rows = 0i64;
    for symbol_info in &info.symbols {
        if !symbol_info.symbol.eq_ignore_ascii_case(symbol) {
            continue;
        }

        let is_active = symbol_info
            .status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("TRADING"))
            .unwrap_or(true);
        let metadata = json!({
            "timezone": info.timezone,
            "status": symbol_info.status,
            "filters": symbol_info.filters,
        });

        if let Err(err) = ops_writer
            .upsert_instrument_metadata(
                market,
                &symbol_info.symbol,
                symbol_info.contract_type.as_deref(),
                symbol_info.quote_asset.as_deref(),
                symbol_info.base_asset.as_deref(),
                symbol_info.price_precision,
                symbol_info.quantity_precision,
                is_active,
                metadata,
            )
            .await
        {
            warn!(
                error = %err,
                market,
                symbol = %symbol_info.symbol,
                "upsert cfg.instrument metadata failed"
            );
            continue;
        }
        rows += 1;
    }

    if let Err(err) = ops_writer
        .insert_backfill_job_run(
            Some(market),
            Some(symbol),
            endpoint,
            "bootstrap_metadata",
            trigger_type,
            json!({ "symbol": symbol }),
            Some(rows),
            "success",
            None,
        )
        .await
    {
        warn!(error = %err, market, "write exchange info job run failed");
    }
}
