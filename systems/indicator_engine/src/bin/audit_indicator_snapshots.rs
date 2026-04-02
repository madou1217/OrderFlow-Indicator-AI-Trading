use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use indicator_engine::app::bootstrap::{build_db_pool, load_config};
use indicator_engine::app::runtime::{
    build_indicator_runtime_options, fetch_backfill_batch, load_kline_history_supplement,
    replay_row_to_engine_event, ReplayRow,
};
use indicator_engine::indicators::context::{IndicatorContext, IndicatorSnapshotRow};
use indicator_engine::indicators::indicator_trait::Indicator;
use indicator_engine::indicators::registry::build_registry;
use indicator_engine::runtime::state_store::StateStore;
use indicator_engine::runtime::window_scheduler::WindowScheduler;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::Arc;

const DEFAULT_CONFIG_PATH: &str = "config/config.yaml";
const DEFAULT_WARMUP_DAYS: i64 = 8;
const DEFAULT_BATCH_SIZE: i64 = 5_000;
const DEFAULT_COMPARE_MINUTES: i64 = 60;
const FLOAT_EPSILON: f64 = 1e-7;

const ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.agg.orderbook.1m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.agg.%s.orderbook.1m.%s', market::text, lower(symbol)) AS routing_key,
        ts_bucket AS b_ts_bucket,
        chunk_start_ts AS b_chunk_start_ts,
        chunk_end_ts AS b_chunk_end_ts,
        source_event_count AS b_source_event_count,
        sample_count AS b_sample_count,
        bbo_updates AS b_bbo_updates,
        spread_sum AS b_spread_sum,
        topk_depth_sum AS b_topk_depth_sum,
        obi_sum AS b_obi_sum,
        obi_l1_sum AS b_obi_l1_sum,
        obi_k_sum AS b_obi_k_sum,
        obi_k_dw_sum AS b_obi_k_dw_sum,
        obi_k_dw_change_sum AS b_obi_k_dw_change_sum,
        obi_k_dw_adj_sum AS b_obi_k_dw_adj_sum,
        microprice_sum AS b_microprice_sum,
        microprice_classic_sum AS b_microprice_classic_sum,
        microprice_kappa_sum AS b_microprice_kappa_sum,
        microprice_adj_sum AS b_microprice_adj_sum,
        ofi_sum AS b_ofi_sum,
        obi_k_dw_close AS b_obi_k_dw_close,
        heatmap_levels AS b_heatmap_levels,
        TRUE AS b_heatmap_loaded
    FROM md.agg_orderbook_1m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

#[derive(Debug)]
struct CliArgs {
    config_path: String,
    symbol: Option<String>,
    from_ts: Option<DateTime<Utc>>,
    to_ts: Option<DateTime<Utc>>,
    warmup_days: i64,
    batch_size: i64,
    max_failures: usize,
}

#[derive(Debug, Default, Serialize)]
struct IndicatorSummary {
    compared_rows: usize,
    mismatched_rows: usize,
    rows_with_null: usize,
}

#[derive(Debug, Serialize)]
struct Failure {
    ts_snapshot: DateTime<Utc>,
    key: String,
    diff: String,
}

fn parse_args() -> Result<CliArgs> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut symbol = None;
    let mut from_ts = None;
    let mut to_ts = None;
    let mut warmup_days = DEFAULT_WARMUP_DAYS;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut max_failures = 200usize;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config_path = take_arg_value(&mut args, "--config")?,
            "--symbol" => symbol = Some(take_arg_value(&mut args, "--symbol")?),
            "--from" => {
                let raw = take_arg_value(&mut args, "--from")?;
                from_ts =
                    Some(parse_rfc3339_utc(&raw).with_context(|| format!("parse --from {raw}"))?);
            }
            "--to" => {
                let raw = take_arg_value(&mut args, "--to")?;
                to_ts = Some(parse_rfc3339_utc(&raw).with_context(|| format!("parse --to {raw}"))?);
            }
            "--warmup-days" => {
                let raw = take_arg_value(&mut args, "--warmup-days")?;
                warmup_days = raw
                    .parse::<i64>()
                    .with_context(|| format!("parse --warmup-days {raw}"))?;
            }
            "--batch-size" => {
                let raw = take_arg_value(&mut args, "--batch-size")?;
                batch_size = raw
                    .parse::<i64>()
                    .with_context(|| format!("parse --batch-size {raw}"))?;
            }
            "--max-failures" => {
                let raw = take_arg_value(&mut args, "--max-failures")?;
                max_failures = raw
                    .parse::<usize>()
                    .with_context(|| format!("parse --max-failures {raw}"))?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unsupported arg: {other}"),
        }
    }

    if warmup_days <= 0 {
        bail!("--warmup-days must be > 0");
    }
    if batch_size <= 0 {
        bail!("--batch-size must be > 0");
    }

    Ok(CliArgs {
        config_path,
        symbol,
        from_ts,
        to_ts,
        warmup_days,
        batch_size,
        max_failures,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: cargo run -p indicator_engine --bin audit_indicator_snapshots -- [options]

Options:
  --config <path>         Config path, default config/config.yaml
  --symbol <SYMBOL>       Symbol to audit, default from config
  --from <RFC3339>        Compare window start, default latest_snapshot - 59m
  --to <RFC3339>          Compare window end, default latest stored snapshot minute
  --warmup-days <N>       Replay warmup days before compare window, default 8
  --batch-size <N>        Backfill fetch batch size, default 5000
  --max-failures <N>      Max detailed failures to print, default 200"
    );
}

fn take_arg_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn parse_rfc3339_utc(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("parse RFC3339 timestamp {value}"))?
        .with_timezone(&Utc))
}

async fn latest_stored_snapshot_ts(pool: &PgPool, symbol: &str) -> Result<DateTime<Utc>> {
    let row = sqlx::query(
        r#"
        SELECT MAX(ts_snapshot) AS ts_snapshot
        FROM feat.indicator_snapshot
        WHERE symbol = $1
        "#,
    )
    .bind(symbol)
    .fetch_one(pool)
    .await
    .context("fetch latest stored snapshot ts")?;
    row.try_get::<Option<DateTime<Utc>>, _>("ts_snapshot")?
        .context("no stored snapshots found for symbol")
}

async fn load_stored_snapshots(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<String, Value>>> {
    let rows = sqlx::query(
        r#"
        SELECT ts_snapshot, indicator_code, window_code, payload_json
        FROM feat.v_indicator_snapshot_hydrated
        WHERE symbol = $1
          AND ts_snapshot >= $2
          AND ts_snapshot <= $3
        ORDER BY ts_snapshot, indicator_code, window_code
        "#,
    )
    .bind(symbol)
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(pool)
    .await
    .context("load stored snapshots")?;

    let mut out = BTreeMap::<DateTime<Utc>, BTreeMap<String, Value>>::new();
    for row in rows {
        let ts_snapshot: DateTime<Utc> = row.get("ts_snapshot");
        let indicator_code: String = row.get("indicator_code");
        let window_code: String = row.get("window_code");
        let payload: Value = row.get("payload_json");
        out.entry(ts_snapshot)
            .or_default()
            .insert(format!("{indicator_code}:{window_code}"), payload);
    }
    Ok(out)
}

fn collect_indicator_snapshots(
    registry: &[Arc<dyn Indicator>],
    ctx: &IndicatorContext,
) -> Vec<IndicatorSnapshotRow> {
    let mut snapshots = Vec::new();
    for indicator in registry {
        let comp = indicator.evaluate(ctx);
        if let Some(snapshot) = comp.snapshot {
            snapshots.push(snapshot);
        }
        if !comp.snapshot_rows.is_empty() {
            snapshots.extend(comp.snapshot_rows);
        }
    }
    snapshots.sort_by(|a, b| {
        a.indicator_code
            .cmp(b.indicator_code)
            .then_with(|| a.window_code.cmp(b.window_code))
    });
    snapshots
}

async fn compute_snapshots_for_bundle(
    pool: &PgPool,
    registry: &[Arc<dyn Indicator>],
    runtime_options: &indicator_engine::indicators::context::IndicatorRuntimeOptions,
    symbol: &str,
    bundle: &indicator_engine::runtime::state_store::WindowBundle,
) -> Result<Vec<IndicatorSnapshotRow>> {
    let supplement = load_kline_history_supplement(
        pool,
        symbol,
        &bundle.history_futures,
        &bundle.history_spot,
        runtime_options.kline_history_bars_4h,
        runtime_options.kline_history_bars_1d,
        runtime_options.kline_history_bars_3d,
        runtime_options.kline_history_fill_1d_from_db,
        runtime_options.ema_fill_from_db,
        &runtime_options.ema_htf_windows,
        runtime_options.ema_db_bars_4h,
        runtime_options.ema_db_bars_1d,
        runtime_options.ema_db_bars_3d,
        runtime_options.fvg_fill_from_db,
        &runtime_options.fvg_windows,
        runtime_options.fvg_db_bars_4h,
        runtime_options.fvg_db_bars_1d,
        bundle.ts_bucket + ChronoDuration::minutes(1),
    )
    .await;
    let ctx = IndicatorContext::from_bundle(bundle.clone(), runtime_options, supplement);
    Ok(collect_indicator_snapshots(registry, &ctx))
}

fn snapshot_map(rows: &[IndicatorSnapshotRow]) -> BTreeMap<String, Value> {
    rows.iter()
        .map(|row| {
            (
                format!("{}:{}", row.indicator_code, row.window_code),
                row.payload_json.clone(),
            )
        })
        .collect()
}

fn indicator_from_key(key: &str) -> &str {
    key.split_once(':').map(|(code, _)| code).unwrap_or(key)
}

fn has_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(items) => items.iter().any(has_null),
        Value::Object(map) => map.values().any(has_null),
        _ => false,
    }
}

fn diff_json(path: &str, left: &Value, right: &Value) -> Option<String> {
    match (left, right) {
        (Value::Null, Value::Null) => None,
        (Value::Bool(a), Value::Bool(b)) if a == b => None,
        (Value::String(a), Value::String(b)) if a == b => None,
        (Value::Number(a), Value::Number(b)) => {
            let left_num = a.as_f64()?;
            let right_num = b.as_f64()?;
            let scale = left_num.abs().max(right_num.abs()).max(1.0);
            if (left_num - right_num).abs() <= FLOAT_EPSILON * scale {
                None
            } else {
                Some(format!("{path}: left={left_num} right={right_num}"))
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Some(format!(
                    "{path}: array length mismatch left={} right={}",
                    a.len(),
                    b.len()
                ));
            }
            for (idx, (left_item, right_item)) in a.iter().zip(b.iter()).enumerate() {
                let child_path = format!("{path}[{idx}]");
                if let Some(diff) = diff_json(&child_path, left_item, right_item) {
                    return Some(diff);
                }
            }
            None
        }
        (Value::Object(a), Value::Object(b)) => {
            let left_keys = a.keys().collect::<BTreeSet<_>>();
            let right_keys = b.keys().collect::<BTreeSet<_>>();
            if left_keys != right_keys {
                return Some(format!(
                    "{path}: object key mismatch left={:?} right={:?}",
                    left_keys, right_keys
                ));
            }
            for key in left_keys {
                let child_path = format!("{path}.{key}");
                if let Some(diff) = diff_json(&child_path, &a[key.as_str()], &b[key.as_str()]) {
                    return Some(diff);
                }
            }
            None
        }
        _ => Some(format!("{path}: left={left:?} right={right:?}")),
    }
}

async fn fetch_futures_orderbook_heatmap_rows(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol_upper: &str,
) -> Result<Vec<ReplayRow>> {
    let rows = sqlx::query(ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP)
        .bind(from_ts)
        .bind(to_ts)
        .bind(symbol_upper)
        .bind("futures")
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "fetch futures orderbook heatmap rows from_ts={from_ts} to_ts={to_ts} symbol={symbol_upper}"
            )
        })?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let data_json = build_orderbook_backfill_data_json(&row)?;
        out.push(ReplayRow {
            event_ts: row.get("event_ts"),
            msg_type: row.get("msg_type"),
            market: row.get("market"),
            symbol: row.get("symbol"),
            routing_key: row.get("routing_key"),
            data_json,
            row_tid_text: String::new(),
        });
    }
    Ok(out)
}

fn require_field<T>(field: &'static str, value: Option<T>) -> Result<T> {
    value.with_context(|| format!("missing required field {field}"))
}

fn build_orderbook_backfill_data_json(row: &PgRow) -> Result<Value> {
    Ok(json!({
        "ts_bucket": require_field("b_ts_bucket", row.get::<Option<DateTime<Utc>>, _>("b_ts_bucket"))?,
        "chunk_start_ts": require_field("b_chunk_start_ts", row.get::<Option<DateTime<Utc>>, _>("b_chunk_start_ts"))?,
        "chunk_end_ts": require_field("b_chunk_end_ts", row.get::<Option<DateTime<Utc>>, _>("b_chunk_end_ts"))?,
        "source_event_count": row.get::<Option<i64>, _>("b_source_event_count"),
        "sample_count": require_field("b_sample_count", row.get::<Option<i64>, _>("b_sample_count"))?,
        "bbo_updates": require_field("b_bbo_updates", row.get::<Option<i64>, _>("b_bbo_updates"))?,
        "spread_sum": require_field("b_spread_sum", row.get::<Option<f64>, _>("b_spread_sum"))?,
        "topk_depth_sum": require_field("b_topk_depth_sum", row.get::<Option<f64>, _>("b_topk_depth_sum"))?,
        "obi_sum": require_field("b_obi_sum", row.get::<Option<f64>, _>("b_obi_sum"))?,
        "obi_l1_sum": require_field("b_obi_l1_sum", row.get::<Option<f64>, _>("b_obi_l1_sum"))?,
        "obi_k_sum": require_field("b_obi_k_sum", row.get::<Option<f64>, _>("b_obi_k_sum"))?,
        "obi_k_dw_sum": require_field("b_obi_k_dw_sum", row.get::<Option<f64>, _>("b_obi_k_dw_sum"))?,
        "obi_k_dw_change_sum": require_field("b_obi_k_dw_change_sum", row.get::<Option<f64>, _>("b_obi_k_dw_change_sum"))?,
        "obi_k_dw_adj_sum": require_field("b_obi_k_dw_adj_sum", row.get::<Option<f64>, _>("b_obi_k_dw_adj_sum"))?,
        "microprice_sum": require_field("b_microprice_sum", row.get::<Option<f64>, _>("b_microprice_sum"))?,
        "microprice_classic_sum": require_field("b_microprice_classic_sum", row.get::<Option<f64>, _>("b_microprice_classic_sum"))?,
        "microprice_kappa_sum": require_field("b_microprice_kappa_sum", row.get::<Option<f64>, _>("b_microprice_kappa_sum"))?,
        "microprice_adj_sum": require_field("b_microprice_adj_sum", row.get::<Option<f64>, _>("b_microprice_adj_sum"))?,
        "ofi_sum": require_field("b_ofi_sum", row.get::<Option<f64>, _>("b_ofi_sum"))?,
        "obi_k_dw_close": row.get::<Option<f64>, _>("b_obi_k_dw_close"),
        "heatmap_levels": require_field("b_heatmap_levels", row.get::<Option<Value>, _>("b_heatmap_levels"))?,
        "heatmap_loaded": row.get::<Option<bool>, _>("b_heatmap_loaded").unwrap_or(true),
    }))
}

fn floor_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts - ChronoDuration::seconds(ts.timestamp().rem_euclid(60))
        - ChronoDuration::nanoseconds(i64::from(ts.timestamp_subsec_nanos()))
}

fn ceil_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    let floored = floor_minute(ts);
    if ts == floored {
        floored
    } else {
        floored + ChronoDuration::minutes(1)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = load_config(&args.config_path)
        .with_context(|| format!("load config {}", args.config_path))?;
    let pool = build_db_pool(&config).await?;
    let symbol = args
        .symbol
        .unwrap_or_else(|| config.indicator.symbol.to_uppercase());
    let latest_snapshot = latest_stored_snapshot_ts(&pool, &symbol).await?;
    let compare_to = args.to_ts.map(floor_minute).unwrap_or(latest_snapshot);
    let compare_from = args
        .from_ts
        .map(ceil_minute)
        .unwrap_or(compare_to - ChronoDuration::minutes(DEFAULT_COMPARE_MINUTES - 1));
    if compare_from > compare_to {
        bail!(
            "compare window invalid: from {} > to {}",
            compare_from,
            compare_to
        );
    }
    let replay_from = compare_from - ChronoDuration::days(args.warmup_days);
    let replay_to_exclusive = compare_to + ChronoDuration::minutes(1);

    let stored_snapshots = load_stored_snapshots(&pool, &symbol, compare_from, compare_to).await?;
    if stored_snapshots.is_empty() {
        bail!(
            "no stored snapshots found for symbol={} in range {}..={}",
            symbol,
            compare_from,
            compare_to
        );
    }

    let registry = build_registry();
    let runtime_options = build_indicator_runtime_options(&config);
    let mut state_store = StateStore::new(symbol.clone(), config.indicator.whale_threshold_usdt);
    let mut scheduler = WindowScheduler::new(config.indicator.watermark_lateness_secs);
    let mut cursor = None;
    let mut max_seen_event_ts: Option<DateTime<Utc>> = None;
    let mut actual_by_minute = BTreeMap::<DateTime<Utc>, BTreeMap<String, Value>>::new();
    let mut replay_rows_ingested = 0usize;
    let mut hydrated_rows = 0usize;

    loop {
        let rows = fetch_backfill_batch(
            &pool,
            replay_from,
            replay_to_exclusive,
            &symbol,
            "all",
            args.batch_size,
            cursor.as_ref(),
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let batch_start = rows.first().map(|row| floor_minute(row.event_ts));
        let batch_end_exclusive = rows
            .last()
            .map(|row| floor_minute(row.event_ts) + ChronoDuration::minutes(1));
        for row in rows {
            cursor = Some(indicator_engine::app::runtime::BackfillCursor {
                event_ts: row.event_ts,
                msg_type: row.msg_type.clone(),
                market: row.market.clone(),
                symbol: row.symbol.clone(),
                routing_key: row.routing_key.clone(),
                row_tid_text: row.row_tid_text.clone(),
            });
            let event = replay_row_to_engine_event(row)?;
            max_seen_event_ts = Some(match max_seen_event_ts {
                Some(prev) => prev.max(event.event_ts),
                None => event.event_ts,
            });
            state_store.ingest(event);
            replay_rows_ingested += 1;
        }
        if let (Some(batch_from), Some(batch_to_exclusive)) = (batch_start, batch_end_exclusive) {
            let heatmap_rows = fetch_futures_orderbook_heatmap_rows(
                &pool,
                batch_from,
                batch_to_exclusive,
                &symbol.to_uppercase(),
            )
            .await?;
            hydrated_rows += heatmap_rows.len();
            for row in heatmap_rows {
                let event = replay_row_to_engine_event(row)?;
                state_store.ingest(event);
            }
        }
        if let Some(watermark_ts) = max_seen_event_ts {
            for minute in scheduler.ready_minutes(watermark_ts) {
                let bundle = state_store.finalize_minute(minute);
                if minute >= compare_from && minute <= compare_to {
                    let snapshots = compute_snapshots_for_bundle(
                        &pool,
                        &registry,
                        &runtime_options,
                        &symbol,
                        &bundle,
                    )
                    .await?;
                    actual_by_minute.insert(minute, snapshot_map(&snapshots));
                }
            }
        }
    }

    for minute in scheduler.ready_minutes_through(compare_to) {
        let bundle = state_store.finalize_minute(minute);
        if minute >= compare_from && minute <= compare_to {
            let snapshots =
                compute_snapshots_for_bundle(&pool, &registry, &runtime_options, &symbol, &bundle)
                    .await?;
            actual_by_minute.insert(minute, snapshot_map(&snapshots));
        }
    }

    let mut summaries = BTreeMap::<String, IndicatorSummary>::new();
    let mut failures = Vec::<Failure>::new();
    let mut compared_minutes = 0usize;

    for (minute, stored_rows) in &stored_snapshots {
        compared_minutes += 1;
        let Some(actual_rows) = actual_by_minute.get(minute) else {
            failures.push(Failure {
                ts_snapshot: *minute,
                key: "*minute*".to_string(),
                diff: "missing recomputed minute".to_string(),
            });
            continue;
        };

        let stored_keys = stored_rows.keys().cloned().collect::<BTreeSet<_>>();
        let actual_keys = actual_rows.keys().cloned().collect::<BTreeSet<_>>();
        if stored_keys != actual_keys {
            let missing = stored_keys
                .difference(&actual_keys)
                .cloned()
                .collect::<Vec<_>>();
            let extra = actual_keys
                .difference(&stored_keys)
                .cloned()
                .collect::<Vec<_>>();
            for key in &missing {
                summaries
                    .entry(indicator_from_key(key).to_string())
                    .or_default()
                    .mismatched_rows += 1;
            }
            for key in &extra {
                summaries
                    .entry(indicator_from_key(key).to_string())
                    .or_default()
                    .mismatched_rows += 1;
            }
            if failures.len() < args.max_failures {
                failures.push(Failure {
                    ts_snapshot: *minute,
                    key: "*keyset*".to_string(),
                    diff: format!("missing={missing:?} extra={extra:?}"),
                });
            }
        }

        for (key, stored_payload) in stored_rows {
            let summary = summaries
                .entry(indicator_from_key(key).to_string())
                .or_default();
            summary.compared_rows += 1;
            if has_null(stored_payload) {
                summary.rows_with_null += 1;
            }

            let Some(actual_payload) = actual_rows.get(key) else {
                continue;
            };
            if let Some(diff) = diff_json("$", stored_payload, actual_payload) {
                summary.mismatched_rows += 1;
                if failures.len() < args.max_failures {
                    failures.push(Failure {
                        ts_snapshot: *minute,
                        key: key.clone(),
                        diff,
                    });
                }
            }
        }
    }

    println!("symbol={symbol}");
    println!("replay_from={replay_from}");
    println!("compare_from={compare_from}");
    println!("compare_to={compare_to}");
    println!("stored_minutes={}", stored_snapshots.len());
    println!("compared_minutes={compared_minutes}");
    println!("replay_rows_ingested={replay_rows_ingested}");
    println!("futures_orderbook_heatmap_rows_hydrated={hydrated_rows}");
    println!("failure_count={}", failures.len());
    println!("per_indicator_summary:");
    for (code, summary) in &summaries {
        println!(
            "  {code}: compared_rows={} mismatched_rows={} rows_with_null={}",
            summary.compared_rows, summary.mismatched_rows, summary.rows_with_null
        );
    }
    if !failures.is_empty() {
        println!("failures:");
        for failure in &failures {
            println!(
                "  ts={} key={} diff={}",
                failure.ts_snapshot, failure.key, failure.diff
            );
        }
    }

    if summaries
        .values()
        .all(|summary| summary.mismatched_rows == 0)
        && failures.is_empty()
    {
        Ok(())
    } else {
        bail!("replay audit found mismatches");
    }
}
