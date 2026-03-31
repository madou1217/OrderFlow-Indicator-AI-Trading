use crate::indicators::context::IndicatorSnapshotRow;
use crate::publish::ind_publisher::BundleOutboxMessage;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;
use tracing::warn;

const OUTBOX_PROGRESS_TX_WARN_MS: u128 = 1_000;
const SNAPSHOT_BLOB_REF_KEY: &str = "__snapshot_blob_ref_v1";
const SNAPSHOT_BLOB_CHUNKS_KEY: &str = "__snapshot_blob_chunks_v1";
const SNAPSHOT_BLOB_MIN_BYTES: usize = 4 * 1024;
const SNAPSHOT_BLOB_CHUNK_SIZE: usize = 256;

#[derive(Clone)]
pub struct SnapshotWriter {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct SnapshotBundleRow {
    indicator_code: String,
    window_code: String,
    payload_json: Value,
}

#[derive(Debug, Clone)]
struct SnapshotBlobSpec {
    blob_hash: String,
    payload_json: Value,
}

#[derive(sqlx::FromRow)]
struct SnapshotBlobRow {
    blob_hash: String,
    payload_json: Value,
}

impl SnapshotWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS feat.indicator_snapshot_blob (
                blob_hash TEXT PRIMARY KEY,
                payload_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create feat.indicator_snapshot_blob")?;

        sqlx::query(
            r#"
            ALTER TABLE feat.indicator_snapshot_blob
            ADD COLUMN IF NOT EXISTS payload_json JSONB
            "#,
        )
        .execute(&self.pool)
        .await
        .context("ensure feat.indicator_snapshot_blob.payload_json column")?;

        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION feat.hydrate_indicator_snapshot_payload(payload jsonb)
            RETURNS jsonb
            LANGUAGE plpgsql
            STABLE
            AS $$
            DECLARE
                ref_meta jsonb;
                chunk_meta jsonb;
                blob_hash text;
                hydrated jsonb;
                child_key text;
                child_value jsonb;
                out_obj jsonb;
                out_arr jsonb;
            BEGIN
                IF payload IS NULL THEN
                    RETURN NULL;
                END IF;

                IF jsonb_typeof(payload) = 'object' THEN
                    ref_meta := payload -> '__snapshot_blob_ref_v1';
                    IF jsonb_typeof(ref_meta) = 'object' AND (ref_meta ? 'hash') THEN
                        blob_hash := ref_meta ->> 'hash';
                        SELECT b.payload_json
                          INTO hydrated
                          FROM feat.indicator_snapshot_blob b
                         WHERE b.blob_hash = blob_hash;
                        IF hydrated IS NULL THEN
                            RAISE EXCEPTION 'missing indicator snapshot blob hash=%', blob_hash;
                        END IF;
                        RETURN feat.hydrate_indicator_snapshot_payload(hydrated);
                    END IF;

                    chunk_meta := payload -> '__snapshot_blob_chunks_v1';
                    IF jsonb_typeof(chunk_meta) = 'object' AND jsonb_typeof(chunk_meta -> 'chunk_hashes') = 'array' THEN
                        out_arr := '[]'::jsonb;
                        FOR blob_hash IN
                            SELECT value
                            FROM jsonb_array_elements_text(chunk_meta -> 'chunk_hashes')
                        LOOP
                            SELECT b.payload_json
                              INTO hydrated
                              FROM feat.indicator_snapshot_blob b
                             WHERE b.blob_hash = blob_hash;
                            IF hydrated IS NULL THEN
                                RAISE EXCEPTION 'missing indicator snapshot chunk hash=%', blob_hash;
                            END IF;
                            IF jsonb_typeof(hydrated) <> 'array' THEN
                                RAISE EXCEPTION 'indicator snapshot chunk blob is not an array hash=%', blob_hash;
                            END IF;
                            FOR child_value IN
                                SELECT value
                                FROM jsonb_array_elements(hydrated)
                            LOOP
                                out_arr := out_arr || jsonb_build_array(
                                    feat.hydrate_indicator_snapshot_payload(child_value)
                                );
                            END LOOP;
                        END LOOP;
                        RETURN out_arr;
                    END IF;

                    out_obj := '{}'::jsonb;
                    FOR child_key, child_value IN
                        SELECT key, value
                        FROM jsonb_each(payload)
                    LOOP
                        out_obj := out_obj || jsonb_build_object(
                            child_key,
                            feat.hydrate_indicator_snapshot_payload(child_value)
                        );
                    END LOOP;
                    RETURN out_obj;
                ELSIF jsonb_typeof(payload) = 'array' THEN
                    out_arr := '[]'::jsonb;
                    FOR child_value IN
                        SELECT value
                        FROM jsonb_array_elements(payload)
                    LOOP
                        out_arr := out_arr || jsonb_build_array(
                            feat.hydrate_indicator_snapshot_payload(child_value)
                        );
                    END LOOP;
                    RETURN out_arr;
                END IF;

                RETURN payload;
            END
            $$;
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create feat.hydrate_indicator_snapshot_payload")?;

        sqlx::query(
            r#"
            CREATE OR REPLACE VIEW feat.v_indicator_snapshot_hydrated AS
            SELECT
                ts_snapshot,
                bar_interval,
                venue,
                symbol,
                market_scope,
                indicator_code,
                window_code,
                primary_market,
                param_set_id,
                calc_version,
                feat.hydrate_indicator_snapshot_payload(payload_json) AS payload_json,
                tags,
                created_at
            FROM feat.indicator_snapshot
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create feat.v_indicator_snapshot_hydrated")?;

        Ok(())
    }

    pub async fn write_snapshots(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        rows: &[IndicatorSnapshotRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Most indicators write one snapshot row per minute. A small number of
        // indicators can additionally materialize explicit per-window rows so
        // feat.indicator_snapshot exposes stable window_code coverage.
        let mut ts_snapshots: Vec<DateTime<Utc>> = Vec::new();
        let mut bar_intervals: Vec<String> = Vec::new();
        let mut symbols: Vec<String> = Vec::new();
        let mut indicator_codes: Vec<String> = Vec::new();
        let mut window_codes: Vec<String> = Vec::new();
        let mut primary_markets: Vec<Option<String>> = Vec::new();
        let mut payload_jsons: Vec<Value> = Vec::new();
        let mut blob_specs = BTreeMap::<String, SnapshotBlobSpec>::new();

        for row in rows {
            let primary_market = primary_market_for_code(row.indicator_code);
            let bar_interval = interval_text_by_window(row.window_code);
            ts_snapshots.push(ts_bucket);
            bar_intervals.push(bar_interval);
            symbols.push(symbol.to_string());
            indicator_codes.push(row.indicator_code.to_string());
            window_codes.push(row.window_code.to_string());
            primary_markets.push(primary_market.map(|s| s.to_string()));
            let compacted = compact_snapshot_payload(row.indicator_code, &row.payload_json);
            payload_jsons.push(refize_snapshot_payload(
                row.indicator_code,
                &compacted,
                &mut blob_specs,
            )?);
        }

        if ts_snapshots.is_empty() {
            return Ok(());
        }

        let blob_specs = blob_specs.into_values().collect::<Vec<_>>();

        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator snapshot tx")?;

        insert_snapshot_blobs_in_tx(&mut tx, &blob_specs).await?;

        // Single round-trip: batch all rows with UNNEST.
        sqlx::query(
            r#"
            INSERT INTO feat.indicator_snapshot (
                ts_snapshot, bar_interval, symbol, market_scope,
                indicator_code, window_code, primary_market,
                calc_version, payload_json
            )
            SELECT
                ts, bi::interval, sym, 'futures_primary',
                ic, wc,
                CASE WHEN pm IS NULL THEN NULL ELSE pm::cfg.market_type END,
                'indicator_engine.v1', pj
            FROM UNNEST(
                $1::timestamptz[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::jsonb[]
            ) AS t(ts, bi, sym, ic, wc, pm, pj)
            ON CONFLICT (ts_snapshot, symbol, indicator_code, window_code)
            DO UPDATE SET
                bar_interval = EXCLUDED.bar_interval,
                market_scope = EXCLUDED.market_scope,
                primary_market = EXCLUDED.primary_market,
                calc_version = EXCLUDED.calc_version,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(&ts_snapshots)
        .bind(&bar_intervals)
        .bind(&symbols)
        .bind(&indicator_codes)
        .bind(&window_codes)
        .bind(&primary_markets)
        .bind(&payload_jsons)
        .execute(&mut *tx)
        .await
        .with_context(|| {
            format!(
                "batch insert indicator_snapshot: {} rows, symbol={}",
                ts_snapshots.len(),
                symbol
            )
        })?;

        tx.commit().await.context("commit indicator snapshot tx")?;

        Ok(())
    }

    pub async fn advance_progress(&self, symbol: &str, ts_bucket: DateTime<Utc>) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator progress tx")?;
        upsert_indicator_progress(&mut tx, symbol, ts_bucket).await?;
        tx.commit().await.context("commit indicator progress tx")?;
        Ok(())
    }

    pub async fn advance_progress_with_outbox(
        &self,
        symbol: &str,
        ts_bucket: DateTime<Utc>,
        messages: &[BundleOutboxMessage],
    ) -> Result<()> {
        let total_started_at = Instant::now();
        let begin_started_at = Instant::now();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator progress+outbox tx")?;
        let begin_ms = begin_started_at.elapsed().as_millis();
        let enqueue_started_at = Instant::now();
        enqueue_outbox_batch_in_tx(&mut tx, messages).await?;
        let enqueue_ms = enqueue_started_at.elapsed().as_millis();
        let progress_started_at = Instant::now();
        upsert_indicator_progress(&mut tx, symbol, ts_bucket).await?;
        let progress_ms = progress_started_at.elapsed().as_millis();
        let commit_started_at = Instant::now();
        tx.commit()
            .await
            .context("commit indicator progress+outbox tx")?;
        let commit_ms = commit_started_at.elapsed().as_millis();
        let total_ms = total_started_at.elapsed().as_millis();
        if total_ms >= OUTBOX_PROGRESS_TX_WARN_MS {
            warn!(
                symbol = symbol,
                ts_bucket = %ts_bucket,
                message_count = messages.len(),
                begin_ms = begin_ms,
                enqueue_ms = enqueue_ms,
                progress_ms = progress_ms,
                commit_ms = commit_ms,
                total_ms = total_ms,
                "slow indicator progress+outbox transaction"
            );
        }
        Ok(())
    }

    pub async fn enqueue_bundle_repairs(&self, messages: &[BundleOutboxMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator repair outbox tx")?;
        enqueue_outbox_batch_in_tx(&mut tx, messages).await?;
        tx.commit()
            .await
            .context("commit indicator repair outbox tx")?;
        Ok(())
    }

    pub async fn load_minute_bundle_indicators_json(
        &self,
        symbol: &str,
        ts_bucket: DateTime<Utc>,
    ) -> Result<(Value, usize)> {
        let mut rows: Vec<SnapshotBundleRow> = sqlx::query_as(
            r#"
            SELECT indicator_code, window_code, payload_json
            FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot = $2
            ORDER BY indicator_code
            "#,
        )
        .bind(symbol.to_uppercase())
        .bind(ts_bucket)
        .fetch_all(&self.pool)
        .await
        .context("fetch snapshot rows for repair bundle rebuild")?;

        let mut payloads = rows
            .iter()
            .map(|row| row.payload_json.clone())
            .collect::<Vec<_>>();
        hydrate_snapshot_payload_values(&self.pool, &mut payloads).await?;
        for (row, payload_json) in rows.iter_mut().zip(payloads.into_iter()) {
            row.payload_json = payload_json;
        }

        if rows.is_empty() {
            anyhow::bail!(
                "no indicator_snapshot rows found for symbol={} ts_bucket={}",
                symbol,
                ts_bucket
            );
        }

        let row_count = rows.len();
        Ok((assemble_bundle_indicators_json(rows), row_count))
    }

    pub async fn rewind_progress(&self, symbol: &str, ts_bucket: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO feat.indicator_progress (symbol, last_success_ts)
            VALUES ($1, $2)
            ON CONFLICT (symbol)
            DO UPDATE SET
                last_success_ts = EXCLUDED.last_success_ts,
                updated_at = now()
            "#,
        )
        .bind(symbol.to_uppercase())
        .bind(ts_bucket)
        .execute(&self.pool)
        .await
        .context("rewind feat.indicator_progress")?;
        Ok(())
    }

    pub async fn rewind_persisted_tail(
        &self,
        symbol: &str,
        repair_start_ts: DateTime<Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        let rewind_target_ts = repair_start_ts - ChronoDuration::minutes(1);
        let symbol_upper = symbol.to_uppercase();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin indicator persisted tail rewind tx")?;

        sqlx::query(
            r#"
            DELETE FROM ops.indicator_bundle_outbox
            WHERE exchange_name = $1
              AND COALESCE(NULLIF(upper(symbol), ''), upper(payload_json->>'symbol')) = $2
              AND COALESCE(
                    ts_bucket,
                    NULLIF(payload_json->>'ts_bucket', '')::timestamptz,
                    NULLIF(payload_json->>'event_ts', '')::timestamptz
                  ) >= $3
            "#,
        )
        .bind(exchange_name)
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete indicator bundle outbox tail for overlap repair")?;

        sqlx::query(
            r#"
            DELETE FROM ops.indicator_bundle_payload_cache
            WHERE symbol = $1
              AND ts_bucket >= $2
            "#,
        )
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete indicator bundle payload cache tail for overlap repair")?;

        sqlx::query(
            r#"
            DELETE FROM ops.outbox_event
            WHERE exchange_name = $1
              AND upper(payload_json->>'symbol') = $2
              AND COALESCE(
                    NULLIF(payload_json->>'ts_bucket', '')::timestamptz,
                    NULLIF(payload_json->>'event_ts', '')::timestamptz
                  ) >= $3
            "#,
        )
        .bind(exchange_name)
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete indicator outbox tail for overlap repair")?;

        sqlx::query(
            r#"
            DELETE FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot >= $2
            "#,
        )
        .bind(&symbol_upper)
        .bind(repair_start_ts)
        .execute(&mut *tx)
        .await
        .context("delete feat.indicator_snapshot tail for overlap repair")?;

        for (table, ts_col) in [
            ("feat.indicator_level_value", "ts_snapshot"),
            ("feat.liquidation_density_level", "ts_snapshot"),
            ("feat.trade_flow_feature", "ts_bucket"),
            ("feat.orderbook_feature", "ts_bucket"),
            ("feat.funding_feature", "ts_bucket"),
            ("feat.avwap_feature", "ts_bucket"),
            ("feat.cvd_pack", "ts_bucket"),
            ("feat.whale_trade_rollup", "ts_bucket"),
            ("feat.funding_change_event", "ts_change"),
        ] {
            let query = format!("DELETE FROM {table} WHERE symbol = $1 AND {ts_col} >= $2");
            sqlx::query(&query)
                .bind(&symbol_upper)
                .bind(repair_start_ts)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("delete {table} tail for overlap repair"))?;
        }

        for table in [
            "evt.indicator_event",
            "evt.divergence_event",
            "evt.absorption_event",
            "evt.initiation_event",
            "evt.exhaustion_event",
        ] {
            let query = format!(
                "DELETE FROM {table} WHERE symbol = $1 AND COALESCE(event_available_ts, ts_event_end, ts_event_start) >= $2"
            );
            sqlx::query(&query)
                .bind(&symbol_upper)
                .bind(repair_start_ts)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("delete {table} tail for overlap repair"))?;
        }

        sqlx::query(
            r#"
            INSERT INTO ops.indicator_snapshot_fanout_progress (
                symbol,
                last_published_snapshot_ts
            )
            VALUES ($1, $2)
            ON CONFLICT (symbol)
            DO UPDATE SET
                last_published_snapshot_ts = CASE
                    WHEN ops.indicator_snapshot_fanout_progress.last_published_snapshot_ts IS NULL
                        THEN EXCLUDED.last_published_snapshot_ts
                    ELSE LEAST(
                        ops.indicator_snapshot_fanout_progress.last_published_snapshot_ts,
                        EXCLUDED.last_published_snapshot_ts
                    )
                END,
                updated_at = now()
            "#,
        )
        .bind(&symbol_upper)
        .bind(rewind_target_ts)
        .execute(&mut *tx)
        .await
        .context("rewind indicator snapshot fanout progress")?;

        set_indicator_progress_exact(&mut tx, &symbol_upper, rewind_target_ts).await?;
        tx.commit()
            .await
            .context("commit indicator persisted tail rewind tx")?;
        Ok(())
    }
}

async fn enqueue_outbox_batch_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    messages: &[BundleOutboxMessage],
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }

    let published_at = Utc::now();
    let payload_bytes = messages
        .iter()
        .map(|message| message.payload_bytes_with_published_at(published_at))
        .collect::<Result<Vec<_>>>()?;

    let mut payload_builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO ops.indicator_bundle_payload_cache (
            symbol, ts_bucket, schema_version, indicator_count, payload_encoding, payload_bytes
        )
        "#,
    );

    payload_builder.push_values(messages.iter().zip(payload_bytes.iter()), |mut b, (message, payload_bytes)| {
        b.push_bind(&message.symbol)
            .push_bind(message.ts_bucket)
            .push_bind(message.schema_version)
            .push_bind(message.indicator_count)
            .push_bind(&message.payload_encoding)
            .push_bind(payload_bytes);
    });

    payload_builder.push(
        r#"
        ON CONFLICT (symbol, ts_bucket)
        DO UPDATE SET
            schema_version = EXCLUDED.schema_version,
            indicator_count = EXCLUDED.indicator_count,
            payload_encoding = EXCLUDED.payload_encoding,
            payload_bytes = EXCLUDED.payload_bytes,
            created_at = now()
        "#,
    );
    payload_builder
        .build()
        .execute(tx.as_mut())
        .await
        .with_context(|| {
            format!(
                "upsert indicator bundle payload cache count={}",
                messages.len()
            )
        })?;

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO ops.indicator_bundle_outbox (
            exchange_name, routing_key, message_id, schema_version, headers_json,
            symbol, ts_bucket, indicator_count, payload_json
        )
        "#,
    );

    builder.push_values(messages, |mut b, message| {
        b.push_bind(&message.exchange_name)
            .push_bind(&message.routing_key)
            .push_bind(message.message_id)
            .push_bind(message.schema_version)
            .push_bind(&message.headers_json)
            .push_bind(&message.symbol)
            .push_bind(message.ts_bucket)
            .push_bind(message.indicator_count)
            .push_bind(&message.payload_json);
    });

    builder.push(" ON CONFLICT DO NOTHING");
    builder
        .build()
        .execute(tx.as_mut())
        .await
        .with_context(|| {
            format!(
                "batch insert indicator outbox messages count={}",
                messages.len()
            )
        })?;

    Ok(())
}

async fn insert_snapshot_blobs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    blobs: &[SnapshotBlobSpec],
) -> Result<()> {
    if blobs.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO feat.indicator_snapshot_blob (
            blob_hash, payload_json
        )
        "#,
    );

    builder.push_values(blobs, |mut b, blob| {
        b.push_bind(&blob.blob_hash).push_bind(&blob.payload_json);
    });
    builder.push(" ON CONFLICT (blob_hash) DO NOTHING");
    builder
        .build()
        .execute(tx.as_mut())
        .await
        .context("insert indicator snapshot blobs in snapshot tx")?;
    Ok(())
}

pub async fn hydrate_snapshot_payload_values(pool: &PgPool, payloads: &mut [Value]) -> Result<()> {
    let mut blob_hashes = BTreeSet::new();
    for payload in payloads.iter() {
        collect_snapshot_blob_hashes(payload, &mut blob_hashes);
    }
    if blob_hashes.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query_as::<_, SnapshotBlobRow>(
        r#"
        SELECT blob_hash, payload_json
        FROM feat.indicator_snapshot_blob
        WHERE blob_hash = ANY($1)
        "#,
    )
    .bind(blob_hashes.iter().cloned().collect::<Vec<_>>())
    .fetch_all(pool)
    .await
    .context("fetch indicator snapshot blobs for hydration")?;

    let mut blob_map = HashMap::<String, Value>::new();
    for row in rows {
        blob_map.insert(row.blob_hash.clone(), row.payload_json);
    }

    for payload in payloads.iter_mut() {
        hydrate_snapshot_payload_value(payload, &blob_map)?;
    }

    Ok(())
}

fn refize_snapshot_payload(
    indicator_code: &str,
    payload: &Value,
    blobs: &mut BTreeMap<String, SnapshotBlobSpec>,
) -> Result<Value> {
    let Some(obj) = payload.as_object() else {
        return Ok(payload.clone());
    };

    let mut out = Map::new();
    for (key, value) in obj {
        let transformed = if key == "recent_7d" {
            refize_recent_7d_array(value, blobs)?
        } else if should_blob_ref_field(indicator_code, key, value) {
            build_snapshot_blob_ref(value, blobs)?
        } else {
            refize_nested_snapshot_value(indicator_code, value, blobs)?
        };
        out.insert(key.clone(), transformed);
    }
    Ok(Value::Object(out))
}

fn refize_nested_snapshot_value(
    indicator_code: &str,
    value: &Value,
    blobs: &mut BTreeMap<String, SnapshotBlobSpec>,
) -> Result<Value> {
    match value {
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, child) in obj {
                let transformed = if key == "recent_7d" {
                    refize_recent_7d_array(child, blobs)?
                } else if should_blob_ref_field(indicator_code, key, child) {
                    build_snapshot_blob_ref(child, blobs)?
                } else {
                    refize_nested_snapshot_value(indicator_code, child, blobs)?
                };
                out.insert(key.clone(), transformed);
            }
            Ok(Value::Object(out))
        }
        Value::Array(arr) => Ok(Value::Array(
            arr.iter()
                .map(|child| refize_nested_snapshot_value(indicator_code, child, blobs))
                .collect::<Result<Vec<_>>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn should_blob_ref_field(indicator_code: &str, key: &str, value: &Value) -> bool {
    if serialized_json_len(value) < SNAPSHOT_BLOB_MIN_BYTES {
        return false;
    }

    matches!((indicator_code, key), ("kline_history", "bars"))
        || matches!(
            (indicator_code, key),
            (
                "footprint",
                "by_window"
                    | "buy_imbalance_prices"
                    | "sell_imbalance_prices"
                    | "buy_stacks"
                    | "sell_stacks"
            )
        )
}

fn refize_recent_7d_array(
    value: &Value,
    blobs: &mut BTreeMap<String, SnapshotBlobSpec>,
) -> Result<Value> {
    let Some(arr) = value.as_array() else {
        return Ok(value.clone());
    };
    if arr.is_empty() || serialized_json_len(value) < SNAPSHOT_BLOB_MIN_BYTES {
        return Ok(value.clone());
    }

    let mut chunk_hashes = Vec::new();
    for chunk in arr.chunks(SNAPSHOT_BLOB_CHUNK_SIZE) {
        let chunk_value = Value::Array(chunk.to_vec());
        let blob_hash = ensure_snapshot_blob(&chunk_value, blobs)?;
        chunk_hashes.push(blob_hash);
    }

    Ok(json!({
        SNAPSHOT_BLOB_CHUNKS_KEY: {
            "chunk_hashes": chunk_hashes,
            "chunk_size": SNAPSHOT_BLOB_CHUNK_SIZE,
            "total_items": arr.len(),
        }
    }))
}

fn build_snapshot_blob_ref(
    value: &Value,
    blobs: &mut BTreeMap<String, SnapshotBlobSpec>,
) -> Result<Value> {
    let blob_hash = ensure_snapshot_blob(value, blobs)?;
    Ok(json!({
        SNAPSHOT_BLOB_REF_KEY: {
            "hash": blob_hash,
        }
    }))
}

fn ensure_snapshot_blob(
    value: &Value,
    blobs: &mut BTreeMap<String, SnapshotBlobSpec>,
) -> Result<String> {
    let raw_bytes = serde_json::to_vec(value).context("serialize indicator snapshot blob")?;
    let blob_hash = format!("{:x}", Sha256::digest(&raw_bytes));
    if !blobs.contains_key(&blob_hash) {
        blobs.insert(
            blob_hash.clone(),
            SnapshotBlobSpec {
                blob_hash: blob_hash.clone(),
                payload_json: value.clone(),
            },
        );
    }
    Ok(blob_hash)
}

fn collect_snapshot_blob_hashes(value: &Value, out: &mut BTreeSet<String>) {
    if let Some(hash) = snapshot_blob_ref_hash(value) {
        out.insert(hash.to_string());
        return;
    }
    if let Some(chunk_hashes) = snapshot_blob_chunk_hashes(value) {
        out.extend(chunk_hashes);
        return;
    }

    match value {
        Value::Object(obj) => {
            for child in obj.values() {
                collect_snapshot_blob_hashes(child, out);
            }
        }
        Value::Array(arr) => {
            for child in arr {
                collect_snapshot_blob_hashes(child, out);
            }
        }
        _ => {}
    }
}

fn hydrate_snapshot_payload_value(
    value: &mut Value,
    blob_map: &HashMap<String, Value>,
) -> Result<()> {
    if let Some(hash) = snapshot_blob_ref_hash(value) {
        let replacement = blob_map
            .get(hash)
            .cloned()
            .with_context(|| format!("missing indicator snapshot blob hash={hash}"))?;
        *value = replacement;
        return hydrate_snapshot_payload_value(value, blob_map);
    }

    if let Some(chunk_hashes) = snapshot_blob_chunk_hashes(value) {
        let mut items = Vec::new();
        for hash in chunk_hashes {
            let chunk = blob_map
                .get(&hash)
                .cloned()
                .with_context(|| format!("missing indicator snapshot chunk hash={hash}"))?;
            let Value::Array(chunk_items) = chunk else {
                return Err(anyhow::anyhow!(
                    "indicator snapshot chunk blob is not an array hash={hash}"
                ));
            };
            items.extend(chunk_items);
        }
        *value = Value::Array(items);
        return hydrate_snapshot_payload_value(value, blob_map);
    }

    match value {
        Value::Object(obj) => {
            for child in obj.values_mut() {
                hydrate_snapshot_payload_value(child, blob_map)?;
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                hydrate_snapshot_payload_value(child, blob_map)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn snapshot_blob_ref_hash(value: &Value) -> Option<&str> {
    let obj = value.as_object()?;
    let meta = obj.get(SNAPSHOT_BLOB_REF_KEY)?.as_object()?;
    meta.get("hash")?.as_str()
}

fn snapshot_blob_chunk_hashes(value: &Value) -> Option<Vec<String>> {
    let obj = value.as_object()?;
    let meta = obj.get(SNAPSHOT_BLOB_CHUNKS_KEY)?.as_object()?;
    let hashes = meta.get("chunk_hashes")?.as_array()?;
    hashes
        .iter()
        .map(|hash| hash.as_str().map(str::to_string))
        .collect()
}

fn serialized_json_len(value: &Value) -> usize {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or_default()
}

fn interval_text_by_window(window_code: &str) -> String {
    match window_code {
        "5m" => "5 minutes".to_string(),
        "15m" => "15 minutes".to_string(),
        "1h" => "1 hour".to_string(),
        "4h" => "4 hours".to_string(),
        "1d" => "1 day".to_string(),
        "3d" => "3 days".to_string(),
        _ => "1 minute".to_string(),
    }
}

fn snapshot_window_rank(window_code: &str) -> usize {
    match window_code {
        "5m" => 0,
        "1m" => 1,
        "15m" => 2,
        "1h" => 3,
        "4h" => 4,
        "1d" => 5,
        "3d" => 6,
        _ => usize::MAX,
    }
}

fn primary_market_for_code(code: &str) -> Option<&'static str> {
    match code {
        "cvd_pack" | "whale_trades" | "vpin" | "avwap" => None,
        _ => Some("futures"),
    }
}

async fn upsert_indicator_progress(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO feat.indicator_progress (symbol, last_success_ts)
        VALUES ($1, $2)
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_success_ts = GREATEST(feat.indicator_progress.last_success_ts, EXCLUDED.last_success_ts),
            updated_at = now()
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(ts_bucket)
    .execute(&mut **tx)
    .await
    .context("upsert feat.indicator_progress")?;
    Ok(())
}

async fn set_indicator_progress_exact(
    tx: &mut Transaction<'_, Postgres>,
    symbol: &str,
    ts_bucket: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO feat.indicator_progress (symbol, last_success_ts)
        VALUES ($1, $2)
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_success_ts = EXCLUDED.last_success_ts,
            updated_at = now()
        "#,
    )
    .bind(symbol)
    .bind(ts_bucket)
    .execute(&mut **tx)
    .await
    .context("set feat.indicator_progress exact")?;
    Ok(())
}

fn compact_snapshot_payload(indicator_code: &str, payload: &Value) -> Value {
    if indicator_code == "kline_history" {
        return payload.clone();
    }

    let targeted = match indicator_code {
        "price_volume_structure" => compact_pvs_payload(payload),
        "footprint" => compact_footprint_payload(payload),
        "orderbook_depth" => compact_orderbook_depth_payload(payload),
        _ => payload.clone(),
    };
    compact_payload_value(None, &targeted)
}

fn compact_pvs_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    if let Some(levels) = out
        .remove("value_area_levels")
        .and_then(|v| v.as_array().cloned())
    {
        out.insert("value_area_levels_count".to_string(), json!(levels.len()));
    }
    if let Some(by_window) = out.remove("by_window") {
        out.insert("by_window".to_string(), compact_pvs_by_window(&by_window));
    }
    Value::Object(out)
}

fn compact_pvs_by_window(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut out = Map::new();
    for (window_code, window_value) in obj {
        let Some(window_obj) = window_value.as_object() else {
            out.insert(window_code.clone(), window_value.clone());
            continue;
        };
        let mut compacted = window_obj.clone();
        if let Some(levels) = compacted
            .remove("levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("levels_count".to_string(), json!(levels.len()));
        }
        if let Some(levels) = compacted
            .remove("value_area_levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("value_area_levels_count".to_string(), json!(levels.len()));
        }
        out.insert(window_code.clone(), Value::Object(compacted));
    }
    Value::Object(out)
}

fn compact_footprint_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    if let Some(by_window) = out.remove("by_window") {
        out.insert(
            "by_window".to_string(),
            compact_levels_only_by_window(&by_window),
        );
    }
    Value::Object(out)
}

fn compact_orderbook_depth_payload(payload: &Value) -> Value {
    let Some(obj) = payload.as_object() else {
        return payload.clone();
    };
    let mut out = obj.clone();
    if let Some(levels) = out.remove("levels").and_then(|v| v.as_array().cloned()) {
        out.insert("levels_count".to_string(), json!(levels.len()));
    }
    Value::Object(out)
}

fn compact_levels_only_by_window(value: &Value) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut out = Map::new();
    for (window_code, window_value) in obj {
        let Some(window_obj) = window_value.as_object() else {
            out.insert(window_code.clone(), window_value.clone());
            continue;
        };
        let mut compacted = window_obj.clone();
        if let Some(levels) = compacted
            .remove("levels")
            .and_then(|v| v.as_array().cloned())
        {
            compacted.insert("levels_count".to_string(), json!(levels.len()));
        }
        out.insert(window_code.clone(), Value::Object(compacted));
    }
    Value::Object(out)
}

fn compact_payload_value(parent_key: Option<&str>, value: &Value) -> Value {
    match value {
        Value::Object(obj) => {
            let mut out = Map::new();
            for (key, child) in obj {
                if let Some(summary) = compact_array_field(parent_key, key, child) {
                    match summary {
                        CompactField::Replace(replacement) => {
                            out.insert(key.clone(), replacement);
                        }
                        CompactField::Expand(fields) => {
                            for (field_key, field_value) in fields {
                                out.insert(field_key, field_value);
                            }
                        }
                    }
                } else {
                    out.insert(
                        key.clone(),
                        compact_payload_value(Some(key.as_str()), child),
                    );
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => {
            if let Some(parent) = parent_key {
                if is_series_parent(parent) {
                    return compact_window_series_array(arr);
                }
            }
            Value::Array(arr.clone())
        }
        _ => value.clone(),
    }
}

enum CompactField {
    Replace(Value),
    Expand(Vec<(String, Value)>),
}

fn compact_array_field(parent_key: Option<&str>, key: &str, value: &Value) -> Option<CompactField> {
    let arr = value.as_array()?;
    if is_count_only_array_key(key) {
        return Some(CompactField::Expand(vec![(
            format!("{key}_count"),
            json!(arr.len()),
        )]));
    }
    if key == "bars" {
        return Some(CompactField::Expand(vec![
            ("bars_count".to_string(), json!(arr.len())),
            (
                "latest_bar".to_string(),
                arr.last().cloned().unwrap_or(Value::Null),
            ),
        ]));
    }
    if matches!(key, "series" | "changes") {
        return Some(CompactField::Expand(vec![
            (format!("{key}_count"), json!(arr.len())),
            (
                format!("latest_{}", singularize_array_key(key)),
                arr.last().cloned().unwrap_or(Value::Null),
            ),
        ]));
    }
    if parent_key.is_some_and(is_series_parent) {
        return Some(CompactField::Replace(compact_window_series_array(arr)));
    }
    None
}

fn compact_window_series_array(arr: &[Value]) -> Value {
    json!({
        "count": arr.len(),
        "latest_point": arr.last().cloned().unwrap_or(Value::Null),
    })
}

fn is_count_only_array_key(key: &str) -> bool {
    matches!(
        key,
        "levels"
            | "value_area_levels"
            | "hvn_levels"
            | "lvn_levels"
            | "peak_levels"
            | "tpo_single_print_zones"
    )
}

fn is_series_parent(key: &str) -> bool {
    matches!(
        key,
        "series_by_window"
            | "series_by_output_window"
            | "ffill_series_by_output_window"
            | "dev_series"
    )
}

fn singularize_array_key(key: &str) -> &'static str {
    match key {
        "series" => "point",
        "changes" => "change",
        _ => "item",
    }
}

fn assemble_bundle_indicators_json(mut rows: Vec<SnapshotBundleRow>) -> Value {
    rows.sort_by(|a, b| {
        a.indicator_code.cmp(&b.indicator_code).then_with(|| {
            snapshot_window_rank(&a.window_code).cmp(&snapshot_window_rank(&b.window_code))
        })
    });

    let mut indicators = Map::new();
    for row in &rows {
        indicators
            .entry(row.indicator_code.clone())
            .or_insert_with(|| {
                json!({
                    "window_code": row.window_code,
                    "payload": row.payload_json,
                })
            });
    }
    Value::Object(indicators)
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_bundle_indicators_json, collect_snapshot_blob_hashes, compact_snapshot_payload,
        hydrate_snapshot_payload_value, refize_snapshot_payload, SnapshotBundleRow,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    #[test]
    fn compacts_price_volume_structure_levels() {
        let payload = json!({
            "levels": [1, 2, 3],
            "value_area_levels": [4, 5],
            "by_window": {
                "15m": {
                    "levels": [1, 2],
                    "value_area_levels": [3]
                }
            }
        });
        let compacted = compact_snapshot_payload("price_volume_structure", &payload);
        assert_eq!(compacted["levels_count"], json!(3));
        assert_eq!(compacted["value_area_levels_count"], json!(2));
        assert!(compacted.get("levels").is_none());
        assert_eq!(compacted["by_window"]["15m"]["levels_count"], json!(2));
        assert_eq!(
            compacted["by_window"]["15m"]["value_area_levels_count"],
            json!(1)
        );
    }

    #[test]
    fn preserves_kline_history_bars_in_snapshot_payload() {
        let payload = json!({
            "intervals": {
                "1m": {
                    "interval_code": "1m",
                    "markets": {
                        "futures": {
                            "returned_count": 2,
                            "bars": [{"close": 1.0}, {"close": 2.0}]
                        }
                    }
                }
            }
        });
        let compacted = compact_snapshot_payload("kline_history", &payload);
        assert_eq!(compacted, payload);
    }

    #[test]
    fn compacts_footprint_levels() {
        let payload = json!({
            "levels": [1, 2, 3, 4],
            "by_window": {
                "15m": {
                    "levels": [1, 2]
                }
            }
        });
        let compacted = compact_snapshot_payload("footprint", &payload);
        assert_eq!(compacted["levels_count"], json!(4));
        assert_eq!(compacted["by_window"]["15m"]["levels_count"], json!(2));
        assert!(compacted.get("levels").is_none());
    }

    #[test]
    fn compacts_orderbook_depth_levels() {
        let payload = json!({
            "levels": [{"price": 1.0}, {"price": 2.0}],
            "obi": 0.5
        });
        let compacted = compact_snapshot_payload("orderbook_depth", &payload);
        assert_eq!(compacted["levels_count"], json!(2));
        assert_eq!(compacted["obi"], json!(0.5));
        assert!(compacted.get("levels").is_none());
    }

    #[test]
    fn compacts_series_and_changes_for_other_indicators() {
        let payload = json!({
            "changes": [{"ts": "a"}, {"ts": "b"}],
            "series_by_window": {
                "15m": [{"ts": "1"}, {"ts": "2"}]
            },
            "series_by_output_window": {
                "1h": [{"ts": "3"}]
            },
            "ffill_series_by_output_window": {
                "4h": [{"ts": "4"}, {"ts": "5"}]
            },
            "dev_series": {
                "1d": [{"ts": "6"}]
            }
        });

        let compacted = compact_snapshot_payload("avwap", &payload);
        assert_eq!(compacted["changes_count"], json!(2));
        assert_eq!(compacted["latest_change"]["ts"], json!("b"));
        assert_eq!(compacted["series_by_window"]["15m"]["count"], json!(2));
        assert_eq!(
            compacted["series_by_window"]["15m"]["latest_point"]["ts"],
            json!("2")
        );
        assert_eq!(
            compacted["series_by_output_window"]["1h"]["count"],
            json!(1)
        );
        assert_eq!(
            compacted["ffill_series_by_output_window"]["4h"]["latest_point"]["ts"],
            json!("5")
        );
        assert_eq!(compacted["dev_series"]["1d"]["count"], json!(1));
    }

    #[test]
    fn compacts_tpo_and_liq_level_arrays() {
        let payload = json!({
            "peak_levels": [1, 2, 3],
            "tpo_single_print_zones": [{"lo": 1.0}, {"lo": 2.0}],
        });

        let compacted = compact_snapshot_payload("tpo_market_profile", &payload);
        assert_eq!(compacted["peak_levels_count"], json!(3));
        assert_eq!(compacted["tpo_single_print_zones_count"], json!(2));
        assert!(compacted.get("peak_levels").is_none());
        assert!(compacted.get("tpo_single_print_zones").is_none());
    }

    #[test]
    fn repair_bundle_prefers_primary_window_rank_over_lexical_order() {
        let indicators = assemble_bundle_indicators_json(vec![
            SnapshotBundleRow {
                indicator_code: "open_interest".to_string(),
                window_code: "15m".to_string(),
                payload_json: json!({"window":"15m"}),
            },
            SnapshotBundleRow {
                indicator_code: "open_interest".to_string(),
                window_code: "5m".to_string(),
                payload_json: json!({"window":"5m"}),
            },
            SnapshotBundleRow {
                indicator_code: "long_short_ratios".to_string(),
                window_code: "1d".to_string(),
                payload_json: json!({"window":"1d"}),
            },
            SnapshotBundleRow {
                indicator_code: "long_short_ratios".to_string(),
                window_code: "5m".to_string(),
                payload_json: json!({"window":"5m"}),
            },
        ]);

        assert_eq!(indicators["open_interest"]["window_code"], json!("5m"));
        assert_eq!(indicators["long_short_ratios"]["window_code"], json!("5m"));
    }

    #[test]
    fn refizes_large_recent_7d_arrays_and_hydrates_them_back() {
        let payload = json!({
            "funding_current": -0.0001,
            "recent_7d": (0..600)
                .map(|idx| json!({
                    "change_ts": format!("2026-03-29T00:{:02}:00Z", idx % 60),
                    "funding_rate": idx as f64 / 10_000.0,
                }))
                .collect::<Vec<_>>(),
            "by_window": {
                "1h": {
                    "change_count": 3
                }
            }
        });

        let mut blobs = BTreeMap::new();
        let refized = refize_snapshot_payload("funding_rate", &payload, &mut blobs).unwrap();
        assert!(refized.get("recent_7d").is_some());
        assert!(refized["recent_7d"]
            .get("__snapshot_blob_chunks_v1")
            .is_some());
        assert!(!blobs.is_empty());

        let mut hashes = BTreeSet::new();
        collect_snapshot_blob_hashes(&refized, &mut hashes);
        assert_eq!(hashes.len(), blobs.len());

        let blob_map = blobs
            .values()
            .map(|blob| (blob.blob_hash.clone(), blob.payload_json.clone()))
            .collect::<HashMap<_, _>>();

        let mut hydrated = refized.clone();
        hydrate_snapshot_payload_value(&mut hydrated, &blob_map).unwrap();
        assert_eq!(hydrated, payload);
    }

    #[test]
    fn refizes_large_footprint_fields_and_hydrates_them_back() {
        let price_rows = (0..900)
            .map(|idx| format!("{:.2}", 2000.0 + idx as f64 * 0.5))
            .collect::<Vec<_>>();
        let stack_rows = (0..700)
            .map(|idx| {
                json!({
                    "start_price": 2000.0 + idx as f64,
                    "end_price": 2000.5 + idx as f64,
                    "score": idx as f64 / 100.0,
                })
            })
            .collect::<Vec<_>>();
        let payload = json!({
            "levels_count": 474,
            "buy_imbalance_prices": price_rows,
            "sell_imbalance_prices": (0..900)
                .map(|idx| format!("{:.2}", 2100.0 + idx as f64 * 0.5))
                .collect::<Vec<_>>(),
            "buy_stacks": stack_rows,
            "sell_stacks": (0..700)
                .map(|idx| {
                    json!({
                        "start_price": 2200.0 + idx as f64,
                        "end_price": 2200.5 + idx as f64,
                        "score": idx as f64 / 100.0,
                    })
                })
                .collect::<Vec<_>>(),
            "by_window": {
                "1m": {
                    "levels_count": 474,
                    "levels": (0..474)
                        .map(|idx| json!({"price": 2000.0 + idx as f64, "delta": idx}))
                        .collect::<Vec<_>>()
                }
            }
        });

        let mut blobs = BTreeMap::new();
        let refized = refize_snapshot_payload("footprint", &payload, &mut blobs).unwrap();
        for key in [
            "buy_imbalance_prices",
            "sell_imbalance_prices",
            "buy_stacks",
            "sell_stacks",
            "by_window",
        ] {
            assert!(refized[key].get("__snapshot_blob_ref_v1").is_some());
        }

        let blob_map = blobs
            .values()
            .map(|blob| (blob.blob_hash.clone(), blob.payload_json.clone()))
            .collect::<HashMap<_, _>>();

        let mut hydrated = refized.clone();
        hydrate_snapshot_payload_value(&mut hydrated, &blob_map).unwrap();
        assert_eq!(hydrated, payload);
    }

    #[test]
    fn refizes_large_kline_history_bars_and_hydrates_them_back() {
        let bars = (0..600)
            .map(|idx| {
                json!({
                    "open_time": format!("2026-03-29T10:{:02}:00Z", idx % 60),
                    "close_time": format!("2026-03-29T10:{:02}:59Z", idx % 60),
                    "open": 2000.0 + idx as f64,
                    "high": 2000.5 + idx as f64,
                    "low": 1999.5 + idx as f64,
                    "close": 2000.2 + idx as f64,
                    "is_closed": true,
                    "volume_base": 10.0 + idx as f64,
                    "volume_quote": 20.0 + idx as f64
                })
            })
            .collect::<Vec<_>>();
        let payload = json!({
            "indicator": "kline_history",
            "window": "1m",
            "intervals": {
                "1m": {
                    "markets": {
                        "futures": {
                            "returned_count": bars.len(),
                            "bars": bars
                        }
                    }
                }
            }
        });

        let mut blobs = BTreeMap::new();
        let refized = refize_snapshot_payload("kline_history", &payload, &mut blobs).unwrap();
        assert!(refized["intervals"]["1m"]["markets"]["futures"]["bars"]
            .get("__snapshot_blob_ref_v1")
            .is_some());
        assert!(!blobs.is_empty());

        let blob_map = blobs
            .values()
            .map(|blob| (blob.blob_hash.clone(), blob.payload_json.clone()))
            .collect::<HashMap<_, _>>();

        let mut hydrated = refized.clone();
        hydrate_snapshot_payload_value(&mut hydrated, &blob_map).unwrap();
        assert_eq!(hydrated, payload);
    }
}
