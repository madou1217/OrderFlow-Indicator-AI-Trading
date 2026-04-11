use crate::publish::ind_publisher::IndPublisher;
use crate::publish::outbox_dispatcher::ensure_indicator_bundle_outbox_schema;
use crate::storage::snapshot_writer::hydrate_snapshot_payload_values;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{FromRow, PgPool, QueryBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

const INTENT_NOTIFY_CHANNEL: &str = "indicator_bundle_intent_ready";
const RELAY_NOTIFY_TIMEOUT_SECS: u64 = 5;
const RELAY_BATCH_SIZE: i64 = 512;
const RELAY_MAX_BATCHES_PER_WAKE: usize = 16;
const RELAY_RESYNC_INTERVAL_SECS: u64 = 30;
const RELAY_WARN_MS: u128 = 2_000;

#[derive(Clone)]
pub struct DurableIntentRelay {
    main_pool: PgPool,
    ops_pool: PgPool,
    exchange_name: String,
    publisher: IndPublisher,
}

impl DurableIntentRelay {
    pub fn new(
        main_pool: PgPool,
        ops_pool: PgPool,
        exchange_name: String,
        publisher: IndPublisher,
    ) -> Self {
        Self {
            main_pool,
            ops_pool,
            exchange_name,
            publisher,
        }
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        ensure_indicator_bundle_intent_schema(&self.main_pool).await?;
        ensure_indicator_bundle_outbox_schema(&self.ops_pool).await?;
        Ok(())
    }

    pub async fn run_loop(&self) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.main_pool)
            .await
            .context("create indicator bundle intent PgListener")?;
        listener
            .listen(INTENT_NOTIFY_CHANNEL)
            .await
            .context("listen indicator bundle intent ready")?;

        self.ensure_schema().await?;

        let mut next_resync_at = Instant::now() + Duration::from_secs(RELAY_RESYNC_INTERVAL_SECS);

        info!(
            exchange_name = %self.exchange_name,
            notify_channel = INTENT_NOTIFY_CHANNEL,
            batch_size = RELAY_BATCH_SIZE,
            resync_interval_secs = RELAY_RESYNC_INTERVAL_SECS,
            "indicator durable intent relay started"
        );

        loop {
            let notified = tokio::time::timeout(
                Duration::from_secs(RELAY_NOTIFY_TIMEOUT_SECS),
                listener.recv(),
            )
            .await;

            match notified {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    warn!(
                        error = %err,
                        "indicator durable intent relay listener recv error, will retry"
                    );
                }
                Err(_) => {}
            }

            for batch_idx in 0..RELAY_MAX_BATCHES_PER_WAKE {
                let mut made_progress = false;
                made_progress |= self.finalize_sent_rows(RELAY_BATCH_SIZE).await?;
                made_progress |= self.prune_orphaned_ops_rows(RELAY_BATCH_SIZE).await?;
                made_progress |= self.mirror_due_intents(RELAY_BATCH_SIZE, false).await?;
                if !made_progress {
                    break;
                }
                if batch_idx + 1 == RELAY_MAX_BATCHES_PER_WAKE {
                    warn!(
                        exchange_name = %self.exchange_name,
                        max_batches_per_wake = RELAY_MAX_BATCHES_PER_WAKE,
                        "indicator durable intent relay hit per-wake drain cap"
                    );
                }
            }

            if Instant::now() >= next_resync_at {
                for _ in 0..RELAY_MAX_BATCHES_PER_WAKE {
                    if !self.mirror_due_intents(RELAY_BATCH_SIZE, true).await? {
                        break;
                    }
                }
                next_resync_at = Instant::now() + Duration::from_secs(RELAY_RESYNC_INTERVAL_SECS);
            }
        }
    }

    async fn mirror_due_intents(
        &self,
        batch_size: i64,
        include_stale_relayed: bool,
    ) -> Result<bool> {
        let started_at = Instant::now();
        let intents = self
            .load_due_intents(batch_size, include_stale_relayed)
            .await?;
        if intents.is_empty() {
            return Ok(false);
        }

        self.upsert_ops_outbox_rows(&intents).await?;

        for intent in &intents {
            if let Err(err) = self.ensure_ops_payload_cache(intent).await {
                warn!(
                    error = %err,
                    message_id = %intent.message_id,
                    symbol = %intent.symbol,
                    ts_bucket = %intent.ts_bucket,
                    "ensure ops payload cache for indicator durable intent failed"
                );
            }
        }

        self.mark_intents_relayed(
            &intents
                .iter()
                .map(|intent| intent.intent_id)
                .collect::<Vec<_>>(),
        )
        .await?;

        let total_ms = started_at.elapsed().as_millis();
        if total_ms >= RELAY_WARN_MS {
            warn!(
                exchange_name = %self.exchange_name,
                include_stale_relayed = include_stale_relayed,
                intent_count = intents.len(),
                total_ms = total_ms,
                "slow indicator durable intent relay mirror batch"
            );
        }

        Ok(true)
    }

    async fn load_due_intents(
        &self,
        batch_size: i64,
        include_stale_relayed: bool,
    ) -> Result<Vec<IntentRow>> {
        let sql = if include_stale_relayed {
            r#"
            SELECT
                intent_id,
                exchange_name,
                routing_key,
                message_id,
                schema_version,
                headers_json,
                symbol,
                ts_bucket,
                indicator_count,
                payload_json
            FROM ops.indicator_bundle_intent
            WHERE exchange_name = $1
              AND (
                    relayed_at IS NULL
                    OR relayed_at < now() - ($3::INT * interval '1 second')
                  )
            ORDER BY relayed_at NULLS FIRST, intent_id
            LIMIT $2
            "#
        } else {
            r#"
            SELECT
                intent_id,
                exchange_name,
                routing_key,
                message_id,
                schema_version,
                headers_json,
                symbol,
                ts_bucket,
                indicator_count,
                payload_json
            FROM ops.indicator_bundle_intent
            WHERE exchange_name = $1
              AND relayed_at IS NULL
            ORDER BY intent_id
            LIMIT $2
            "#
        };

        let mut query = sqlx::query_as::<_, IntentRow>(sql)
            .bind(&self.exchange_name)
            .bind(batch_size);
        if include_stale_relayed {
            query = query.bind(RELAY_RESYNC_INTERVAL_SECS as i32);
        }
        query.fetch_all(&self.main_pool).await.with_context(|| {
            format!(
                "load due indicator durable intents exchange={}",
                self.exchange_name
            )
        })
    }

    async fn upsert_ops_outbox_rows(&self, intents: &[IntentRow]) -> Result<()> {
        if intents.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(
            r#"
            INSERT INTO ops.indicator_bundle_outbox (
                source_intent_id,
                exchange_name,
                routing_key,
                message_id,
                schema_version,
                headers_json,
                symbol,
                ts_bucket,
                indicator_count,
                payload_json
            )
            "#,
        );

        builder.push_values(intents, |mut b, intent| {
            b.push_bind(intent.intent_id)
                .push_bind(&intent.exchange_name)
                .push_bind(&intent.routing_key)
                .push_bind(intent.message_id)
                .push_bind(intent.schema_version)
                .push_bind(&intent.headers_json)
                .push_bind(&intent.symbol)
                .push_bind(intent.ts_bucket)
                .push_bind(intent.indicator_count)
                .push_bind(&intent.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (message_id)
            DO UPDATE SET
                source_intent_id = EXCLUDED.source_intent_id,
                exchange_name = EXCLUDED.exchange_name,
                routing_key = EXCLUDED.routing_key,
                schema_version = EXCLUDED.schema_version,
                headers_json = EXCLUDED.headers_json,
                symbol = EXCLUDED.symbol,
                ts_bucket = EXCLUDED.ts_bucket,
                indicator_count = EXCLUDED.indicator_count,
                payload_json = EXCLUDED.payload_json,
                status = 'pending',
                available_at = now(),
                retry_count = 0,
                error_text = NULL
            "#,
        );

        builder
            .build()
            .execute(&self.ops_pool)
            .await
            .context("upsert indicator durable intents into ops outbox")?;
        Ok(())
    }

    async fn ensure_ops_payload_cache(&self, intent: &IntentRow) -> Result<()> {
        let mut rows: Vec<SnapshotPayloadRow> = sqlx::query_as(
            r#"
            SELECT indicator_code, window_code, payload_json
            FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot = $2
            ORDER BY indicator_code, window_code
            "#,
        )
        .bind(intent.symbol.to_uppercase())
        .bind(intent.ts_bucket)
        .fetch_all(&self.main_pool)
        .await
        .with_context(|| {
            format!(
                "fetch snapshot rows for durable intent relay symbol={} ts_bucket={}",
                intent.symbol, intent.ts_bucket
            )
        })?;

        if rows.is_empty() {
            return Ok(());
        }

        let mut payloads = rows
            .iter()
            .map(|row| row.payload_json.clone())
            .collect::<Vec<_>>();
        hydrate_snapshot_payload_values(&self.main_pool, &mut payloads).await?;
        for (row, payload_json) in rows.iter_mut().zip(payloads.into_iter()) {
            row.payload_json = payload_json;
        }

        let mut indicators = serde_json::Map::new();
        for row in &rows {
            indicators.insert(
                row.indicator_code.clone(),
                serde_json::json!({
                    "window_code": row.window_code,
                    "payload": row.payload_json,
                }),
            );
        }

        let rebuilt = self
            .publisher
            .build_minute_bundle_outbox_message(
                intent.ts_bucket,
                &intent.symbol,
                &Value::Object(indicators),
                intent.indicator_count.max(rows.len() as i32).max(0) as usize,
            )
            .context("rebuild minute bundle payload for durable intent relay")?;

        sqlx::query(
            r#"
            INSERT INTO ops.indicator_bundle_payload_cache (
                symbol,
                ts_bucket,
                schema_version,
                indicator_count,
                payload_encoding,
                payload_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (symbol, ts_bucket)
            DO UPDATE SET
                schema_version = EXCLUDED.schema_version,
                indicator_count = EXCLUDED.indicator_count,
                payload_encoding = EXCLUDED.payload_encoding,
                payload_bytes = EXCLUDED.payload_bytes,
                created_at = now()
            "#,
        )
        .bind(&intent.symbol)
        .bind(intent.ts_bucket)
        .bind(rebuilt.schema_version)
        .bind(intent.indicator_count.max(rows.len() as i32))
        .bind(&rebuilt.payload_encoding)
        .bind(
            rebuilt
                .payload_bytes_with_published_at(Utc::now())
                .context("materialize durable intent relay bundle payload bytes")?,
        )
        .execute(&self.ops_pool)
        .await
        .with_context(|| {
            format!(
                "upsert ops payload cache for durable intent symbol={} ts_bucket={}",
                intent.symbol, intent.ts_bucket
            )
        })?;

        Ok(())
    }

    async fn mark_intents_relayed(&self, intent_ids: &[i64]) -> Result<()> {
        if intent_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE ops.indicator_bundle_intent
            SET relayed_at = now()
            WHERE intent_id = ANY($1::BIGINT[])
            "#,
        )
        .bind(intent_ids)
        .execute(&self.main_pool)
        .await
        .context("mark indicator bundle intents relayed")?;
        Ok(())
    }

    async fn finalize_sent_rows(&self, batch_size: i64) -> Result<bool> {
        let rows = sqlx::query_as::<_, OpsSentRow>(
            r#"
            SELECT outbox_id, source_intent_id, message_id, symbol, ts_bucket
            FROM ops.indicator_bundle_outbox
            WHERE exchange_name = $1
              AND status = 'sent'
            ORDER BY available_at, outbox_id
            LIMIT $2
            "#,
        )
        .bind(&self.exchange_name)
        .bind(batch_size)
        .fetch_all(&self.ops_pool)
        .await
        .with_context(|| format!("load sent ops outbox rows exchange={}", self.exchange_name))?;

        if rows.is_empty() {
            return Ok(false);
        }

        let intent_ids = rows
            .iter()
            .filter_map(|row| row.source_intent_id)
            .collect::<Vec<_>>();
        if !intent_ids.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM ops.indicator_bundle_intent
                WHERE intent_id = ANY($1::BIGINT[])
                "#,
            )
            .bind(&intent_ids)
            .execute(&self.main_pool)
            .await
            .context("delete delivered indicator durable intents by intent_id")?;
        }

        let legacy_message_ids = rows
            .iter()
            .filter(|row| row.source_intent_id.is_none())
            .map(|row| row.message_id)
            .collect::<Vec<_>>();
        if !legacy_message_ids.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM ops.indicator_bundle_intent
                WHERE message_id = ANY($1::UUID[])
                "#,
            )
            .bind(&legacy_message_ids)
            .execute(&self.main_pool)
            .await
            .context("delete delivered legacy indicator durable intents by message_id")?;
        }

        self.delete_ops_rows_by_outbox_ids(
            &rows.iter().map(|row| row.outbox_id).collect::<Vec<_>>(),
        )
        .await?;

        Ok(true)
    }

    async fn prune_orphaned_ops_rows(&self, batch_size: i64) -> Result<bool> {
        let rows = sqlx::query_as::<_, OpsPendingRow>(
            r#"
            SELECT outbox_id, source_intent_id, message_id
            FROM ops.indicator_bundle_outbox
            WHERE exchange_name = $1
              AND status IN ('pending', 'failed', 'sending')
            ORDER BY available_at, outbox_id
            LIMIT $2
            "#,
        )
        .bind(&self.exchange_name)
        .bind(batch_size)
        .fetch_all(&self.ops_pool)
        .await
        .with_context(|| {
            format!(
                "load pending ops outbox rows for orphan prune exchange={}",
                self.exchange_name
            )
        })?;

        if rows.is_empty() {
            return Ok(false);
        }

        let existing_intent_ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT intent_id
            FROM ops.indicator_bundle_intent
            WHERE intent_id = ANY($1::BIGINT[])
            "#,
        )
        .bind(
            rows.iter()
                .filter_map(|row| row.source_intent_id)
                .collect::<Vec<_>>(),
        )
        .fetch_all(&self.main_pool)
        .await
        .context("load existing durable intent ids for orphan prune")?
        .into_iter()
        .collect::<HashSet<_>>();
        let existing_message_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            r#"
            SELECT message_id
            FROM ops.indicator_bundle_intent
            WHERE message_id = ANY($1::UUID[])
            "#,
        )
        .bind(
            rows.iter()
                .filter(|row| row.source_intent_id.is_none())
                .map(|row| row.message_id)
                .collect::<Vec<_>>(),
        )
        .fetch_all(&self.main_pool)
        .await
        .context("load existing durable intent message ids for orphan prune")?
        .into_iter()
        .collect::<HashSet<_>>();

        let orphaned_outbox_ids = rows
            .into_iter()
            .filter(|row| match row.source_intent_id {
                Some(intent_id) => !existing_intent_ids.contains(&intent_id),
                None => !existing_message_ids.contains(&row.message_id),
            })
            .map(|row| row.outbox_id)
            .collect::<Vec<_>>();

        if orphaned_outbox_ids.is_empty() {
            return Ok(false);
        }

        self.delete_ops_rows_by_outbox_ids(&orphaned_outbox_ids)
            .await?;
        Ok(true)
    }

    async fn delete_ops_rows_by_outbox_ids(&self, outbox_ids: &[i64]) -> Result<()> {
        if outbox_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            WITH doomed AS (
                DELETE FROM ops.indicator_bundle_outbox
                WHERE outbox_id = ANY($1::BIGINT[])
                RETURNING symbol, ts_bucket
            )
            DELETE FROM ops.indicator_bundle_payload_cache p
            USING doomed d
            WHERE p.symbol = d.symbol
              AND p.ts_bucket = d.ts_bucket
            "#,
        )
        .bind(outbox_ids)
        .execute(&self.ops_pool)
        .await
        .context("delete ops outbox rows and payload cache")?;
        Ok(())
    }
}

pub async fn ensure_indicator_bundle_intent_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ops.indicator_bundle_intent (
            intent_id BIGSERIAL PRIMARY KEY,
            exchange_name TEXT NOT NULL,
            routing_key TEXT NOT NULL,
            message_id UUID NOT NULL UNIQUE,
            schema_version INTEGER NOT NULL,
            headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            symbol TEXT NOT NULL,
            ts_bucket TIMESTAMPTZ NOT NULL,
            indicator_count INTEGER,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            relayed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT indicator_bundle_intent_schema_version_pos_chk
                CHECK (schema_version > 0)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create ops.indicator_bundle_intent")?;

    for ddl in [
        "ALTER TABLE ops.indicator_bundle_intent ADD COLUMN IF NOT EXISTS symbol TEXT",
        "ALTER TABLE ops.indicator_bundle_intent ADD COLUMN IF NOT EXISTS ts_bucket TIMESTAMPTZ",
        "ALTER TABLE ops.indicator_bundle_intent ADD COLUMN IF NOT EXISTS indicator_count INTEGER",
        "ALTER TABLE ops.indicator_bundle_intent ADD COLUMN IF NOT EXISTS payload_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "ALTER TABLE ops.indicator_bundle_intent ADD COLUMN IF NOT EXISTS relayed_at TIMESTAMPTZ",
    ] {
        sqlx::query(ddl)
            .execute(pool)
            .await
            .with_context(|| format!("ensure indicator_bundle_intent schema fragment: {ddl}"))?;
    }

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_indicator_bundle_intent_exchange_relay
        ON ops.indicator_bundle_intent (exchange_name, relayed_at, intent_id)
        "#,
    )
    .execute(pool)
    .await
    .context("create idx_indicator_bundle_intent_exchange_relay")?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_indicator_bundle_intent_symbol_ts
        ON ops.indicator_bundle_intent (symbol, ts_bucket)
        "#,
    )
    .execute(pool)
    .await
    .context("create idx_indicator_bundle_intent_symbol_ts")?;

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION ops.notify_indicator_bundle_intent_ready()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'INSERT' OR NEW.relayed_at IS NULL THEN
                PERFORM pg_notify('indicator_bundle_intent_ready', NEW.exchange_name);
            END IF;
            RETURN NEW;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .context("create ops.notify_indicator_bundle_intent_ready()")?;

    sqlx::query(
        r#"
        DROP TRIGGER IF EXISTS trg_indicator_bundle_intent_notify
        ON ops.indicator_bundle_intent
        "#,
    )
    .execute(pool)
    .await
    .context("drop trg_indicator_bundle_intent_notify")?;

    sqlx::query(
        r#"
        CREATE TRIGGER trg_indicator_bundle_intent_notify
        AFTER INSERT OR UPDATE ON ops.indicator_bundle_intent
        FOR EACH ROW EXECUTE FUNCTION ops.notify_indicator_bundle_intent_ready()
        "#,
    )
    .execute(pool)
    .await
    .context("create trg_indicator_bundle_intent_notify")?;

    Ok(())
}

#[derive(Debug, Clone, FromRow)]
struct IntentRow {
    intent_id: i64,
    exchange_name: String,
    routing_key: String,
    message_id: uuid::Uuid,
    schema_version: i32,
    headers_json: Value,
    symbol: String,
    ts_bucket: DateTime<Utc>,
    indicator_count: i32,
    payload_json: Value,
}

#[derive(Debug, Clone, FromRow)]
struct SnapshotPayloadRow {
    indicator_code: String,
    window_code: String,
    payload_json: Value,
}

#[derive(Debug, Clone, FromRow)]
struct OpsSentRow {
    outbox_id: i64,
    source_intent_id: Option<i64>,
    message_id: uuid::Uuid,
    symbol: Option<String>,
    ts_bucket: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, FromRow)]
struct OpsPendingRow {
    outbox_id: i64,
    source_intent_id: Option<i64>,
    message_id: uuid::Uuid,
}
