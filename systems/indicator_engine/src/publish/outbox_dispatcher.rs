use anyhow::{anyhow, Context, Result};
use lapin::{
    options::BasicPublishOptions,
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{FromRow, PgPool};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

const OUTBOX_DISPATCH_BATCH_SIZE: i64 = 1_000;
const OUTBOX_NOTIFY_CHANNEL: &str = "indicator_bundle_outbox_ready";
const OUTBOX_NOTIFY_TIMEOUT_SECS: u64 = 5;
const OUTBOX_HOUSEKEEPING_INTERVAL_SECS: u64 = 600;
const OUTBOX_DEAD_RETENTION_HOURS: i32 = 24;
const OUTBOX_GC_BATCH_SIZE: i64 = 10_000;
const OUTBOX_GC_MAX_BATCHES_PER_ROUND: usize = 3;
const OUTBOX_CLAIM_WARN_MS: u128 = 1_000;
const OUTBOX_DISPATCH_WARN_MS: u128 = 2_000;
const OUTBOX_MAX_BATCHES_PER_WAKE: usize = 32;

#[derive(Clone)]
pub struct OutboxDispatcher {
    pool: PgPool,
    channel: Channel,
    exchange_name: String,
}

impl OutboxDispatcher {
    pub fn new(pool: PgPool, channel: Channel, exchange_name: String) -> Self {
        Self {
            pool,
            channel,
            exchange_name,
        }
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        ensure_indicator_bundle_outbox_schema(&self.pool).await
    }

    pub async fn run_loop(&self) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .context("create indicator bundle outbox PgListener")?;
        listener
            .listen(OUTBOX_NOTIFY_CHANNEL)
            .await
            .context("listen indicator bundle outbox ready")?;

        self.ensure_schema().await?;

        let mut next_housekeeping_at =
            Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);

        info!(
            exchange_name = %self.exchange_name,
            notify_channel = OUTBOX_NOTIFY_CHANNEL,
            notify_timeout_secs = OUTBOX_NOTIFY_TIMEOUT_SECS,
            batch_size = OUTBOX_DISPATCH_BATCH_SIZE,
            "indicator bundle outbox dispatcher started"
        );

        loop {
            let notified = tokio::time::timeout(
                Duration::from_secs(OUTBOX_NOTIFY_TIMEOUT_SECS),
                listener.recv(),
            )
            .await;

            match notified {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    warn!(
                        error = %err,
                        "indicator bundle outbox listener recv error, will retry"
                    );
                }
                Err(_) => {}
            }

            for batch_idx in 0..OUTBOX_MAX_BATCHES_PER_WAKE {
                match self.dispatch_batch(OUTBOX_DISPATCH_BATCH_SIZE).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if batch_idx + 1 == OUTBOX_MAX_BATCHES_PER_WAKE {
                            warn!(
                                exchange_name = %self.exchange_name,
                                max_batches_per_wake = OUTBOX_MAX_BATCHES_PER_WAKE,
                                "indicator bundle outbox dispatcher hit per-wake drain cap"
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            debug_error = ?err,
                            exchange_name = %self.exchange_name,
                            "indicator bundle outbox dispatch batch failed"
                        );
                        break;
                    }
                }
            }

            if Instant::now() >= next_housekeeping_at {
                if let Err(err) = self.prune_dead_rows().await {
                    warn!(
                        error = %err,
                        debug_error = ?err,
                        "indicator bundle outbox dead-row gc failed"
                    );
                }
                next_housekeeping_at =
                    Instant::now() + Duration::from_secs(OUTBOX_HOUSEKEEPING_INTERVAL_SECS);
            }
        }
    }

    async fn dispatch_batch(&self, batch_size: i64) -> Result<usize> {
        let started_at = Instant::now();
        let claim_started_at = Instant::now();
        let rows = self.claim_batch(batch_size).await?;
        let claim_ms = claim_started_at.elapsed().as_millis();
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(0);
        }
        let mut sent_ids: Vec<i64> = Vec::with_capacity(rows.len());

        let publish_started_at = Instant::now();
        for row in rows {
            match publish_amqp_message(
                &self.channel,
                &row.exchange_name,
                &row.routing_key,
                row.message_id,
                &row.headers_json,
                &row.payload_json,
            )
            .await
            {
                Ok(confirm) => match confirm.await.context("wait publisher confirm")? {
                    Confirmation::Ack(_) => sent_ids.push(row.outbox_id),
                    Confirmation::Nack(returned) => {
                        let err_text = format!(
                            "broker nack for outbox_id={} returned={}",
                            row.outbox_id,
                            returned.is_some()
                        );
                        error!(
                            outbox_id = row.outbox_id,
                            "indicator bundle outbox broker nack"
                        );
                        self.mark_failed(row.outbox_id, err_text).await?;
                    }
                    Confirmation::NotRequested => {
                        self.mark_failed(
                            row.outbox_id,
                            "publisher confirm not requested on channel".to_string(),
                        )
                        .await?;
                    }
                },
                Err(err) => {
                    error!(
                        error = %err,
                        outbox_id = row.outbox_id,
                        "indicator bundle outbox publish failed"
                    );
                    self.mark_failed(row.outbox_id, err.to_string()).await?;
                }
            }
        }
        let publish_and_confirm_ms = publish_started_at.elapsed().as_millis();

        let delete_started_at = Instant::now();
        self.delete_sent_batch(&sent_ids).await?;
        let delete_ms = delete_started_at.elapsed().as_millis();
        let total_ms = started_at.elapsed().as_millis();
        if claim_ms >= OUTBOX_CLAIM_WARN_MS || total_ms >= OUTBOX_DISPATCH_WARN_MS {
            warn!(
                exchange_name = %self.exchange_name,
                batch_size = batch_size,
                claimed_rows = row_count,
                sent_rows = sent_ids.len(),
                claim_ms = claim_ms,
                publish_and_confirm_ms = publish_and_confirm_ms,
                delete_ms = delete_ms,
                total_ms = total_ms,
                "slow indicator bundle outbox dispatch batch"
            );
        }
        Ok(row_count)
    }

    async fn claim_batch(&self, batch_size: i64) -> Result<Vec<OutboxRow>> {
        if batch_size <= 0 {
            return Ok(Vec::new());
        }

        sqlx::query_as(
            r#"
            WITH picked AS (
                SELECT outbox_id
                FROM ops.indicator_bundle_outbox
                WHERE status IN ('pending', 'failed', 'sending')
                  AND available_at <= now()
                  AND exchange_name = $2
                ORDER BY available_at, outbox_id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE ops.indicator_bundle_outbox o
            SET status = 'sending',
                available_at = now() + interval '30 seconds'
            FROM picked
            WHERE o.outbox_id = picked.outbox_id
            RETURNING
                o.outbox_id,
                o.exchange_name,
                o.routing_key,
                o.message_id,
                o.headers_json,
                o.payload_json
            "#,
        )
        .bind(batch_size)
        .bind(&self.exchange_name)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!(
                "claim indicator bundle outbox rows exchange={}",
                self.exchange_name
            )
        })
    }

    async fn delete_sent_batch(&self, outbox_ids: &[i64]) -> Result<()> {
        if outbox_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            DELETE FROM ops.indicator_bundle_outbox
            WHERE outbox_id = ANY($1::BIGINT[])
            "#,
        )
        .bind(outbox_ids)
        .execute(&self.pool)
        .await
        .context("delete delivered indicator bundle outbox rows")?;
        Ok(())
    }

    async fn mark_failed(&self, outbox_id: i64, err_text: String) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE ops.indicator_bundle_outbox
            SET status = CASE WHEN retry_count + 1 >= 10 THEN 'dead' ELSE 'failed' END,
                retry_count = retry_count + 1,
                available_at = now() + make_interval(secs => LEAST(3 * (retry_count + 1), 60)),
                error_text = $2
            WHERE outbox_id = $1
            "#,
        )
        .bind(outbox_id)
        .bind(err_text)
        .execute(&self.pool)
        .await
        .context("mark indicator bundle outbox failed")?;
        Ok(())
    }

    async fn prune_dead_rows(&self) -> Result<()> {
        for _ in 0..OUTBOX_GC_MAX_BATCHES_PER_ROUND {
            let result = sqlx::query(
                r#"
                WITH doomed AS (
                    SELECT outbox_id
                    FROM ops.indicator_bundle_outbox
                    WHERE status = 'dead'
                      AND available_at < now() - ($1::INT * interval '1 hour')
                    ORDER BY available_at
                    LIMIT $2
                )
                DELETE FROM ops.indicator_bundle_outbox o
                USING doomed d
                WHERE o.outbox_id = d.outbox_id
                "#,
            )
            .bind(OUTBOX_DEAD_RETENTION_HOURS)
            .bind(OUTBOX_GC_BATCH_SIZE)
            .execute(&self.pool)
            .await
            .context("delete old dead indicator bundle outbox rows")?;

            if result.rows_affected() < OUTBOX_GC_BATCH_SIZE as u64 {
                break;
            }
        }
        Ok(())
    }
}

pub async fn ensure_indicator_bundle_outbox_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ops.indicator_bundle_outbox (
            outbox_id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            retry_count INTEGER NOT NULL DEFAULT 0,
            exchange_name TEXT NOT NULL,
            routing_key TEXT NOT NULL,
            message_id UUID NOT NULL UNIQUE,
            schema_version INTEGER NOT NULL,
            headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            error_text TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT indicator_bundle_outbox_retry_count_nonneg_chk
                CHECK (retry_count >= 0),
            CONSTRAINT indicator_bundle_outbox_schema_version_pos_chk
                CHECK (schema_version > 0),
            CONSTRAINT indicator_bundle_outbox_status_chk
                CHECK (status IN ('pending', 'sending', 'failed', 'dead'))
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create ops.indicator_bundle_outbox")?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_indicator_bundle_outbox_ready
        ON ops.indicator_bundle_outbox (exchange_name, available_at, outbox_id)
        WHERE status IN ('pending', 'failed', 'sending')
        "#,
    )
    .execute(pool)
    .await
    .context("create idx_indicator_bundle_outbox_ready")?;

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION ops.notify_indicator_bundle_outbox_ready()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('indicator_bundle_outbox_ready', NEW.exchange_name);
            RETURN NEW;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .context("create ops.notify_indicator_bundle_outbox_ready()")?;

    sqlx::query(
        r#"
        DROP TRIGGER IF EXISTS trg_indicator_bundle_outbox_notify
        ON ops.indicator_bundle_outbox
        "#,
    )
    .execute(pool)
    .await
    .context("drop trg_indicator_bundle_outbox_notify")?;

    sqlx::query(
        r#"
        CREATE TRIGGER trg_indicator_bundle_outbox_notify
        AFTER INSERT ON ops.indicator_bundle_outbox
        FOR EACH ROW EXECUTE FUNCTION ops.notify_indicator_bundle_outbox_ready()
        "#,
    )
    .execute(pool)
    .await
    .context("create trg_indicator_bundle_outbox_notify")?;

    Ok(())
}

pub(crate) async fn publish_amqp_message(
    channel: &Channel,
    exchange_name: &str,
    routing_key: &str,
    message_id: Uuid,
    headers_json: &Value,
    payload_json: &Value,
) -> Result<PublisherConfirm> {
    let payload = serde_json::to_vec(payload_json).context("serialize outbox payload")?;
    let headers = json_to_field_table(headers_json).context("build amqp headers")?;

    let properties = BasicProperties::default()
        .with_content_type(ShortString::from("application/json"))
        .with_delivery_mode(2)
        .with_headers(headers)
        .with_message_id(ShortString::from(message_id.to_string()));

    channel
        .basic_publish(
            exchange_name,
            routing_key,
            BasicPublishOptions::default(),
            &payload,
            properties,
        )
        .await
        .with_context(|| {
            format!(
                "basic_publish exchange={} routing_key={}",
                exchange_name, routing_key
            )
        })
}

#[derive(Debug, Clone, FromRow)]
struct OutboxRow {
    outbox_id: i64,
    exchange_name: String,
    routing_key: String,
    message_id: uuid::Uuid,
    headers_json: Value,
    payload_json: Value,
}

fn json_to_field_table(raw: &Value) -> Result<FieldTable> {
    let map = raw
        .as_object()
        .ok_or_else(|| anyhow!("headers_json is not object"))?;

    let mut table = FieldTable::default();
    for (k, v) in map {
        let key = ShortString::from(k.as_str());
        let value = json_to_amqp_value(v);
        table.insert(key, value);
    }
    Ok(table)
}

fn json_to_amqp_value(v: &Value) -> AMQPValue {
    match v {
        Value::Bool(b) => AMQPValue::Boolean(*b),
        Value::Number(n) => AMQPValue::LongString(LongString::from(n.to_string())),
        Value::String(s) => AMQPValue::LongString(LongString::from(s.clone())),
        Value::Null => AMQPValue::LongString(LongString::from("")),
        other => AMQPValue::LongString(LongString::from(other.to_string())),
    }
}
