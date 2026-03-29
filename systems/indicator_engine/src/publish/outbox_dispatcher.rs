use crate::app::bootstrap::AmqpConnectionManager;
use crate::publish::ind_publisher::IndPublisher;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use lapin::{
    options::BasicPublishOptions,
    publisher_confirm::{Confirmation, PublisherConfirm},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel,
};
use serde_json::{Map, Value};
use sqlx::postgres::PgListener;
use sqlx::{FromRow, PgPool};
use std::io::Write;
use std::sync::Arc;
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
    mq: Arc<AmqpConnectionManager>,
    exchange_name: String,
    publisher: IndPublisher,
}

impl OutboxDispatcher {
    pub fn new(
        pool: PgPool,
        mq: Arc<AmqpConnectionManager>,
        exchange_name: String,
        publisher: IndPublisher,
    ) -> Self {
        Self {
            pool,
            mq,
            exchange_name,
            publisher,
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
        let channel = self
            .mq
            .create_confirm_channel()
            .await
            .context("acquire indicator publish channel for outbox batch")?;

        let publish_started_at = Instant::now();
        for row in rows {
            let payload = self
                .build_publish_payload(&row)
                .await
                .with_context(|| format!("build bundle payload outbox_id={}", row.outbox_id))?;
            match publish_amqp_message(
                &channel,
                &row.exchange_name,
                &row.routing_key,
                row.message_id,
                &row.headers_json,
                &payload,
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
                o.symbol,
                o.ts_bucket,
                o.indicator_count,
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
            WITH delivered AS (
                DELETE FROM ops.indicator_bundle_outbox
                WHERE outbox_id = ANY($1::BIGINT[])
                RETURNING symbol, ts_bucket
            )
            DELETE FROM ops.indicator_bundle_payload_cache p
            USING delivered d
            WHERE p.symbol = d.symbol
              AND p.ts_bucket = d.ts_bucket
            "#,
        )
        .bind(outbox_ids)
        .execute(&self.pool)
        .await
        .context("delete delivered indicator bundle outbox rows and payload cache")?;
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

    async fn build_publish_payload(&self, row: &OutboxRow) -> Result<PublishPayload> {
        if row
            .payload_json
            .get("indicators")
            .and_then(Value::as_object)
            .is_some()
        {
            return gzip_json_value(&row.payload_json);
        }

        let symbol = row
            .symbol
            .as_deref()
            .or_else(|| row.payload_json.get("symbol").and_then(Value::as_str))
            .ok_or_else(|| anyhow!("outbox row missing symbol"))?;
        let ts_bucket = row.ts_bucket.or_else(|| {
            row.payload_json
                .get("ts_bucket")
                .and_then(Value::as_str)
                .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                .map(|ts| ts.with_timezone(&Utc))
        });
        let ts_bucket = ts_bucket.ok_or_else(|| anyhow!("outbox row missing ts_bucket"))?;

        if let Some(payload) = self
            .fetch_cached_bundle_payload(symbol, ts_bucket)
            .await
            .with_context(|| {
                format!(
                    "fetch cached bundle payload symbol={} ts_bucket={}",
                    symbol, ts_bucket
                )
            })?
        {
            return Ok(payload);
        }

        let rows: Vec<SnapshotPayloadRow> = sqlx::query_as(
            r#"
            SELECT indicator_code, window_code, payload_json
            FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot = $2
            ORDER BY indicator_code, window_code
            "#,
        )
        .bind(symbol.to_uppercase())
        .bind(ts_bucket)
        .fetch_all(&self.pool)
        .await
        .context("fetch snapshot rows for minute bundle rebuild")?;

        if rows.is_empty() {
            return Err(anyhow!(
                "no snapshot rows found for symbol={} ts_bucket={}",
                symbol,
                ts_bucket
            ));
        }

        let mut indicators = Map::new();
        for snap in &rows {
            indicators.insert(
                snap.indicator_code.clone(),
                serde_json::json!({
                    "window_code": snap.window_code,
                    "payload": snap.payload_json,
                }),
            );
        }

        let rebuilt = self
            .publisher
            .build_minute_bundle_outbox_message(
                ts_bucket,
                symbol,
                &Value::Object(indicators),
                row.indicator_count.unwrap_or(rows.len() as i32).max(0) as usize,
            )
            .context("rebuild minute bundle payload from snapshots")?;

        Ok(PublishPayload {
            body: rebuilt.payload_bytes,
            content_encoding: normalize_payload_encoding(&rebuilt.payload_encoding)?,
        })
    }

    async fn fetch_cached_bundle_payload(
        &self,
        symbol: &str,
        ts_bucket: DateTime<Utc>,
    ) -> Result<Option<PublishPayload>> {
        let row = sqlx::query_as::<_, CachedBundlePayloadRow>(
            r#"
            SELECT payload_encoding, payload_bytes
            FROM ops.indicator_bundle_payload_cache
            WHERE symbol = $1
              AND ts_bucket = $2
            "#,
        )
        .bind(symbol.to_uppercase())
        .bind(ts_bucket)
        .fetch_optional(&self.pool)
        .await
        .context("fetch cached indicator bundle payload row")?;

        row.map(|r| {
            Ok(PublishPayload {
                body: r.payload_bytes,
                content_encoding: normalize_payload_encoding(&r.payload_encoding)?,
            })
        })
        .transpose()
    }
}

pub async fn ensure_indicator_bundle_outbox_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ops.indicator_bundle_payload_cache (
            symbol TEXT NOT NULL,
            ts_bucket TIMESTAMPTZ NOT NULL,
            schema_version INTEGER NOT NULL,
            indicator_count INTEGER,
            payload_encoding TEXT NOT NULL DEFAULT 'gzip',
            payload_bytes BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, ts_bucket)
        )
        "#,
    )
    .execute(pool)
    .await
    .context("create ops.indicator_bundle_payload_cache")?;

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
            symbol TEXT,
            ts_bucket TIMESTAMPTZ,
            indicator_count INTEGER,
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

    for ddl in [
        "ALTER TABLE ops.indicator_bundle_outbox ADD COLUMN IF NOT EXISTS symbol TEXT",
        "ALTER TABLE ops.indicator_bundle_outbox ADD COLUMN IF NOT EXISTS ts_bucket TIMESTAMPTZ",
        "ALTER TABLE ops.indicator_bundle_outbox ADD COLUMN IF NOT EXISTS indicator_count INTEGER",
    ] {
        sqlx::query(ddl)
            .execute(pool)
            .await
            .with_context(|| format!("ensure indicator_bundle_outbox schema fragment: {ddl}"))?;
    }

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
    payload: &PublishPayload,
) -> Result<PublisherConfirm> {
    let headers = build_publish_headers(headers_json, payload.content_encoding.as_deref())
        .context("build amqp headers")?;

    let mut properties = BasicProperties::default()
        .with_content_type(ShortString::from("application/json"))
        .with_delivery_mode(2)
        .with_headers(headers)
        .with_message_id(ShortString::from(message_id.to_string()));
    if let Some(content_encoding) = payload.content_encoding.as_deref() {
        properties = properties.with_content_encoding(ShortString::from(content_encoding));
    }

    channel
        .basic_publish(
            exchange_name,
            routing_key,
            BasicPublishOptions::default(),
            &payload.body,
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
    symbol: Option<String>,
    ts_bucket: Option<DateTime<Utc>>,
    indicator_count: Option<i32>,
    payload_json: Value,
}

#[derive(Debug, Clone, FromRow)]
struct SnapshotPayloadRow {
    indicator_code: String,
    window_code: String,
    payload_json: Value,
}

#[derive(Debug, Clone, FromRow)]
struct CachedBundlePayloadRow {
    payload_encoding: String,
    payload_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishPayload {
    pub(crate) body: Vec<u8>,
    pub(crate) content_encoding: Option<String>,
}

fn gzip_json_value(payload_json: &Value) -> Result<PublishPayload> {
    let raw = serde_json::to_vec(payload_json).context("serialize indicator bundle payload")?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(&raw)
        .context("gzip indicator bundle payload")?;
    let body = encoder
        .finish()
        .context("finish gzip indicator bundle payload")?;
    Ok(PublishPayload {
        body,
        content_encoding: Some("gzip".to_string()),
    })
}

pub(crate) fn identity_json_publish_payload(payload_json: &Value) -> Result<PublishPayload> {
    Ok(PublishPayload {
        body: serde_json::to_vec(payload_json).context("serialize json publish payload")?,
        content_encoding: None,
    })
}

fn normalize_payload_encoding(payload_encoding: &str) -> Result<Option<String>> {
    let trimmed = payload_encoding.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("identity") {
        return Ok(None);
    }
    if trimmed.eq_ignore_ascii_case("gzip") || trimmed.eq_ignore_ascii_case("x-gzip") {
        return Ok(Some("gzip".to_string()));
    }
    Err(anyhow!(
        "unsupported indicator bundle payload encoding: {}",
        payload_encoding
    ))
}

fn build_publish_headers(raw: &Value, content_encoding: Option<&str>) -> Result<FieldTable> {
    let mut table = json_to_field_table(raw)?;
    if let Some(content_encoding) = content_encoding {
        table.insert(
            ShortString::from("content_encoding"),
            AMQPValue::LongString(LongString::from(content_encoding.to_string())),
        );
    }
    Ok(table)
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

#[cfg(test)]
mod tests {
    use super::{build_publish_headers, gzip_json_value, normalize_payload_encoding};
    use flate2::read::GzDecoder;
    use serde_json::json;
    use std::io::Read;

    #[test]
    fn gzip_json_value_round_trips_and_marks_gzip_encoding() {
        let payload = json!({
            "msg_type": "ind.minute_bundle",
            "symbol": "BTCUSDT",
            "indicators": {"footprint": {"payload": {"levels": [1, 2, 3]}}}
        });

        let encoded = gzip_json_value(&payload).expect("gzip payload");
        assert_eq!(encoded.content_encoding.as_deref(), Some("gzip"));

        let mut decoder = GzDecoder::new(encoded.body.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).expect("decode gzip body");
        let decoded: serde_json::Value =
            serde_json::from_slice(&raw).expect("parse decompressed json");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn normalize_payload_encoding_accepts_identity_and_gzip() {
        assert_eq!(normalize_payload_encoding("").expect("identity"), None);
        assert_eq!(
            normalize_payload_encoding("identity").expect("identity"),
            None
        );
        assert_eq!(
            normalize_payload_encoding("gzip").expect("gzip"),
            Some("gzip".to_string())
        );
        assert_eq!(
            normalize_payload_encoding("X-GZIP").expect("x-gzip"),
            Some("gzip".to_string())
        );
    }

    #[test]
    fn build_publish_headers_adds_content_encoding_header() {
        let headers =
            build_publish_headers(&json!({"schema": "v1"}), Some("gzip")).expect("build headers");
        assert!(headers.contains_key("schema"));
        assert!(headers.contains_key("content_encoding"));
    }
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
