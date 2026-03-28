use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};
use tracing::{info, warn};
use uuid::Uuid;

const OUTBOX_INSERT_WORKERS: usize = 3;
const OUTBOX_INSERT_BATCH_SIZE: usize = 1_000;
const OUTBOX_INSERT_CHANNEL_CAPACITY: usize = 100_000;
const OUTBOX_INSERT_COALESCE_MS: u64 = 5;
const OUTBOX_INSERT_STARTUP_PROTECTION_SECS: u64 = 180;
const OUTBOX_INSERT_STARTUP_BATCH_SIZE: usize = 256;
const OUTBOX_INSERT_STARTUP_BACKLOG_THRESHOLD: usize = 1_024;

#[derive(Clone)]
pub struct OutboxWriter {
    senders: Arc<Vec<mpsc::Sender<PendingOutboxRecord>>>,
}

impl OutboxWriter {
    pub fn new(pool: PgPool) -> Self {
        let mut senders = Vec::with_capacity(OUTBOX_INSERT_WORKERS);
        for worker_id in 0..OUTBOX_INSERT_WORKERS {
            let (sender, receiver) = mpsc::channel(OUTBOX_INSERT_CHANNEL_CAPACITY);
            tokio::spawn(run_insert_worker(pool.clone(), worker_id, receiver));
            senders.push(sender);
        }
        info!(
            workers = OUTBOX_INSERT_WORKERS,
            batch_size = OUTBOX_INSERT_BATCH_SIZE,
            startup_batch_size = OUTBOX_INSERT_STARTUP_BATCH_SIZE,
            channel_capacity = OUTBOX_INSERT_CHANNEL_CAPACITY,
            coalesce_ms = OUTBOX_INSERT_COALESCE_MS,
            "outbox writer batch insert worker started"
        );
        Self {
            senders: Arc::new(senders),
        }
    }

    pub async fn enqueue(
        &self,
        exchange_name: &str,
        routing_key: &str,
        message_id: Uuid,
        schema_version: i32,
        headers_json: Value,
        payload_json: Value,
    ) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let record = PendingOutboxRecord {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            message_id,
            schema_version,
            headers_json,
            payload_json,
            ack_tx: Some(ack_tx),
        };

        self.select_sender(&record.routing_key, record.message_id)
            .send(record)
            .await
            .context("enqueue outbox batch channel")?;

        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err_text)) => Err(anyhow::anyhow!(err_text)).context("insert ops.outbox_event"),
            Err(_) => Err(anyhow::anyhow!("outbox batch worker dropped ack channel")),
        }?;

        Ok(())
    }

    pub async fn enqueue_without_ack(
        &self,
        exchange_name: &str,
        routing_key: &str,
        message_id: Uuid,
        schema_version: i32,
        headers_json: Value,
        payload_json: Value,
    ) -> Result<()> {
        let record = PendingOutboxRecord {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            message_id,
            schema_version,
            headers_json,
            payload_json,
            ack_tx: None,
        };

        self.select_sender(&record.routing_key, record.message_id)
            .send(record)
            .await
            .context("enqueue outbox batch channel")?;

        Ok(())
    }

    pub async fn enqueue_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        exchange_name: &str,
        routing_key: &str,
        message_id: Uuid,
        schema_version: i32,
        headers_json: Value,
        payload_json: Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ops.outbox_event (
                exchange_name, routing_key, message_id, schema_version, headers_json, payload_json
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(exchange_name)
        .bind(routing_key)
        .bind(message_id)
        .bind(schema_version)
        .bind(headers_json)
        .bind(payload_json)
        .execute(tx.as_mut())
        .await
        .context("insert ops.outbox_event in tx")?;

        Ok(())
    }

    fn select_sender(
        &self,
        routing_key: &str,
        message_id: Uuid,
    ) -> &mpsc::Sender<PendingOutboxRecord> {
        let mut hasher = DefaultHasher::new();
        routing_key.hash(&mut hasher);
        message_id.hash(&mut hasher);
        let shard_id = (hasher.finish() as usize) % self.senders.len();
        &self.senders[shard_id]
    }
}

struct PendingOutboxRecord {
    exchange_name: String,
    routing_key: String,
    message_id: Uuid,
    schema_version: i32,
    headers_json: Value,
    payload_json: Value,
    ack_tx: Option<oneshot::Sender<std::result::Result<(), String>>>,
}

async fn run_insert_worker(
    pool: PgPool,
    worker_id: usize,
    mut receiver: mpsc::Receiver<PendingOutboxRecord>,
) {
    let worker_started_at = Instant::now();
    loop {
        let Some(first) = receiver.recv().await else {
            break;
        };

        let target_batch_size = effective_outbox_insert_batch_size(
            &worker_started_at,
            receiver.len().saturating_add(1),
        );
        let mut batch = Vec::with_capacity(target_batch_size);
        batch.push(first);

        let deadline = Instant::now() + Duration::from_millis(OUTBOX_INSERT_COALESCE_MS);
        while batch.len() < target_batch_size {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remain = deadline - now;
            match timeout(remain, receiver.recv()).await {
                Ok(Some(next)) => batch.push(next),
                Ok(None) => break,
                Err(_) => break,
            }
        }

        while batch.len() < target_batch_size {
            match receiver.try_recv() {
                Ok(next) => batch.push(next),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        if let Err(err) = insert_outbox_batch(&pool, &batch).await {
            warn!(
                error = %err,
                worker_id = worker_id,
                batch_size = batch.len(),
                "insert ops.outbox_event batch failed"
            );
            let message = err.to_string();
            for record in batch {
                if let Some(ack_tx) = record.ack_tx {
                    let _ = ack_tx.send(Err(message.clone()));
                }
            }
            continue;
        }

        for record in batch {
            if let Some(ack_tx) = record.ack_tx {
                let _ = ack_tx.send(Ok(()));
            }
        }
    }
}

async fn insert_outbox_batch(pool: &PgPool, batch: &[PendingOutboxRecord]) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO ops.outbox_event (
            exchange_name, routing_key, message_id, schema_version, headers_json, payload_json
        )
        "#,
    );

    builder.push_values(batch, |mut b, record| {
        b.push_bind(&record.exchange_name)
            .push_bind(&record.routing_key)
            .push_bind(record.message_id)
            .push_bind(record.schema_version)
            .push_bind(&record.headers_json)
            .push_bind(&record.payload_json);
    });

    builder.push(" ON CONFLICT DO NOTHING");
    builder
        .build()
        .execute(pool)
        .await
        .context("insert ops.outbox_event batch")?;

    Ok(())
}

fn effective_outbox_insert_batch_size(worker_started_at: &Instant, queued_records: usize) -> usize {
    if worker_started_at.elapsed() < Duration::from_secs(OUTBOX_INSERT_STARTUP_PROTECTION_SECS)
        && queued_records >= OUTBOX_INSERT_STARTUP_BACKLOG_THRESHOLD
    {
        OUTBOX_INSERT_STARTUP_BATCH_SIZE
    } else {
        OUTBOX_INSERT_BATCH_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::{
        effective_outbox_insert_batch_size, OUTBOX_INSERT_BATCH_SIZE,
        OUTBOX_INSERT_STARTUP_BACKLOG_THRESHOLD, OUTBOX_INSERT_STARTUP_BATCH_SIZE,
    };
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn insert_batch_size_shrinks_only_for_startup_backlog() {
        let fresh = Instant::now();
        assert_eq!(
            effective_outbox_insert_batch_size(&fresh, OUTBOX_INSERT_STARTUP_BACKLOG_THRESHOLD),
            OUTBOX_INSERT_STARTUP_BATCH_SIZE
        );
        assert_eq!(
            effective_outbox_insert_batch_size(&fresh, 1),
            OUTBOX_INSERT_BATCH_SIZE
        );

        let old = Instant::now() - Duration::from_secs(600);
        assert_eq!(
            effective_outbox_insert_batch_size(&old, OUTBOX_INSERT_STARTUP_BACKLOG_THRESHOLD),
            OUTBOX_INSERT_BATCH_SIZE
        );
    }
}
