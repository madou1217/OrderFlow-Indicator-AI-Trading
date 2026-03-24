use crate::publish::ind_publisher::IndPublisher;
use crate::publish::outbox_dispatcher::publish_amqp_message;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lapin::{publisher_confirm::Confirmation, Channel};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use std::time::Duration;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{info, warn};

const SNAPSHOT_FANOUT_POLL_SECS: u64 = 5;
const SNAPSHOT_FANOUT_MAX_MINUTES_PER_WAKE: usize = 4;
const SNAPSHOT_FANOUT_WARN_MS: u128 = 2_000;

#[derive(Clone)]
pub struct SnapshotFanoutProjector {
    pool: PgPool,
    channel: Channel,
    publisher: IndPublisher,
    symbol: String,
}

impl SnapshotFanoutProjector {
    pub fn new(pool: PgPool, channel: Channel, publisher: IndPublisher, symbol: String) -> Self {
        Self {
            pool,
            channel,
            publisher,
            symbol,
        }
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ops.indicator_snapshot_fanout_progress (
                symbol TEXT PRIMARY KEY,
                last_published_snapshot_ts TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create ops.indicator_snapshot_fanout_progress")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_indicator_snapshot_symbol_ts
            ON feat.indicator_snapshot (symbol, ts_snapshot)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create idx_indicator_snapshot_symbol_ts")?;

        Ok(())
    }

    pub async fn initialize_progress_if_absent(&self) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ops.indicator_snapshot_fanout_progress (symbol, last_published_snapshot_ts)
            SELECT
                $1,
                COALESCE(
                    (SELECT max(ts_snapshot)
                     FROM feat.indicator_snapshot
                     WHERE symbol = $1),
                    TIMESTAMPTZ '1970-01-01 00:00:00+00'
                )
            WHERE NOT EXISTS (
                SELECT 1
                FROM ops.indicator_snapshot_fanout_progress
                WHERE symbol = $1
            )
            "#,
        )
        .bind(self.symbol_upper())
        .execute(&self.pool)
        .await
        .context("initialize indicator snapshot fanout progress if absent")?;

        Ok(())
    }

    pub async fn run_loop(&self) -> Result<()> {
        self.ensure_schema().await?;
        self.initialize_progress_if_absent().await?;

        let mut tick = interval(Duration::from_secs(SNAPSHOT_FANOUT_POLL_SECS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(
            symbol = %self.symbol,
            poll_secs = SNAPSHOT_FANOUT_POLL_SECS,
            max_minutes_per_wake = SNAPSHOT_FANOUT_MAX_MINUTES_PER_WAKE,
            "indicator snapshot fanout projector started"
        );

        loop {
            tick.tick().await;
            for _ in 0..SNAPSHOT_FANOUT_MAX_MINUTES_PER_WAKE {
                match self.project_next_minute().await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(err) => {
                        warn!(
                            error = %err,
                            debug_error = ?err,
                            symbol = %self.symbol,
                            "indicator snapshot fanout projector batch failed"
                        );
                        break;
                    }
                }
            }
        }
    }

    async fn project_next_minute(&self) -> Result<usize> {
        let symbol = self.symbol_upper();
        let last_published_ts = self.last_published_snapshot_ts().await?;
        let Some(next_ts) = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
            SELECT ts_snapshot
            FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot > $2
            ORDER BY ts_snapshot ASC
            LIMIT 1
            "#,
        )
        .bind(&symbol)
        .bind(last_published_ts)
        .fetch_optional(&self.pool)
        .await
        .context("fetch next indicator snapshot fanout ts")?
        else {
            return Ok(0);
        };

        let started_at = Instant::now();
        let rows: Vec<SnapshotFanoutRow> = sqlx::query_as(
            r#"
            SELECT ts_snapshot, indicator_code, window_code, payload_json
            FROM feat.indicator_snapshot
            WHERE symbol = $1
              AND ts_snapshot = $2
            ORDER BY indicator_code, window_code
            "#,
        )
        .bind(&symbol)
        .bind(next_ts)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("load snapshot fanout rows symbol={} ts={}", symbol, next_ts))?;

        if rows.is_empty() {
            self.advance_progress(next_ts).await?;
            return Ok(0);
        }

        for row in &rows {
            let message = self.publisher.build_snapshot_message_from_parts(
                row.ts_snapshot,
                &symbol,
                &row.indicator_code,
                &row.window_code,
                &row.payload_json,
            )?;
            match publish_amqp_message(
                &self.channel,
                &message.exchange_name,
                &message.routing_key,
                message.message_id,
                &message.headers_json,
                &message.payload_json,
            )
            .await?
            .await
            .context("wait snapshot fanout publisher confirm")?
            {
                Confirmation::Ack(_) => {}
                Confirmation::Nack(returned) => {
                    return Err(anyhow::anyhow!(
                        "snapshot fanout broker nack returned={}",
                        returned.is_some()
                    ));
                }
                Confirmation::NotRequested => {
                    return Err(anyhow::anyhow!(
                        "snapshot fanout publisher confirm not requested"
                    ));
                }
            }
        }

        self.advance_progress(next_ts).await?;
        let total_ms = started_at.elapsed().as_millis();
        if total_ms >= SNAPSHOT_FANOUT_WARN_MS {
            warn!(
                symbol = %symbol,
                ts_snapshot = %next_ts,
                snapshot_count = rows.len(),
                total_ms = total_ms,
                "slow indicator snapshot fanout minute"
            );
        }
        Ok(rows.len())
    }

    async fn last_published_snapshot_ts(&self) -> Result<DateTime<Utc>> {
        let ts = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            r#"
            SELECT last_published_snapshot_ts
            FROM ops.indicator_snapshot_fanout_progress
            WHERE symbol = $1
            "#,
        )
        .bind(self.symbol_upper())
        .fetch_optional(&self.pool)
        .await
        .context("fetch indicator snapshot fanout progress row")?
        .flatten()
        .unwrap_or_else(epoch_utc);
        Ok(ts)
    }

    async fn advance_progress(&self, ts_snapshot: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ops.indicator_snapshot_fanout_progress (
                symbol,
                last_published_snapshot_ts
            )
            VALUES ($1, $2)
            ON CONFLICT (symbol)
            DO UPDATE SET
                last_published_snapshot_ts = EXCLUDED.last_published_snapshot_ts,
                updated_at = now()
            "#,
        )
        .bind(self.symbol_upper())
        .bind(ts_snapshot)
        .execute(&self.pool)
        .await
        .context("advance indicator snapshot fanout progress")?;
        Ok(())
    }

    fn symbol_upper(&self) -> String {
        self.symbol.to_uppercase()
    }
}

#[derive(Debug, Clone, FromRow)]
struct SnapshotFanoutRow {
    ts_snapshot: DateTime<Utc>,
    indicator_code: String,
    window_code: String,
    payload_json: Value,
}

fn epoch_utc() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is valid")
}
