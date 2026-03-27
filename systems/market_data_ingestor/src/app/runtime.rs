use crate::app::bootstrap::AppContext;
use crate::exchange::binance::rest::client::{BinanceRestClient, RestRetryPolicy};
use crate::observability::{heartbeat, metrics::AppMetrics};
use crate::pipelines::{futures_pipeline, persist_async, spot_pipeline};
use crate::sinks::{
    md_db_writer::MdDbWriter, mq_publisher::MqPublisher, ops_db_writer::OpsDbWriter,
    outbox_dispatcher::OutboxDispatcher, outbox_writer::OutboxWriter, parquet_sink::ParquetSink,
};
use crate::state::{backfill_scheduler, depth_rebuilder};
use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions},
    types::FieldTable,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

const OUTBOX_DISPATCH_WORKERS: usize = 3;
const SELFCHECK_CONSUMER_RECONNECT_BACKOFF_SECS: u64 = 5;

pub async fn run(ctx: AppContext) -> Result<()> {
    let ctx = Arc::new(ctx);

    let publisher = Arc::new(MqPublisher::new(
        ctx.mq.clone(),
        ctx.config.mq.exchanges.md_live.name.clone(),
        ctx.config.mq.exchanges.md_replay.name.clone(),
        ctx.producer_instance_id.clone(),
    ));
    let db_writer = Arc::new(MdDbWriter::new(ctx.md_db_pool.clone()));
    let ops_writer = Arc::new(OpsDbWriter::new(ctx.ops_db_pool.clone()));
    let outbox_writer = Arc::new(OutboxWriter::new(ctx.ops_db_pool.clone()));
    if ctx.config.parquet.enabled {
        let parquet_sink = Arc::new(ParquetSink::from_config(&ctx.config.parquet));
        persist_async::configure_parquet_sink(parquet_sink);
    } else {
        info!("cold-store parquet sink disabled by config");
    }
    let metrics = Arc::new(AppMetrics::default());

    let rest_client = Arc::new(BinanceRestClient::new(
        ctx.http_client.clone(),
        RestRetryPolicy {
            enabled: ctx.config.network.http.retry.enabled,
            max_retries: ctx.config.network.http.retry.max_retries,
            base_backoff_ms: ctx.config.network.http.retry.base_backoff_ms,
            max_backoff_ms: ctx.config.network.http.retry.max_backoff_ms,
        },
    ));

    let mut tasks = JoinSet::new();
    spawn_critical_task(
        &mut tasks,
        "selfcheck mq consumer",
        run_selfcheck_consumer(ctx.clone()),
    );
    spawn_critical_task(
        &mut tasks,
        "ops heartbeat loop",
        heartbeat::run_heartbeat_loop(ctx.clone(), ops_writer.clone(), metrics.clone()),
    );
    // Give each outbox dispatcher its own AMQP channel so that a channel error or
    // backpressure on one worker cannot affect the others, and concurrent basic_publish
    // calls are fully serialized per channel rather than competing on a shared one.
    for worker_id in 0..OUTBOX_DISPATCH_WORKERS {
        let outbox_dispatcher = OutboxDispatcher::new(
            ctx.ops_db_pool.clone(),
            ctx.mq.clone(),
            ctx.config.mq.exchanges.md_live.name.clone(),
        );
        spawn_critical_task(&mut tasks, "outbox dispatcher worker", async move {
            let perform_housekeeping = worker_id == 0;
            info!(
                worker_id = worker_id,
                workers = OUTBOX_DISPATCH_WORKERS,
                perform_housekeeping = perform_housekeeping,
                "starting outbox dispatcher worker"
            );
            outbox_dispatcher
                .run_loop_worker(perform_housekeeping)
                .await
        });
    }

    spawn_critical_task(
        &mut tasks,
        "spot pipeline",
        spot_pipeline::run(
            ctx.clone(),
            rest_client.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            metrics.clone(),
        ),
    );

    spawn_critical_task(
        &mut tasks,
        "futures pipeline",
        futures_pipeline::run(
            ctx.clone(),
            rest_client.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            metrics.clone(),
        ),
    );

    spawn_critical_task(
        &mut tasks,
        "depth snapshot rebuilder",
        depth_rebuilder::run_depth_snapshot_loop(
            ctx.clone(),
            rest_client.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            metrics.clone(),
        ),
    );

    spawn_critical_task(
        &mut tasks,
        "funding rate backfill loop",
        backfill_scheduler::run_funding_rate_backfill_loop(
            ctx.clone(),
            rest_client.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            metrics.clone(),
        ),
    );

    spawn_critical_task(
        &mut tasks,
        "open interest + long short ratio loop",
        backfill_scheduler::run_open_interest_ratio_loop(
            ctx.clone(),
            rest_client.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            metrics.clone(),
        ),
    );

    spawn_critical_task(
        &mut tasks,
        "options surface loop",
        backfill_scheduler::run_options_surface_loop(
            ctx.clone(),
            rest_client.clone(),
            db_writer.clone(),
            ops_writer.clone(),
            publisher.clone(),
            outbox_writer.clone(),
            metrics.clone(),
        ),
    );

    info!("market_data_ingestor started; press Ctrl+C to stop");
    let mut shutdown_signal = std::pin::pin!(wait_for_shutdown_signal());
    loop {
        tokio::select! {
            signal = &mut shutdown_signal => {
                let signal = signal?;
                info!(signal = signal, "shutdown signal received, stopping tasks");
                break;
            }
            task_result = tasks.join_next() => {
                let Some(task_result) = task_result else {
                    return Err(anyhow!("all critical tasks exited unexpectedly"));
                };
                let (task_name, result) = match task_result {
                    Ok(result) => result,
                    Err(err) => {
                        tasks.abort_all();
                        return Err(anyhow!("critical task join failed: {}", err));
                    }
                };

                tasks.abort_all();
                return match result {
                    Ok(()) => Err(anyhow!("critical task exited unexpectedly: {}", task_name)),
                    Err(err) => Err(err.context(format!("critical task {task_name} failed"))),
                };
            }
        }
    }

    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(err) = result {
            if !err.is_cancelled() {
                warn!(error = %err, "critical task join failed during shutdown");
            }
        }
    }

    Ok(())
}

async fn run_selfcheck_consumer(ctx: Arc<AppContext>) -> Result<()> {
    loop {
        match run_selfcheck_consumer_session(ctx.clone()).await {
            Ok(()) => {
                ctx.mq
                    .mark_connection_stale("selfcheck consumer stream ended");
                warn!(
                    backoff_secs = SELFCHECK_CONSUMER_RECONNECT_BACKOFF_SECS,
                    "selfcheck mq consumer stream ended, recreating"
                );
            }
            Err(err) => {
                ctx.mq
                    .mark_connection_stale("selfcheck consumer session failed");
                warn!(
                    error = %err,
                    backoff_secs = SELFCHECK_CONSUMER_RECONNECT_BACKOFF_SECS,
                    "selfcheck mq consumer failed, reconnecting"
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(
            SELFCHECK_CONSUMER_RECONNECT_BACKOFF_SECS,
        ))
        .await;
    }
}

async fn run_selfcheck_consumer_session(ctx: Arc<AppContext>) -> Result<()> {
    let channel = ctx
        .mq
        .create_channel()
        .await
        .context("create selfcheck mq consume channel")?;

    channel.basic_qos(200, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            &ctx.selfcheck_queue,
            "market_data_ingestor_selfcheck_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(queue = %ctx.selfcheck_queue, "selfcheck mq consumer started");

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let payload = std::str::from_utf8(&delivery.data).unwrap_or("<non-utf8>");
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
                    let msg_type = value
                        .get("msg_type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let routing_key = value
                        .get("routing_key")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    debug!(msg_type, routing_key, "selfcheck received mq message");
                } else {
                    warn!("selfcheck received non-json message");
                }

                if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                    ctx.mq.mark_connection_stale("selfcheck ack failed");
                    error!(error = %err, "selfcheck ack failed");
                    return Err(err).context("ack selfcheck message");
                }
            }
            Err(err) => {
                ctx.mq
                    .mark_connection_stale("selfcheck consumer delivery error");
                error!(error = %err, "selfcheck consumer error");
                return Err(err).context("receive selfcheck delivery");
            }
        }
    }

    Ok(())
}

fn spawn_critical_task<F>(
    tasks: &mut JoinSet<(&'static str, Result<()>)>,
    name: &'static str,
    future: F,
) where
    F: Future<Output = Result<()>> + Send + 'static,
{
    tasks.spawn(async move { (name, future.await) });
}

async fn wait_for_shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).context("register SIGTERM handler")?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("wait for shutdown signal")?;
        Ok("SIGINT")
    }
}
