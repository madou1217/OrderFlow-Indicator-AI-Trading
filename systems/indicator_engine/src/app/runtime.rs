use crate::app::bootstrap::{build_db_pool, AppContext, DbPoolConfig, RootConfig};
use crate::indicators::context::{
    DivergenceSigTestMode, IndicatorContext, IndicatorRuntimeOptions, IndicatorSnapshotRow,
    KlineHistoryBar, KlineHistorySupplement, OptionsSurfacePoint,
};
use crate::indicators::i19_kline_history::build_interval_bar_records;
use crate::indicators::i27_options_surface::OPTIONS_SURFACE_WINDOWS;
use crate::indicators::shared::incremental::IncrementalIndicatorConfig;
use crate::ingest::decoder::{build_engine_event, EngineEvent, EngineEventEnvelope, MdData};
use crate::ingest::mq_consumer;
use crate::ingest::watermark::floor_minute;
use crate::observability::heartbeat;
use crate::observability::metrics::AppMetrics;
use crate::publish::ind_publisher::IndPublisher;
use crate::publish::outbox_dispatcher::OutboxDispatcher;
use crate::publish::snapshot_fanout_projector::SnapshotFanoutProjector;
use crate::runtime::dispatcher::{DispatchMode, Dispatcher};
use crate::runtime::state_store::{
    CanonicalFrontierSnapshot, CanonicalMinutePresence, IngestOutcome, MinuteHistory,
    StateSnapshot, StateStore, HISTORY_LIMIT_MINUTES, STATE_SNAPSHOT_VERSION,
};
use crate::runtime::window_scheduler::WindowScheduler;
use crate::storage::event_writer::EventWriter;
use crate::storage::feature_writer::FeatureWriter;
use crate::storage::level_writer::LevelWriter;
use crate::storage::snapshot_writer::SnapshotWriter;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgPool, Row};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES: i64 = 30;
const STARTUP_BACKFILL_OVERLAP_MINUTES: i64 = 30;
const MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES: i64 = 7 * 24 * 60;
const STARTUP_BACKFILL_SAFETY_LAG_SECS: i64 = 10;
const STARTUP_BACKFILL_MARKET: &str = "all";
const STALE_DROP_REPORT_INTERVAL_SECS: u64 = 10;
const INGEST_TRADE_CHANNEL_CAPACITY: usize = 50_000;
const INGEST_NON_TRADE_CHANNEL_CAPACITY: usize = 100_000;
const PREPARE_INGEST_QUEUE_CAPACITY: usize = 150_000;
const INGEST_DRAIN_PER_TICK_LIMIT: usize = 25_000;
const DIRTY_RECOMPUTE_BATCH_SIZE: usize = 5;
const DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK: usize = 50;
const OI_RATIO_PATCH_BATCH_SIZE: usize = 6;
const OI_RATIO_PATCH_WINDOW_BUDGET_PER_TICK: usize = 24;
const PROCESS_READY_MINUTES_WARN_MS: u128 = 2_000;
const LIVE_READY_JOB_QUEUE_CAPACITY: usize = 8;
const LIVE_PREPARE_TASK_QUEUE_CAPACITY: usize = 8;
const DIRTY_READY_JOB_QUEUE_CAPACITY: usize = 4;
const LIVE_TAIL_RECONCILE_MAX_BACKLOG_MINUTES: i64 = 5;
const OI_RATIO_PATCH_MAX_BACKLOG_MINUTES: i64 = 5;
const STUCK_PROGRESS_IDLE_SECS: u64 = 60;
const STUCK_WARN_INTERVAL_SECS: u64 = 60;
const LIVE_CANONICAL_GAP_REPAIR_RETRY_SECS: u64 = 15;
const LIVE_CANONICAL_TAIL_RECONCILE_INTERVAL_SECS: u64 = 24 * 60;
const LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES: i64 = 31;
const CANONICAL_REPLAY_FETCH_WINDOW_MINUTES: i64 = 360;
const STARTUP_BACKFILL_YIELD_EVERY_MINUTES: usize = 64;

async fn build_publish_db_pool(config: &Arc<RootConfig>) -> Result<PgPool> {
    let mut publish_cfg = (**config).clone();
    let base_name = publish_cfg
        .database
        .application_name
        .clone()
        .unwrap_or_else(|| "indicator_engine".to_string());
    publish_cfg.database.application_name = Some(format!("{base_name}_publish"));
    let existing = publish_cfg.database.pool.clone();
    publish_cfg.database.pool = Some(DbPoolConfig {
        min_connections: Some(1),
        max_connections: Some(
            existing
                .as_ref()
                .and_then(|p| p.max_connections)
                .unwrap_or(20)
                .min(36),
        ),
        acquire_timeout_secs: existing.as_ref().and_then(|p| p.acquire_timeout_secs),
        idle_timeout_secs: existing.as_ref().and_then(|p| p.idle_timeout_secs),
        max_lifetime_secs: existing.as_ref().and_then(|p| p.max_lifetime_secs),
        test_before_acquire: existing.as_ref().and_then(|p| p.test_before_acquire),
    });
    build_db_pool(&publish_cfg)
        .await
        .context("build publish db pool")
}

#[derive(Debug, Clone)]
pub struct ReplayRow {
    pub event_ts: DateTime<Utc>,
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub routing_key: String,
    pub data_json: Value,
}

#[derive(Debug, Clone)]
pub struct BackfillCursor {
    pub event_ts: DateTime<Utc>,
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub routing_key: String,
}

#[derive(Debug)]
struct RuntimeStallDetector {
    last_persisted_ts: Option<DateTime<Utc>>,
    last_progress_advance_at: Instant,
    last_warn_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct LiveCanonicalRepairController {
    last_gap_repair_attempt_at: Option<Instant>,
    last_gap_repair_minute: Option<DateTime<Utc>>,
    last_tail_reconcile_at: Option<Instant>,
}

struct OiRatioPatchTask {
    minutes: Vec<DateTime<Utc>>,
    started_at: Instant,
    handle: JoinHandle<Result<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadyJobSource {
    Live,
    DirtyRecompute,
}

struct ReadyMinuteJob {
    ts_bucket: DateTime<Utc>,
    mode: DispatchMode,
    source: ReadyJobSource,
    enqueued_at: Instant,
    bundle: crate::runtime::state_store::WindowBundle,
}

struct PrepareMinuteTask {
    minutes: Vec<DateTime<Utc>>,
    mode: DispatchMode,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializeWorkerKind {
    Live,
    DirtyRecompute,
}

impl MaterializeWorkerKind {
    fn label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::DirtyRecompute => "dirty_recompute",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestQueueLane {
    Trade,
    NonTrade,
}

struct QueuedIngestEvent {
    lane: IngestQueueLane,
    event: EngineEvent,
}

impl LiveCanonicalRepairController {
    fn gap_repair_due(&self, next_minute: DateTime<Utc>) -> bool {
        match (self.last_gap_repair_minute, self.last_gap_repair_attempt_at) {
            (Some(prev_minute), Some(last_attempt))
                if prev_minute == next_minute
                    && last_attempt.elapsed()
                        < Duration::from_secs(LIVE_CANONICAL_GAP_REPAIR_RETRY_SECS) =>
            {
                false
            }
            _ => true,
        }
    }

    fn mark_gap_repair_attempt(&mut self, next_minute: DateTime<Utc>) {
        self.last_gap_repair_minute = Some(next_minute);
        self.last_gap_repair_attempt_at = Some(Instant::now());
    }

    fn tail_reconcile_due(&self) -> bool {
        self.last_tail_reconcile_at
            .map(|last| {
                last.elapsed() >= Duration::from_secs(LIVE_CANONICAL_TAIL_RECONCILE_INTERVAL_SECS)
            })
            .unwrap_or(true)
    }

    fn mark_tail_reconcile_attempt(&mut self) {
        self.last_tail_reconcile_at = Some(Instant::now());
    }
}

#[derive(Debug, Default)]
struct CanonicalRepairStats {
    fetched_rows: usize,
    ingested_rows: usize,
    touched_minutes: HashSet<i64>,
    changed_rows: usize,
    dirty_recompute_marked_rows: usize,
    oi_ratio_patch_marked_rows: usize,
    changed_minutes: HashSet<i64>,
    first_bucket: Option<DateTime<Utc>>,
    last_bucket: Option<DateTime<Utc>>,
}

impl CanonicalRepairStats {
    fn record_event(&mut self, event: &EngineEvent) {
        let bucket = logical_event_bucket_ts(event);
        self.first_bucket = Some(
            self.first_bucket
                .map(|prev| prev.min(bucket))
                .unwrap_or(bucket),
        );
        self.last_bucket = Some(
            self.last_bucket
                .map(|prev| prev.max(bucket))
                .unwrap_or(bucket),
        );
        self.touched_minutes.insert(bucket.timestamp());
    }

    fn touched_minute_count(&self) -> usize {
        self.touched_minutes.len()
    }

    fn record_material_change(&mut self, bucket: DateTime<Utc>, outcome: IngestOutcome) {
        if !outcome.material_change {
            return;
        }
        self.changed_rows += 1;
        if outcome.dirty_recompute_marked {
            self.dirty_recompute_marked_rows += 1;
        }
        if outcome.oi_ratio_patch_marked {
            self.oi_ratio_patch_marked_rows += 1;
        }
        self.changed_minutes.insert(bucket.timestamp());
    }

    fn changed_minute_count(&self) -> usize {
        self.changed_minutes.len()
    }
}

impl RuntimeStallDetector {
    fn new(last_persisted_ts: Option<DateTime<Utc>>) -> Self {
        Self {
            last_persisted_ts,
            last_progress_advance_at: Instant::now(),
            last_warn_at: None,
        }
    }

    fn observe_persisted_ts(&mut self, current: Option<DateTime<Utc>>) {
        if current != self.last_persisted_ts {
            self.last_persisted_ts = current;
            self.last_progress_advance_at = Instant::now();
        }
    }

    fn idle_for_too_long(&self) -> bool {
        self.last_progress_advance_at.elapsed() >= Duration::from_secs(STUCK_PROGRESS_IDLE_SECS)
    }

    fn should_emit_warn(&mut self) -> bool {
        let now = Instant::now();
        let allow = self
            .last_warn_at
            .map(|last| now.duration_since(last) >= Duration::from_secs(STUCK_WARN_INTERVAL_SECS))
            .unwrap_or(true);
        if allow {
            self.last_warn_at = Some(now);
        }
        allow
    }
}

fn live_backlog_minutes(next_minute: Option<DateTime<Utc>>, latest_closed: DateTime<Utc>) -> i64 {
    next_minute
        .map(|minute| (latest_closed - minute).num_minutes().max(0))
        .unwrap_or(0)
}

fn allow_live_tail_reconcile(
    state_store: &StateStore,
    next_minute: Option<DateTime<Utc>>,
    latest_closed: DateTime<Utc>,
) -> bool {
    !state_store.has_pending_dirty_recompute()
        && !state_store.has_pending_oi_ratio_patch()
        && live_backlog_minutes(next_minute, latest_closed)
            <= LIVE_TAIL_RECONCILE_MAX_BACKLOG_MINUTES
}

fn allow_oi_ratio_patch_processing(
    next_minute: Option<DateTime<Utc>>,
    latest_closed: DateTime<Utc>,
) -> bool {
    live_backlog_minutes(next_minute, latest_closed) <= OI_RATIO_PATCH_MAX_BACKLOG_MINUTES
}

pub fn build_indicator_runtime_options(
    config: &crate::app::bootstrap::RootConfig,
) -> IndicatorRuntimeOptions {
    IndicatorRuntimeOptions {
        whale_threshold_usdt: config.indicator.whale_threshold_usdt,
        kline_history_bars_1m: config.indicator.kline_history.bars_1m,
        kline_history_bars_15m: config.indicator.kline_history.bars_15m,
        kline_history_bars_4h: config.indicator.kline_history.bars_4h,
        kline_history_bars_1d: config.indicator.kline_history.bars_1d,
        kline_history_bars_3d: config.indicator.kline_history.bars_3d,
        kline_history_fill_1d_from_db: config.indicator.kline_history.fill_1d_from_db,
        fvg_windows: config.indicator.fvg.windows.clone(),
        fvg_fill_from_db: config.indicator.fvg.fill_from_db,
        fvg_db_bars_4h: config.indicator.fvg.db_bars_4h,
        fvg_db_bars_1d: config.indicator.fvg.db_bars_1d,
        fvg_epsilon_gap_ticks: config.indicator.fvg.epsilon_gap_ticks,
        fvg_atr_lookback: config.indicator.fvg.atr_lookback,
        fvg_min_body_ratio: config.indicator.fvg.min_body_ratio,
        fvg_min_impulse_atr_ratio: config.indicator.fvg.min_impulse_atr_ratio,
        fvg_min_gap_atr_ratio: config.indicator.fvg.min_gap_atr_ratio,
        fvg_max_gap_atr_ratio: config.indicator.fvg.max_gap_atr_ratio,
        fvg_mitigated_fill_threshold: config.indicator.fvg.mitigated_fill_threshold,
        fvg_invalid_close_bars: config.indicator.fvg.invalid_close_bars,
        tpo_rows_nb: config.indicator.tpo_market_profile.rows_nb,
        tpo_value_area_pct: config.indicator.tpo_market_profile.value_area_pct,
        tpo_session_windows: config.indicator.tpo_market_profile.session_windows.clone(),
        tpo_ib_minutes: config.indicator.tpo_market_profile.ib_minutes,
        tpo_dev_output_windows: config
            .indicator
            .tpo_market_profile
            .dev_output_windows
            .clone(),
        rvwap_windows: config.indicator.rvwap_sigma_bands.windows.clone(),
        rvwap_output_windows: config.indicator.rvwap_sigma_bands.output_windows.clone(),
        rvwap_min_samples: config.indicator.rvwap_sigma_bands.min_samples,
        high_volume_pulse_z_windows: config.indicator.high_volume_pulse.z_windows.clone(),
        high_volume_pulse_summary_windows: config
            .indicator
            .high_volume_pulse
            .summary_windows
            .clone(),
        high_volume_pulse_min_samples: config.indicator.high_volume_pulse.min_samples,
        ema_base_periods: config.indicator.ema_trend_regime.base_periods.clone(),
        ema_htf_periods: config.indicator.ema_trend_regime.htf_periods.clone(),
        ema_htf_windows: config.indicator.ema_trend_regime.htf_windows.clone(),
        ema_output_windows: config.indicator.ema_trend_regime.output_windows.clone(),
        ema_fill_from_db: config.indicator.ema_trend_regime.fill_from_db,
        ema_db_bars_4h: config.indicator.ema_trend_regime.db_bars_4h,
        ema_db_bars_1d: config.indicator.ema_trend_regime.db_bars_1d,
        ema_db_bars_3d: config.indicator.ema_trend_regime.db_bars_3d,
        divergence_sig_test_mode: DivergenceSigTestMode::from_str(
            &config.indicator.divergence.sig_test_mode,
        ),
        divergence_bootstrap_b: config.indicator.divergence.bootstrap_b,
        divergence_bootstrap_block_len: config.indicator.divergence.bootstrap_block_len,
        divergence_p_value_threshold: config.indicator.divergence.p_value_threshold,
        window_codes: config.indicator.window_codes.clone(),
    }
}

fn build_incremental_indicator_config(
    runtime_options: &IndicatorRuntimeOptions,
) -> IncrementalIndicatorConfig {
    IncrementalIndicatorConfig {
        tpo_rows_nb: runtime_options.tpo_rows_nb,
        tpo_value_area_pct: runtime_options.tpo_value_area_pct,
        tpo_session_windows: runtime_options
            .tpo_session_windows
            .iter()
            .filter_map(|code| match code.as_str() {
                "4h" => Some((code.clone(), 240)),
                "1d" => Some((code.clone(), 1440)),
                "3d" => Some((code.clone(), 4320)),
                _ => None,
            })
            .collect(),
        tpo_ib_minutes: runtime_options.tpo_ib_minutes,
        tpo_dev_output_windows: runtime_options
            .tpo_dev_output_windows
            .iter()
            .filter_map(|code| match code.as_str() {
                "15m" => Some((code.clone(), 15)),
                "1h" => Some((code.clone(), 60)),
                _ => None,
            })
            .collect(),
        rvwap_windows: runtime_options
            .rvwap_windows
            .iter()
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
            .collect(),
        rvwap_output_windows: runtime_options
            .rvwap_output_windows
            .iter()
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
            .collect(),
        rvwap_min_samples: runtime_options.rvwap_min_samples,
        high_volume_pulse_z_windows: runtime_options
            .high_volume_pulse_z_windows
            .iter()
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
            .collect(),
        high_volume_pulse_summary_windows: runtime_options
            .high_volume_pulse_summary_windows
            .iter()
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
            .collect(),
        high_volume_pulse_min_samples: runtime_options.high_volume_pulse_min_samples,
    }
}

fn window_code_minutes(code: &str) -> Option<i64> {
    match code {
        "15m" => Some(15),
        "1h" => Some(60),
        "4h" => Some(240),
        "1d" => Some(1440),
        "3d" => Some(4320),
        _ => None,
    }
}

fn decrement_ingest_pending(
    lane: IngestQueueLane,
    trade_ingest_pending: &Arc<AtomicUsize>,
    non_trade_ingest_pending: &Arc<AtomicUsize>,
) {
    match lane {
        IngestQueueLane::Trade => {
            trade_ingest_pending.fetch_sub(1, Ordering::AcqRel);
        }
        IngestQueueLane::NonTrade => {
            non_trade_ingest_pending.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

async fn run_ingest_forwarder(
    lane: IngestQueueLane,
    mut source_rx: mpsc::Receiver<EngineEvent>,
    prepare_tx: mpsc::Sender<QueuedIngestEvent>,
    pending_counter: Arc<AtomicUsize>,
) -> Result<()> {
    while let Some(event) = source_rx.recv().await {
        pending_counter.fetch_add(1, Ordering::AcqRel);
        if prepare_tx
            .send(QueuedIngestEvent { lane, event })
            .await
            .is_err()
        {
            pending_counter.fetch_sub(1, Ordering::AcqRel);
            break;
        }
    }
    Ok(())
}

pub async fn run(ctx: AppContext) -> Result<()> {
    let ctx = Arc::new(ctx);

    let metrics = Arc::new(AppMetrics::default());
    let (trade_tx, trade_rx) = mpsc::channel(INGEST_TRADE_CHANNEL_CAPACITY);
    let (non_trade_tx, non_trade_rx) = mpsc::channel(INGEST_NON_TRADE_CHANNEL_CAPACITY);

    let consumer_handles =
        mq_consumer::spawn_consumers(ctx.clone(), trade_tx, non_trade_tx, metrics.clone());
    let heartbeat_ctx = ctx.clone();
    let heartbeat_metrics = metrics.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat::run_heartbeat_loop(heartbeat_ctx, heartbeat_metrics).await
    });

    let feature_writer = FeatureWriter::new(ctx.db_pool.clone());
    let snapshot_writer = SnapshotWriter::new(ctx.db_pool.clone());
    snapshot_writer
        .ensure_schema()
        .await
        .context("ensure indicator snapshot blob schema")?;
    let level_writer = LevelWriter::new(ctx.db_pool.clone());
    let event_writer = EventWriter::new(ctx.db_pool.clone(), metrics.clone());
    let publisher = IndPublisher::new(
        ctx.config.mq.exchanges.ind.name.clone(),
        ctx.producer_instance_id.clone(),
    );

    let dispatcher = Arc::new(Dispatcher::new(
        feature_writer,
        snapshot_writer,
        level_writer,
        event_writer,
        publisher.clone(),
    ));
    let publish_db_pool = build_publish_db_pool(&ctx.config).await?;
    let outbox_dispatcher = OutboxDispatcher::new(
        publish_db_pool.clone(),
        ctx.mq.clone(),
        ctx.config.mq.exchanges.ind.name.clone(),
        publisher.clone(),
    );
    outbox_dispatcher
        .ensure_schema()
        .await
        .context("ensure indicator bundle outbox schema")?;
    let snapshot_fanout_projector = SnapshotFanoutProjector::new(
        publish_db_pool,
        ctx.mq.clone(),
        publisher.clone(),
        ctx.config.indicator.symbol.clone(),
    );
    snapshot_fanout_projector
        .ensure_schema()
        .await
        .context("ensure indicator snapshot fanout projector schema")?;
    let mut state_store = StateStore::new(
        ctx.config.indicator.symbol.to_uppercase(),
        ctx.config.indicator.whale_threshold_usdt,
    );
    let mut scheduler = WindowScheduler::new(ctx.config.indicator.watermark_lateness_secs);
    let consume_mode_live = ctx
        .config
        .indicator
        .consume_mode
        .eq_ignore_ascii_case("live");
    let live_drop_stale_enabled = ctx.config.indicator.live_drop_stale_enabled;
    let stale_limit_secs = ctx.config.indicator.live_drop_stale_event_secs;
    let mut stale_drop_count: u64 = 0;
    let mut stale_drop_max_lag_secs: i64 = 0;
    let mut stale_drop_max_publish_delay_secs: i64 = 0;
    let mut stale_drop_max_transport_lag_secs: i64 = 0;
    let mut stale_drop_oldest_ts: Option<DateTime<Utc>> = None;
    let mut stale_drop_newest_ts: Option<DateTime<Utc>> = None;
    let mut stale_drop_by_msg_type: HashMap<String, u64> = HashMap::new();
    let mut stale_drop_last_report = Instant::now();
    let runtime_options = build_indicator_runtime_options(&ctx.config);
    state_store.set_divergence_runtime_options(
        runtime_options.divergence_sig_test_mode,
        runtime_options.divergence_bootstrap_b,
        runtime_options.divergence_bootstrap_block_len,
        runtime_options.divergence_p_value_threshold,
    );
    state_store
        .set_incremental_runtime_options(build_incremental_indicator_config(&runtime_options));
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Register signal handlers BEFORE backfill so SIGTERM/Ctrl+C during
    // the long backfill phase is also caught and triggers a clean snapshot save.
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler setup failed");

    info!(
        symbol = %ctx.config.indicator.symbol,
        queue_count = ctx.indicator_queues.len(),
        export_interval_secs = ctx.config.indicator.export_interval_secs,
        startup_mode = "live_plus_auto_backfill",
        startup_max_catchup_minutes = ctx.config.indicator.startup_max_catchup_minutes,
        stale_limit_secs = stale_limit_secs,
        watermark_lateness_secs = ctx.config.indicator.watermark_lateness_secs,
        "indicator_engine started; press Ctrl+C to stop"
    );

    // Race backfill against shutdown signals. If a signal arrives during backfill,
    // save the partial snapshot (valid — next start gap-fills missing minutes) and exit.
    let mut got_signal_before_live = false;
    metrics.set_backfill_mode(true);
    let startup_replay_cutoff_bucket = tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C received during backfill, saving snapshot and shutting down");
            got_signal_before_live = true;
            None
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received during backfill, saving snapshot and shutting down");
            got_signal_before_live = true;
            None
        }
        result = run_startup_backfill(
            &ctx,
            metrics.clone(),
            dispatcher.as_ref(),
            &mut state_store,
            &mut scheduler,
            &runtime_options,
        ) => {
            match result {
                Ok(v) => v,
                Err(err) => {
                    return Err(err).context("startup historical backfill failed");
                }
            }
        }
    };

    if got_signal_before_live {
        let snapshot_path = ctx
            .config
            .indicator
            .snapshot_file_path
            .replace("{symbol}", &ctx.config.indicator.symbol);
        let snap = state_store.extract_snapshot();
        let history_len = snap.history_futures.len();
        if !snapshot_path.is_empty() && snapshot_has_required_history(&snap) {
            info!(
                history_bars = history_len,
                "Saving state snapshot before exit (mid-backfill)..."
            );
            match save_state_snapshot(&snap, &snapshot_path).await {
                Ok(()) => {
                    info!(path = %snapshot_path, history_bars = history_len, "State snapshot saved (mid-backfill)")
                }
                Err(e) => warn!(error = %e, "Failed to save state snapshot"),
            }
        } else {
            info!(
                history_bars = history_len,
                effective_history_floor_ts = ?snap.effective_history_floor_ts,
                "Skipping snapshot save — state does not yet cover a reusable restart window"
            );
        }
        heartbeat_handle.abort();
        for h in consumer_handles {
            h.abort();
        }
        return Ok(());
    }
    metrics.set_backfill_mode(false);

    let state_store = Arc::new(Mutex::new(state_store));
    let (live_ready_job_tx_raw, live_ready_job_rx) = mpsc::channel(LIVE_READY_JOB_QUEUE_CAPACITY);
    let mut live_ready_job_tx = Some(live_ready_job_tx_raw);
    let live_ready_job_pending = Arc::new(AtomicUsize::new(0));
    let (live_prepare_task_tx_raw, live_prepare_task_rx) =
        mpsc::channel(LIVE_PREPARE_TASK_QUEUE_CAPACITY);
    let mut live_prepare_task_tx = Some(live_prepare_task_tx_raw);
    let live_prepare_minute_pending = Arc::new(AtomicUsize::new(0));
    let mut live_prepare_handle = Some(tokio::spawn(run_live_prepare_loop(
        ctx.clone(),
        state_store.clone(),
        live_prepare_task_rx,
        live_prepare_minute_pending.clone(),
        live_ready_job_tx
            .as_ref()
            .expect("live ready job sender must exist before prepare worker spawn")
            .clone(),
        live_ready_job_pending.clone(),
    )));
    let mut live_materialize_handle = Some(tokio::spawn(run_live_materialize_loop(
        ctx.clone(),
        metrics.clone(),
        dispatcher.clone(),
        runtime_options.clone(),
        MaterializeWorkerKind::Live,
        live_ready_job_rx,
        live_ready_job_pending.clone(),
    )));
    let (dirty_ready_job_tx_raw, dirty_ready_job_rx) =
        mpsc::channel(DIRTY_READY_JOB_QUEUE_CAPACITY);
    let mut dirty_ready_job_tx = Some(dirty_ready_job_tx_raw);
    let dirty_ready_job_pending = Arc::new(AtomicUsize::new(0));
    let mut dirty_materialize_handle = Some(tokio::spawn(run_live_materialize_loop(
        ctx.clone(),
        metrics.clone(),
        dispatcher.clone(),
        runtime_options.clone(),
        MaterializeWorkerKind::DirtyRecompute,
        dirty_ready_job_rx,
        dirty_ready_job_pending.clone(),
    )));

    let mut startup_cutover_completed = startup_replay_cutoff_bucket.is_none();
    let outbox_handle = tokio::spawn(async move { outbox_dispatcher.run_loop().await });
    snapshot_fanout_projector
        .initialize_progress_if_absent()
        .await
        .context("initialize indicator snapshot fanout progress")?;
    let snapshot_fanout_handle =
        tokio::spawn(async move { snapshot_fanout_projector.run_loop().await });

    let (prepare_ingest_tx, mut prepare_ingest_rx) = mpsc::channel(PREPARE_INGEST_QUEUE_CAPACITY);
    let trade_ingest_pending = Arc::new(AtomicUsize::new(0));
    let non_trade_ingest_pending = Arc::new(AtomicUsize::new(0));
    let mut trade_ingest_forwarder_handle = Some(tokio::spawn(run_ingest_forwarder(
        IngestQueueLane::Trade,
        trade_rx,
        prepare_ingest_tx.clone(),
        trade_ingest_pending.clone(),
    )));
    let mut non_trade_ingest_forwarder_handle = Some(tokio::spawn(run_ingest_forwarder(
        IngestQueueLane::NonTrade,
        non_trade_rx,
        prepare_ingest_tx.clone(),
        non_trade_ingest_pending.clone(),
    )));
    drop(prepare_ingest_tx);

    let mut ingest_channel_closed = false;
    let mut shutdown_requested = false;
    let mut shutdown_closed_minute: Option<DateTime<Utc>> = None;
    let mut stall_detector =
        RuntimeStallDetector::new(ts_from_millis(metrics.snapshot().last_persisted_ts_ms));
    let mut live_repair_controller = LiveCanonicalRepairController::default();
    let mut oi_ratio_patch_task: Option<OiRatioPatchTask> = None;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let cutoff = scheduler.closed_minute(Utc::now());
                info!(shutdown_closed_minute = %cutoff, "Ctrl+C received, shutting down");
                shutdown_requested = true;
                shutdown_closed_minute = Some(cutoff);
                break;
            }
            _ = sigterm.recv() => {
                let cutoff = scheduler.closed_minute(Utc::now());
                info!(shutdown_closed_minute = %cutoff, "SIGTERM received, shutting down");
                shutdown_requested = true;
                shutdown_closed_minute = Some(cutoff);
                break;
            }
            maybe_event = prepare_ingest_rx.recv(), if !ingest_channel_closed => {
                if let Some(queued) = maybe_event {
                    decrement_ingest_pending(
                        queued.lane,
                        &trade_ingest_pending,
                        &non_trade_ingest_pending,
                    );
                    let mut state_store = state_store.lock().await;
                    handle_ingest_event(
                        queued.event,
                        startup_replay_cutoff_bucket,
                        &mut startup_cutover_completed,
                        consume_mode_live,
                        live_drop_stale_enabled,
                        stale_limit_secs,
                        &mut stale_drop_count,
                        &mut stale_drop_max_lag_secs,
                        &mut stale_drop_max_publish_delay_secs,
                        &mut stale_drop_max_transport_lag_secs,
                        &mut stale_drop_oldest_ts,
                        &mut stale_drop_newest_ts,
                        &mut stale_drop_by_msg_type,
                        &metrics,
                        &mut state_store,
                        &mut scheduler,
                    );
                } else {
                    ingest_channel_closed = true;
                    warn!("all mq consumers ended, stopping indicator engine");
                    break;
                }
            }
            _ = tick.tick() => {
                settle_finished_oi_ratio_patch_task(
                    &mut oi_ratio_patch_task,
                    &state_store,
                    &metrics,
                )
                .await;
                poll_prepare_handle(&mut live_prepare_handle).await?;
                poll_materialize_handle(&mut live_materialize_handle, MaterializeWorkerKind::Live)
                    .await?;
                poll_materialize_handle(
                    &mut dirty_materialize_handle,
                    MaterializeWorkerKind::DirtyRecompute,
                )
                .await?;

                let drain_result = drain_pending_ingest_events_shared(
                    &mut prepare_ingest_rx,
                    &mut ingest_channel_closed,
                    &trade_ingest_pending,
                    &non_trade_ingest_pending,
                    startup_replay_cutoff_bucket,
                    &mut startup_cutover_completed,
                    consume_mode_live,
                    live_drop_stale_enabled,
                    stale_limit_secs,
                    &mut stale_drop_count,
                    &mut stale_drop_max_lag_secs,
                    &mut stale_drop_max_publish_delay_secs,
                    &mut stale_drop_max_transport_lag_secs,
                    &mut stale_drop_oldest_ts,
                    &mut stale_drop_newest_ts,
                    &mut stale_drop_by_msg_type,
                    &metrics,
                    &state_store,
                    &mut scheduler,
                )
                .await;
                if drain_result.all_channels_closed {
                    warn!("all mq consumers ended, stopping indicator engine");
                    break;
                }

                let trade_channel_len = trade_ingest_pending.load(Ordering::Acquire);
                let non_trade_channel_len = non_trade_ingest_pending.load(Ordering::Acquire);
                let queue_lag = (trade_channel_len + non_trade_channel_len) as i64;
                metrics.set_queue_lag(queue_lag);
                if stale_drop_count > 0
                    && stale_drop_last_report.elapsed()
                        >= Duration::from_secs(STALE_DROP_REPORT_INTERVAL_SECS)
                {
                    warn!(
                        dropped = stale_drop_count,
                        oldest_event_ts = ?stale_drop_oldest_ts,
                        newest_event_ts = ?stale_drop_newest_ts,
                        max_lag_secs = stale_drop_max_lag_secs,
                        max_publish_delay_secs = stale_drop_max_publish_delay_secs,
                        max_transport_lag_secs = stale_drop_max_transport_lag_secs,
                        stale_limit_secs = stale_limit_secs,
                        trade_channel_len = trade_channel_len,
                        non_trade_channel_len = non_trade_channel_len,
                        drop_by_msg_type = %format_stale_msg_type_distribution(&stale_drop_by_msg_type),
                        "drop stale md event in live mode (aggregated)"
                    );
                    stale_drop_count = 0;
                    stale_drop_max_lag_secs = 0;
                    stale_drop_max_publish_delay_secs = 0;
                    stale_drop_max_transport_lag_secs = 0;
                    stale_drop_oldest_ts = None;
                    stale_drop_newest_ts = None;
                    stale_drop_by_msg_type.clear();
                    stale_drop_last_report = Instant::now();
                }

                let latest_closed = scheduler.closed_minute(Utc::now());
                let next_minute_before_repairs = scheduler.next_minute_to_emit();
                if startup_cutover_completed {
                    if let Some(next_minute) = next_minute_before_repairs {
                        let next_minute_presence = {
                            let state_store = state_store.lock().await;
                            state_store.canonical_minute_presence(next_minute)
                        };
                        if next_minute <= latest_closed
                            && !next_minute_presence.complete_under_current_policy()
                            && live_repair_controller.gap_repair_due(next_minute)
                        {
                            live_repair_controller.mark_gap_repair_attempt(next_minute);
                            let repair_to_ts = latest_closed + ChronoDuration::minutes(1);
                            let mut state_store = state_store.lock().await;
                            match ingest_canonical_range_from_db(
                                &ctx.db_pool,
                                &ctx.config.indicator.symbol,
                                &metrics,
                                &mut state_store,
                                &mut scheduler,
                                next_minute,
                                repair_to_ts,
                                "live_gap_repair",
                            )
                            .await
                            {
                                Ok(stats) => {
                                    let next_minute_presence_after =
                                        state_store.canonical_minute_presence(next_minute);
                                    let healed = next_minute_presence_after
                                        .complete_under_current_policy();
                                    let healed_ready_through = if healed {
                                        state_store.latest_contiguous_complete_canonical_minute_from(
                                            next_minute,
                                            latest_closed,
                                        )
                                    } else {
                                        None
                                    };
                                    if stats.ingested_rows > 0 || healed {
                                        info!(
                                            reason = "live_gap_repair",
                                            from_ts = %next_minute,
                                            to_ts_exclusive = %repair_to_ts,
                                            latest_closed = %latest_closed,
                                            fetched_rows = stats.fetched_rows,
                                            ingested_rows = stats.ingested_rows,
                                            touched_minutes = stats.touched_minute_count(),
                                            first_bucket = ?stats.first_bucket,
                                            last_bucket = ?stats.last_bucket,
                                            next_minute_complete_before = next_minute_presence.complete_under_current_policy(),
                                            next_minute_complete_after = next_minute_presence_after.complete_under_current_policy(),
                                            ready_through_after = ?healed_ready_through,
                                            missing_required_before = %format_missing_required_sources(&next_minute_presence),
                                            missing_required_after = %format_missing_required_sources(&next_minute_presence_after),
                                            "live canonical gap repair completed"
                                        );
                                    } else {
                                        warn!(
                                            reason = "live_gap_repair",
                                            from_ts = %next_minute,
                                            to_ts_exclusive = %repair_to_ts,
                                            latest_closed = %latest_closed,
                                            fetched_rows = stats.fetched_rows,
                                            ingested_rows = stats.ingested_rows,
                                            next_minute_complete_after = next_minute_presence_after.complete_under_current_policy(),
                                            missing_required_after = %format_missing_required_sources(&next_minute_presence_after),
                                            "live canonical gap repair found no new rows"
                                        );
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        reason = "live_gap_repair",
                                        from_ts = %next_minute,
                                        to_ts_exclusive = %repair_to_ts,
                                        latest_closed = %latest_closed,
                                        "live canonical gap repair failed"
                                    );
                                }
                            }
                        }
                    }

                    let last_finalized_minute = {
                        let state_store = state_store.lock().await;
                        state_store.last_finalized_minute()
                    };
                    if let Some(last_finalized_minute) = last_finalized_minute {
                        let allow_tail_reconcile = {
                            let state_store = state_store.lock().await;
                            live_repair_controller.tail_reconcile_due()
                                && allow_live_tail_reconcile(
                                    &state_store,
                                    next_minute_before_repairs,
                                    latest_closed,
                                )
                        };
                        if allow_tail_reconcile {
                            live_repair_controller.mark_tail_reconcile_attempt();
                            let effective_history_floor_ts = {
                                let state_store = state_store.lock().await;
                                state_store
                                    .canonical_frontier_snapshot()
                                    .effective_history_floor_ts
                            };
                            let tail_start_ts = live_tail_reconcile_start_ts(
                                last_finalized_minute,
                                effective_history_floor_ts,
                            );
                            let tail_to_ts =
                                last_finalized_minute + ChronoDuration::minutes(1);
                            let mut state_store = state_store.lock().await;
                            match ingest_canonical_range_from_db(
                                &ctx.db_pool,
                                &ctx.config.indicator.symbol,
                                &metrics,
                                &mut state_store,
                                &mut scheduler,
                                tail_start_ts,
                                tail_to_ts,
                                "live_tail_reconcile",
                            )
                            .await
                            {
                                Ok(stats) => {
                                    if stats.changed_rows > 0 {
                                        info!(
                                            reason = "live_tail_reconcile",
                                            from_ts = %tail_start_ts,
                                            to_ts_exclusive = %tail_to_ts,
                                            last_finalized_minute = %last_finalized_minute,
                                            fetched_rows = stats.fetched_rows,
                                            ingested_rows = stats.ingested_rows,
                                            touched_minutes = stats.touched_minute_count(),
                                            changed_rows = stats.changed_rows,
                                            changed_minutes = stats.changed_minute_count(),
                                            dirty_recompute_marked_rows = stats.dirty_recompute_marked_rows,
                                            oi_ratio_patch_marked_rows = stats.oi_ratio_patch_marked_rows,
                                            first_bucket = ?stats.first_bucket,
                                            last_bucket = ?stats.last_bucket,
                                            dirty_recompute_pending = state_store.has_pending_dirty_recompute(),
                                            oi_ratio_patch_pending = state_store.has_pending_oi_ratio_patch(),
                                            "live canonical tail reconcile ingested db truth"
                                        );
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        reason = "live_tail_reconcile",
                                        from_ts = %tail_start_ts,
                                        to_ts_exclusive = %tail_to_ts,
                                        last_finalized_minute = %last_finalized_minute,
                                        "live canonical tail reconcile failed"
                                    );
                                }
                            }
                        }
                    }
                }

                let next_minute = scheduler.next_minute_to_emit();
                let (ready_through_ts, frontier_snapshot, next_minute_presence, has_pending_oi_ratio_patch) =
                    {
                        let state_store = state_store.lock().await;
                        let ready_through_ts = next_minute.and_then(|minute| {
                            state_store.latest_contiguous_complete_canonical_minute_from(
                                minute,
                                latest_closed,
                            )
                        });
                        let frontier_snapshot = refresh_runtime_observability_metrics(
                            &metrics,
                            &state_store,
                            next_minute,
                            ready_through_ts,
                            trade_channel_len,
                            non_trade_channel_len,
                        );
                        let next_minute_presence = next_minute
                            .map(|minute| state_store.canonical_minute_presence(minute))
                            .unwrap_or_default();
                        (
                            ready_through_ts,
                            frontier_snapshot,
                            next_minute_presence,
                            state_store.has_pending_oi_ratio_patch(),
                        )
                    };
                let allow_oi_ratio_patches =
                    allow_oi_ratio_patch_processing(next_minute, latest_closed);
                maybe_warn_runtime_stall(
                    &mut stall_detector,
                    &metrics,
                    &frontier_snapshot,
                    next_minute,
                    ready_through_ts,
                    &next_minute_presence,
                    trade_channel_len,
                    non_trade_channel_len,
                );

                let Some(_next_minute) = next_minute else {
                    if has_pending_oi_ratio_patch && allow_oi_ratio_patches && oi_ratio_patch_task.is_none() {
                        let mut state_store = state_store.lock().await;
                        oi_ratio_patch_task = spawn_oi_ratio_patch_task(
                            dispatcher.clone(),
                            &mut state_store,
                            &runtime_options,
                            &metrics,
                        )?;
                    }
                    continue;
                };
                let Some(ready_through_ts) = ready_through_ts else {
                    if has_pending_oi_ratio_patch && allow_oi_ratio_patches && oi_ratio_patch_task.is_none() {
                        let mut state_store = state_store.lock().await;
                        oi_ratio_patch_task = spawn_oi_ratio_patch_task(
                            dispatcher.clone(),
                            &mut state_store,
                            &runtime_options,
                            &metrics,
                        )?;
                    }
                    continue;
                };
                if let Err(err) = enqueue_live_ready_jobs(
                    &ctx,
                    metrics.clone(),
                    &state_store,
                    &mut scheduler,
                    live_prepare_task_tx
                        .as_ref()
                        .expect("live prepare task sender must exist while runtime loop is active"),
                    &live_prepare_minute_pending,
                    &live_ready_job_pending,
                    dirty_ready_job_tx
                        .as_ref()
                        .expect("dirty ready job sender must exist while runtime loop is active"),
                    &dirty_ready_job_pending,
                    ready_through_ts,
                    DispatchMode::Live,
                )
                .await
                {
                    error!(error = %err, "enqueue ready indicator minutes failed");
                    return Err(err).context("enqueue ready indicator minutes failed");
                }
                if allow_oi_ratio_patches && oi_ratio_patch_task.is_none() {
                    let mut state_store = state_store.lock().await;
                    if state_store.has_pending_oi_ratio_patch() {
                        oi_ratio_patch_task = spawn_oi_ratio_patch_task(
                            dispatcher.clone(),
                            &mut state_store,
                            &runtime_options,
                            &metrics,
                        )?;
                    }
                }
            }
        }
    }

    {
        let mut state_store = state_store.lock().await;
        abort_oi_ratio_patch_task(&mut oi_ratio_patch_task, &mut state_store).await;
    }

    if shutdown_requested {
        let shutdown_closed_minute =
            shutdown_closed_minute.unwrap_or_else(|| scheduler.closed_minute(Utc::now()));
        info!(
            shutdown_closed_minute = %shutdown_closed_minute,
            "stopping ingress and flushing ready indicator minutes before snapshot save"
        );
        for h in &consumer_handles {
            h.abort();
        }
        drop(live_prepare_task_tx.take());
        drop(live_ready_job_tx.take());
        drop(dirty_ready_job_tx.take());
        drain_prepare_handle(&mut live_prepare_handle).await;
        drain_materialize_handle(&mut live_materialize_handle, MaterializeWorkerKind::Live).await;
        drain_materialize_handle(
            &mut dirty_materialize_handle,
            MaterializeWorkerKind::DirtyRecompute,
        )
        .await;
        outbox_handle.abort();
        snapshot_fanout_handle.abort();

        let mut state_store = Arc::try_unwrap(state_store)
            .map_err(|_| anyhow::anyhow!("state_store still shared during shutdown drain"))?
            .into_inner();

        if let Err(err) = shutdown_drain_and_persist(
            &ctx,
            metrics.clone(),
            dispatcher.as_ref(),
            &mut state_store,
            &mut scheduler,
            &runtime_options,
            &mut prepare_ingest_rx,
            &mut ingest_channel_closed,
            &trade_ingest_pending,
            &non_trade_ingest_pending,
            startup_replay_cutoff_bucket,
            &mut startup_cutover_completed,
            shutdown_closed_minute,
            consume_mode_live,
            live_drop_stale_enabled,
            stale_limit_secs,
            &mut stale_drop_count,
            &mut stale_drop_max_lag_secs,
            &mut stale_drop_max_publish_delay_secs,
            &mut stale_drop_max_transport_lag_secs,
            &mut stale_drop_oldest_ts,
            &mut stale_drop_newest_ts,
            &mut stale_drop_by_msg_type,
        )
        .await
        {
            warn!(error = %err, "shutdown drain + persist failed before snapshot save");
        }

        let snapshot_path = ctx
            .config
            .indicator
            .snapshot_file_path
            .replace("{symbol}", &ctx.config.indicator.symbol);
        if !snapshot_path.is_empty() {
            info!("Saving state snapshot before exit...");
            let snap = state_store.extract_snapshot();
            match save_state_snapshot(&snap, &snapshot_path).await {
                Ok(()) => info!(
                    path = %snapshot_path,
                    futures_bars = snap.history_futures.len(),
                    spot_bars = snap.history_spot.len(),
                    last_ts = %snap.last_finalized_ts,
                    "State snapshot saved successfully"
                ),
                Err(e) => warn!(error = %e, "Failed to save state snapshot"),
            }
        }

        heartbeat_handle.abort();
        if let Some(handle) = trade_ingest_forwarder_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(handle) = non_trade_ingest_forwarder_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        outbox_handle.abort();
        snapshot_fanout_handle.abort();
        for h in consumer_handles {
            h.abort();
        }

        return Ok(());
    }

    heartbeat_handle.abort();
    if let Some(handle) = trade_ingest_forwarder_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    if let Some(handle) = non_trade_ingest_forwarder_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    drop(live_prepare_task_tx.take());
    drop(live_ready_job_tx.take());
    drop(dirty_ready_job_tx.take());
    drain_prepare_handle(&mut live_prepare_handle).await;
    if let Some(handle) = live_materialize_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    if let Some(handle) = dirty_materialize_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    outbox_handle.abort();
    snapshot_fanout_handle.abort();
    for h in consumer_handles {
        h.abort();
    }

    let snapshot_path = ctx
        .config
        .indicator
        .snapshot_file_path
        .replace("{symbol}", &ctx.config.indicator.symbol);
    let state_store = Arc::try_unwrap(state_store)
        .map_err(|_| anyhow::anyhow!("state_store still shared while saving snapshot"))?
        .into_inner();
    let state_store = state_store;
    if !snapshot_path.is_empty() {
        info!("Saving state snapshot before exit...");
        let snap = state_store.extract_snapshot();
        match save_state_snapshot(&snap, &snapshot_path).await {
            Ok(()) => info!(
                path = %snapshot_path,
                futures_bars = snap.history_futures.len(),
                spot_bars = snap.history_spot.len(),
                last_ts = %snap.last_finalized_ts,
                "State snapshot saved successfully"
            ),
            Err(e) => warn!(error = %e, "Failed to save state snapshot"),
        }
    }

    Ok(())
}

struct DrainPendingIngestResult {
    all_channels_closed: bool,
    drained_count: usize,
}

async fn drain_pending_ingest_events_shared(
    ingest_rx: &mut mpsc::Receiver<QueuedIngestEvent>,
    ingest_channel_closed: &mut bool,
    trade_ingest_pending: &Arc<AtomicUsize>,
    non_trade_ingest_pending: &Arc<AtomicUsize>,
    startup_replay_cutoff_bucket: Option<DateTime<Utc>>,
    startup_cutover_completed: &mut bool,
    consume_mode_live: bool,
    live_drop_stale_enabled: bool,
    stale_limit_secs: i64,
    stale_drop_count: &mut u64,
    stale_drop_max_lag_secs: &mut i64,
    stale_drop_max_publish_delay_secs: &mut i64,
    stale_drop_max_transport_lag_secs: &mut i64,
    stale_drop_oldest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_newest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_by_msg_type: &mut HashMap<String, u64>,
    metrics: &Arc<AppMetrics>,
    state_store: &Arc<Mutex<StateStore>>,
    scheduler: &mut WindowScheduler,
) -> DrainPendingIngestResult {
    let mut drained = 0usize;
    let mut drained_events = Vec::new();

    while drained < INGEST_DRAIN_PER_TICK_LIMIT {
        match ingest_rx.try_recv() {
            Ok(queued) => {
                decrement_ingest_pending(
                    queued.lane,
                    trade_ingest_pending,
                    non_trade_ingest_pending,
                );
                drained_events.push(queued.event);
                drained += 1;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *ingest_channel_closed = true;
                break;
            }
        }
    }

    if !drained_events.is_empty() {
        let mut state_store = state_store.lock().await;
        for event in drained_events {
            handle_ingest_event(
                event,
                startup_replay_cutoff_bucket,
                startup_cutover_completed,
                consume_mode_live,
                live_drop_stale_enabled,
                stale_limit_secs,
                stale_drop_count,
                stale_drop_max_lag_secs,
                stale_drop_max_publish_delay_secs,
                stale_drop_max_transport_lag_secs,
                stale_drop_oldest_ts,
                stale_drop_newest_ts,
                stale_drop_by_msg_type,
                metrics,
                &mut state_store,
                scheduler,
            );
        }
    }

    DrainPendingIngestResult {
        all_channels_closed: *ingest_channel_closed,
        drained_count: drained,
    }
}

fn drain_pending_ingest_events_owned(
    ingest_rx: &mut mpsc::Receiver<QueuedIngestEvent>,
    ingest_channel_closed: &mut bool,
    trade_ingest_pending: &Arc<AtomicUsize>,
    non_trade_ingest_pending: &Arc<AtomicUsize>,
    startup_replay_cutoff_bucket: Option<DateTime<Utc>>,
    startup_cutover_completed: &mut bool,
    consume_mode_live: bool,
    live_drop_stale_enabled: bool,
    stale_limit_secs: i64,
    stale_drop_count: &mut u64,
    stale_drop_max_lag_secs: &mut i64,
    stale_drop_max_publish_delay_secs: &mut i64,
    stale_drop_max_transport_lag_secs: &mut i64,
    stale_drop_oldest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_newest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_by_msg_type: &mut HashMap<String, u64>,
    metrics: &Arc<AppMetrics>,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
) -> DrainPendingIngestResult {
    let mut drained = 0usize;

    while drained < INGEST_DRAIN_PER_TICK_LIMIT {
        match ingest_rx.try_recv() {
            Ok(queued) => {
                decrement_ingest_pending(
                    queued.lane,
                    trade_ingest_pending,
                    non_trade_ingest_pending,
                );
                handle_ingest_event(
                    queued.event,
                    startup_replay_cutoff_bucket,
                    startup_cutover_completed,
                    consume_mode_live,
                    live_drop_stale_enabled,
                    stale_limit_secs,
                    stale_drop_count,
                    stale_drop_max_lag_secs,
                    stale_drop_max_publish_delay_secs,
                    stale_drop_max_transport_lag_secs,
                    stale_drop_oldest_ts,
                    stale_drop_newest_ts,
                    stale_drop_by_msg_type,
                    metrics,
                    state_store,
                    scheduler,
                );
                drained += 1;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *ingest_channel_closed = true;
                break;
            }
        }
    }

    DrainPendingIngestResult {
        all_channels_closed: *ingest_channel_closed,
        drained_count: drained,
    }
}

async fn shutdown_drain_and_persist(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    runtime_options: &IndicatorRuntimeOptions,
    ingest_rx: &mut mpsc::Receiver<QueuedIngestEvent>,
    ingest_channel_closed: &mut bool,
    trade_ingest_pending: &Arc<AtomicUsize>,
    non_trade_ingest_pending: &Arc<AtomicUsize>,
    startup_replay_cutoff_bucket: Option<DateTime<Utc>>,
    startup_cutover_completed: &mut bool,
    shutdown_closed_minute: DateTime<Utc>,
    consume_mode_live: bool,
    live_drop_stale_enabled: bool,
    stale_limit_secs: i64,
    stale_drop_count: &mut u64,
    stale_drop_max_lag_secs: &mut i64,
    stale_drop_max_publish_delay_secs: &mut i64,
    stale_drop_max_transport_lag_secs: &mut i64,
    stale_drop_oldest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_newest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_by_msg_type: &mut HashMap<String, u64>,
) -> Result<()> {
    let started_at = Instant::now();
    let mut rounds = 0usize;
    let mut total_drained = 0usize;
    let mut total_processed = 0usize;

    loop {
        rounds += 1;
        let last_persisted_before = ts_from_millis(metrics.snapshot().last_persisted_ts_ms);
        let next_minute_before = scheduler.next_minute_to_emit();
        let dirty_pending_before = state_store.has_pending_dirty_recompute();

        let drain_result = drain_pending_ingest_events_owned(
            ingest_rx,
            ingest_channel_closed,
            trade_ingest_pending,
            non_trade_ingest_pending,
            startup_replay_cutoff_bucket,
            startup_cutover_completed,
            consume_mode_live,
            live_drop_stale_enabled,
            stale_limit_secs,
            stale_drop_count,
            stale_drop_max_lag_secs,
            stale_drop_max_publish_delay_secs,
            stale_drop_max_transport_lag_secs,
            stale_drop_oldest_ts,
            stale_drop_newest_ts,
            stale_drop_by_msg_type,
            &metrics,
            state_store,
            scheduler,
        );
        total_drained += drain_result.drained_count;

        let ready_through_ts = shutdown_ready_through_candidate(
            scheduler.next_minute_to_emit(),
            shutdown_closed_minute,
        )
        .and_then(|latest_closed| {
            scheduler.next_minute_to_emit().and_then(|minute| {
                state_store.latest_contiguous_complete_canonical_minute_from(minute, latest_closed)
            })
        });

        if let Some(ready_through) = ready_through_ts {
            process_ready_minutes(
                ctx,
                metrics.clone(),
                dispatcher,
                state_store,
                scheduler,
                runtime_options,
                ready_through,
                DispatchMode::ShutdownFlush,
                true,
            )
            .await?;
        }

        let last_persisted_after = ts_from_millis(metrics.snapshot().last_persisted_ts_ms);
        let next_minute_after = scheduler.next_minute_to_emit();
        let dirty_pending_after = state_store.has_pending_dirty_recompute();
        let oi_ratio_patch_pending_after = state_store.has_pending_oi_ratio_patch();

        let processed_this_round = match (next_minute_before, next_minute_after) {
            (Some(before), Some(after)) if after > before => {
                (after - before).num_minutes().max(0) as usize
            }
            (Some(_), None) => 1,
            _ => 0,
        };
        total_processed += processed_this_round;

        let made_progress = drain_result.drained_count > 0
            || last_persisted_before != last_persisted_after
            || next_minute_before != next_minute_after
            || (dirty_pending_before && !dirty_pending_after);

        if !made_progress {
            info!(
                rounds = rounds,
                total_drained = total_drained,
                total_processed = total_processed,
                channels_closed = drain_result.all_channels_closed,
                next_minute = ?next_minute_after,
                ready_through_ts = ?ready_through_ts,
                shutdown_closed_minute = %shutdown_closed_minute,
                last_persisted_ts = ?last_persisted_after,
                dirty_recompute_pending = dirty_pending_after,
                oi_ratio_patch_pending = oi_ratio_patch_pending_after,
                elapsed_ms = started_at.elapsed().as_millis(),
                "shutdown drain + persist reached a stable frontier"
            );
            break;
        }
    }

    Ok(())
}

const INDICATOR_COVERAGE_ORDER: [(&str, &str); 27] = [
    ("i01", "price_volume_structure"),
    ("i02", "footprint"),
    ("i03", "divergence"),
    ("i04", "liquidation_density"),
    ("i05", "orderbook_depth"),
    ("i06", "absorption"),
    ("i07", "initiation"),
    ("i08", "bullish_absorption"),
    ("i09", "bullish_initiation"),
    ("i10", "bearish_absorption"),
    ("i11", "bearish_initiation"),
    ("i12", "buying_exhaustion"),
    ("i13", "selling_exhaustion"),
    ("i14", "cvd_pack"),
    ("i15", "whale_trades"),
    ("i16", "funding_rate"),
    ("i17", "vpin"),
    ("i18", "avwap"),
    ("i19", "kline_history"),
    ("i20", "tpo_market_profile"),
    ("i21", "rvwap_sigma_bands"),
    ("i22", "high_volume_pulse"),
    ("i23", "ema_trend_regime"),
    ("i24", "fvg"),
    ("i25", "open_interest"),
    ("i26", "long_short_ratios"),
    ("i27", "options_surface"),
];

fn indicator_coverage(snapshots: &[IndicatorSnapshotRow]) -> (Vec<String>, Vec<String>) {
    let snapshot_codes: HashSet<&str> = snapshots.iter().map(|s| s.indicator_code).collect();
    let mut computed = Vec::with_capacity(INDICATOR_COVERAGE_ORDER.len());
    let mut missing = Vec::new();

    for (prefix, code) in INDICATOR_COVERAGE_ORDER {
        let label = format!("{}:{}", prefix, code);
        if snapshot_codes.contains(code) {
            computed.push(label);
        } else {
            missing.push(label);
        }
    }

    (computed, missing)
}

fn format_stale_msg_type_distribution(counter: &HashMap<String, u64>) -> String {
    if counter.is_empty() {
        return "none".to_string();
    }
    let mut pairs = counter.iter().collect::<Vec<_>>();
    pairs.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

fn try_load_state_snapshot(path: &str, symbol: &str, max_age_hours: u64) -> Option<StateSnapshot> {
    use flate2::read::GzDecoder;
    use std::fs::File;

    const MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT: usize = 60;
    const MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT: usize = 1440;

    if path.is_empty() {
        return None;
    }
    let file = File::open(path).ok()?;
    let gz = GzDecoder::new(file);
    let snap: StateSnapshot = serde_json::from_reader(gz).ok()?;
    // Version check
    if snap.version != STATE_SNAPSHOT_VERSION {
        warn!(
            found = snap.version,
            expected = STATE_SNAPSHOT_VERSION,
            "State snapshot version mismatch, ignoring"
        );
        return None;
    }
    // Symbol check
    if snap.symbol != symbol {
        warn!(snap_symbol = %snap.symbol, "State snapshot symbol mismatch, ignoring");
        return None;
    }
    // Age check
    let age = Utc::now() - snap.saved_at;
    if age > chrono::Duration::hours(max_age_hours as i64) {
        warn!(
            age_hours = age.num_hours(),
            max_age_hours, "State snapshot too old, ignoring"
        );
        return None;
    }
    if !snapshot_has_required_history(&snap) {
        return None;
    }
    if !minute_history_is_strictly_contiguous(&snap.history_futures, snap.last_finalized_ts) {
        warn!("State snapshot futures history is not a strict contiguous minute series, ignoring");
        return None;
    }
    if let Some((start, end, len)) = find_long_null_price_run(
        &snap.history_futures,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        if snapshot_null_price_run_reaches_recent_tail(
            snap.last_finalized_ts,
            end,
            MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
        ) {
            warn!(
                run_start = %start,
                run_end = %end,
                run_len = len,
                max_allowed = MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
                min_recent_bars_after_run = MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
                "State snapshot futures history contains recent long null-price run, ignoring"
            );
            return None;
        }
        info!(
            run_start = %start,
            run_end = %end,
            run_len = len,
            min_recent_bars_after_run = MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
            "State snapshot futures history contains only historical long null-price run, accepting snapshot"
        );
    }
    if !snap.history_spot.is_empty()
        && !minute_history_is_strictly_contiguous(&snap.history_spot, snap.last_finalized_ts)
    {
        warn!("State snapshot spot history is not a strict contiguous minute series, ignoring");
        return None;
    }
    if let Some((start, end, len)) = find_long_null_price_run(
        &snap.history_spot,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        if snapshot_null_price_run_reaches_recent_tail(
            snap.last_finalized_ts,
            end,
            MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
        ) {
            warn!(
                run_start = %start,
                run_end = %end,
                run_len = len,
                max_allowed = MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
                min_recent_bars_after_run = MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
                "State snapshot spot history contains recent long null-price run, ignoring"
            );
            return None;
        }
        info!(
            run_start = %start,
            run_end = %end,
            run_len = len,
            min_recent_bars_after_run = MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
            "State snapshot spot history contains only historical long null-price run, accepting snapshot"
        );
    }
    Some(snap)
}

fn minute_history_is_strictly_contiguous(
    history: &[MinuteHistory],
    last_finalized_ts: DateTime<Utc>,
) -> bool {
    let Some(first) = history.first() else {
        return false;
    };
    let Some(last) = history.last() else {
        return false;
    };
    if last.ts_bucket != last_finalized_ts {
        return false;
    }
    if first.ts_bucket > last.ts_bucket {
        return false;
    }
    history.windows(2).all(|pair| {
        let prev = &pair[0];
        let next = &pair[1];
        next.ts_bucket > prev.ts_bucket
            && (next.ts_bucket - prev.ts_bucket) == ChronoDuration::minutes(1)
    })
}

fn find_long_null_price_run(
    history: &[MinuteHistory],
    min_run_len: usize,
) -> Option<(DateTime<Utc>, DateTime<Utc>, usize)> {
    if history.is_empty() || min_run_len == 0 {
        return None;
    }

    let mut run_start: Option<DateTime<Utc>> = None;
    let mut run_end: Option<DateTime<Utc>> = None;
    let mut run_len = 0usize;

    for row in history {
        if minute_history_has_any_price(row) {
            if run_len >= min_run_len {
                return Some((run_start?, run_end?, run_len));
            }
            run_start = None;
            run_end = None;
            run_len = 0;
            continue;
        }

        if run_start.is_none() {
            run_start = Some(row.ts_bucket);
        }
        run_end = Some(row.ts_bucket);
        run_len += 1;
    }

    if run_len >= min_run_len {
        Some((run_start?, run_end?, run_len))
    } else {
        None
    }
}

fn snapshot_null_price_run_reaches_recent_tail(
    last_finalized_ts: DateTime<Utc>,
    run_end: DateTime<Utc>,
    min_recent_bars_after_run: usize,
) -> bool {
    (last_finalized_ts - run_end).num_minutes() < min_recent_bars_after_run as i64
}

fn minute_history_has_any_price(row: &MinuteHistory) -> bool {
    row.open_price.is_some()
        || row.high_price.is_some()
        || row.low_price.is_some()
        || row.close_price.is_some()
        || row.last_price.is_some()
}

fn snapshot_has_required_history(snap: &StateSnapshot) -> bool {
    let required_start_ts = required_snapshot_history_start_ts(snap);
    if !snapshot_history_covers_required_window(
        &snap.history_futures,
        required_start_ts,
        snap.last_finalized_ts,
    ) {
        warn!(
            required_start_ts = %required_start_ts,
            history_start_ts = ?snap.history_futures.first().map(|row| row.ts_bucket),
            history_end_ts = ?snap.history_futures.last().map(|row| row.ts_bucket),
            history_len = snap.history_futures.len(),
            effective_history_floor_ts = ?snap.effective_history_floor_ts,
            history_limit_minutes = HISTORY_LIMIT_MINUTES,
            "State snapshot futures history does not provide contiguous required restart coverage, ignoring"
        );
        return false;
    }
    if !snap.history_spot.is_empty()
        && !snapshot_history_covers_required_window(
            &snap.history_spot,
            required_start_ts,
            snap.last_finalized_ts,
        )
    {
        warn!(
            required_start_ts = %required_start_ts,
            history_start_ts = ?snap.history_spot.first().map(|row| row.ts_bucket),
            history_end_ts = ?snap.history_spot.last().map(|row| row.ts_bucket),
            history_len = snap.history_spot.len(),
            effective_history_floor_ts = ?snap.effective_history_floor_ts,
            history_limit_minutes = HISTORY_LIMIT_MINUTES,
            "State snapshot spot history does not provide contiguous required restart coverage, ignoring"
        );
        return false;
    }
    true
}

fn required_snapshot_history_start_ts(snap: &StateSnapshot) -> DateTime<Utc> {
    let retention_floor =
        snap.last_finalized_ts - ChronoDuration::minutes((HISTORY_LIMIT_MINUTES as i64) - 1);
    let rolling_history_floor =
        snap.last_finalized_ts - ChronoDuration::minutes(MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES - 1);
    retention_floor.max(rolling_history_floor)
}

fn snapshot_history_reaches_required_start(
    history: &[MinuteHistory],
    required_start_ts: DateTime<Utc>,
) -> bool {
    history
        .first()
        .map(|row| row.ts_bucket <= required_start_ts)
        .unwrap_or(false)
}

fn snapshot_history_covers_required_window(
    history: &[MinuteHistory],
    required_start_ts: DateTime<Utc>,
    last_finalized_ts: DateTime<Utc>,
) -> bool {
    snapshot_history_reaches_required_start(history, required_start_ts)
        && minute_history_is_strictly_contiguous(history, last_finalized_ts)
}

async fn save_state_snapshot(snap: &StateSnapshot, path: &str) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use std::path::Path;

    if path.is_empty() {
        return Ok(());
    }
    // Ensure parent directory exists
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp_path = format!("{}.tmp", path);
    let file = File::create(&tmp_path)?;
    let gz = GzEncoder::new(file, Compression::fast());
    serde_json::to_writer(gz, snap)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub async fn load_kline_history_supplement(
    pool: &PgPool,
    symbol: &str,
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    bars_4h: usize,
    bars_1d: usize,
    bars_3d: usize,
    fill_1d_from_db: bool,
    ema_fill_from_db: bool,
    ema_htf_windows: &[String],
    ema_db_bars_4h: usize,
    ema_db_bars_1d: usize,
    ema_db_bars_3d: usize,
    fvg_fill_from_db: bool,
    fvg_windows: &[String],
    fvg_db_bars_4h: usize,
    fvg_db_bars_1d: usize,
    current_minute_close: DateTime<Utc>,
) -> KlineHistorySupplement {
    if !fill_1d_from_db && !ema_fill_from_db && !fvg_fill_from_db && bars_4h == 0 && bars_3d == 0 {
        return KlineHistorySupplement::default();
    }

    let fvg_needs_4h = fvg_fill_from_db && fvg_windows.iter().any(|code| code == "4h");
    let fvg_needs_1d = fvg_fill_from_db && fvg_windows.iter().any(|code| code == "1d");
    let ema_needs_3d = ema_fill_from_db && ema_htf_windows.iter().any(|code| code == "3d");

    let in_mem_futures_1d =
        build_interval_bar_records(history_futures, 1440, usize::MAX, current_minute_close);
    let in_mem_spot_1d =
        build_interval_bar_records(history_spot, 1440, usize::MAX, current_minute_close);
    let in_mem_futures_4h =
        build_interval_bar_records(history_futures, 240, usize::MAX, current_minute_close);
    let in_mem_spot_4h =
        build_interval_bar_records(history_spot, 240, usize::MAX, current_minute_close);

    let required_futures_1d = if fill_1d_from_db { bars_1d } else { 0 }
        .max(bars_3d.saturating_mul(3))
        .max(if ema_fill_from_db { ema_db_bars_1d } else { 0 })
        .max(if ema_needs_3d {
            ema_db_bars_3d.saturating_mul(3)
        } else {
            0
        })
        .max(if fvg_needs_1d { fvg_db_bars_1d } else { 0 });
    let required_spot_1d = if fill_1d_from_db { bars_1d } else { 0 }.max(bars_3d.saturating_mul(3));
    let required_futures_4h = bars_4h
        .max(if ema_fill_from_db { ema_db_bars_4h } else { 0 })
        .max(if fvg_needs_4h { fvg_db_bars_4h } else { 0 });
    let required_spot_4h = bars_4h;

    let in_mem_futures_count = in_mem_futures_1d.len();
    let in_mem_spot_count = in_mem_spot_1d.len();
    let in_mem_futures_4h_count = in_mem_futures_4h.len();
    let in_mem_spot_4h_count = in_mem_spot_4h.len();

    let futures_1d_missing = required_futures_1d.saturating_sub(in_mem_futures_count);
    let spot_1d_missing = required_spot_1d.saturating_sub(in_mem_spot_count);
    let futures_4h_missing = required_futures_4h.saturating_sub(in_mem_futures_4h_count);
    let spot_4h_missing = required_spot_4h.saturating_sub(in_mem_spot_4h_count);

    if futures_1d_missing == 0
        && spot_1d_missing == 0
        && futures_4h_missing == 0
        && spot_4h_missing == 0
    {
        return KlineHistorySupplement::default();
    }

    let futures_oldest_open = in_mem_futures_1d
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let spot_oldest_open = in_mem_spot_1d
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let futures_4h_oldest_open = in_mem_futures_4h
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);
    let spot_4h_oldest_open = in_mem_spot_4h
        .first()
        .map(|b| b.open_time)
        .unwrap_or(current_minute_close);

    let futures_1d_db = if futures_1d_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "futures",
            "1d",
            futures_oldest_open,
            futures_1d_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "futures",
                    interval_code = "1d",
                    limit = futures_1d_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let spot_1d_db = if spot_1d_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "spot",
            "1d",
            spot_oldest_open,
            spot_1d_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "spot",
                    interval_code = "1d",
                    limit = spot_1d_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let futures_4h_db = if futures_4h_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "futures",
            "4h",
            futures_4h_oldest_open,
            futures_4h_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "futures",
                    interval_code = "4h",
                    limit = futures_4h_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let spot_4h_db = if spot_4h_missing > 0 {
        match fetch_older_interval_bars(
            pool,
            symbol,
            "spot",
            "4h",
            spot_4h_oldest_open,
            spot_4h_missing,
        )
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    error = %err,
                    symbol = %symbol,
                    market = "spot",
                    interval_code = "4h",
                    limit = spot_4h_missing,
                    "load kline supplement from DB failed"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    KlineHistorySupplement {
        futures_4h_db,
        futures_1d_db,
        spot_4h_db,
        spot_1d_db,
        ..KlineHistorySupplement::default()
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct OptionsSurfaceFeatureRow {
    ts_bucket: DateTime<Utc>,
    front_expiry_ts: Option<DateTime<Utc>>,
    second_expiry_ts: Option<DateTime<Utc>>,
    atm_strike_front: Option<f64>,
    atm_iv_front: Option<f64>,
    atm_iv_second: Option<f64>,
    atm_iv_30d_proxy: Option<f64>,
    rr_25d_front: Option<f64>,
    rr_25d_second: Option<f64>,
    skew_state: String,
    term_structure_state: String,
}

fn required_options_surface_history_points() -> usize {
    OPTIONS_SURFACE_WINDOWS
        .iter()
        .map(|(_, _, samples)| samples.saturating_add(1))
        .max()
        .unwrap_or(0)
}

async fn load_options_surface_history_supplement(
    pool: &PgPool,
    symbol: &str,
    existing_points: &[OptionsSurfacePoint],
    current_minute_close: DateTime<Utc>,
) -> (Option<DateTime<Utc>>, Vec<OptionsSurfacePoint>) {
    let required_points = required_options_surface_history_points();
    if required_points == 0 || existing_points.len() >= required_points {
        return (
            existing_points.last().map(|point| point.ts_bucket),
            Vec::new(),
        );
    }

    let limit = if existing_points.is_empty() {
        required_points
    } else {
        required_points.saturating_sub(existing_points.len())
    };
    if limit == 0 {
        return (
            existing_points.last().map(|point| point.ts_bucket),
            Vec::new(),
        );
    }

    let rows_result: Result<Vec<OptionsSurfaceFeatureRow>> =
        if let Some(oldest_bucket) = existing_points.first().map(|point| point.ts_bucket) {
            sqlx::query_as(
                r#"
            SELECT
                ts_bucket,
                front_expiry_ts,
                second_expiry_ts,
                atm_strike_front,
                atm_iv_front,
                atm_iv_second,
                atm_iv_30d_proxy,
                rr_25d_front,
                rr_25d_second,
                skew_state,
                term_structure_state
            FROM feat.options_surface_feature
            WHERE symbol = $1
              AND bar_interval = interval '5 minutes'
              AND ts_bucket < $2
            ORDER BY ts_bucket DESC
            LIMIT $3
            "#,
            )
            .bind(symbol.to_uppercase())
            .bind(oldest_bucket)
            .bind(limit as i64)
            .fetch_all(pool)
            .await
            .context("query options surface supplement before oldest bundle bucket")
        } else {
            sqlx::query_as(
                r#"
            SELECT
                ts_bucket,
                front_expiry_ts,
                second_expiry_ts,
                atm_strike_front,
                atm_iv_front,
                atm_iv_second,
                atm_iv_30d_proxy,
                rr_25d_front,
                rr_25d_second,
                skew_state,
                term_structure_state
            FROM feat.options_surface_feature
            WHERE symbol = $1
              AND bar_interval = interval '5 minutes'
              AND ts_bucket <= $2
            ORDER BY ts_bucket DESC
            LIMIT $3
            "#,
            )
            .bind(symbol.to_uppercase())
            .bind(current_minute_close)
            .bind(limit as i64)
            .fetch_all(pool)
            .await
            .context("query latest options surface supplement")
        };

    let rows = match rows_result {
        Ok(rows) => rows,
        Err(err) => {
            warn!(
                error = %err,
                symbol = %symbol,
                current_points = existing_points.len(),
                required_points = required_points,
                "load options surface supplement from DB failed"
            );
            return (
                existing_points.last().map(|point| point.ts_bucket),
                Vec::new(),
            );
        }
    };

    let mut points = rows
        .into_iter()
        .map(|row| OptionsSurfacePoint {
            ts_bucket: row.ts_bucket,
            front_expiry_ts: row.front_expiry_ts,
            second_expiry_ts: row.second_expiry_ts,
            atm_strike_front: row.atm_strike_front,
            atm_iv_front: row.atm_iv_front,
            atm_iv_second: row.atm_iv_second,
            atm_iv_30d_proxy: row.atm_iv_30d_proxy,
            rr_25d_front: row.rr_25d_front,
            rr_25d_second: row.rr_25d_second,
            skew_state: row.skew_state,
            term_structure_state: row.term_structure_state,
        })
        .collect::<Vec<_>>();
    points.sort_by_key(|point| point.ts_bucket);

    (
        points
            .last()
            .map(|point| point.ts_bucket)
            .or_else(|| existing_points.last().map(|point| point.ts_bucket)),
        points,
    )
}

async fn fetch_older_interval_bars(
    pool: &PgPool,
    symbol: &str,
    market: &str,
    interval_code: &str,
    older_than_open_time: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<KlineHistoryBar>> {
    if limit == 0 {
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
            COALESCE(quote_volume, 0.0) AS volume_quote
        FROM md.kline_bar
        WHERE market = $1::cfg.market_type
          AND symbol = $2
          AND interval_code = $3
          AND is_closed = true
          AND open_time < $4
        ORDER BY open_time DESC
        LIMIT $5
        "#,
    )
    .bind(market)
    .bind(symbol.to_uppercase())
    .bind(interval_code)
    .bind(older_than_open_time)
    .bind(limit as i64)
    .fetch_all(pool)
    .await
    .context("query kline supplement")?;

    let expected_minutes = interval_code_to_minutes(interval_code);

    let mut out = rows
        .into_iter()
        .map(|row| {
            let open_time: DateTime<Utc> = row.get("open_time");
            let close_time: DateTime<Utc> = row.get("close_time");
            KlineHistoryBar {
                open_time,
                close_time,
                open: Some(row.get::<f64, _>("open_price")),
                high: Some(row.get::<f64, _>("high_price")),
                low: Some(row.get::<f64, _>("low_price")),
                close: Some(row.get::<f64, _>("close_price")),
                volume_base: row.get::<f64, _>("volume_base"),
                volume_quote: row.get::<f64, _>("volume_quote"),
                is_closed: close_time <= older_than_open_time,
                minutes_covered: expected_minutes,
                expected_minutes,
            }
        })
        .collect::<Vec<_>>();

    out.reverse();
    Ok(out)
}

fn interval_code_to_minutes(interval_code: &str) -> i64 {
    match interval_code {
        "1h" => 60,
        "4h" => 240,
        "1d" => 1440,
        _ => 1,
    }
}

fn minute_exclusive_upper_bound(ts: DateTime<Utc>) -> DateTime<Utc> {
    let floored = floor_minute(ts);
    if ts == floored {
        floored
    } else {
        floored + ChronoDuration::minutes(1)
    }
}

fn ts_to_millis(ts: Option<DateTime<Utc>>) -> Option<i64> {
    ts.map(|value| value.timestamp_millis())
}

fn ts_from_millis(ts_ms: i64) -> Option<DateTime<Utc>> {
    if ts_ms > 0 {
        Utc.timestamp_millis_opt(ts_ms).single()
    } else {
        None
    }
}

fn refresh_runtime_observability_metrics(
    metrics: &Arc<AppMetrics>,
    state_store: &StateStore,
    next_minute: Option<DateTime<Utc>>,
    ready_through_ts: Option<DateTime<Utc>>,
    trade_channel_len: usize,
    non_trade_channel_len: usize,
) -> CanonicalFrontierSnapshot {
    let frontier_snapshot = state_store.canonical_frontier_snapshot();
    metrics.set_runtime_window_state(
        ts_to_millis(next_minute),
        ts_to_millis(ready_through_ts),
        ts_to_millis(frontier_snapshot.latest_contiguous_complete_minute_ts),
    );
    metrics.set_dirty_recompute_bounds(
        ts_to_millis(frontier_snapshot.dirty_recompute_from_ts),
        ts_to_millis(frontier_snapshot.dirty_recompute_end_ts),
    );
    metrics.set_oi_ratio_patch_bounds(
        ts_to_millis(frontier_snapshot.oi_ratio_patch_from_ts),
        ts_to_millis(frontier_snapshot.oi_ratio_patch_end_ts),
    );
    metrics.set_channel_lengths(trade_channel_len, non_trade_channel_len);
    metrics.set_canonical_frontiers(
        ts_to_millis(frontier_snapshot.trade_futures_ts),
        ts_to_millis(frontier_snapshot.trade_spot_ts),
        ts_to_millis(frontier_snapshot.orderbook_futures_ts),
        ts_to_millis(frontier_snapshot.orderbook_spot_ts),
        ts_to_millis(frontier_snapshot.liq_futures_ts),
        ts_to_millis(frontier_snapshot.funding_futures_ts),
    );
    frontier_snapshot
}

fn format_missing_required_sources(presence: &CanonicalMinutePresence) -> String {
    let missing = presence.missing_required_sources();
    if missing.is_empty() {
        "none".to_string()
    } else {
        missing.join(",")
    }
}

fn maybe_warn_runtime_stall(
    stall_detector: &mut RuntimeStallDetector,
    metrics: &Arc<AppMetrics>,
    frontier_snapshot: &CanonicalFrontierSnapshot,
    next_minute: Option<DateTime<Utc>>,
    ready_through_ts: Option<DateTime<Utc>>,
    next_minute_presence: &CanonicalMinutePresence,
    trade_channel_len: usize,
    non_trade_channel_len: usize,
) {
    let metrics_snapshot = metrics.snapshot();
    let persisted_ts = ts_from_millis(metrics_snapshot.last_persisted_ts_ms);
    stall_detector.observe_persisted_ts(persisted_ts);
    if !stall_detector.idle_for_too_long() {
        return;
    }

    let queue_lag = trade_channel_len + non_trade_channel_len;
    let ready_ahead = ready_through_ts
        .map(|ready| {
            persisted_ts
                .map(|persisted| ready > persisted)
                .unwrap_or(true)
        })
        .unwrap_or(false);
    let complete_frontier_ahead = frontier_snapshot
        .latest_contiguous_complete_minute_ts
        .map(|ready| {
            persisted_ts
                .map(|persisted| ready > persisted)
                .unwrap_or(true)
        })
        .unwrap_or(false);
    let canonical_source_ahead = frontier_snapshot
        .last_canonical_minute_ts
        .map(|ready| {
            persisted_ts
                .map(|persisted| ready > persisted)
                .unwrap_or(true)
        })
        .unwrap_or(false);

    if !(ready_ahead || complete_frontier_ahead || canonical_source_ahead || queue_lag > 0) {
        return;
    }
    if !stall_detector.should_emit_warn() {
        return;
    }

    warn!(
        persisted_ts = ?persisted_ts,
        idle_secs = stall_detector.last_progress_advance_at.elapsed().as_secs(),
        next_minute = ?next_minute,
        ready_through_ts = ?ready_through_ts,
        queue_lag = queue_lag,
        trade_channel_len = trade_channel_len,
        non_trade_channel_len = non_trade_channel_len,
        latest_complete_canonical_ts = ?frontier_snapshot.latest_contiguous_complete_minute_ts,
        last_canonical_minute_ts = ?frontier_snapshot.last_canonical_minute_ts,
        trade_futures_ts = ?frontier_snapshot.trade_futures_ts,
        trade_spot_ts = ?frontier_snapshot.trade_spot_ts,
        orderbook_futures_ts = ?frontier_snapshot.orderbook_futures_ts,
        orderbook_spot_ts = ?frontier_snapshot.orderbook_spot_ts,
        liq_futures_ts = ?frontier_snapshot.liq_futures_ts,
        funding_futures_ts = ?frontier_snapshot.funding_futures_ts,
        dirty_recompute_from_ts = ?frontier_snapshot.dirty_recompute_from_ts,
        dirty_recompute_end_ts = ?frontier_snapshot.dirty_recompute_end_ts,
        oi_ratio_patch_from_ts = ?frontier_snapshot.oi_ratio_patch_from_ts,
        oi_ratio_patch_end_ts = ?frontier_snapshot.oi_ratio_patch_end_ts,
        last_finalized_minute_ts = ?frontier_snapshot.last_finalized_minute_ts,
        effective_history_floor_ts = ?frontier_snapshot.effective_history_floor_ts,
        next_minute_present = next_minute_presence.minute_present,
        next_minute_complete_under_current_policy = next_minute_presence.complete_under_current_policy(),
        next_minute_trade_futures = next_minute_presence.trade_futures,
        next_minute_orderbook_futures = next_minute_presence.orderbook_futures,
        next_minute_funding_futures = next_minute_presence.funding_futures,
        next_minute_trade_spot = next_minute_presence.trade_spot,
        next_minute_orderbook_spot = next_minute_presence.orderbook_spot,
        next_minute_liq_futures = next_minute_presence.liq_futures,
        next_minute_missing_required_sources = %format_missing_required_sources(next_minute_presence),
        "indicator runtime has not advanced persisted frontier; inspect readiness, queues, and outbox timings"
    );
}

fn handle_ingest_event(
    event: EngineEvent,
    startup_replay_cutoff_bucket: Option<DateTime<Utc>>,
    startup_cutover_completed: &mut bool,
    consume_mode_live: bool,
    live_drop_stale_enabled: bool,
    stale_limit_secs: i64,
    stale_drop_count: &mut u64,
    stale_drop_max_lag_secs: &mut i64,
    stale_drop_max_publish_delay_secs: &mut i64,
    stale_drop_max_transport_lag_secs: &mut i64,
    stale_drop_oldest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_newest_ts: &mut Option<DateTime<Utc>>,
    stale_drop_by_msg_type: &mut HashMap<String, u64>,
    metrics: &Arc<AppMetrics>,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
) {
    let event_bucket_ts = logical_event_bucket_ts(&event);
    if let Some(cutoff_bucket_ts) = startup_replay_cutoff_bucket {
        if event_bucket_ts < cutoff_bucket_ts {
            return;
        }
        if !*startup_cutover_completed {
            if event_bucket_ts > cutoff_bucket_ts {
                warn!(
                    cutoff_bucket_ts = %cutoff_bucket_ts,
                    first_live_bucket_ts = %event_bucket_ts,
                    "startup replay cutover detected continuity gap; preserving warm state and waiting for live gap repair to heal missing minutes"
                );
            }
            info!(
                cutoff_bucket_ts = %cutoff_bucket_ts,
                first_live_event_ts = %event.event_ts,
                first_live_bucket_ts = %event_bucket_ts,
                "startup replay cutover completed; switch to pure live processing"
            );
            *startup_cutover_completed = true;
        }
    }

    if live_drop_stale_enabled && consume_mode_live && *startup_cutover_completed {
        let lag_secs = (Utc::now() - event.event_ts).num_seconds();
        if lag_secs > stale_limit_secs {
            let publish_delay_secs = (event.published_at - event.event_ts).num_seconds().max(0);
            let transport_lag_secs = (Utc::now() - event.published_at).num_seconds().max(0);

            *stale_drop_count += 1;
            *stale_drop_max_lag_secs = (*stale_drop_max_lag_secs).max(lag_secs);
            *stale_drop_max_publish_delay_secs =
                (*stale_drop_max_publish_delay_secs).max(publish_delay_secs);
            *stale_drop_max_transport_lag_secs =
                (*stale_drop_max_transport_lag_secs).max(transport_lag_secs);
            *stale_drop_by_msg_type
                .entry(event.msg_type.clone())
                .or_insert(0) += 1;

            let next_oldest = stale_drop_oldest_ts
                .as_ref()
                .cloned()
                .map_or(event.event_ts, |ts| ts.min(event.event_ts));
            let next_newest = stale_drop_newest_ts
                .as_ref()
                .cloned()
                .map_or(event.event_ts, |ts| ts.max(event.event_ts));
            *stale_drop_oldest_ts = Some(next_oldest);
            *stale_drop_newest_ts = Some(next_newest);
            return;
        }
    }

    metrics.inc_processed(event.event_ts.timestamp_millis());
    if matches!(
        &event.data,
        MdData::AggTrade1m(_)
            | MdData::AggOrderbook1m(_)
            | MdData::AggLiq1m(_)
            | MdData::AggFundingMark1m(_)
    ) {
        scheduler.prime_start_from(event_bucket_ts);
    }
    // EventBuffer was a no-op wrapper (push immediately followed by pop with no watermark
    // gating). Ingest directly to avoid the unnecessary allocation round-trip.
    state_store.ingest(event);
}

async fn ingest_canonical_range_from_db(
    pool: &PgPool,
    symbol: &str,
    metrics: &Arc<AppMetrics>,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    reason: &'static str,
) -> Result<CanonicalRepairStats> {
    if from_ts >= to_ts_exclusive {
        return Ok(CanonicalRepairStats::default());
    }

    let mut stats = CanonicalRepairStats::default();
    let mut window_from_ts = from_ts;
    while window_from_ts < to_ts_exclusive {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts_exclusive);
        let rows = fetch_backfill_window(
            pool,
            window_from_ts,
            window_to_ts,
            symbol,
            STARTUP_BACKFILL_MARKET,
        )
        .await
        .with_context(|| {
            format!(
                "{reason} fetch canonical replay rows from_ts={window_from_ts} to_ts_exclusive={window_to_ts}"
            )
        })?;
        stats.fetched_rows += rows.len();

        for row in rows {
            match replay_row_to_engine_event(row) {
                Ok(event) => {
                    let bucket = logical_event_bucket_ts(&event);
                    metrics.inc_processed(event.event_ts.timestamp_millis());
                    if matches!(
                        &event.data,
                        MdData::AggTrade1m(_)
                            | MdData::AggOrderbook1m(_)
                            | MdData::AggLiq1m(_)
                            | MdData::AggFundingMark1m(_)
                    ) {
                        scheduler.prime_start_from(bucket);
                    }
                    stats.record_event(&event);
                    let outcome = state_store.ingest(event);
                    stats.ingested_rows += 1;
                    stats.record_material_change(bucket, outcome);
                }
                Err(err) => {
                    metrics.inc_decode_error();
                    warn!(
                        error = %err,
                        reason = reason,
                        from_ts = %window_from_ts,
                        to_ts_exclusive = %window_to_ts,
                        "decode live canonical repair row failed"
                    );
                }
            }
        }

        window_from_ts = window_to_ts;
    }

    Ok(stats)
}

fn logical_event_bucket_ts(event: &EngineEvent) -> DateTime<Utc> {
    match &event.data {
        MdData::AggTrade1m(v) => v.ts_bucket,
        MdData::AggOrderbook1m(v) => v.ts_bucket,
        MdData::AggLiq1m(v) => v.ts_bucket,
        MdData::AggFundingMark1m(v) => v.ts_bucket,
        MdData::OpenInterestHist5m(v) => v.ts_bucket,
        MdData::LongShortRatio5m(v) => v.ts_bucket,
        MdData::OptionMarkGreeks5m(v) => v.ts_bucket,
        MdData::OpenInterestCurrent(v) => floor_minute(v.ts_effective),
        _ => floor_minute(event.event_ts),
    }
}

fn live_tail_reconcile_start_ts(
    last_finalized_minute: DateTime<Utc>,
    effective_history_floor_ts: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let lookback_start = last_finalized_minute
        - ChronoDuration::minutes(LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES);
    effective_history_floor_ts
        .map(|floor| lookback_start.max(floor))
        .unwrap_or(lookback_start)
}

fn shutdown_ready_through_candidate(
    next_minute: Option<DateTime<Utc>>,
    shutdown_closed_minute: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    next_minute
        .filter(|minute| *minute <= shutdown_closed_minute)
        .map(|_| shutdown_closed_minute)
}

async fn process_ready_minutes(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    runtime_options: &IndicatorRuntimeOptions,
    ready_through_ts: DateTime<Utc>,
    dispatch_mode: DispatchMode,
    allow_oi_ratio_patches: bool,
) -> Result<()> {
    let batch_started_at = Instant::now();
    let batch_start_minute = scheduler.next_minute_to_emit();
    let planned_ready_minutes = batch_start_minute
        .map(|start| (ready_through_ts - start).num_minutes().max(0) as usize + 1)
        .unwrap_or(0);
    let mut windows_processed = 0usize;
    let mut first_bucket: Option<DateTime<Utc>> = None;
    let mut last_bucket: Option<DateTime<Utc>> = None;
    let mut last_computed: Vec<String> = Vec::new();
    let mut missing_union: BTreeSet<String> = BTreeSet::new();
    let had_dirty_pending_at_start = state_store.has_pending_dirty_recompute();

    // Accuracy first: dirty recompute truncates the finalized suffix before rebuilding it.
    // If we publish new live minutes while that suffix is only partially rebuilt, any
    // indicator that reads rolling minute history (for example orderbook_depth/liquidation
    // windows or recent_7d event coverage) can observe a temporary tail gap and emit
    // false low-coverage snapshots. Drain dirty recompute to completion before releasing
    // additional ready minutes.
    let mut dirty_windows_processed = 0usize;
    loop {
        if dirty_windows_processed >= DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK {
            break;
        }
        let remaining_budget =
            DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK.saturating_sub(dirty_windows_processed);
        if let Some((dirty_from, dirty_to)) = state_store
            .pending_dirty_recompute_batch_range(DIRTY_RECOMPUTE_BATCH_SIZE.min(remaining_budget))
        {
            hydrate_futures_orderbook_heatmaps_for_range(
                &ctx.db_pool,
                &ctx.config.indicator.symbol,
                state_store,
                dirty_from,
                dirty_to + ChronoDuration::minutes(1),
                "live dirty recompute",
            )
            .await?;
        }
        let dirty_batch = state_store
            .recompute_dirty_finalized_minutes(DIRTY_RECOMPUTE_BATCH_SIZE.min(remaining_budget));
        if dirty_batch.is_empty() {
            break;
        }
        dirty_windows_processed += dirty_batch.len();
        for window in dirty_batch {
            let minute = window.ts_bucket;
            let snapshots =
                process_window_bundle(ctx, dispatcher, runtime_options, window, dispatch_mode)
                    .await?;
            metrics.inc_exported_window();
            metrics.set_last_persisted_ts(Some(minute.timestamp_millis()));
            let (computed, missing) = indicator_coverage(&snapshots);
            windows_processed += 1;
            if first_bucket.is_none() {
                first_bucket = Some(minute);
            }
            last_bucket = Some(minute);
            last_computed = computed;
            for item in missing {
                missing_union.insert(item);
            }
            if ctx.config.indicator.enable_file_export {
                if let Err(err) = export_snapshots(
                    &ctx.config.indicator.export_dir,
                    minute,
                    &ctx.config.indicator.symbol,
                    &snapshots,
                )
                .await
                {
                    warn!(error = %err, "export indicator snapshot file failed");
                }
            }
        }
    }

    if state_store.has_pending_dirty_recompute() {
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        if dirty_windows_processed > 0 || elapsed_ms >= PROCESS_READY_MINUTES_WARN_MS {
            warn!(
                batch_start_minute = ?batch_start_minute,
                ready_through_ts = %ready_through_ts,
                dirty_windows_processed = dirty_windows_processed,
                dirty_budget_per_tick = DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK,
                planned_ready_minutes = planned_ready_minutes,
                elapsed_ms = elapsed_ms,
                "process_ready_minutes yielded early with dirty recompute still pending"
            );
        }
        return Ok(());
    }

    if let Some(first_ready_minute) = scheduler.next_minute_to_emit() {
        hydrate_futures_orderbook_heatmaps_for_range(
            &ctx.db_pool,
            &ctx.config.indicator.symbol,
            state_store,
            first_ready_minute,
            ready_through_ts + ChronoDuration::minutes(1),
            "live ready minute materialization",
        )
        .await?;
    }

    for minute in scheduler.ready_minutes_through(ready_through_ts) {
        let window = state_store.finalize_minute(minute);
        match process_window_bundle(ctx, dispatcher, runtime_options, window, dispatch_mode).await {
            Ok(snapshots) => {
                metrics.inc_exported_window();
                metrics.set_last_persisted_ts(Some(minute.timestamp_millis()));
                let (computed, missing) = indicator_coverage(&snapshots);
                windows_processed += 1;
                if first_bucket.is_none() {
                    first_bucket = Some(minute);
                }
                last_bucket = Some(minute);
                last_computed = computed;
                for item in missing {
                    missing_union.insert(item);
                }
                if ctx.config.indicator.enable_file_export {
                    if let Err(err) = export_snapshots(
                        &ctx.config.indicator.export_dir,
                        minute,
                        &ctx.config.indicator.symbol,
                        &snapshots,
                    )
                    .await
                    {
                        warn!(error = %err, "export indicator snapshot file failed");
                    }
                }
            }
            Err(err) => {
                metrics.inc_db_error();
                error!(
                    error = %err,
                    error_chain = %format!("{err:#}"),
                    minute = %minute,
                    "process indicator window failed"
                );
                return Err(err)
                    .with_context(|| format!("process indicator window minute={} failed", minute));
            }
        }
    }

    let oi_ratio_patch_windows_processed = if allow_oi_ratio_patches {
        process_pending_oi_ratio_patches(dispatcher, state_store, runtime_options, &metrics).await?
    } else {
        0
    };

    if let Some(last_bucket) = last_bucket {
        let first_bucket = first_bucket.unwrap_or(last_bucket);
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        metrics.set_live_ready_to_bundle_ms(elapsed_ms);
        if missing_union.is_empty() {
            info!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = windows_processed,
                planned_ready_minutes = planned_ready_minutes,
                dirty_windows_processed = dirty_windows_processed,
                oi_ratio_patch_windows_processed = oi_ratio_patch_windows_processed,
                dirty_pending_at_start = had_dirty_pending_at_start,
                ready_through_ts = %ready_through_ts,
                elapsed_ms = elapsed_ms,
                computed_count = last_computed.len(),
                missing_count = 0,
                computed_indicators = %last_computed.join(","),
                missing_indicators = "none",
                "indicator coverage"
            );
        } else {
            let missing_indicators = missing_union.iter().cloned().collect::<Vec<_>>().join(",");
            // Warn but do not crash: missing indicators are expected during warmup (e.g. VPIN
            // needs 120 minutes of history) and after transient data gaps. A fatal return here
            // would restart the whole engine on every cold start, causing a restart loop.
            warn!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = windows_processed,
                planned_ready_minutes = planned_ready_minutes,
                dirty_windows_processed = dirty_windows_processed,
                oi_ratio_patch_windows_processed = oi_ratio_patch_windows_processed,
                dirty_pending_at_start = had_dirty_pending_at_start,
                ready_through_ts = %ready_through_ts,
                elapsed_ms = elapsed_ms,
                computed_count = last_computed.len(),
                missing_count = missing_union.len(),
                computed_indicators = %last_computed.join(","),
                missing_indicators = %missing_indicators,
                "indicator coverage has missing indicators (warmup or data gap)"
            );
        }
    }

    Ok(())
}

async fn process_pending_oi_ratio_patches(
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    runtime_options: &IndicatorRuntimeOptions,
    metrics: &Arc<AppMetrics>,
) -> Result<usize> {
    let mut processed = 0usize;
    while processed < OI_RATIO_PATCH_WINDOW_BUDGET_PER_TICK {
        let batch_started_at = Instant::now();
        let batch_size = OI_RATIO_PATCH_BATCH_SIZE
            .min(OI_RATIO_PATCH_WINDOW_BUDGET_PER_TICK.saturating_sub(processed));
        let patch_range = state_store.pending_oi_ratio_patch_batch_range(batch_size);
        if let Some((patch_from, patch_to)) = patch_range {
            debug!(
                patch_from = %patch_from,
                patch_to = %patch_to,
                "processing pending oi_ratio patch batch"
            );
        }
        let patch_batch = state_store.take_oi_ratio_patch_batch(batch_size);
        if patch_batch.is_empty() {
            break;
        }
        let patch_batch_len = patch_batch.len();
        processed += patch_batch_len;
        for minute in patch_batch {
            let bundle = state_store.build_oi_ratio_patch_bundle_for_minute(minute);
            let ictx = Arc::new(IndicatorContext::from_bundle(
                bundle,
                runtime_options,
                KlineHistorySupplement::default(),
            ));
            dispatcher.process_oi_ratio_patch_window(ictx).await?;
        }
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        metrics.record_oi_ratio_patch_batch(patch_batch_len, elapsed_ms);
        metrics.record_oi_ratio_patch_republish(patch_batch_len);
        if let Some((patch_from, patch_to)) = patch_range {
            debug!(
                reason = "oi_ratio_patch",
                from_ts = %patch_from,
                to_ts = %patch_to,
                windows_processed = patch_batch_len,
                elapsed_ms = elapsed_ms,
                batch_size = batch_size,
                live_windows_skipped_due_to_budget = 0,
                "oi_ratio patch batch processed"
            );
        }
    }
    Ok(processed)
}

fn spawn_oi_ratio_patch_task(
    dispatcher: Arc<Dispatcher>,
    state_store: &mut StateStore,
    runtime_options: &IndicatorRuntimeOptions,
    metrics: &Arc<AppMetrics>,
) -> Result<Option<OiRatioPatchTask>> {
    let patch_batch = state_store.take_oi_ratio_patch_batch(OI_RATIO_PATCH_BATCH_SIZE);
    if patch_batch.is_empty() {
        return Ok(None);
    }

    let patch_from = patch_batch.first().copied();
    let patch_to = patch_batch.last().copied();
    let patch_batch_len = patch_batch.len();
    let contexts = patch_batch
        .iter()
        .map(|minute| {
            let bundle = state_store.build_oi_ratio_patch_bundle_for_minute(*minute);
            Ok(Arc::new(IndicatorContext::from_bundle(
                bundle,
                runtime_options,
                KlineHistorySupplement::default(),
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    let metrics = metrics.clone();
    let started_at = Instant::now();
    let handle = tokio::spawn(async move {
        let batch_started_at = Instant::now();
        for ictx in contexts {
            dispatcher.process_oi_ratio_patch_window(ictx).await?;
        }
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        metrics.record_oi_ratio_patch_batch(patch_batch_len, elapsed_ms);
        metrics.record_oi_ratio_patch_republish(patch_batch_len);
        debug!(
            reason = "oi_ratio_patch_async",
            from_ts = ?patch_from,
            to_ts = ?patch_to,
            windows_processed = patch_batch_len,
            elapsed_ms = elapsed_ms,
            batch_size = OI_RATIO_PATCH_BATCH_SIZE,
            "background oi_ratio patch batch processed"
        );
        Ok(patch_batch_len)
    });

    Ok(Some(OiRatioPatchTask {
        minutes: patch_batch,
        started_at,
        handle,
    }))
}

async fn settle_finished_oi_ratio_patch_task(
    task_slot: &mut Option<OiRatioPatchTask>,
    state_store: &Arc<Mutex<StateStore>>,
    metrics: &Arc<AppMetrics>,
) {
    let finished = task_slot
        .as_ref()
        .map(|task| task.handle.is_finished())
        .unwrap_or(false);
    if !finished {
        return;
    }

    let task = task_slot
        .take()
        .expect("finished oi_ratio patch task must exist");
    let patch_from = task.minutes.first().copied();
    let patch_to = task.minutes.last().copied();
    let queued_elapsed_ms = task.started_at.elapsed().as_millis();
    match task.handle.await {
        Ok(Ok(processed)) => {
            debug!(
                reason = "oi_ratio_patch_async",
                from_ts = ?patch_from,
                to_ts = ?patch_to,
                windows_processed = processed,
                queued_elapsed_ms = queued_elapsed_ms,
                "background oi_ratio patch task completed"
            );
        }
        Ok(Err(err)) => {
            let mut state_store = state_store.lock().await;
            state_store.requeue_oi_ratio_patch_batch(&task.minutes);
            metrics.inc_db_error();
            warn!(
                error = %err,
                from_ts = ?patch_from,
                to_ts = ?patch_to,
                windows_requeued = task.minutes.len(),
                "background oi_ratio patch task failed; batch requeued"
            );
        }
        Err(err) => {
            let mut state_store = state_store.lock().await;
            state_store.requeue_oi_ratio_patch_batch(&task.minutes);
            metrics.inc_db_error();
            warn!(
                error = %err,
                from_ts = ?patch_from,
                to_ts = ?patch_to,
                windows_requeued = task.minutes.len(),
                "background oi_ratio patch task join failed; batch requeued"
            );
        }
    }
}

async fn abort_oi_ratio_patch_task(
    task_slot: &mut Option<OiRatioPatchTask>,
    state_store: &mut StateStore,
) {
    let Some(task) = task_slot.take() else {
        return;
    };
    let patch_from = task.minutes.first().copied();
    let patch_to = task.minutes.last().copied();
    task.handle.abort();
    let _ = task.handle.await;
    state_store.requeue_oi_ratio_patch_batch(&task.minutes);
    info!(
        from_ts = ?patch_from,
        to_ts = ?patch_to,
        windows_requeued = task.minutes.len(),
        "aborted in-flight oi_ratio patch task during shutdown and requeued batch"
    );
}

fn available_ready_job_slots(queue_capacity: usize, ready_job_pending: &Arc<AtomicUsize>) -> usize {
    queue_capacity.saturating_sub(
        ready_job_pending
            .load(Ordering::Acquire)
            .min(queue_capacity),
    )
}

fn available_live_pipeline_slots(
    queue_capacity: usize,
    ready_job_pending: &Arc<AtomicUsize>,
    prepare_minute_pending: &Arc<AtomicUsize>,
) -> usize {
    queue_capacity.saturating_sub(
        (ready_job_pending.load(Ordering::Acquire)
            + prepare_minute_pending.load(Ordering::Acquire))
        .min(queue_capacity),
    )
}

fn try_enqueue_ready_minute_job(
    ready_job_tx: &mpsc::Sender<ReadyMinuteJob>,
    ready_job_pending: &Arc<AtomicUsize>,
    job: ReadyMinuteJob,
) -> Result<bool> {
    ready_job_pending.fetch_add(1, Ordering::AcqRel);
    match ready_job_tx.try_send(job) {
        Ok(()) => Ok(true),
        Err(tokio::sync::mpsc::error::TrySendError::Full(_job)) => {
            ready_job_pending.fetch_sub(1, Ordering::AcqRel);
            Ok(false)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_job)) => {
            ready_job_pending.fetch_sub(1, Ordering::AcqRel);
            anyhow::bail!("live materialize worker queue is closed");
        }
    }
}

fn try_enqueue_prepare_minute_task(
    prepare_task_tx: &mpsc::Sender<PrepareMinuteTask>,
    prepare_minute_pending: &Arc<AtomicUsize>,
    task: PrepareMinuteTask,
) -> Result<bool> {
    let minute_count = task.minutes.len();
    prepare_minute_pending.fetch_add(minute_count, Ordering::AcqRel);
    match prepare_task_tx.try_send(task) {
        Ok(()) => Ok(true),
        Err(tokio::sync::mpsc::error::TrySendError::Full(task)) => {
            prepare_minute_pending.fetch_sub(task.minutes.len(), Ordering::AcqRel);
            Ok(false)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(task)) => {
            prepare_minute_pending.fetch_sub(task.minutes.len(), Ordering::AcqRel);
            anyhow::bail!("live prepare worker queue is closed");
        }
    }
}

fn should_enqueue_dirty_ready_jobs(
    live_windows_enqueued: usize,
    live_queue_pending: usize,
    next_live_minute: Option<DateTime<Utc>>,
    ready_through_ts: DateTime<Utc>,
    dirty_pending: bool,
) -> bool {
    dirty_pending
        && live_windows_enqueued == 0
        && live_queue_pending == 0
        && next_live_minute
            .map(|minute| minute > ready_through_ts)
            .unwrap_or(true)
}

fn log_materialized_coverage(
    source: ReadyJobSource,
    ts_bucket: DateTime<Utc>,
    snapshots: &[IndicatorSnapshotRow],
    enqueued_at: Instant,
) {
    let (computed, missing) = indicator_coverage(snapshots);
    let source_label = match source {
        ReadyJobSource::Live => "live",
        ReadyJobSource::DirtyRecompute => "dirty_recompute",
    };
    let total_ms = enqueued_at.elapsed().as_millis();
    if missing.is_empty() {
        info!(
            ts_bucket = %ts_bucket,
            source = source_label,
            computed_count = computed.len(),
            missing_count = 0,
            computed_indicators = %computed.join(","),
            missing_indicators = "none",
            total_ms = total_ms,
            "indicator coverage"
        );
    } else {
        let missing_indicators = missing.into_iter().collect::<Vec<_>>().join(",");
        warn!(
            ts_bucket = %ts_bucket,
            source = source_label,
            computed_count = computed.len(),
            missing_count = missing_indicators.split(',').count(),
            computed_indicators = %computed.join(","),
            missing_indicators = %missing_indicators,
            total_ms = total_ms,
            "indicator coverage has missing indicators (warmup or data gap)"
        );
    }
}

async fn run_live_materialize_loop(
    ctx: Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: Arc<Dispatcher>,
    runtime_options: IndicatorRuntimeOptions,
    worker_kind: MaterializeWorkerKind,
    mut ready_job_rx: mpsc::Receiver<ReadyMinuteJob>,
    ready_job_pending: Arc<AtomicUsize>,
) -> Result<()> {
    while let Some(job) = ready_job_rx.recv().await {
        let ts_bucket = job.ts_bucket;
        let source = job.source;
        let enqueued_at = job.enqueued_at;
        let result = process_window_bundle(
            &ctx,
            dispatcher.as_ref(),
            &runtime_options,
            job.bundle,
            job.mode,
        )
        .await;
        ready_job_pending.fetch_sub(1, Ordering::AcqRel);

        match result {
            Ok(snapshots) => {
                metrics.inc_exported_window();
                metrics.set_last_persisted_ts(Some(ts_bucket.timestamp_millis()));
                if matches!(worker_kind, MaterializeWorkerKind::Live) {
                    metrics.set_live_ready_to_bundle_ms(enqueued_at.elapsed().as_millis());
                }
                log_materialized_coverage(source, ts_bucket, &snapshots, enqueued_at);
            }
            Err(err) => {
                metrics.inc_db_error();
                return Err(err).with_context(|| {
                    format!(
                        "{} materialize worker failed for minute={} source={:?}",
                        worker_kind.label(),
                        ts_bucket,
                        source
                    )
                });
            }
        }
    }

    Ok(())
}

async fn run_live_prepare_loop(
    ctx: Arc<AppContext>,
    state_store: Arc<Mutex<StateStore>>,
    mut prepare_task_rx: mpsc::Receiver<PrepareMinuteTask>,
    prepare_minute_pending: Arc<AtomicUsize>,
    ready_job_tx: mpsc::Sender<ReadyMinuteJob>,
    ready_job_pending: Arc<AtomicUsize>,
) -> Result<()> {
    while let Some(task) = prepare_task_rx.recv().await {
        let first_minute = task.minutes.first().copied();
        let last_minute = task.minutes.last().copied();
        let fetched_rows = if let (Some(first_minute), Some(last_minute)) =
            (first_minute, last_minute)
        {
            let needs_hydration = {
                let state_store = state_store.lock().await;
                state_store
                    .has_unhydrated_futures_orderbook_heatmap_in_range(first_minute, last_minute)
            };
            if needs_hydration {
                fetch_futures_orderbook_heatmap_rows_for_range(
                    &ctx.db_pool,
                    &ctx.config.indicator.symbol,
                    first_minute,
                    last_minute + ChronoDuration::minutes(1),
                    "live ready minute materialization",
                )
                .await?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let mut prepared_jobs = Vec::with_capacity(task.minutes.len());
        {
            let mut state_store = state_store.lock().await;
            if let (Some(first_minute), Some(last_minute)) = (first_minute, last_minute) {
                let last_minute_exclusive = last_minute + ChronoDuration::minutes(1);
                if state_store
                    .has_unhydrated_futures_orderbook_heatmap_in_range(first_minute, last_minute)
                {
                    let ingested_rows = ingest_futures_orderbook_heatmap_rows(
                        &mut state_store,
                        fetched_rows,
                        first_minute,
                        last_minute_exclusive,
                        "live ready minute materialization",
                    )?;
                    if state_store.has_unhydrated_futures_orderbook_heatmap_in_range(
                        first_minute,
                        last_minute,
                    ) {
                        anyhow::bail!(
                            "live ready minute materialization left unhydrated futures orderbook heatmap in range {first_minute}..={last_minute}"
                        );
                    }
                    if ingested_rows > 0 {
                        info!(
                            reason = "live ready minute materialization",
                            from_ts = %first_minute,
                            to_ts_exclusive = %last_minute_exclusive,
                            fetched_rows = ingested_rows,
                            "hydrated futures orderbook heatmaps for live prepare range"
                        );
                    }
                }
            }

            for minute in &task.minutes {
                let bundle = state_store.finalize_minute_for_live_job(*minute);
                prepared_jobs.push(ReadyMinuteJob {
                    ts_bucket: *minute,
                    mode: task.mode,
                    source: ReadyJobSource::Live,
                    enqueued_at: task.enqueued_at,
                    bundle,
                });
            }
        }

        for job in prepared_jobs {
            let minute = job.ts_bucket;
            let enqueued = try_enqueue_ready_minute_job(&ready_job_tx, &ready_job_pending, job)?;
            prepare_minute_pending.fetch_sub(1, Ordering::AcqRel);
            if !enqueued {
                anyhow::bail!("live materialize worker queue was unexpectedly full during prepare handoff for minute={minute}");
            }
        }
    }

    Ok(())
}

async fn poll_prepare_handle(handle_slot: &mut Option<JoinHandle<Result<()>>>) -> Result<()> {
    if !handle_slot
        .as_ref()
        .map(|handle| handle.is_finished())
        .unwrap_or(false)
    {
        return Ok(());
    }

    let handle = handle_slot
        .take()
        .expect("finished prepare handle must exist");
    match handle.await {
        Ok(Ok(())) => anyhow::bail!("live prepare worker exited unexpectedly"),
        Ok(Err(err)) => Err(err).context("live prepare worker failed"),
        Err(err) => Err(err).context("live prepare worker join failed"),
    }
}

async fn drain_prepare_handle(handle_slot: &mut Option<JoinHandle<Result<()>>>) {
    let Some(handle) = handle_slot.take() else {
        return;
    };
    match handle.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(error = %err, "prepare worker failed during shutdown drain");
        }
        Err(err) => {
            warn!(error = %err, "prepare worker join failed during shutdown drain");
        }
    }
}

async fn poll_materialize_handle(
    handle_slot: &mut Option<JoinHandle<Result<()>>>,
    worker_kind: MaterializeWorkerKind,
) -> Result<()> {
    if !handle_slot
        .as_ref()
        .map(|handle| handle.is_finished())
        .unwrap_or(false)
    {
        return Ok(());
    }

    let handle = handle_slot
        .take()
        .expect("finished materialize handle must exist");
    match handle.await {
        Ok(Ok(())) => anyhow::bail!(
            "{} materialize worker exited unexpectedly",
            worker_kind.label()
        ),
        Ok(Err(err)) => {
            Err(err).with_context(|| format!("{} materialize worker failed", worker_kind.label()))
        }
        Err(err) => Err(err)
            .with_context(|| format!("{} materialize worker join failed", worker_kind.label())),
    }
}

async fn drain_materialize_handle(
    handle_slot: &mut Option<JoinHandle<Result<()>>>,
    worker_kind: MaterializeWorkerKind,
) {
    let Some(handle) = handle_slot.take() else {
        return;
    };
    match handle.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(
                error = %err,
                worker = worker_kind.label(),
                "materialize worker failed during shutdown drain"
            );
        }
        Err(err) => {
            warn!(
                error = %err,
                worker = worker_kind.label(),
                "materialize worker join failed during shutdown drain"
            );
        }
    }
}

async fn enqueue_live_ready_jobs(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    state_store: &Arc<Mutex<StateStore>>,
    scheduler: &mut WindowScheduler,
    live_prepare_task_tx: &mpsc::Sender<PrepareMinuteTask>,
    live_prepare_minute_pending: &Arc<AtomicUsize>,
    live_ready_job_pending: &Arc<AtomicUsize>,
    dirty_ready_job_tx: &mpsc::Sender<ReadyMinuteJob>,
    dirty_ready_job_pending: &Arc<AtomicUsize>,
    ready_through_ts: DateTime<Utc>,
    dispatch_mode: DispatchMode,
) -> Result<()> {
    let batch_started_at = Instant::now();
    let batch_start_minute = scheduler.next_minute_to_emit();
    let planned_ready_minutes = batch_start_minute
        .map(|start| (ready_through_ts - start).num_minutes().max(0) as usize + 1)
        .unwrap_or(0);
    let mut live_windows_enqueued = 0usize;
    let mut dirty_windows_enqueued = 0usize;
    let mut first_bucket: Option<DateTime<Utc>> = None;
    let mut last_bucket: Option<DateTime<Utc>> = None;

    let available_slots = available_live_pipeline_slots(
        LIVE_READY_JOB_QUEUE_CAPACITY,
        live_ready_job_pending,
        live_prepare_minute_pending,
    );
    if available_slots == 0 {
        return Ok(());
    }

    let mut ready_minutes = Vec::new();
    let mut next_minute = scheduler.next_minute_to_emit();
    while ready_minutes.len() < available_slots {
        let Some(minute) = next_minute else {
            break;
        };
        if minute > ready_through_ts {
            break;
        }
        ready_minutes.push(minute);
        next_minute = Some(minute + ChronoDuration::minutes(1));
    }

    if !ready_minutes.is_empty() {
        let enqueued = try_enqueue_prepare_minute_task(
            live_prepare_task_tx,
            live_prepare_minute_pending,
            PrepareMinuteTask {
                minutes: ready_minutes.clone(),
                mode: dispatch_mode,
                enqueued_at: Instant::now(),
            },
        )?;
        if !enqueued {
            return Ok(());
        }
        for minute in ready_minutes {
            scheduler.mark_emitted_through(minute);
            live_windows_enqueued += 1;
            if first_bucket.is_none() {
                first_bucket = Some(minute);
            }
            last_bucket = Some(minute);
        }
    }

    let live_queue_pending = live_ready_job_pending.load(Ordering::Acquire);
    let next_live_minute = scheduler.next_minute_to_emit();
    let live_minutes_still_ready = next_live_minute
        .map(|minute| minute <= ready_through_ts)
        .unwrap_or(false);
    let dirty_pending = {
        let state_store = state_store.lock().await;
        state_store.has_pending_dirty_recompute()
    };
    let allow_dirty_enqueue = should_enqueue_dirty_ready_jobs(
        live_windows_enqueued,
        live_queue_pending,
        next_live_minute,
        ready_through_ts,
        dirty_pending,
    );

    if allow_dirty_enqueue {
        loop {
            let available_dirty_slots =
                available_ready_job_slots(DIRTY_READY_JOB_QUEUE_CAPACITY, dirty_ready_job_pending);
            if available_dirty_slots == 0
                || dirty_windows_enqueued >= DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK
            {
                break;
            }
            let remaining_budget =
                DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK.saturating_sub(dirty_windows_enqueued);
            let batch_limit = DIRTY_RECOMPUTE_BATCH_SIZE
                .min(remaining_budget)
                .min(available_dirty_slots);
            let dirty_batch_plan = {
                let state_store = state_store.lock().await;
                state_store
                    .pending_dirty_recompute_batch_range(batch_limit)
                    .map(|(dirty_from, dirty_to)| {
                        (
                            dirty_from,
                            dirty_to,
                            state_store.has_unhydrated_futures_orderbook_heatmap_in_range(
                                dirty_from, dirty_to,
                            ),
                        )
                    })
            };
            let Some((dirty_from, dirty_to, needs_hydration)) = dirty_batch_plan else {
                break;
            };
            let heatmap_rows = if needs_hydration {
                fetch_futures_orderbook_heatmap_rows_for_range(
                    &ctx.db_pool,
                    &ctx.config.indicator.symbol,
                    dirty_from,
                    dirty_to + ChronoDuration::minutes(1),
                    "live dirty recompute",
                )
                .await?
            } else {
                Vec::new()
            };
            let dirty_batch = {
                let mut state_store = state_store.lock().await;
                if needs_hydration
                    && state_store
                        .has_unhydrated_futures_orderbook_heatmap_in_range(dirty_from, dirty_to)
                {
                    ingest_futures_orderbook_heatmap_rows(
                        &mut state_store,
                        heatmap_rows,
                        dirty_from,
                        dirty_to + ChronoDuration::minutes(1),
                        "live dirty recompute",
                    )?;
                }
                state_store.recompute_dirty_finalized_minutes(batch_limit)
            };
            if dirty_batch.is_empty() {
                break;
            }
            for window in dirty_batch {
                let minute = window.ts_bucket;
                let enqueued = try_enqueue_ready_minute_job(
                    dirty_ready_job_tx,
                    dirty_ready_job_pending,
                    ReadyMinuteJob {
                        ts_bucket: minute,
                        mode: dispatch_mode,
                        source: ReadyJobSource::DirtyRecompute,
                        enqueued_at: Instant::now(),
                        bundle: window,
                    },
                )?;
                if !enqueued {
                    break;
                }
                dirty_windows_enqueued += 1;
            }
            if dirty_windows_enqueued >= DIRTY_RECOMPUTE_WINDOW_BUDGET_PER_TICK {
                break;
            }
        }
    }

    let dirty_still_pending = {
        let state_store = state_store.lock().await;
        state_store.has_pending_dirty_recompute()
    };
    if dirty_still_pending && !allow_dirty_enqueue {
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        if elapsed_ms >= PROCESS_READY_MINUTES_WARN_MS || live_queue_pending > 0 {
            info!(
                batch_start_minute = ?batch_start_minute,
                ready_through_ts = %ready_through_ts,
                live_windows_enqueued = live_windows_enqueued,
                live_queue_pending = live_queue_pending,
                live_minutes_still_ready = live_minutes_still_ready,
                dirty_pending = true,
                dirty_queue_pending = dirty_ready_job_pending.load(Ordering::Acquire),
                elapsed_ms = elapsed_ms,
                "deferred dirty recompute enqueue to preserve live priority"
            );
        }
    }

    if let Some(last_bucket) = last_bucket {
        let first_bucket = first_bucket.unwrap_or(last_bucket);
        let elapsed_ms = batch_started_at.elapsed().as_millis();
        metrics.set_live_ready_to_bundle_ms(elapsed_ms);
        info!(
            ts_bucket_from = %first_bucket,
            ts_bucket_to = %last_bucket,
            live_windows_enqueued = live_windows_enqueued,
            dirty_windows_enqueued = dirty_windows_enqueued,
            planned_ready_minutes = planned_ready_minutes,
            ready_through_ts = %ready_through_ts,
            live_queue_pending = live_ready_job_pending.load(Ordering::Acquire),
            dirty_queue_pending = dirty_ready_job_pending.load(Ordering::Acquire),
            elapsed_ms = elapsed_ms,
            "indicator ready-minute jobs enqueued"
        );
    }

    Ok(())
}

fn replay_heatmap_hydration_batch_end(
    start_ts: DateTime<Utc>,
    inclusive_end_ts: DateTime<Utc>,
) -> DateTime<Utc> {
    let batch_span_minutes =
        ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES.saturating_sub(1));
    inclusive_end_ts.min(start_ts + batch_span_minutes)
}

async fn process_window_bundle(
    ctx: &Arc<AppContext>,
    dispatcher: &Dispatcher,
    runtime_options: &IndicatorRuntimeOptions,
    window: crate::runtime::state_store::WindowBundle,
    mode: DispatchMode,
) -> Result<Vec<IndicatorSnapshotRow>> {
    let minute = window.ts_bucket;
    let mut kline_history_supplement = load_kline_history_supplement(
        &ctx.db_pool,
        &ctx.config.indicator.symbol,
        &window.history_futures,
        &window.history_spot,
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
        minute + ChronoDuration::minutes(1),
    )
    .await;
    let (latest_options_surface_bucket, options_surface_5m) =
        load_options_surface_history_supplement(
            &ctx.db_pool,
            &ctx.config.indicator.symbol,
            &window.options_surface_5m,
            minute + ChronoDuration::minutes(1),
        )
        .await;
    if !options_surface_5m.is_empty() {
        debug!(
            symbol = %ctx.config.indicator.symbol,
            minute = %minute,
            loaded_points = options_surface_5m.len(),
            latest_options_surface_bucket = ?latest_options_surface_bucket,
            "loaded options surface history supplement"
        );
    }
    kline_history_supplement.latest_options_surface_bucket = latest_options_surface_bucket;
    kline_history_supplement.options_surface_5m = options_surface_5m;
    let ictx = Arc::new(IndicatorContext::from_bundle(
        window,
        runtime_options,
        kline_history_supplement,
    ));

    dispatcher.process_window(ictx, mode).await
}

#[allow(dead_code)]
fn compare_minute_histories(
    snap_history: &[MinuteHistory],
    full_history: &[MinuteHistory],
    tolerance: f64,
) -> Vec<String> {
    let mut diffs = Vec::new();
    for (snap, full) in snap_history.iter().zip(full_history.iter()) {
        let ts = snap.ts_bucket;
        let check = |name: &str, a: f64, b: f64| {
            if (a - b).abs() > tolerance {
                format!(
                    "{} ts={} snap={:.6} full={:.6} diff={:.6}",
                    name,
                    ts,
                    a,
                    b,
                    a - b
                )
            } else {
                String::new()
            }
        };
        for s in [
            check("cvd", snap.cvd, full.cvd),
            check("vpin", snap.vpin, full.vpin),
            check("delta", snap.delta, full.delta),
            check("relative_delta", snap.relative_delta, full.relative_delta),
            check("buy_qty", snap.buy_qty, full.buy_qty),
            check("sell_qty", snap.sell_qty, full.sell_qty),
            check(
                "obi_twa",
                snap.obi_twa.unwrap_or(0.0),
                full.obi_twa.unwrap_or(0.0),
            ),
            check(
                "whale_notional_buy",
                snap.whale_notional_buy,
                full.whale_notional_buy,
            ),
            check(
                "whale_notional_sell",
                snap.whale_notional_sell,
                full.whale_notional_sell,
            ),
        ] {
            if !s.is_empty() {
                diffs.push(s);
            }
        }
    }
    diffs
}

async fn run_startup_backfill(
    ctx: &Arc<AppContext>,
    metrics: Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    runtime_options: &IndicatorRuntimeOptions,
) -> Result<Option<DateTime<Utc>>> {
    let now = Utc::now();
    let latest_progress_ts =
        latest_indicator_progress_ts(&ctx.db_pool, &ctx.config.indicator.symbol)
            .await
            .context("query latest indicator progress ts")?;
    let latest_snapshot_table_ts =
        latest_indicator_snapshot_ts(&ctx.db_pool, &ctx.config.indicator.symbol)
            .await
            .context("query latest indicator snapshot ts")?;
    let persisted_frontier_ts = latest_progress_ts.or(latest_snapshot_table_ts);
    metrics.set_last_persisted_ts(persisted_frontier_ts.map(|ts| ts.timestamp_millis()));
    let mut from_ts = match persisted_frontier_ts {
        Some(ts) => ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES),
        None => now - ChronoDuration::minutes(STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES),
    };
    let raw_to_ts = now - ChronoDuration::seconds(STARTUP_BACKFILL_SAFETY_LAG_SECS);
    // Backfill must stop at a full-minute boundary so we never ingest a partial
    // minute from replay and then mix it with live stream data.
    // `fetch_backfill_batch` uses `ts_bucket < to_ts_exclusive`, so partial minutes
    // must round up to include the last fully closed bucket before `raw_to_ts`.
    let to_ts = minute_exclusive_upper_bound(raw_to_ts);
    let mut startup_max_catchup_minutes = ctx.config.indicator.startup_max_catchup_minutes;
    if startup_max_catchup_minutes > 0
        && startup_max_catchup_minutes < MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES
    {
        warn!(
            configured_startup_max_catchup_minutes = startup_max_catchup_minutes,
            enforced_startup_max_catchup_minutes = MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES,
            "startup catch-up window raised to 7d to satisfy rolling-7d indicators"
        );
        startup_max_catchup_minutes = MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES;
    }
    if startup_max_catchup_minutes > 0 {
        let catchup_floor = to_ts - ChronoDuration::minutes(startup_max_catchup_minutes);
        if from_ts < catchup_floor {
            info!(
                original_from_ts = %from_ts,
                capped_from_ts = %catchup_floor,
                to_ts = %to_ts,
                startup_max_catchup_minutes = startup_max_catchup_minutes,
                "capped startup historical backfill window by configured max catch-up"
            );
            from_ts = catchup_floor;
        }
    }

    // Try to load state snapshot for fast startup
    // Support {symbol} placeholder in path (e.g. "/tmp/indicator_engine_{symbol}.json.gz")
    let snapshot_file_path = ctx
        .config
        .indicator
        .snapshot_file_path
        .replace("{symbol}", &ctx.config.indicator.symbol);
    let mut snapshot_was_loaded = false;
    if !snapshot_file_path.is_empty() {
        if let Some(snap) = try_load_state_snapshot(
            &snapshot_file_path,
            &ctx.config.indicator.symbol,
            ctx.config.indicator.snapshot_max_age_hours,
        ) {
            let snap_ts = snap.last_finalized_ts;
            state_store.restore_from_snapshot(snap);
            snapshot_was_loaded = true;
            let snap_overlap_from_ts =
                floor_minute(snap_ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES));
            from_ts = from_ts.min(snap_overlap_from_ts);
            info!(
                snap_ts = %snap_ts,
                overlap_from_ts = %snap_overlap_from_ts,
                effective_from_ts = %from_ts,
                "State snapshot loaded successfully, running overlap repair backfill"
            );
        } else {
            info!("No valid state snapshot found, running full backfill");
            let strict_history_floor_minutes = HISTORY_LIMIT_MINUTES as i64;
            let strict_history_floor =
                to_ts - ChronoDuration::minutes(strict_history_floor_minutes);
            if from_ts > strict_history_floor {
                info!(
                    original_from_ts = %from_ts,
                    strict_history_floor = %strict_history_floor,
                    history_limit_minutes = strict_history_floor_minutes,
                    "startup historical backfill expanded to rebuild full in-memory retention"
                );
                from_ts = strict_history_floor;
            }
        }
    } else {
        let strict_history_floor_minutes = HISTORY_LIMIT_MINUTES as i64;
        let strict_history_floor = to_ts - ChronoDuration::minutes(strict_history_floor_minutes);
        if from_ts > strict_history_floor {
            info!(
                original_from_ts = %from_ts,
                strict_history_floor = %strict_history_floor,
                history_limit_minutes = strict_history_floor_minutes,
                "startup historical backfill expanded to rebuild full in-memory retention"
            );
            from_ts = strict_history_floor;
        }
    }
    // Startup replay is bucket-based for canonical 1m rows. Floor the lower bound so
    // we never drop a completed ts_bucket just because the resume timestamp carried
    // non-zero seconds.
    from_ts = floor_minute(from_ts);
    if from_ts >= to_ts {
        if let Some(last_finalized_ts) = state_store.last_finalized_minute() {
            scheduler.mark_emitted_through(last_finalized_ts);
            let replay_cutoff_bucket = last_finalized_ts + ChronoDuration::minutes(1);
            info!(
                last_finalized_ts = %last_finalized_ts,
                replay_cutoff_bucket = %replay_cutoff_bucket,
                "skip startup historical backfill because snapshot already covers current frontier"
            );
            return Ok(Some(replay_cutoff_bucket));
        }
        info!(
            from_ts = %from_ts,
            to_ts = %to_ts,
            "skip startup historical backfill due to invalid range"
        );
        return Ok(None);
    }

    info!(
        from_ts = %from_ts,
        to_ts = %to_ts,
        raw_to_ts = %raw_to_ts,
        persisted_frontier_ts = ?persisted_frontier_ts,
        symbol = %ctx.config.indicator.symbol,
        fallback_lookback_minutes = STARTUP_BACKFILL_FALLBACK_LOOKBACK_MINUTES,
        overlap_minutes = STARTUP_BACKFILL_OVERLAP_MINUTES,
        startup_max_catchup_minutes = startup_max_catchup_minutes,
        startup_backfill_batch_size = ctx.config.indicator.startup_backfill_batch_size.max(100),
        backfill_source = "md.agg.*.1m",
        "startup historical backfill begin"
    );

    let mut total_rows = 0_u64;
    let mut window_from_ts = from_ts;
    while window_from_ts < to_ts {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts);
        let rows = fetch_backfill_window(
            &ctx.db_pool,
            window_from_ts,
            window_to_ts,
            &ctx.config.indicator.symbol,
            STARTUP_BACKFILL_MARKET,
        )
        .await?;

        for row in rows {
            match replay_row_to_engine_event(row) {
                Ok(event) => {
                    metrics.inc_processed(event.event_ts.timestamp_millis());
                    state_store.ingest(event);
                    total_rows += 1;
                }
                Err(err) => {
                    warn!(error = %err, "decode startup backfill row failed");
                }
            }
        }

        window_from_ts = window_to_ts;
    }

    if total_rows == 0 {
        if let Some(last_finalized_ts) = state_store.last_finalized_minute() {
            scheduler.mark_emitted_through(last_finalized_ts);
            return Ok(Some(last_finalized_ts + ChronoDuration::minutes(1)));
        }
        info!("startup historical backfill found no canonical rows in requested range");
        return Ok(None);
    }

    let Some((continuous_start_ts, continuous_end_ts)) =
        state_store.latest_continuous_canonical_segment()
    else {
        warn!(
            from_ts = %from_ts,
            to_ts = %to_ts,
            "startup historical backfill found no continuous canonical segment"
        );
        if let Some(last_finalized_ts) = state_store.last_finalized_minute() {
            scheduler.mark_emitted_through(last_finalized_ts);
            return Ok(Some(last_finalized_ts + ChronoDuration::minutes(1)));
        }
        return Ok(None);
    };
    let (history_continuous_start_ts, history_continuous_end_ts) = state_store
        .latest_continuous_trade_history_segment()
        .unwrap_or((continuous_start_ts, continuous_end_ts));

    let replay_start_ts = continuous_start_ts.max(from_ts);
    let replay_end_ts = continuous_end_ts.min(to_ts - ChronoDuration::minutes(1));
    if replay_start_ts > replay_end_ts {
        warn!(
            from_ts = %from_ts,
            continuous_start_ts = %continuous_start_ts,
            continuous_end_ts = %continuous_end_ts,
            to_ts = %to_ts,
            "startup historical backfill continuity window does not overlap requested range"
        );
        if let Some(last_finalized_ts) = state_store.last_finalized_minute() {
            scheduler.mark_emitted_through(last_finalized_ts);
            return Ok(Some(last_finalized_ts + ChronoDuration::minutes(1)));
        }
        return Ok(None);
    }
    let history_replay_start_ts = history_continuous_start_ts.max(from_ts);
    let history_replay_end_ts = history_continuous_end_ts.min(to_ts - ChronoDuration::minutes(1));

    if snapshot_was_loaded {
        state_store.rewind_finalized_state_from(history_replay_start_ts);
        // Startup replay rows have already been ingested into canonical storage in
        // order to discover the continuity window. When resuming from a snapshot,
        // those overlap rows can temporarily mark the already-finalized snapshot
        // tail as dirty before we rewind the finalized state. The startup replay
        // below explicitly rebuilds the entire overlap/materialization range, so any
        // dirty suffix left over from the pre-rewind ingest is stale and must be
        // cleared before we cut over to live processing.
        state_store.clear_dirty_recompute_state();
    }
    state_store.set_effective_history_floor(Some(history_replay_start_ts));

    let repair_start_candidate = if snapshot_was_loaded {
        from_ts
    } else {
        persisted_frontier_ts
            .map(|ts| floor_minute(ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES)))
            .unwrap_or(replay_start_ts)
    };
    let repair_start_ts = replay_start_ts.max(repair_start_candidate);
    let warm_end_ts = repair_start_ts - ChronoDuration::minutes(1);

    if persisted_frontier_ts
        .map(|frontier| repair_start_ts <= frontier)
        .unwrap_or(false)
    {
        let rewind_target_ts = repair_start_ts - ChronoDuration::minutes(1);
        dispatcher
            .rewind_persisted_tail(
                &ctx.config.indicator.symbol,
                repair_start_ts,
                &ctx.config.mq.exchanges.ind.name,
            )
            .await?;
        info!(
            rewind_target_ts = %rewind_target_ts,
            repair_start_ts = %repair_start_ts,
            persisted_frontier_ts = ?persisted_frontier_ts,
            exchange_name = %ctx.config.mq.exchanges.ind.name,
            "rewound persisted indicator tail to protect overlap repair checkpoint"
        );
        metrics.set_last_persisted_ts(Some(rewind_target_ts.timestamp_millis()));
    }

    info!(
        history_replay_start_ts = %history_replay_start_ts,
        replay_start_ts = %replay_start_ts,
        repair_start_ts = %repair_start_ts,
        history_replay_end_ts = %history_replay_end_ts,
        replay_end_ts = %replay_end_ts,
        history_continuity_start_ts = %history_continuous_start_ts,
        history_continuity_end_ts = %history_continuous_end_ts,
        continuity_start_ts = %continuous_start_ts,
        continuity_end_ts = %continuous_end_ts,
        snapshot_was_loaded = snapshot_was_loaded,
        "startup backfill replay plan"
    );

    if history_replay_start_ts <= warm_end_ts {
        let mut minute = history_replay_start_ts;
        let mut warmed_minutes = 0usize;
        while minute <= warm_end_ts {
            state_store.advance_finalized_state(minute);
            warmed_minutes += 1;
            if warmed_minutes % STARTUP_BACKFILL_YIELD_EVERY_MINUTES == 0 {
                tokio::task::yield_now().await;
            }
            minute += ChronoDuration::minutes(1);
        }
        refresh_runtime_observability_metrics(
            &metrics,
            state_store,
            Some(repair_start_ts),
            Some(replay_end_ts),
            0,
            0,
        );
        info!(
            warm_start_ts = %history_replay_start_ts,
            warm_end_ts = %warm_end_ts,
            "startup warm-state replay completed"
        );
    }

    let mut materialized_windows = 0usize;
    let mut last_computed: Vec<String> = Vec::new();
    let mut missing_union: BTreeSet<String> = BTreeSet::new();
    let mut first_materialized_bucket: Option<DateTime<Utc>> = None;
    let mut last_materialized_bucket: Option<DateTime<Utc>> = None;
    let mut materialized_since_yield = 0usize;

    let mut minute = repair_start_ts;
    while minute <= replay_end_ts {
        let batch_end_ts = replay_heatmap_hydration_batch_end(minute, replay_end_ts);
        hydrate_futures_orderbook_heatmaps_for_range(
            &ctx.db_pool,
            &ctx.config.indicator.symbol,
            state_store,
            minute,
            batch_end_ts + ChronoDuration::minutes(1),
            "startup materialization",
        )
        .await?;

        refresh_runtime_observability_metrics(
            &metrics,
            state_store,
            Some(minute),
            Some(replay_end_ts),
            0,
            0,
        );
        while minute <= batch_end_ts {
            let window = state_store.finalize_minute(minute);
            let snapshots = process_window_bundle(
                ctx,
                dispatcher,
                runtime_options,
                window,
                DispatchMode::ReplayMaterialize,
            )
            .await?;
            metrics.inc_exported_window();
            metrics.set_last_persisted_ts(Some(minute.timestamp_millis()));
            materialized_windows += 1;
            first_materialized_bucket.get_or_insert(minute);
            last_materialized_bucket = Some(minute);
            let (computed, missing) = indicator_coverage(&snapshots);
            last_computed = computed;
            for item in missing {
                missing_union.insert(item);
            }
            if ctx.config.indicator.enable_file_export {
                if let Err(err) = export_snapshots(
                    &ctx.config.indicator.export_dir,
                    minute,
                    &ctx.config.indicator.symbol,
                    &snapshots,
                )
                .await
                {
                    warn!(error = %err, "export indicator snapshot file failed");
                }
            }
            minute += ChronoDuration::minutes(1);
            materialized_since_yield += 1;
            if materialized_since_yield % STARTUP_BACKFILL_YIELD_EVERY_MINUTES == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    if let Some(last_bucket) = last_materialized_bucket {
        let first_bucket = first_materialized_bucket.unwrap_or(last_bucket);
        if missing_union.is_empty() {
            info!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = materialized_windows,
                computed_count = last_computed.len(),
                missing_count = 0,
                computed_indicators = %last_computed.join(","),
                missing_indicators = "none",
                "startup backfill indicator coverage"
            );
        } else {
            let missing_indicators = missing_union.iter().cloned().collect::<Vec<_>>().join(",");
            warn!(
                ts_bucket_from = %first_bucket,
                ts_bucket_to = %last_bucket,
                processed_windows = materialized_windows,
                computed_count = last_computed.len(),
                missing_count = missing_union.len(),
                computed_indicators = %last_computed.join(","),
                missing_indicators = %missing_indicators,
                "startup backfill indicator coverage has missing indicators (warmup or data gap)"
            );
        }
    }

    scheduler.mark_emitted_through(replay_end_ts);

    info!(
        total_rows = total_rows,
        replay_start_ts = %replay_start_ts,
        replay_end_ts = %replay_end_ts,
        replay_cutoff_bucket_ts = %(replay_end_ts + ChronoDuration::minutes(1)),
        materialized_windows = materialized_windows,
        "startup historical backfill completed"
    );

    if ctx.config.indicator.snapshot_verify_full_recompute {
        // TODO: Run a separate full in-memory backfill and compare MinuteHistory fields
        // with the snapshot-based path for the gap period. This is a development-only
        // validation feature. Not implemented in this iteration.
        info!("snapshot_verify_full_recompute is configured but not yet implemented");
    }
    let _ = snapshot_was_loaded; // used by snapshot_verify_full_recompute when implemented

    Ok(Some(replay_end_ts + ChronoDuration::minutes(1)))
}

async fn latest_indicator_snapshot_ts(
    pool: &PgPool,
    symbol: &str,
) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT max(ts_snapshot) AS max_ts
        FROM feat.indicator_snapshot
        WHERE symbol = $1
        "#,
    )
    .bind(symbol.to_uppercase())
    .fetch_one(pool)
    .await
    .context("query latest indicator snapshot ts")?;

    let max_ts: Option<DateTime<Utc>> = row.get("max_ts");
    Ok(max_ts)
}

async fn latest_indicator_progress_ts(
    pool: &PgPool,
    symbol: &str,
) -> Result<Option<DateTime<Utc>>> {
    let row = sqlx::query(
        r#"
        SELECT last_success_ts
        FROM feat.indicator_progress
        WHERE symbol = $1
        "#,
    )
    .bind(symbol.to_uppercase())
    .fetch_optional(pool)
    .await
    .context("query latest indicator progress ts")?;
    Ok(row.map(|r| r.get("last_success_ts")))
}

fn build_backfill_sql(filter_market: bool, with_cursor: bool) -> String {
    const SQL_TEMPLATE: &str = r#"
    WITH events AS (
        (
            SELECT
                t.ctid AS row_tid,
                'trade'::text AS src,
                t.ts_event AS event_ts,
                'md.agg.trade.1m'::text AS msg_type,
                t.market::text AS market,
                t.symbol AS symbol,
                format('md.agg.%s.trade.1m.%s', t.market::text, lower(t.symbol)) AS routing_key
            FROM md.agg_trade_1m t
            WHERE t.ts_bucket >= $1
              AND t.ts_bucket < $2
              AND t.symbol = $3
__TRADE_MARKET_FILTER__
__TRADE_CURSOR_FILTER__
            ORDER BY t.ts_event ASC, t.market ASC, t.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                b.ctid AS row_tid,
                'orderbook'::text AS src,
                b.ts_event AS event_ts,
                'md.agg.orderbook.1m'::text AS msg_type,
                b.market::text AS market,
                b.symbol AS symbol,
                format('md.agg.%s.orderbook.1m.%s', b.market::text, lower(b.symbol)) AS routing_key
            FROM md.agg_orderbook_1m b
            WHERE b.ts_bucket >= $1
              AND b.ts_bucket < $2
              AND b.symbol = $3
__ORDERBOOK_MARKET_FILTER__
__ORDERBOOK_CURSOR_FILTER__
            ORDER BY b.ts_event ASC, b.market ASC, b.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                l.ctid AS row_tid,
                'liq'::text AS src,
                l.ts_event AS event_ts,
                'md.agg.liq.1m'::text AS msg_type,
                l.market::text AS market,
                l.symbol AS symbol,
                format('md.agg.%s.liq.1m.%s', l.market::text, lower(l.symbol)) AS routing_key
            FROM md.agg_liq_1m l
            WHERE l.ts_bucket >= $1
              AND l.ts_bucket < $2
              AND l.symbol = $3
__LIQ_MARKET_FILTER__
__LIQ_CURSOR_FILTER__
            ORDER BY l.ts_event ASC, l.market ASC, l.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                f.ctid AS row_tid,
                'funding_mark'::text AS src,
                f.ts_event AS event_ts,
                'md.agg.funding_mark.1m'::text AS msg_type,
                f.market::text AS market,
                f.symbol AS symbol,
                format('md.agg.%s.funding_mark.1m.%s', f.market::text, lower(f.symbol)) AS routing_key
            FROM md.agg_funding_mark_1m f
            WHERE f.ts_bucket >= $1
              AND f.ts_bucket < $2
              AND f.symbol = $3
__FUNDING_MARKET_FILTER__
__FUNDING_CURSOR_FILTER__
            ORDER BY f.ts_event ASC, f.market ASC, f.symbol ASC
            LIMIT $__LIMIT_PARAM__
        )
    )
    , picked AS (
        SELECT row_tid, src, event_ts, msg_type, market, symbol, routing_key
        FROM events
__OUTER_WHERE__
        ORDER BY event_ts ASC, msg_type ASC, market ASC, symbol ASC, routing_key ASC
        LIMIT $__LIMIT_PARAM__
    )
    , expanded AS (
        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            t.ts_bucket AS t_ts_bucket,
            t.chunk_start_ts AS t_chunk_start_ts,
            t.chunk_end_ts AS t_chunk_end_ts,
            t.source_event_count AS t_source_event_count,
            t.trade_count AS t_trade_count,
            t.buy_qty AS t_buy_qty,
            t.sell_qty AS t_sell_qty,
            t.buy_notional AS t_buy_notional,
            t.sell_notional AS t_sell_notional,
            t.first_price AS t_first_price,
            t.last_price AS t_last_price,
            t.high_price AS t_high_price,
            t.low_price AS t_low_price,
            t.profile_levels AS t_profile_levels,
            t.whale_json AS t_whale_json,
            t.payload_json AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_trade_1m t
          ON t.ctid = p.row_tid
        WHERE p.src = 'trade'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            b.ts_bucket AS b_ts_bucket,
            b.chunk_start_ts AS b_chunk_start_ts,
            b.chunk_end_ts AS b_chunk_end_ts,
            b.source_event_count AS b_source_event_count,
            b.sample_count AS b_sample_count,
            b.bbo_updates AS b_bbo_updates,
            b.spread_sum AS b_spread_sum,
            b.topk_depth_sum AS b_topk_depth_sum,
            b.obi_sum AS b_obi_sum,
            b.obi_l1_sum AS b_obi_l1_sum,
            b.obi_k_sum AS b_obi_k_sum,
            b.obi_k_dw_sum AS b_obi_k_dw_sum,
            b.obi_k_dw_change_sum AS b_obi_k_dw_change_sum,
            b.obi_k_dw_adj_sum AS b_obi_k_dw_adj_sum,
            b.microprice_sum AS b_microprice_sum,
            b.microprice_classic_sum AS b_microprice_classic_sum,
            b.microprice_kappa_sum AS b_microprice_kappa_sum,
            b.microprice_adj_sum AS b_microprice_adj_sum,
            b.ofi_sum AS b_ofi_sum,
            b.obi_k_dw_close AS b_obi_k_dw_close,
            b.heatmap_levels AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_orderbook_1m b
          ON b.ctid = p.row_tid
        WHERE p.src = 'orderbook'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            l.ts_bucket AS l_ts_bucket,
            l.chunk_start_ts AS l_chunk_start_ts,
            l.chunk_end_ts AS l_chunk_end_ts,
            l.source_event_count AS l_source_event_count,
            l.force_liq_levels AS l_force_liq_levels,
            NULL::timestamptz AS f_ts_bucket,
            NULL::timestamptz AS f_chunk_start_ts,
            NULL::timestamptz AS f_chunk_end_ts,
            NULL::bigint AS f_source_event_count,
            NULL::jsonb AS f_mark_points,
            NULL::jsonb AS f_funding_points
        FROM picked p
        JOIN md.agg_liq_1m l
          ON l.ctid = p.row_tid
        WHERE p.src = 'liq'

        UNION ALL

        SELECT
            p.event_ts,
            p.msg_type,
            p.market,
            p.symbol,
            p.routing_key,
            p.src,
            NULL::timestamptz AS t_ts_bucket,
            NULL::timestamptz AS t_chunk_start_ts,
            NULL::timestamptz AS t_chunk_end_ts,
            NULL::bigint AS t_source_event_count,
            NULL::bigint AS t_trade_count,
            NULL::double precision AS t_buy_qty,
            NULL::double precision AS t_sell_qty,
            NULL::double precision AS t_buy_notional,
            NULL::double precision AS t_sell_notional,
            NULL::double precision AS t_first_price,
            NULL::double precision AS t_last_price,
            NULL::double precision AS t_high_price,
            NULL::double precision AS t_low_price,
            NULL::jsonb AS t_profile_levels,
            NULL::jsonb AS t_whale_json,
            NULL::jsonb AS t_payload_json,
            NULL::timestamptz AS b_ts_bucket,
            NULL::timestamptz AS b_chunk_start_ts,
            NULL::timestamptz AS b_chunk_end_ts,
            NULL::bigint AS b_source_event_count,
            NULL::bigint AS b_sample_count,
            NULL::bigint AS b_bbo_updates,
            NULL::double precision AS b_spread_sum,
            NULL::double precision AS b_topk_depth_sum,
            NULL::double precision AS b_obi_sum,
            NULL::double precision AS b_obi_l1_sum,
            NULL::double precision AS b_obi_k_sum,
            NULL::double precision AS b_obi_k_dw_sum,
            NULL::double precision AS b_obi_k_dw_change_sum,
            NULL::double precision AS b_obi_k_dw_adj_sum,
            NULL::double precision AS b_microprice_sum,
            NULL::double precision AS b_microprice_classic_sum,
            NULL::double precision AS b_microprice_kappa_sum,
            NULL::double precision AS b_microprice_adj_sum,
            NULL::double precision AS b_ofi_sum,
            NULL::double precision AS b_obi_k_dw_close,
            NULL::jsonb AS b_heatmap_levels,
            NULL::timestamptz AS l_ts_bucket,
            NULL::timestamptz AS l_chunk_start_ts,
            NULL::timestamptz AS l_chunk_end_ts,
            NULL::bigint AS l_source_event_count,
            NULL::jsonb AS l_force_liq_levels,
            f.ts_bucket AS f_ts_bucket,
            f.chunk_start_ts AS f_chunk_start_ts,
            f.chunk_end_ts AS f_chunk_end_ts,
            f.source_event_count AS f_source_event_count,
            f.mark_points AS f_mark_points,
            f.funding_points AS f_funding_points
        FROM picked p
        JOIN md.agg_funding_mark_1m f
          ON f.ctid = p.row_tid
        WHERE p.src = 'funding_mark'
    )
    SELECT *
    FROM expanded
    ORDER BY event_ts ASC, msg_type ASC, market ASC, symbol ASC, routing_key ASC
    "#;

    let market_param = 4usize;
    let limit_param = if filter_market { 5usize } else { 4usize };
    let cursor_ts_param = if filter_market { 6usize } else { 5usize };
    let cursor_msg_type_param = cursor_ts_param + 1;
    let cursor_market_param = cursor_ts_param + 2;
    let cursor_symbol_param = cursor_ts_param + 3;
    let cursor_routing_key_param = cursor_ts_param + 4;

    let mk_market_filter = |alias: &str| -> String {
        if filter_market {
            format!("              AND {alias}.market::text = ${market_param}")
        } else {
            String::new()
        }
    };

    let mk_cursor_filter = |alias: &str| -> String {
        if with_cursor {
            format!("              AND {alias}.ts_event >= ${cursor_ts_param}")
        } else {
            String::new()
        }
    };

    let outer_where = match (filter_market, with_cursor) {
        (false, false) => String::new(),
        (true, false) => format!("    WHERE market = ${market_param}\n"),
        (false, true) => format!(
            "    WHERE (event_ts, msg_type, market, symbol, routing_key)\n        > (${cursor_ts_param}::timestamptz, ${cursor_msg_type_param}::text, ${cursor_market_param}::text, ${cursor_symbol_param}::text, ${cursor_routing_key_param}::text)\n"
        ),
        (true, true) => format!(
            "    WHERE market = ${market_param}\n      AND (event_ts, msg_type, market, symbol, routing_key)\n        > (${cursor_ts_param}::timestamptz, ${cursor_msg_type_param}::text, ${cursor_market_param}::text, ${cursor_symbol_param}::text, ${cursor_routing_key_param}::text)\n"
        ),
    };

    SQL_TEMPLATE
        .replace("__TRADE_MARKET_FILTER__", &mk_market_filter("t"))
        .replace("__ORDERBOOK_MARKET_FILTER__", &mk_market_filter("b"))
        .replace("__LIQ_MARKET_FILTER__", &mk_market_filter("l"))
        .replace("__FUNDING_MARKET_FILTER__", &mk_market_filter("f"))
        .replace("__TRADE_CURSOR_FILTER__", &mk_cursor_filter("t"))
        .replace("__ORDERBOOK_CURSOR_FILTER__", &mk_cursor_filter("b"))
        .replace("__LIQ_CURSOR_FILTER__", &mk_cursor_filter("l"))
        .replace("__FUNDING_CURSOR_FILTER__", &mk_cursor_filter("f"))
        .replace("__OUTER_WHERE__", &outer_where)
        .replace("__LIMIT_PARAM__", &limit_param.to_string())
}

fn require_backfill_field<T>(src: &str, field: &'static str, value: Option<T>) -> Result<T> {
    value.with_context(|| {
        format!("startup backfill row missing required field {field} for src={src}")
    })
}

fn build_backfill_data_json(row: &PgRow, src: &str) -> Result<Value> {
    match src {
        "trade" => {
            let ts_bucket = require_backfill_field(
                src,
                "t_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("t_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "t_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("t_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "t_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("t_chunk_end_ts"),
            )?;
            let trade_count = require_backfill_field(
                src,
                "t_trade_count",
                row.get::<Option<i64>, _>("t_trade_count"),
            )?;
            let buy_qty =
                require_backfill_field(src, "t_buy_qty", row.get::<Option<f64>, _>("t_buy_qty"))?;
            let sell_qty =
                require_backfill_field(src, "t_sell_qty", row.get::<Option<f64>, _>("t_sell_qty"))?;
            let buy_notional = require_backfill_field(
                src,
                "t_buy_notional",
                row.get::<Option<f64>, _>("t_buy_notional"),
            )?;
            let sell_notional = require_backfill_field(
                src,
                "t_sell_notional",
                row.get::<Option<f64>, _>("t_sell_notional"),
            )?;
            let profile_levels = require_backfill_field(
                src,
                "t_profile_levels",
                row.get::<Option<Value>, _>("t_profile_levels"),
            )?;
            let whale = require_backfill_field(
                src,
                "t_whale_json",
                row.get::<Option<Value>, _>("t_whale_json"),
            )?;
            let payload_json = row
                .get::<Option<Value>, _>("t_payload_json")
                .unwrap_or_else(|| json!({}));

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("t_source_event_count"),
                "trade_count": trade_count,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "first_price": row.get::<Option<f64>, _>("t_first_price"),
                "last_price": row.get::<Option<f64>, _>("t_last_price"),
                "high_price": row.get::<Option<f64>, _>("t_high_price"),
                "low_price": row.get::<Option<f64>, _>("t_low_price"),
                "profile_levels": profile_levels,
                "whale": whale,
                "payload_json": payload_json
            }))
        }
        "orderbook" => {
            let ts_bucket = require_backfill_field(
                src,
                "b_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("b_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "b_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("b_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "b_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("b_chunk_end_ts"),
            )?;
            let sample_count = require_backfill_field(
                src,
                "b_sample_count",
                row.get::<Option<i64>, _>("b_sample_count"),
            )?;
            let bbo_updates = require_backfill_field(
                src,
                "b_bbo_updates",
                row.get::<Option<i64>, _>("b_bbo_updates"),
            )?;
            let spread_sum = require_backfill_field(
                src,
                "b_spread_sum",
                row.get::<Option<f64>, _>("b_spread_sum"),
            )?;
            let topk_depth_sum = require_backfill_field(
                src,
                "b_topk_depth_sum",
                row.get::<Option<f64>, _>("b_topk_depth_sum"),
            )?;
            let obi_sum =
                require_backfill_field(src, "b_obi_sum", row.get::<Option<f64>, _>("b_obi_sum"))?;
            let obi_l1_sum = require_backfill_field(
                src,
                "b_obi_l1_sum",
                row.get::<Option<f64>, _>("b_obi_l1_sum"),
            )?;
            let obi_k_sum = require_backfill_field(
                src,
                "b_obi_k_sum",
                row.get::<Option<f64>, _>("b_obi_k_sum"),
            )?;
            let obi_k_dw_sum = require_backfill_field(
                src,
                "b_obi_k_dw_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_sum"),
            )?;
            let obi_k_dw_change_sum = require_backfill_field(
                src,
                "b_obi_k_dw_change_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_change_sum"),
            )?;
            let obi_k_dw_adj_sum = require_backfill_field(
                src,
                "b_obi_k_dw_adj_sum",
                row.get::<Option<f64>, _>("b_obi_k_dw_adj_sum"),
            )?;
            let microprice_sum = require_backfill_field(
                src,
                "b_microprice_sum",
                row.get::<Option<f64>, _>("b_microprice_sum"),
            )?;
            let microprice_classic_sum = require_backfill_field(
                src,
                "b_microprice_classic_sum",
                row.get::<Option<f64>, _>("b_microprice_classic_sum"),
            )?;
            let microprice_kappa_sum = require_backfill_field(
                src,
                "b_microprice_kappa_sum",
                row.get::<Option<f64>, _>("b_microprice_kappa_sum"),
            )?;
            let microprice_adj_sum = require_backfill_field(
                src,
                "b_microprice_adj_sum",
                row.get::<Option<f64>, _>("b_microprice_adj_sum"),
            )?;
            let ofi_sum =
                require_backfill_field(src, "b_ofi_sum", row.get::<Option<f64>, _>("b_ofi_sum"))?;
            let heatmap_loaded = row
                .get::<Option<bool>, _>("b_heatmap_loaded")
                .unwrap_or(true);
            let heatmap_levels = require_backfill_field(
                src,
                "b_heatmap_levels",
                row.get::<Option<Value>, _>("b_heatmap_levels"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("b_source_event_count"),
                "sample_count": sample_count,
                "bbo_updates": bbo_updates,
                "spread_sum": spread_sum,
                "topk_depth_sum": topk_depth_sum,
                "obi_sum": obi_sum,
                "obi_l1_sum": obi_l1_sum,
                "obi_k_sum": obi_k_sum,
                "obi_k_dw_sum": obi_k_dw_sum,
                "obi_k_dw_change_sum": obi_k_dw_change_sum,
                "obi_k_dw_adj_sum": obi_k_dw_adj_sum,
                "microprice_sum": microprice_sum,
                "microprice_classic_sum": microprice_classic_sum,
                "microprice_kappa_sum": microprice_kappa_sum,
                "microprice_adj_sum": microprice_adj_sum,
                "ofi_sum": ofi_sum,
                "obi_k_dw_close": row.get::<Option<f64>, _>("b_obi_k_dw_close"),
                "heatmap_levels": heatmap_levels,
                "heatmap_loaded": heatmap_loaded
            }))
        }
        "liq" => {
            let ts_bucket = require_backfill_field(
                src,
                "l_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("l_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "l_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("l_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "l_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("l_chunk_end_ts"),
            )?;
            let force_liq_levels = require_backfill_field(
                src,
                "l_force_liq_levels",
                row.get::<Option<Value>, _>("l_force_liq_levels"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("l_source_event_count"),
                "force_liq_levels": force_liq_levels
            }))
        }
        "funding_mark" => {
            let ts_bucket = require_backfill_field(
                src,
                "f_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("f_ts_bucket"),
            )?;
            let chunk_start_ts = require_backfill_field(
                src,
                "f_chunk_start_ts",
                row.get::<Option<DateTime<Utc>>, _>("f_chunk_start_ts"),
            )?;
            let chunk_end_ts = require_backfill_field(
                src,
                "f_chunk_end_ts",
                row.get::<Option<DateTime<Utc>>, _>("f_chunk_end_ts"),
            )?;
            let mark_points = require_backfill_field(
                src,
                "f_mark_points",
                row.get::<Option<Value>, _>("f_mark_points"),
            )?;
            let funding_points = require_backfill_field(
                src,
                "f_funding_points",
                row.get::<Option<Value>, _>("f_funding_points"),
            )?;

            Ok(json!({
                "ts_bucket": ts_bucket,
                "chunk_start_ts": chunk_start_ts,
                "chunk_end_ts": chunk_end_ts,
                "source_event_count": row.get::<Option<i64>, _>("f_source_event_count"),
                "mark_points": mark_points,
                "funding_points": funding_points
            }))
        }
        "oi_current" => Ok(json!({
            "ts_effective": require_backfill_field(
                src,
                "oi_current_ts_effective",
                row.get::<Option<DateTime<Utc>>, _>("oi_current_ts_effective"),
            )?,
            "open_interest_contracts": require_backfill_field(
                src,
                "oi_current_contracts",
                row.get::<Option<f64>, _>("oi_current_contracts"),
            )?,
            "mark_price": row.get::<Option<f64>, _>("oi_current_mark_price"),
            "open_interest_value_usdt": row.get::<Option<f64>, _>("oi_current_value_usdt"),
        })),
        "oi_hist_5m" => Ok(json!({
            "ts_effective": require_backfill_field(
                src,
                "oi_hist_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("oi_hist_ts_bucket"),
            )?,
            "ts_bucket": row.get::<Option<DateTime<Utc>>, _>("oi_hist_ts_bucket"),
            "open_interest_contracts": require_backfill_field(
                src,
                "oi_hist_contracts",
                row.get::<Option<f64>, _>("oi_hist_contracts"),
            )?,
            "open_interest_value_usdt": require_backfill_field(
                src,
                "oi_hist_value_usdt",
                row.get::<Option<f64>, _>("oi_hist_value_usdt"),
            )?,
        })),
        "long_short_ratio_5m" => Ok(json!({
            "ts_effective": require_backfill_field(
                src,
                "lsr_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("lsr_ts_bucket"),
            )?,
            "ts_bucket": row.get::<Option<DateTime<Utc>>, _>("lsr_ts_bucket"),
            "ratio_type": require_backfill_field(
                src,
                "lsr_ratio_type",
                row.get::<Option<String>, _>("lsr_ratio_type"),
            )?,
            "long_short_ratio": require_backfill_field(
                src,
                "lsr_long_short_ratio",
                row.get::<Option<f64>, _>("lsr_long_short_ratio"),
            )?,
            "long_account_ratio": row.get::<Option<f64>, _>("lsr_long_account_ratio"),
            "short_account_ratio": row.get::<Option<f64>, _>("lsr_short_account_ratio"),
        })),
        "option_mark_greeks_5m" => Ok(json!({
            "ts_effective": require_backfill_field(
                src,
                "opt_ts_bucket",
                row.get::<Option<DateTime<Utc>>, _>("opt_ts_bucket"),
            )?,
            "ts_bucket": row.get::<Option<DateTime<Utc>>, _>("opt_ts_bucket"),
            "option_symbol": require_backfill_field(
                src,
                "opt_option_symbol",
                row.get::<Option<String>, _>("opt_option_symbol"),
            )?,
            "underlying_asset": require_backfill_field(
                src,
                "opt_underlying_asset",
                row.get::<Option<String>, _>("opt_underlying_asset"),
            )?,
            "expiry_ts": require_backfill_field(
                src,
                "opt_expiry_ts",
                row.get::<Option<DateTime<Utc>>, _>("opt_expiry_ts"),
            )?,
            "strike_price": require_backfill_field(
                src,
                "opt_strike_price",
                row.get::<Option<f64>, _>("opt_strike_price"),
            )?,
            "contract_side": require_backfill_field(
                src,
                "opt_contract_side",
                row.get::<Option<String>, _>("opt_contract_side"),
            )?,
            "unit": row.get::<Option<f64>, _>("opt_unit"),
            "index_price": row.get::<Option<f64>, _>("opt_index_price"),
            "mark_price": row.get::<Option<f64>, _>("opt_mark_price"),
            "bid_iv": row.get::<Option<f64>, _>("opt_bid_iv"),
            "ask_iv": row.get::<Option<f64>, _>("opt_ask_iv"),
            "mark_iv": row.get::<Option<f64>, _>("opt_mark_iv"),
            "delta": row.get::<Option<f64>, _>("opt_delta"),
            "gamma": row.get::<Option<f64>, _>("opt_gamma"),
            "vega": row.get::<Option<f64>, _>("opt_vega"),
            "theta": row.get::<Option<f64>, _>("opt_theta"),
            "risk_free_interest": row.get::<Option<f64>, _>("opt_risk_free_interest"),
        })),
        _ => anyhow::bail!("unsupported startup backfill source: {src}"),
    }
}

async fn fetch_backfill_window(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
) -> Result<Vec<ReplayRow>> {
    if from_ts >= to_ts {
        return Ok(Vec::new());
    }

    let symbol_upper = symbol.to_uppercase();
    let include_all = market.eq_ignore_ascii_case("all");
    let include_futures = include_all || market.eq_ignore_ascii_case("futures");
    let include_spot = include_all || market.eq_ignore_ascii_case("spot");
    let mut rows = Vec::new();

    if include_futures {
        let (
            trade_rows,
            orderbook_rows,
            liq_rows,
            funding_rows,
            oi_current_rows,
            oi_hist_rows,
            long_short_ratio_rows,
            option_mark_rows,
        ) = tokio::try_join!(
            fetch_backfill_source_rows(
                pool,
                TRADE_BACKFILL_WINDOW_SQL,
                "trade",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR,
                "orderbook",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                LIQ_BACKFILL_WINDOW_SQL,
                "liq",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                FUNDING_BACKFILL_WINDOW_SQL,
                "funding_mark",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                OI_CURRENT_BACKFILL_WINDOW_SQL,
                "oi_current",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                OI_HIST_5M_BACKFILL_WINDOW_SQL,
                "oi_hist_5m",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                LONG_SHORT_RATIO_5M_BACKFILL_WINDOW_SQL,
                "long_short_ratio_5m",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
            fetch_backfill_source_rows(
                pool,
                OPTION_MARK_GREEKS_5M_BACKFILL_WINDOW_SQL,
                "option_mark_greeks_5m",
                from_ts,
                to_ts,
                &symbol_upper,
                "futures",
            ),
        )?;
        rows.extend(trade_rows);
        rows.extend(orderbook_rows);
        rows.extend(liq_rows);
        rows.extend(funding_rows);
        rows.extend(oi_current_rows);
        rows.extend(oi_hist_rows);
        rows.extend(long_short_ratio_rows);
        rows.extend(option_mark_rows);
    }

    if include_spot {
        let (trade_rows, orderbook_rows) = tokio::try_join!(
            fetch_backfill_source_rows(
                pool,
                TRADE_BACKFILL_WINDOW_SQL,
                "trade",
                from_ts,
                to_ts,
                &symbol_upper,
                "spot",
            ),
            fetch_backfill_source_rows(
                pool,
                ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR,
                "orderbook",
                from_ts,
                to_ts,
                &symbol_upper,
                "spot",
            ),
        )?;
        rows.extend(trade_rows);
        rows.extend(orderbook_rows);
    }

    rows.sort_by(|a, b| {
        a.event_ts
            .cmp(&b.event_ts)
            .then_with(|| a.msg_type.cmp(&b.msg_type))
            .then_with(|| a.market.cmp(&b.market))
            .then_with(|| a.symbol.cmp(&b.symbol))
            .then_with(|| a.routing_key.cmp(&b.routing_key))
    });
    Ok(rows)
}

async fn fetch_backfill_source_rows(
    pool: &PgPool,
    sql: &str,
    src: &'static str,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol_upper: &str,
    market: &str,
) -> Result<Vec<ReplayRow>> {
    let rows = sqlx::query(sql)
        .bind(from_ts)
        .bind(to_ts)
        .bind(symbol_upper)
        .bind(market)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "fetch {src} startup/live repair rows from_ts={from_ts} to_ts={to_ts} symbol={symbol_upper} market={market}"
            )
        })?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let data_json = build_backfill_data_json(&row, src)
            .with_context(|| format!("build replay payload for src={src}"))?;
        out.push(ReplayRow {
            event_ts: row.get("event_ts"),
            msg_type: row.get("msg_type"),
            market: row.get("market"),
            symbol: row.get("symbol"),
            routing_key: row.get("routing_key"),
            data_json,
        });
    }
    Ok(out)
}

const TRADE_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.agg.trade.1m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.agg.%s.trade.1m.%s', market::text, lower(symbol)) AS routing_key,
        ts_bucket AS t_ts_bucket,
        chunk_start_ts AS t_chunk_start_ts,
        chunk_end_ts AS t_chunk_end_ts,
        source_event_count AS t_source_event_count,
        trade_count AS t_trade_count,
        buy_qty AS t_buy_qty,
        sell_qty AS t_sell_qty,
        buy_notional AS t_buy_notional,
        sell_notional AS t_sell_notional,
        first_price AS t_first_price,
        last_price AS t_last_price,
        high_price AS t_high_price,
        low_price AS t_low_price,
        profile_levels AS t_profile_levels,
        whale_json AS t_whale_json,
        payload_json AS t_payload_json
    FROM md.agg_trade_1m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

const ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR: &str = r#"
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
        '[]'::jsonb AS b_heatmap_levels,
        FALSE AS b_heatmap_loaded
    FROM md.agg_orderbook_1m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

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

const LIQ_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.agg.liq.1m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.agg.%s.liq.1m.%s', market::text, lower(symbol)) AS routing_key,
        ts_bucket AS l_ts_bucket,
        chunk_start_ts AS l_chunk_start_ts,
        chunk_end_ts AS l_chunk_end_ts,
        source_event_count AS l_source_event_count,
        force_liq_levels AS l_force_liq_levels
    FROM md.agg_liq_1m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

const FUNDING_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.agg.funding_mark.1m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.agg.%s.funding_mark.1m.%s', market::text, lower(symbol)) AS routing_key,
        ts_bucket AS f_ts_bucket,
        chunk_start_ts AS f_chunk_start_ts,
        chunk_end_ts AS f_chunk_end_ts,
        source_event_count AS f_source_event_count,
        mark_points AS f_mark_points,
        funding_points AS f_funding_points
    FROM md.agg_funding_mark_1m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

const OI_CURRENT_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.open_interest_current'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.%s.open_interest.current.%s', market::text, lower(symbol)) AS routing_key,
        ts_event AS oi_current_ts_effective,
        open_interest_contracts AS oi_current_contracts,
        mark_price AS oi_current_mark_price,
        open_interest_value_usdt AS oi_current_value_usdt
    FROM md.open_interest_current_1m
    WHERE ts_event >= $1
      AND ts_event < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

const OI_HIST_5M_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.open_interest_hist_5m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.%s.open_interest.5m.%s', market::text, lower(symbol)) AS routing_key,
        ts_bucket AS oi_hist_ts_bucket,
        open_interest_contracts AS oi_hist_contracts,
        open_interest_value_usdt AS oi_hist_value_usdt
    FROM md.open_interest_hist_5m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, market ASC, symbol ASC
"#;

const LONG_SHORT_RATIO_5M_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.long_short_ratio_5m'::text AS msg_type,
        market::text AS market,
        symbol,
        format('md.%s.long_short_ratio.%s.5m.%s', market::text, ratio_type, lower(symbol)) AS routing_key,
        ts_bucket AS lsr_ts_bucket,
        ratio_type AS lsr_ratio_type,
        long_short_ratio AS lsr_long_short_ratio,
        long_account_ratio AS lsr_long_account_ratio,
        short_account_ratio AS lsr_short_account_ratio
    FROM md.long_short_ratio_5m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
      AND ratio_type IN ('global_account', 'top_account', 'top_position')
    ORDER BY ts_event ASC, market ASC, symbol ASC, ratio_type ASC
"#;

const OPTION_MARK_GREEKS_5M_BACKFILL_WINDOW_SQL: &str = r#"
    SELECT
        ts_event AS event_ts,
        'md.option_mark_greeks_5m'::text AS msg_type,
        'futures'::text AS market,
        symbol,
        format('md.futures.option_mark_greeks.5m.%s', lower(symbol)) AS routing_key,
        ts_bucket AS opt_ts_bucket,
        option_symbol AS opt_option_symbol,
        underlying_asset AS opt_underlying_asset,
        expiry_ts AS opt_expiry_ts,
        strike_price AS opt_strike_price,
        contract_side AS opt_contract_side,
        unit AS opt_unit,
        index_price AS opt_index_price,
        mark_price AS opt_mark_price,
        bid_iv AS opt_bid_iv,
        ask_iv AS opt_ask_iv,
        mark_iv AS opt_mark_iv,
        delta AS opt_delta,
        gamma AS opt_gamma,
        vega AS opt_vega,
        theta AS opt_theta,
        risk_free_interest AS opt_risk_free_interest
    FROM md.option_mark_greeks_5m
    WHERE ts_bucket >= $1
      AND ts_bucket < $2
      AND symbol = $3
      AND market = $4::cfg.market_type
    ORDER BY ts_event ASC, symbol ASC, option_symbol ASC
"#;

pub async fn fetch_backfill_batch(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    cursor: Option<&BackfillCursor>,
) -> Result<Vec<ReplayRow>> {
    let mut rows = fetch_backfill_window(pool, from_ts, to_ts, symbol, market)
        .await
        .context("fetch startup backfill batch")?;

    if let Some(cursor) = cursor {
        rows.retain(|row| replay_row_after_cursor(row, cursor));
    }

    if limit >= 0 && rows.len() > limit as usize {
        rows.truncate(limit as usize);
    }

    Ok(rows)
}

fn replay_row_after_cursor(row: &ReplayRow, cursor: &BackfillCursor) -> bool {
    (
        row.event_ts,
        row.msg_type.as_str(),
        row.market.as_str(),
        row.symbol.as_str(),
        row.routing_key.as_str(),
    ) > (
        cursor.event_ts,
        cursor.msg_type.as_str(),
        cursor.market.as_str(),
        cursor.symbol.as_str(),
        cursor.routing_key.as_str(),
    )
}

async fn hydrate_futures_orderbook_heatmaps_for_range_with_fetch<F, Fut>(
    _symbol: &str,
    state_store: &mut StateStore,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    reason: &'static str,
    mut fetch_rows: F,
) -> Result<()>
where
    F: FnMut(DateTime<Utc>, DateTime<Utc>) -> Fut,
    Fut: Future<Output = Result<Vec<ReplayRow>>>,
{
    let Some(to_ts_inclusive) = to_ts_exclusive.checked_sub_signed(ChronoDuration::minutes(1))
    else {
        return Ok(());
    };
    if from_ts > to_ts_inclusive {
        return Ok(());
    }
    if !state_store.has_unhydrated_futures_orderbook_heatmap_in_range(from_ts, to_ts_inclusive) {
        return Ok(());
    }

    let mut window_from_ts = from_ts;
    let mut fetched_rows = 0usize;
    while window_from_ts < to_ts_exclusive {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts_exclusive);
        let rows = fetch_rows(window_from_ts, window_to_ts).await.with_context(|| {
            format!(
                "{reason} fetch futures orderbook heatmap rows from_ts={window_from_ts} to_ts_exclusive={window_to_ts}"
            )
        })?;
        fetched_rows += rows.len();

        for row in rows {
            let event = replay_row_to_engine_event(row).with_context(|| {
                format!(
                    "{reason} decode futures orderbook heatmap row from_ts={window_from_ts} to_ts_exclusive={window_to_ts}"
                )
            })?;
            state_store.ingest(event);
        }

        window_from_ts = window_to_ts;
    }

    if state_store.has_unhydrated_futures_orderbook_heatmap_in_range(from_ts, to_ts_inclusive) {
        anyhow::bail!(
            "{reason} left unhydrated futures orderbook heatmap in range {from_ts}..={to_ts_inclusive}"
        );
    }

    info!(
        reason = reason,
        from_ts = %from_ts,
        to_ts_exclusive = %to_ts_exclusive,
        fetched_rows = fetched_rows,
        "hydrated futures orderbook heatmaps for replay range"
    );

    Ok(())
}

async fn hydrate_futures_orderbook_heatmaps_for_range(
    pool: &PgPool,
    symbol: &str,
    state_store: &mut StateStore,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    reason: &'static str,
) -> Result<()> {
    let symbol_upper = symbol.to_uppercase();
    hydrate_futures_orderbook_heatmaps_for_range_with_fetch(
        symbol,
        state_store,
        from_ts,
        to_ts_exclusive,
        reason,
        |window_from_ts, window_to_ts| {
            let symbol_upper = symbol_upper.clone();
            async move {
                fetch_backfill_source_rows(
                    pool,
                    ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP,
                    "orderbook",
                    window_from_ts,
                    window_to_ts,
                    &symbol_upper,
                    "futures",
                )
                .await
            }
        },
    )
    .await
}

async fn fetch_futures_orderbook_heatmap_rows_for_range(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    reason: &'static str,
) -> Result<Vec<ReplayRow>> {
    let Some(to_ts_inclusive) = to_ts_exclusive.checked_sub_signed(ChronoDuration::minutes(1))
    else {
        return Ok(Vec::new());
    };
    if from_ts > to_ts_inclusive {
        return Ok(Vec::new());
    }

    let symbol_upper = symbol.to_uppercase();
    let mut rows = Vec::new();
    let mut window_from_ts = from_ts;
    while window_from_ts < to_ts_exclusive {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts_exclusive);
        let mut batch = fetch_backfill_source_rows(
            pool,
            ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP,
            "orderbook",
            window_from_ts,
            window_to_ts,
            &symbol_upper,
            "futures",
        )
        .await
        .with_context(|| {
            format!(
                "{reason} fetch futures orderbook heatmap rows from_ts={window_from_ts} to_ts_exclusive={window_to_ts}"
            )
        })?;
        rows.append(&mut batch);
        window_from_ts = window_to_ts;
    }

    Ok(rows)
}

fn ingest_futures_orderbook_heatmap_rows(
    state_store: &mut StateStore,
    rows: Vec<ReplayRow>,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    reason: &'static str,
) -> Result<usize> {
    let mut ingested_rows = 0usize;
    for row in rows {
        let event = replay_row_to_engine_event(row).with_context(|| {
            format!(
                "{reason} decode futures orderbook heatmap row from_ts={from_ts} to_ts_exclusive={to_ts_exclusive}"
            )
        })?;
        state_store.ingest(event);
        ingested_rows += 1;
    }

    Ok(ingested_rows)
}

pub fn replay_row_to_engine_event(row: ReplayRow) -> Result<EngineEvent> {
    build_engine_event(
        EngineEventEnvelope {
            schema_version: 1,
            msg_type: row.msg_type,
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: row.routing_key,
            market: row.market,
            symbol: row.symbol,
            source_kind: "replay".to_string(),
            backfill_in_progress: true,
            event_ts: row.event_ts,
            published_at: Utc::now(),
        },
        row.data_json,
    )
    .context("decode startup replay payload")
}

async fn export_snapshots(
    export_dir: &str,
    ts_bucket: chrono::DateTime<Utc>,
    symbol: &str,
    snapshots: &[IndicatorSnapshotRow],
) -> Result<()> {
    let path = Path::new(export_dir);
    tokio::fs::create_dir_all(path).await?;

    let filename = format!(
        "indicators_{}_{}.json",
        symbol.to_lowercase(),
        ts_bucket.format("%Y%m%d_%H%M")
    );
    let full_path = path.join(filename);

    let body = serde_json::json!({
        "ts_bucket": ts_bucket.to_rfc3339(),
        "symbol": symbol,
        "count": snapshots.len(),
        "indicators": snapshots.iter().map(|s| {
            serde_json::json!({
                "indicator_code": s.indicator_code,
                "window_code": s.window_code,
                "payload": s.payload_json
            })
        }).collect::<Vec<_>>()
    });

    let buf = serde_json::to_vec_pretty(&body)?;
    tokio::fs::write(full_path, buf).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        allow_live_tail_reconcile, allow_oi_ratio_patch_processing, build_backfill_sql,
        find_long_null_price_run, handle_ingest_event,
        hydrate_futures_orderbook_heatmaps_for_range_with_fetch, live_tail_reconcile_start_ts,
        minute_exclusive_upper_bound, minute_history_is_strictly_contiguous,
        replay_heatmap_hydration_batch_end, shutdown_ready_through_candidate,
        snapshot_has_required_history, snapshot_null_price_run_reaches_recent_tail,
        LiveCanonicalRepairController, FUNDING_BACKFILL_WINDOW_SQL, LIQ_BACKFILL_WINDOW_SQL,
        LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES, MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES,
        ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR, ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP,
        TRADE_BACKFILL_WINDOW_SQL,
    };
    use crate::ingest::decoder::{
        AggHeatmapLevel, AggOrderbook1mEvent, EngineEvent, MarketKind, MdData, TradeEvent,
    };
    use crate::observability::metrics::AppMetrics;
    use crate::runtime::state_store::{
        FinalizedVpinState, FundingChange, LatestFundingState, LatestMarkState, MinuteHistory,
        StateSnapshot, StateStore, VpinState, HISTORY_LIMIT_MINUTES, STATE_SNAPSHOT_VERSION,
    };
    use crate::runtime::window_scheduler::WindowScheduler;
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use uuid::Uuid;

    const MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT: usize = 60;

    fn history_row(ts_bucket: chrono::DateTime<Utc>) -> MinuteHistory {
        MinuteHistory {
            ts_bucket,
            market: MarketKind::Futures,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            last_price: None,
            buy_qty: 0.0,
            sell_qty: 0.0,
            total_qty: 0.0,
            total_notional: 0.0,
            delta: 0.0,
            relative_delta: 0.0,
            force_liq: BTreeMap::new(),
            ofi: 0.0,
            spread_twa: None,
            topk_depth_twa: None,
            obi_twa: None,
            obi_l1_twa: None,
            obi_k_twa: None,
            obi_k_dw_twa: None,
            obi_k_dw_close: None,
            obi_k_dw_change: None,
            obi_k_dw_adj_twa: None,
            bbo_updates: 0,
            microprice_twa: None,
            microprice_classic_twa: None,
            microprice_kappa_twa: None,
            microprice_adj_twa: None,
            cvd: 0.0,
            vpin: 0.0,
            avwap_minute: None,
            whale_trade_count: 0,
            whale_buy_count: 0,
            whale_sell_count: 0,
            whale_notional_total: 0.0,
            whale_notional_buy: 0.0,
            whale_notional_sell: 0.0,
            whale_qty_eth_total: 0.0,
            whale_qty_eth_buy: 0.0,
            whale_qty_eth_sell: 0.0,
            whale_max_single_notional: 0.0,
            profile: BTreeMap::new(),
        }
    }

    fn priced_history_row(ts_bucket: chrono::DateTime<Utc>, price: f64) -> MinuteHistory {
        MinuteHistory {
            open_price: Some(price),
            high_price: Some(price),
            low_price: Some(price),
            close_price: Some(price),
            last_price: Some(price),
            total_qty: 1.0,
            total_notional: price,
            ..history_row(ts_bucket)
        }
    }

    fn frontier_event(event_ts: chrono::DateTime<Utc>, data: MdData) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "test".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "test".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts,
            published_at: event_ts,
            data,
        }
    }

    fn agg_orderbook_event(
        ts_bucket: chrono::DateTime<Utc>,
        heatmap_loaded: bool,
        bid_liquidity: f64,
    ) -> EngineEvent {
        frontier_event(
            ts_bucket,
            MdData::AggOrderbook1m(AggOrderbook1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::minutes(1),
                source_event_count: 1,
                sample_count: 1,
                bbo_updates: 2,
                spread_sum: 1.0,
                topk_depth_sum: 10.0,
                obi_sum: 0.4,
                obi_l1_sum: 0.3,
                obi_k_sum: 0.5,
                obi_k_dw_sum: 0.6,
                obi_k_dw_change_sum: 0.1,
                obi_k_dw_adj_sum: 0.55,
                microprice_sum: 100.0,
                microprice_classic_sum: 100.0,
                microprice_kappa_sum: 100.0,
                microprice_adj_sum: 100.0,
                ofi_sum: 5.0,
                obi_k_dw_close: Some(0.6),
                heatmap_levels: if heatmap_loaded {
                    vec![AggHeatmapLevel {
                        price: 100.0,
                        bid_liquidity,
                        ask_liquidity: 2.0,
                    }]
                } else {
                    Vec::new()
                },
                heatmap_loaded,
            }),
        )
    }

    fn snapshot_fixture(
        last_finalized_ts: chrono::DateTime<Utc>,
        history_futures: Vec<MinuteHistory>,
        history_spot: Vec<MinuteHistory>,
        effective_history_floor_ts: Option<chrono::DateTime<Utc>>,
    ) -> StateSnapshot {
        StateSnapshot {
            version: STATE_SNAPSHOT_VERSION,
            symbol: "TESTUSDT".to_string(),
            last_finalized_ts,
            saved_at: last_finalized_ts,
            effective_history_floor_ts,
            cvd_futures: 0.0,
            cvd_spot: 0.0,
            vpin_futures: VpinState::new(50.0, 50),
            vpin_spot: VpinState::new(50.0, 50),
            finalized_vpin_futures: Vec::<FinalizedVpinState>::new(),
            finalized_vpin_spot: Vec::<FinalizedVpinState>::new(),
            history_futures,
            history_spot,
            latest_mark: Option::<LatestMarkState>::None,
            latest_funding: Option::<LatestFundingState>::None,
            funding_changes: Vec::<FundingChange>::new(),
            mark_timeline: Vec::<LatestMarkState>::new(),
            funding_timeline: Vec::<LatestFundingState>::new(),
            current_open_interest_timeline: Vec::new(),
            open_interest_hist_5m: Vec::new(),
            global_account_ratio_5m: Vec::new(),
            top_account_ratio_5m: Vec::new(),
            top_position_ratio_5m: Vec::new(),
            option_mark_greeks_5m: Vec::new(),
        }
    }

    #[test]
    fn startup_backfill_sql_filters_canonical_rows_by_ts_bucket() {
        let sql = build_backfill_sql(false, false);
        assert!(sql.contains("WHERE t.ts_bucket >= $1"));
        assert!(sql.contains("AND t.ts_bucket < $2"));
        assert!(sql.contains("WHERE b.ts_bucket >= $1"));
        assert!(sql.contains("AND b.ts_bucket < $2"));
        assert!(sql.contains("WHERE l.ts_bucket >= $1"));
        assert!(sql.contains("AND l.ts_bucket < $2"));
        assert!(sql.contains("WHERE f.ts_bucket >= $1"));
        assert!(sql.contains("AND f.ts_bucket < $2"));
        assert!(!sql.contains("WHERE t.ts_event >= $1"));
        assert!(!sql.contains("AND t.ts_event < $2"));
    }

    #[test]
    fn backfill_source_sql_uses_typed_market_predicate() {
        assert!(TRADE_BACKFILL_WINDOW_SQL.contains("AND market = $4::cfg.market_type"));
        assert!(ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR.contains("AND market = $4::cfg.market_type"));
        assert!(
            ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP.contains("AND market = $4::cfg.market_type")
        );
        assert!(LIQ_BACKFILL_WINDOW_SQL.contains("AND market = $4::cfg.market_type"));
        assert!(FUNDING_BACKFILL_WINDOW_SQL.contains("AND market = $4::cfg.market_type"));
        assert!(!TRADE_BACKFILL_WINDOW_SQL.contains("market::text = $4"));
        assert!(!ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR.contains("market::text = $4"));
        assert!(!ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP.contains("market::text = $4"));
        assert!(!LIQ_BACKFILL_WINDOW_SQL.contains("market::text = $4"));
        assert!(!FUNDING_BACKFILL_WINDOW_SQL.contains("market::text = $4"));
    }

    #[test]
    fn orderbook_backfill_sql_splits_scalar_and_heatmap_paths() {
        assert!(ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR.contains("'[]'::jsonb AS b_heatmap_levels"));
        assert!(ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR.contains("FALSE AS b_heatmap_loaded"));
        assert!(ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP
            .contains("heatmap_levels AS b_heatmap_levels"));
        assert!(ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP.contains("TRUE AS b_heatmap_loaded"));
    }

    #[test]
    fn startup_materialization_heatmap_hydration_is_batch_local() {
        let start = Utc.with_ymd_and_hms(2026, 3, 24, 0, 0, 0).single().unwrap();
        let replay_end = start + ChronoDuration::minutes(800);
        assert_eq!(
            replay_heatmap_hydration_batch_end(start, replay_end),
            start + ChronoDuration::minutes(359)
        );

        let later_start = start + ChronoDuration::minutes(720);
        assert_eq!(
            replay_heatmap_hydration_batch_end(later_start, replay_end),
            replay_end
        );
    }

    #[tokio::test]
    async fn heatmap_hydration_fails_closed_when_rows_remain_missing() {
        let ts = Utc.with_ymd_and_hms(2026, 3, 24, 1, 0, 0).single().unwrap();
        let mut state_store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        state_store.ingest(agg_orderbook_event(ts, false, 0.0));
        assert!(state_store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));

        let err = hydrate_futures_orderbook_heatmaps_for_range_with_fetch(
            "TESTUSDT",
            &mut state_store,
            ts,
            ts + ChronoDuration::minutes(1),
            "test live repair",
            |_from_ts, _to_ts| async { Ok(Vec::new()) },
        )
        .await
        .expect_err("missing heatmap should fail closed");

        let err_text = format!("{err:#}");
        assert!(err_text.contains("left unhydrated futures orderbook heatmap"));
        assert!(state_store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));
    }

    #[test]
    fn startup_backfill_sql_expands_rows_by_source_without_left_join_hash_fanout() {
        let sql = build_backfill_sql(false, true);
        assert!(sql.contains(", expanded AS ("));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_trade_1m t"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_orderbook_1m b"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_liq_1m l"));
        assert!(sql.contains("FROM picked p\n        JOIN md.agg_funding_mark_1m f"));
        assert!(!sql.contains("LEFT JOIN md.agg_trade_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_orderbook_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_liq_1m"));
        assert!(!sql.contains("LEFT JOIN md.agg_funding_mark_1m"));
    }

    #[test]
    fn minute_exclusive_upper_bound_rounds_partial_minute_up() {
        let raw_to_ts = Utc
            .with_ymd_and_hms(2026, 3, 21, 8, 23, 52)
            .single()
            .unwrap();
        let to_ts_exclusive = minute_exclusive_upper_bound(raw_to_ts);
        assert_eq!(
            to_ts_exclusive,
            Utc.with_ymd_and_hms(2026, 3, 21, 8, 24, 0)
                .single()
                .unwrap()
        );
    }

    #[test]
    fn minute_exclusive_upper_bound_keeps_exact_minute_boundary() {
        let raw_to_ts = Utc
            .with_ymd_and_hms(2026, 3, 21, 8, 24, 0)
            .single()
            .unwrap();
        let to_ts_exclusive = minute_exclusive_upper_bound(raw_to_ts);
        assert_eq!(to_ts_exclusive, raw_to_ts);
    }

    #[test]
    fn tail_reconcile_yields_to_live_backlog_and_patch_backlog() {
        let latest_closed = Utc.with_ymd_and_hms(2026, 3, 28, 3, 0, 0).single().unwrap();
        let near_live = latest_closed - ChronoDuration::minutes(3);
        let far_behind = latest_closed - ChronoDuration::minutes(20);
        let mut state_store = StateStore::new("TESTUSDT".to_string(), 1_000.0);

        assert!(allow_live_tail_reconcile(
            &state_store,
            Some(near_live),
            latest_closed
        ));
        assert!(!allow_live_tail_reconcile(
            &state_store,
            Some(far_behind),
            latest_closed
        ));

        state_store.finalize_minute(near_live);
        state_store.ingest(EngineEvent {
            schema_version: 1,
            msg_type: "md.open_interest.hist.5m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.futures.open_interest.hist.5m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: near_live + ChronoDuration::minutes(5),
            published_at: near_live + ChronoDuration::minutes(5),
            data: MdData::OpenInterestHist5m(crate::ingest::decoder::OpenInterestHist5mEvent {
                ts_bucket: near_live,
                open_interest_contracts: 100.0,
                open_interest_value_usdt: 1_000_000.0,
                reference_price: Some(2000.0),
            }),
        });
        assert!(!allow_live_tail_reconcile(
            &state_store,
            Some(near_live),
            latest_closed
        ));
    }

    #[test]
    fn oi_ratio_patch_processing_yields_when_live_backlog_is_large() {
        let latest_closed = Utc.with_ymd_and_hms(2026, 3, 28, 3, 0, 0).single().unwrap();
        let near_live = latest_closed - ChronoDuration::minutes(2);
        let far_behind = latest_closed - ChronoDuration::minutes(18);

        assert!(allow_oi_ratio_patch_processing(
            Some(near_live),
            latest_closed
        ));
        assert!(!allow_oi_ratio_patch_processing(
            Some(far_behind),
            latest_closed
        ));
        assert!(allow_oi_ratio_patch_processing(None, latest_closed));
    }

    #[test]
    fn snapshot_history_contiguity_accepts_strict_minute_series() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let history = vec![
            history_row(start),
            history_row(start + ChronoDuration::minutes(1)),
            history_row(start + ChronoDuration::minutes(2)),
        ];
        assert!(minute_history_is_strictly_contiguous(
            &history,
            start + ChronoDuration::minutes(2)
        ));
    }

    #[test]
    fn snapshot_history_contiguity_rejects_gapped_or_out_of_order_series() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let gapped = vec![
            history_row(start),
            history_row(start + ChronoDuration::minutes(2)),
        ];
        assert!(!minute_history_is_strictly_contiguous(
            &gapped,
            start + ChronoDuration::minutes(2)
        ));

        let out_of_order = vec![
            history_row(start + ChronoDuration::minutes(1)),
            history_row(start),
        ];
        assert!(!minute_history_is_strictly_contiguous(&out_of_order, start));
    }

    #[test]
    fn snapshot_null_price_guard_accepts_short_run() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let mut history = vec![priced_history_row(start, 2000.0)];
        for offset in 1..MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT {
            history.push(history_row(start + ChronoDuration::minutes(offset as i64)));
        }
        history.push(priced_history_row(
            start + ChronoDuration::minutes(MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT as i64),
            2001.0,
        ));

        assert_eq!(
            find_long_null_price_run(&history, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT),
            None
        );
    }

    #[test]
    fn snapshot_null_price_guard_rejects_long_run() {
        let start = Utc.with_ymd_and_hms(2026, 3, 10, 5, 0, 0).single().unwrap();
        let mut history = vec![priced_history_row(start, 2000.0)];
        for offset in 1..=MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT {
            history.push(history_row(start + ChronoDuration::minutes(offset as i64)));
        }

        let run =
            find_long_null_price_run(&history, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT)
                .expect("expected long null-price run");
        assert_eq!(run.0, start + ChronoDuration::minutes(1));
        assert_eq!(
            run.1,
            start + ChronoDuration::minutes(MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT as i64)
        );
        assert_eq!(run.2, MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT);
    }

    #[test]
    fn snapshot_null_price_guard_tolerates_historical_gap_far_from_tail() {
        let last_finalized_ts = Utc
            .with_ymd_and_hms(2026, 3, 20, 22, 48, 0)
            .single()
            .unwrap();
        let run_end = Utc
            .with_ymd_and_hms(2026, 3, 19, 7, 58, 0)
            .single()
            .unwrap();

        assert!(!snapshot_null_price_run_reaches_recent_tail(
            last_finalized_ts,
            run_end,
            1440,
        ));
    }

    #[test]
    fn snapshot_null_price_guard_rejects_gap_that_reaches_recent_tail() {
        let last_finalized_ts = Utc
            .with_ymd_and_hms(2026, 3, 20, 22, 48, 0)
            .single()
            .unwrap();
        let run_end = last_finalized_ts - ChronoDuration::minutes(30);

        assert!(snapshot_null_price_run_reaches_recent_tail(
            last_finalized_ts,
            run_end,
            1440,
        ));
    }

    #[test]
    fn snapshot_required_history_accepts_reusable_rolling_7d_window() {
        let last_finalized_ts = Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).single().unwrap();
        let effective_floor_ts = last_finalized_ts - ChronoDuration::minutes(90);
        let required_minutes = MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES - 1;
        let history_start_ts = last_finalized_ts - ChronoDuration::minutes(required_minutes);
        let history = (0..=required_minutes)
            .map(|offset| {
                priced_history_row(history_start_ts + ChronoDuration::minutes(offset), 2000.0)
            })
            .collect::<Vec<_>>();
        let snap = snapshot_fixture(
            last_finalized_ts,
            history.clone(),
            history,
            Some(effective_floor_ts),
        );

        assert!(snapshot_has_required_history(&snap));
    }

    #[test]
    fn snapshot_required_history_rejects_short_history_even_if_it_starts_at_effective_floor() {
        let last_finalized_ts = Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).single().unwrap();
        let effective_floor_ts = last_finalized_ts - ChronoDuration::minutes(90);
        let history = (0..=90)
            .map(|offset| {
                priced_history_row(effective_floor_ts + ChronoDuration::minutes(offset), 2000.0)
            })
            .collect::<Vec<_>>();
        let snap = snapshot_fixture(
            last_finalized_ts,
            history.clone(),
            history,
            Some(effective_floor_ts),
        );

        assert!(!snapshot_has_required_history(&snap));
    }

    #[test]
    fn snapshot_required_history_rejects_short_history_without_effective_floor() {
        let last_finalized_ts = Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).single().unwrap();
        let history_start_ts =
            last_finalized_ts - ChronoDuration::minutes((HISTORY_LIMIT_MINUTES as i64) - 10);
        let history = (0..10)
            .map(|offset| {
                priced_history_row(history_start_ts + ChronoDuration::minutes(offset), 2000.0)
            })
            .collect::<Vec<_>>();
        let snap = snapshot_fixture(last_finalized_ts, history.clone(), history, None);

        assert!(!snapshot_has_required_history(&snap));
    }

    #[test]
    fn cutover_gap_preserves_warm_state_and_waits_for_gap_repair() {
        let cutoff_bucket_ts = Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).single().unwrap();
        let prior_bucket_ts = cutoff_bucket_ts - ChronoDuration::minutes(1);
        let first_live_bucket_ts = cutoff_bucket_ts + ChronoDuration::minutes(5);
        let mut state_store =
            crate::runtime::state_store::StateStore::new("TESTUSDT".to_string(), 1_000.0);
        state_store.ingest(frontier_event(
            prior_bucket_ts + ChronoDuration::seconds(5),
            MdData::Trade(TradeEvent {
                price: 2000.0,
                qty_eth: 1.0,
                notional_usdt: 2000.0,
                aggressor_side: 1,
            }),
        ));
        state_store.finalize_minute(prior_bucket_ts);

        let mut startup_cutover_completed = false;
        let mut stale_drop_count = 0_u64;
        let mut stale_drop_max_lag_secs = 0_i64;
        let mut stale_drop_max_publish_delay_secs = 0_i64;
        let mut stale_drop_max_transport_lag_secs = 0_i64;
        let mut stale_drop_oldest_ts = None;
        let mut stale_drop_newest_ts = None;
        let mut stale_drop_by_msg_type = HashMap::new();
        let metrics = Arc::new(AppMetrics::default());
        let mut scheduler = WindowScheduler::new(0);
        scheduler.mark_emitted_through(prior_bucket_ts);

        handle_ingest_event(
            frontier_event(
                first_live_bucket_ts + ChronoDuration::seconds(5),
                MdData::Trade(TradeEvent {
                    price: 2100.0,
                    qty_eth: 1.0,
                    notional_usdt: 2100.0,
                    aggressor_side: 1,
                }),
            ),
            Some(cutoff_bucket_ts),
            &mut startup_cutover_completed,
            false,
            false,
            0,
            &mut stale_drop_count,
            &mut stale_drop_max_lag_secs,
            &mut stale_drop_max_publish_delay_secs,
            &mut stale_drop_max_transport_lag_secs,
            &mut stale_drop_oldest_ts,
            &mut stale_drop_newest_ts,
            &mut stale_drop_by_msg_type,
            &metrics,
            &mut state_store,
            &mut scheduler,
        );

        assert!(startup_cutover_completed);
        assert_eq!(state_store.history_futures_len(), 1);
        assert_eq!(scheduler.next_minute_to_emit(), Some(cutoff_bucket_ts));
        assert_eq!(
            state_store.latest_contiguous_complete_canonical_minute_from(
                cutoff_bucket_ts,
                first_live_bucket_ts + ChronoDuration::minutes(2)
            ),
            None
        );
    }

    #[test]
    fn live_tail_reconcile_start_ts_respects_effective_history_floor() {
        let last_finalized = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let effective_floor = last_finalized - ChronoDuration::minutes(30);

        assert_eq!(
            live_tail_reconcile_start_ts(last_finalized, Some(effective_floor)),
            effective_floor
        );
    }

    #[test]
    fn live_tail_reconcile_start_ts_uses_lookback_when_floor_is_older() {
        let last_finalized = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let effective_floor = last_finalized - ChronoDuration::minutes(600);

        assert_eq!(
            live_tail_reconcile_start_ts(last_finalized, Some(effective_floor)),
            last_finalized
                - ChronoDuration::minutes(LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES)
        );
    }

    #[test]
    fn live_gap_repair_controller_throttles_same_blocking_minute() {
        let blocking_minute = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let next_minute = blocking_minute + ChronoDuration::minutes(1);
        let mut controller = LiveCanonicalRepairController::default();

        assert!(controller.gap_repair_due(blocking_minute));
        controller.mark_gap_repair_attempt(blocking_minute);
        assert!(!controller.gap_repair_due(blocking_minute));
        assert!(controller.gap_repair_due(next_minute));
    }

    #[test]
    fn shutdown_ready_through_candidate_freezes_at_shutdown_cutoff() {
        let shutdown_closed_minute = Utc.with_ymd_and_hms(2026, 3, 24, 7, 0, 0).single().unwrap();
        let next_minute = shutdown_closed_minute - ChronoDuration::minutes(2);

        assert_eq!(
            shutdown_ready_through_candidate(Some(next_minute), shutdown_closed_minute),
            Some(shutdown_closed_minute)
        );
    }

    #[test]
    fn shutdown_ready_through_candidate_rejects_minutes_beyond_shutdown_cutoff() {
        let shutdown_closed_minute = Utc.with_ymd_and_hms(2026, 3, 24, 7, 0, 0).single().unwrap();
        let next_minute = shutdown_closed_minute + ChronoDuration::minutes(1);

        assert_eq!(
            shutdown_ready_through_candidate(Some(next_minute), shutdown_closed_minute),
            None
        );
    }

    #[test]
    fn dirty_enqueue_is_deferred_when_live_queue_has_work() {
        let ready_through_ts = Utc
            .with_ymd_and_hms(2026, 3, 24, 7, 10, 0)
            .single()
            .unwrap();
        let next_live_minute = Some(ready_through_ts - ChronoDuration::minutes(1));

        assert!(!super::should_enqueue_dirty_ready_jobs(
            1,
            0,
            next_live_minute,
            ready_through_ts,
            true
        ));
        assert!(!super::should_enqueue_dirty_ready_jobs(
            0,
            2,
            next_live_minute,
            ready_through_ts,
            true
        ));
        assert!(!super::should_enqueue_dirty_ready_jobs(
            0,
            0,
            next_live_minute,
            ready_through_ts,
            true
        ));
    }

    #[test]
    fn dirty_enqueue_is_allowed_only_when_live_is_fully_idle() {
        let ready_through_ts = Utc
            .with_ymd_and_hms(2026, 3, 24, 7, 10, 0)
            .single()
            .unwrap();
        let next_live_minute = Some(ready_through_ts + ChronoDuration::minutes(1));

        assert!(super::should_enqueue_dirty_ready_jobs(
            0,
            0,
            next_live_minute,
            ready_through_ts,
            true
        ));
        assert!(!super::should_enqueue_dirty_ready_jobs(
            0,
            0,
            next_live_minute,
            ready_through_ts,
            false
        ));
    }
}
