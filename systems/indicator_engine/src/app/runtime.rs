use crate::app::bootstrap::{build_db_pool, AppContext, DbPoolConfig, RootConfig};
use crate::indicators::context::{
    daily_window_days, window_code_minutes as context_window_code_minutes, DivergenceSigTestMode,
    IndicatorContext, IndicatorRuntimeOptions, IndicatorSnapshotRow, KlineHistoryBar,
    KlineHistorySupplement, OptionsSurfacePoint,
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
use crate::runtime::dispatcher::{DispatchMode, Dispatcher, ProcessedWindowArtifacts};
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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
const MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES: i64 = 30 * 24 * 60;
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
const LIVE_COMPUTED_JOB_QUEUE_CAPACITY: usize = LIVE_READY_JOB_QUEUE_CAPACITY * 3;
const LIVE_MATERIALIZE_WORKER_COUNT: usize = 3;
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
const STARTUP_BACKFILL_PROGRESS_LOG_INTERVAL_SECS: u64 = 15;
const BACKFILL_PAGED_FETCH_DEFAULT_LIMIT: i64 = 1_000;
const OPTIONS_SURFACE_BUCKET_MINUTES: i64 = 5;
const PERIODIC_RUNTIME_SNAPSHOT_POLL_SECS: u64 = 60;
const PERIODIC_RUNTIME_SNAPSHOT_INTERVAL_SECS: u64 = 15 * 60;
const PERIODIC_RUNTIME_SNAPSHOT_MIN_ADVANCE_MINUTES: i64 = 100;
const MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT: usize = 60;
const MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT: usize = 1440;
const STARTUP_BACKFILL_CHECKPOINT_VERSION: u32 = 1;

enum SnapshotLoadOutcome {
    Fresh(StateSnapshot),
    StaleRecoverySeed { snap: StateSnapshot, age_hours: i64 },
    Rejected,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StartupBackfillCheckpoint {
    version: u32,
    symbol: String,
    saved_at: DateTime<Utc>,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
    next_canonical_window_from_ts: Option<DateTime<Utc>>,
    snapshot_was_loaded: bool,
    persisted_frontier_ts: Option<DateTime<Utc>>,
    snapshot: StateSnapshot,
}

#[derive(Debug, Clone, Default)]
struct StartupBackfillProgress {
    from_ts: Option<DateTime<Utc>>,
    to_ts_exclusive: Option<DateTime<Utc>>,
    next_canonical_window_from_ts: Option<DateTime<Utc>>,
    snapshot_was_loaded: bool,
    persisted_frontier_ts: Option<DateTime<Utc>>,
}

impl StartupBackfillProgress {
    fn record(
        &mut self,
        from_ts: DateTime<Utc>,
        to_ts_exclusive: DateTime<Utc>,
        next_canonical_window_from_ts: Option<DateTime<Utc>>,
        snapshot_was_loaded: bool,
        persisted_frontier_ts: Option<DateTime<Utc>>,
    ) {
        self.from_ts = Some(from_ts);
        self.to_ts_exclusive = Some(to_ts_exclusive);
        self.next_canonical_window_from_ts = next_canonical_window_from_ts;
        self.snapshot_was_loaded = snapshot_was_loaded;
        self.persisted_frontier_ts = persisted_frontier_ts;
    }

    fn to_checkpoint(
        &self,
        symbol: &str,
        snapshot: StateSnapshot,
    ) -> Option<StartupBackfillCheckpoint> {
        Some(StartupBackfillCheckpoint {
            version: STARTUP_BACKFILL_CHECKPOINT_VERSION,
            symbol: symbol.to_string(),
            saved_at: Utc::now(),
            from_ts: self.from_ts?,
            to_ts_exclusive: self.to_ts_exclusive?,
            next_canonical_window_from_ts: self.next_canonical_window_from_ts,
            snapshot_was_loaded: self.snapshot_was_loaded,
            persisted_frontier_ts: self.persisted_frontier_ts,
            snapshot,
        })
    }
}

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
    pub row_tid_text: String,
}

#[derive(Debug, Clone)]
pub struct BackfillCursor {
    pub event_ts: DateTime<Utc>,
    pub msg_type: String,
    pub market: String,
    pub symbol: String,
    pub routing_key: String,
    pub row_tid_text: String,
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

struct ComputedMinuteJob {
    ts_bucket: DateTime<Utc>,
    source: ReadyJobSource,
    enqueued_at: Instant,
    artifacts: ProcessedWindowArtifacts,
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
        kline_history_bars_7d: config.indicator.kline_history.bars_7d,
        kline_history_bars_30d: config.indicator.kline_history.bars_30d,
        kline_history_fill_1d_from_db: config.indicator.kline_history.fill_1d_from_db,
        fvg_windows: config.indicator.fvg.windows.clone(),
        fvg_fill_from_db: config.indicator.fvg.fill_from_db,
        fvg_db_bars_4h: config.indicator.fvg.db_bars_4h,
        fvg_db_bars_1d: config.indicator.fvg.db_bars_1d,
        fvg_db_bars_3d: config.indicator.fvg.db_bars_3d,
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
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
            .collect(),
        tpo_ib_minutes: runtime_options.tpo_ib_minutes,
        tpo_dev_output_windows: runtime_options
            .tpo_dev_output_windows
            .iter()
            .filter_map(|code| window_code_minutes(code).map(|minutes| (code.clone(), minutes)))
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
    context_window_code_minutes(code)
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
    let snapshot_path = ctx
        .config
        .indicator
        .snapshot_file_path
        .replace("{symbol}", &ctx.config.indicator.symbol);
    let startup_checkpoint_path = startup_backfill_checkpoint_path(&snapshot_path);
    let startup_backfill_batch_size = ctx.config.indicator.startup_backfill_batch_size.max(100);

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
    let startup_backfill_progress = Arc::new(Mutex::new(StartupBackfillProgress::default()));
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
            &snapshot_path,
            startup_checkpoint_path.as_deref(),
            startup_backfill_progress.clone(),
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
        let snap = state_store.extract_snapshot();
        let history_len = snap.history_futures.len();
        let checkpoint_progress = startup_backfill_progress.lock().await.clone();
        if let (Some(path), Some(checkpoint)) = (
            startup_checkpoint_path.as_deref(),
            checkpoint_progress.to_checkpoint(&ctx.config.indicator.symbol, snap.clone()),
        ) {
            match save_startup_backfill_checkpoint(&checkpoint, path).await {
                Ok(()) => info!(
                    path = %path,
                    from_ts = %checkpoint.from_ts,
                    to_ts_exclusive = %checkpoint.to_ts_exclusive,
                    next_canonical_window_from_ts = ?checkpoint.next_canonical_window_from_ts,
                    "startup backfill checkpoint saved"
                ),
                Err(err) => {
                    warn!(error = %err, path = %path, "failed to save startup backfill checkpoint")
                }
            }
        }
        if !snapshot_path.is_empty() && snapshot_is_reusable_recovery_seed(&snap) {
            info!(
                history_bars = history_len,
                "Saving state snapshot before exit (mid-backfill)..."
            );
            match save_state_snapshot(&snap, &snapshot_path).await {
                Ok(()) => {
                    remove_startup_backfill_checkpoint(startup_checkpoint_path.as_deref());
                    info!(path = %snapshot_path, history_bars = history_len, "State snapshot saved (mid-backfill)")
                }
                Err(e) => warn!(error = %e, "Failed to save state snapshot"),
            }
        } else {
            info!(
                history_bars = history_len,
                effective_history_floor_ts = ?snap.effective_history_floor_ts,
                "Skipping snapshot save — state does not yet cover a reusable restart seed"
            );
        }
        heartbeat_handle.abort();
        for h in consumer_handles {
            h.abort();
        }
        return Ok(());
    }
    remove_startup_backfill_checkpoint(startup_checkpoint_path.as_deref());
    metrics.set_backfill_mode(false);

    let state_store = Arc::new(Mutex::new(state_store));
    let mut periodic_snapshot_handle = if snapshot_path.is_empty() {
        None
    } else {
        Some(tokio::spawn(run_periodic_runtime_snapshot_loop(
            state_store.clone(),
            snapshot_path.clone(),
        )))
    };
    let (live_ready_job_tx_raw, live_ready_job_rx_raw) =
        mpsc::channel(LIVE_READY_JOB_QUEUE_CAPACITY);
    let mut live_ready_job_tx = Some(live_ready_job_tx_raw);
    let live_ready_job_pending = Arc::new(AtomicUsize::new(0));
    let live_ready_job_rx = Arc::new(Mutex::new(live_ready_job_rx_raw));
    let (live_computed_job_tx, live_computed_job_rx) =
        mpsc::channel(LIVE_COMPUTED_JOB_QUEUE_CAPACITY);
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
    let mut live_materialize_handles = (0..LIVE_MATERIALIZE_WORKER_COUNT)
        .map(|_| {
            tokio::spawn(run_live_materialize_compute_loop(
                ctx.clone(),
                dispatcher.clone(),
                runtime_options.clone(),
                live_ready_job_rx.clone(),
                live_computed_job_tx.clone(),
            ))
        })
        .collect::<Vec<_>>();
    drop(live_computed_job_tx);
    let live_commit_initial_persisted_ts = ts_from_millis(metrics.snapshot().last_persisted_ts_ms);
    let mut live_commit_handle = Some(tokio::spawn(run_live_ordered_commit_loop(
        metrics.clone(),
        dispatcher.clone(),
        live_computed_job_rx,
        live_ready_job_pending.clone(),
        live_commit_initial_persisted_ts,
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
                poll_live_materialize_handles(&mut live_materialize_handles).await?;
                poll_materialize_handle(&mut live_commit_handle, MaterializeWorkerKind::Live)
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
                                startup_backfill_batch_size,
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
                                startup_backfill_batch_size,
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

                let next_minute_before_ready = scheduler.next_minute_to_emit();
                let leading_gap_recovery = {
                    let mut state_store = state_store.lock().await;
                    maybe_recover_from_leading_canonical_gap(
                        &mut state_store,
                        &mut scheduler,
                        next_minute_before_ready,
                    )
                    .await
                };
                if let Some(plan) = leading_gap_recovery {
                    info!(
                        blocked_minute = ?plan.blocked_minute,
                        history_floor_ts = %plan.history_floor_ts,
                        warm_end_ts = ?plan.warm_end_ts,
                        replay_start_ts = %plan.replay_start_ts,
                        continuity_end_ts = %plan.continuity_end_ts,
                        warmed_minutes = plan.warmed_minutes,
                        next_minute_after = ?scheduler.next_minute_to_emit(),
                        "recovered live runtime from leading canonical gap by seeding the latest continuous segment"
                    );
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
        drain_live_materialize_handles(&mut live_materialize_handles).await;
        drain_materialize_handle(&mut live_commit_handle, MaterializeWorkerKind::Live).await;
        drain_materialize_handle(
            &mut dirty_materialize_handle,
            MaterializeWorkerKind::DirtyRecompute,
        )
        .await;
        outbox_handle.abort();
        snapshot_fanout_handle.abort();
        abort_periodic_runtime_snapshot_handle(&mut periodic_snapshot_handle).await;

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

        if !snapshot_path.is_empty() {
            info!("Saving state snapshot before exit...");
            let snap = state_store.extract_snapshot();
            if snapshot_is_reusable_recovery_seed(&snap) {
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
            } else {
                info!(
                    futures_bars = snap.history_futures.len(),
                    spot_bars = snap.history_spot.len(),
                    last_ts = %snap.last_finalized_ts,
                    effective_history_floor_ts = ?snap.effective_history_floor_ts,
                    "Skipping snapshot save — state does not meet restart reuse requirements"
                );
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
    abort_live_materialize_handles(&mut live_materialize_handles).await;
    if let Some(handle) = live_commit_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    if let Some(handle) = dirty_materialize_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    outbox_handle.abort();
    snapshot_fanout_handle.abort();
    abort_periodic_runtime_snapshot_handle(&mut periodic_snapshot_handle).await;
    for h in consumer_handles {
        h.abort();
    }

    let state_store = Arc::try_unwrap(state_store)
        .map_err(|_| anyhow::anyhow!("state_store still shared while saving snapshot"))?
        .into_inner();
    let state_store = state_store;
    if !snapshot_path.is_empty() {
        info!("Saving state snapshot before exit...");
        let snap = state_store.extract_snapshot();
        if snapshot_is_reusable_recovery_seed(&snap) {
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
        } else {
            info!(
                futures_bars = snap.history_futures.len(),
                spot_bars = snap.history_spot.len(),
                last_ts = %snap.last_finalized_ts,
                effective_history_floor_ts = ?snap.effective_history_floor_ts,
                "Skipping snapshot save — state does not meet restart reuse requirements"
            );
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

fn startup_backfill_checkpoint_path(snapshot_path: &str) -> Option<String> {
    if snapshot_path.is_empty() {
        return None;
    }
    if let Some(base) = snapshot_path.strip_suffix(".json.gz") {
        return Some(format!("{base}.startup_backfill.json.gz"));
    }
    if let Some(base) = snapshot_path.strip_suffix(".gz") {
        return Some(format!("{base}.startup_backfill.gz"));
    }
    Some(format!("{snapshot_path}.startup_backfill"))
}

fn load_gzip_json<T: serde::de::DeserializeOwned>(path: &str) -> Option<T> {
    use flate2::read::GzDecoder;
    use std::fs::File;

    let file = File::open(path).ok()?;
    let gz = GzDecoder::new(file);
    serde_json::from_reader(gz).ok()
}

fn try_load_state_snapshot(path: &str, symbol: &str, max_age_hours: u64) -> SnapshotLoadOutcome {
    if path.is_empty() {
        return SnapshotLoadOutcome::Rejected;
    }
    let Some(snap) = load_gzip_json::<StateSnapshot>(path) else {
        return SnapshotLoadOutcome::Rejected;
    };
    // Version check
    let supports_previous_version =
        STATE_SNAPSHOT_VERSION > 1 && snap.version == STATE_SNAPSHOT_VERSION.saturating_sub(1);
    if snap.version != STATE_SNAPSHOT_VERSION && !supports_previous_version {
        warn!(
            found = snap.version,
            expected = STATE_SNAPSHOT_VERSION,
            "State snapshot version mismatch, ignoring"
        );
        return SnapshotLoadOutcome::Rejected;
    }
    // Symbol check
    if snap.symbol != symbol {
        warn!(snap_symbol = %snap.symbol, "State snapshot symbol mismatch, ignoring");
        return SnapshotLoadOutcome::Rejected;
    }
    if !snapshot_has_required_history(&snap) {
        return SnapshotLoadOutcome::Rejected;
    }
    if !minute_history_is_strictly_contiguous(&snap.history_futures, snap.last_finalized_ts) {
        warn!("State snapshot futures history is not a strict contiguous minute series, ignoring");
        return SnapshotLoadOutcome::Rejected;
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
            return SnapshotLoadOutcome::Rejected;
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
        return SnapshotLoadOutcome::Rejected;
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
            return SnapshotLoadOutcome::Rejected;
        }
        info!(
            run_start = %start,
            run_end = %end,
            run_len = len,
            min_recent_bars_after_run = MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
            "State snapshot spot history contains only historical long null-price run, accepting snapshot"
        );
    }
    let age = Utc::now().signed_duration_since(snap.saved_at);
    if age > chrono::Duration::hours(max_age_hours as i64) {
        warn!(
            age_hours = age.num_hours(),
            max_age_hours,
            "State snapshot too old for fast restart; using it only as a recovery seed"
        );
        SnapshotLoadOutcome::StaleRecoverySeed {
            snap,
            age_hours: age.num_hours(),
        }
    } else {
        SnapshotLoadOutcome::Fresh(snap)
    }
}

fn try_load_startup_backfill_checkpoint(
    path: Option<&str>,
    symbol: &str,
) -> Option<StartupBackfillCheckpoint> {
    let path = path?;
    let checkpoint = load_gzip_json::<StartupBackfillCheckpoint>(path)?;
    if checkpoint.version != STARTUP_BACKFILL_CHECKPOINT_VERSION {
        warn!(
            found = checkpoint.version,
            expected = STARTUP_BACKFILL_CHECKPOINT_VERSION,
            path = %path,
            "startup backfill checkpoint version mismatch, ignoring"
        );
        return None;
    }
    if checkpoint.symbol != symbol {
        warn!(
            checkpoint_symbol = %checkpoint.symbol,
            symbol = %symbol,
            path = %path,
            "startup backfill checkpoint symbol mismatch, ignoring"
        );
        return None;
    }
    if checkpoint.next_canonical_window_from_ts.is_some()
        && checkpoint.snapshot.canonical_minutes.is_empty()
    {
        warn!(
            path = %path,
            "startup backfill checkpoint has no canonical replay state, ignoring"
        );
        return None;
    }
    Some(checkpoint)
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

fn minimum_startup_recovery_history_floor(to_ts: DateTime<Utc>) -> DateTime<Utc> {
    to_ts - ChronoDuration::minutes(MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES)
}

fn expand_startup_backfill_to_minimum_recovery_window(
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let minimum_recovery_floor = minimum_startup_recovery_history_floor(to_ts);
    if from_ts > minimum_recovery_floor {
        Some(minimum_recovery_floor)
    } else {
        None
    }
}

fn snapshot_is_reusable_recovery_seed(snap: &StateSnapshot) -> bool {
    if !snapshot_has_required_history(snap) {
        return false;
    }
    if !minute_history_is_strictly_contiguous(&snap.history_futures, snap.last_finalized_ts) {
        return false;
    }
    if let Some((_, end, _)) = find_long_null_price_run(
        &snap.history_futures,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        if snapshot_null_price_run_reaches_recent_tail(
            snap.last_finalized_ts,
            end,
            MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
        ) {
            return false;
        }
    }
    if !snap.history_spot.is_empty()
        && !minute_history_is_strictly_contiguous(&snap.history_spot, snap.last_finalized_ts)
    {
        return false;
    }
    if let Some((_, end, _)) = find_long_null_price_run(
        &snap.history_spot,
        MAX_CONSECUTIVE_NULL_PRICE_MINUTES_IN_SNAPSHOT,
    ) {
        if snapshot_null_price_run_reaches_recent_tail(
            snap.last_finalized_ts,
            end,
            MIN_RECENT_BARS_AFTER_NULL_RUN_IN_SNAPSHOT,
        ) {
            return false;
        }
    }
    true
}

async fn save_gzip_json_atomic<T: serde::Serialize>(value: &T, path: &str) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;

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
    serde_json::to_writer(gz, value)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

async fn save_state_snapshot(snap: &StateSnapshot, path: &str) -> anyhow::Result<()> {
    save_gzip_json_atomic(snap, path).await
}

async fn save_startup_backfill_checkpoint(
    checkpoint: &StartupBackfillCheckpoint,
    path: &str,
) -> anyhow::Result<()> {
    save_gzip_json_atomic(checkpoint, path).await
}

fn remove_startup_backfill_checkpoint(path: Option<&str>) {
    let Some(path) = path else {
        return;
    };
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(error = %err, path = %path, "failed to remove startup backfill checkpoint")
        }
    }
}

async fn run_periodic_runtime_snapshot_loop(
    state_store: Arc<Mutex<StateStore>>,
    snapshot_path: String,
) {
    let mut tick = interval(Duration::from_secs(PERIODIC_RUNTIME_SNAPSHOT_POLL_SECS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut last_snapshot_attempt_at =
        Instant::now() - Duration::from_secs(PERIODIC_RUNTIME_SNAPSHOT_INTERVAL_SECS);
    let mut last_saved_finalized_ts: Option<DateTime<Utc>> = None;

    loop {
        tick.tick().await;

        let current_finalized_ts = {
            let state_store = state_store.lock().await;
            state_store.last_finalized_minute()
        };
        let Some(current_finalized_ts) = current_finalized_ts else {
            continue;
        };

        let has_new_progress = last_saved_finalized_ts
            .map(|last_saved| current_finalized_ts > last_saved)
            .unwrap_or(true);
        if !has_new_progress {
            continue;
        }

        let due_by_interval = last_snapshot_attempt_at.elapsed()
            >= Duration::from_secs(PERIODIC_RUNTIME_SNAPSHOT_INTERVAL_SECS);
        let due_by_finalized_advance = last_saved_finalized_ts
            .map(|last_saved| {
                (current_finalized_ts - last_saved).num_minutes()
                    >= PERIODIC_RUNTIME_SNAPSHOT_MIN_ADVANCE_MINUTES
            })
            .unwrap_or(false);
        if !(due_by_interval || due_by_finalized_advance) {
            continue;
        }

        last_snapshot_attempt_at = Instant::now();
        let snap = {
            let state_store = state_store.lock().await;
            state_store.extract_snapshot()
        };

        if !snapshot_is_reusable_recovery_seed(&snap) {
            debug!(
                path = %snapshot_path,
                last_ts = %snap.last_finalized_ts,
                effective_history_floor_ts = ?snap.effective_history_floor_ts,
                "skipping periodic runtime snapshot save because state is not yet reusable"
            );
            continue;
        }

        match save_state_snapshot(&snap, &snapshot_path).await {
            Ok(()) => {
                last_saved_finalized_ts = Some(snap.last_finalized_ts);
                info!(
                    path = %snapshot_path,
                    futures_bars = snap.history_futures.len(),
                    spot_bars = snap.history_spot.len(),
                    last_ts = %snap.last_finalized_ts,
                    "periodic runtime snapshot saved"
                );
            }
            Err(err) => {
                warn!(
                    error = %err,
                    path = %snapshot_path,
                    last_ts = %snap.last_finalized_ts,
                    "periodic runtime snapshot save failed"
                );
            }
        }
    }
}

async fn abort_periodic_runtime_snapshot_handle(handle_slot: &mut Option<JoinHandle<()>>) {
    let Some(handle) = handle_slot.take() else {
        return;
    };
    handle.abort();
    if let Err(err) = handle.await {
        if !err.is_cancelled() {
            warn!(
                error = %err,
                "periodic runtime snapshot task join failed during shutdown"
            );
        }
    }
}

pub async fn load_kline_history_supplement(
    pool: &PgPool,
    symbol: &str,
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
    bars_4h: usize,
    bars_1d: usize,
    bars_3d: usize,
    bars_7d: usize,
    bars_30d: usize,
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
    fvg_db_bars_3d: usize,
    current_minute_close: DateTime<Utc>,
) -> KlineHistorySupplement {
    if !fill_1d_from_db && !ema_fill_from_db && !fvg_fill_from_db && bars_4h == 0 {
        return KlineHistorySupplement::default();
    }

    let fvg_needs_4h = fvg_fill_from_db && fvg_windows.iter().any(|code| code == "4h");
    let max_fvg_daily_span = if fvg_fill_from_db {
        fvg_windows
            .iter()
            .filter_map(|code| daily_window_days(code))
            .max()
            .unwrap_or(0)
    } else {
        0
    };
    let max_ema_daily_span = if ema_fill_from_db {
        ema_htf_windows
            .iter()
            .filter_map(|code| daily_window_days(code))
            .max()
            .unwrap_or(0)
    } else {
        0
    };

    let in_mem_futures_1d =
        build_interval_bar_records(history_futures, 1440, usize::MAX, current_minute_close);
    let in_mem_spot_1d =
        build_interval_bar_records(history_spot, 1440, usize::MAX, current_minute_close);
    let in_mem_futures_4h =
        build_interval_bar_records(history_futures, 240, usize::MAX, current_minute_close);
    let in_mem_spot_4h =
        build_interval_bar_records(history_spot, 240, usize::MAX, current_minute_close);

    let required_daily_bars = if fill_1d_from_db {
        bars_1d
            .max(bars_3d.saturating_mul(3))
            .max(bars_7d.saturating_mul(7))
            .max(bars_30d.saturating_mul(30))
    } else {
        0
    };
    let required_futures_1d = required_daily_bars
        .max(if ema_fill_from_db { ema_db_bars_1d } else { 0 })
        .max(if max_ema_daily_span > 1 {
            ema_db_bars_3d.saturating_mul(max_ema_daily_span)
        } else {
            0
        })
        .max(if max_fvg_daily_span > 0 {
            if max_fvg_daily_span > 1 {
                fvg_db_bars_3d.saturating_mul(max_fvg_daily_span)
            } else {
                fvg_db_bars_1d
            }
        } else {
            0
        });
    let required_spot_1d = required_daily_bars;
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

fn floor_timestamp_to_interval_minutes(ts: DateTime<Utc>, interval_minutes: i64) -> DateTime<Utc> {
    let interval_secs = interval_minutes.saturating_mul(60).max(60);
    let ts_secs = ts.timestamp();
    let floored = ts_secs - ts_secs.rem_euclid(interval_secs);
    Utc.timestamp_opt(floored, 0).single().unwrap_or(ts)
}

fn merge_time_ranges(
    mut ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    ranges.retain(|(start, end)| start < end);
    if ranges.is_empty() {
        return ranges;
    }

    ranges.sort_by_key(|(start, _)| *start);
    let mut merged = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, current_end)) = merged.last_mut() {
            if start <= *current_end {
                *current_end = (*current_end).max(end);
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

async fn load_options_surface_feature_recovery_seed(
    pool: &PgPool,
    symbol: &str,
    from_ts: DateTime<Utc>,
    to_ts_exclusive: DateTime<Utc>,
) -> Vec<OptionsSurfacePoint> {
    if from_ts >= to_ts_exclusive {
        return Vec::new();
    }

    let rows_result: Result<Vec<OptionsSurfaceFeatureRow>> = sqlx::query_as(
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
          AND calc_version = 'indicator_engine.v1'
          AND ts_bucket >= $2
          AND ts_bucket < $3
        ORDER BY ts_bucket ASC
        "#,
    )
    .bind(symbol.to_uppercase())
    .bind(from_ts)
    .bind(to_ts_exclusive)
    .fetch_all(pool)
    .await
    .context("query options surface recovery seed rows");

    let rows = match rows_result {
        Ok(rows) => rows,
        Err(err) => {
            warn!(
                error = %err,
                symbol = %symbol,
                from_ts = %from_ts,
                to_ts_exclusive = %to_ts_exclusive,
                "load options surface recovery seed from DB failed"
            );
            return Vec::new();
        }
    };

    rows.into_iter()
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
        .collect()
}

async fn replay_option_mark_greeks_recovery_ranges(
    pool: &PgPool,
    symbol: &str,
    state_store: &mut StateStore,
    ranges: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    reason: &'static str,
) -> Result<usize> {
    let merged_ranges = merge_time_ranges(ranges);
    if merged_ranges.is_empty() {
        return Ok(0);
    }

    let symbol_upper = symbol.to_uppercase();
    let mut replayed_rows = 0usize;
    for (range_from_ts, range_to_ts_exclusive) in merged_ranges {
        let rows = fetch_backfill_source_rows(
            pool,
            OPTION_MARK_GREEKS_5M_BACKFILL_WINDOW_SQL,
            "option_mark_greeks_5m",
            range_from_ts,
            range_to_ts_exclusive,
            &symbol_upper,
            "futures",
        )
        .await
        .with_context(|| {
            format!(
                "{reason} fetch option_mark_greeks recovery rows from_ts={range_from_ts} to_ts_exclusive={range_to_ts_exclusive}"
            )
        })?;

        for row in rows {
            let event = replay_row_to_engine_event(row).with_context(|| {
                format!(
                    "{reason} decode option_mark_greeks recovery row from_ts={range_from_ts} to_ts_exclusive={range_to_ts_exclusive}"
                )
            })?;
            state_store.ingest(event);
            replayed_rows += 1;
        }
    }

    Ok(replayed_rows)
}

async fn hydrate_options_surface_startup_recovery(
    pool: &PgPool,
    symbol: &str,
    state_store: &mut StateStore,
    feature_seed_from_ts: DateTime<Utc>,
    repair_start_ts: DateTime<Utc>,
    replay_end_ts: DateTime<Utc>,
) -> Result<(usize, usize, usize)> {
    let options_repair_bucket_start =
        floor_timestamp_to_interval_minutes(repair_start_ts, OPTIONS_SURFACE_BUCKET_MINUTES);
    let options_replay_to_ts_exclusive = replay_end_ts + ChronoDuration::minutes(1);
    let feature_seed_to_ts_exclusive =
        options_repair_bucket_start.min(options_replay_to_ts_exclusive);

    let seeded_points = if feature_seed_from_ts < feature_seed_to_ts_exclusive {
        let feature_points = load_options_surface_feature_recovery_seed(
            pool,
            symbol,
            feature_seed_from_ts,
            feature_seed_to_ts_exclusive,
        )
        .await;
        state_store.seed_options_surface_history(feature_points)
    } else {
        0
    };

    let mut raw_ranges = if feature_seed_from_ts < feature_seed_to_ts_exclusive {
        state_store.missing_options_surface_bucket_ranges(
            feature_seed_from_ts,
            feature_seed_to_ts_exclusive,
        )
    } else {
        Vec::new()
    };
    if options_repair_bucket_start < options_replay_to_ts_exclusive {
        raw_ranges.push((options_repair_bucket_start, options_replay_to_ts_exclusive));
    }
    let merged_raw_ranges = merge_time_ranges(raw_ranges);
    let raw_range_count = merged_raw_ranges.len();
    let replayed_rows = replay_option_mark_greeks_recovery_ranges(
        pool,
        symbol,
        state_store,
        merged_raw_ranges,
        "startup options surface recovery",
    )
    .await?;

    Ok((seeded_points, raw_range_count, replayed_rows))
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
    context_window_code_minutes(interval_code).unwrap_or(1)
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
    batch_limit: i64,
    reason: &'static str,
) -> Result<CanonicalRepairStats> {
    if from_ts >= to_ts_exclusive {
        return Ok(CanonicalRepairStats::default());
    }

    let mut stats = CanonicalRepairStats::default();
    let mut window_from_ts = from_ts;
    let effective_batch_limit = batch_limit.max(1);
    while window_from_ts < to_ts_exclusive {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts_exclusive);
        let mut cursor: Option<BackfillCursor> = None;
        loop {
            let rows = fetch_backfill_batch(
                pool,
                window_from_ts,
                window_to_ts,
                symbol,
                STARTUP_BACKFILL_MARKET,
                effective_batch_limit,
                cursor.as_ref(),
            )
            .await
            .with_context(|| {
                format!(
                    "{reason} fetch canonical replay rows from_ts={window_from_ts} to_ts_exclusive={window_to_ts}"
                )
            })?;
            if rows.is_empty() {
                break;
            }

            stats.fetched_rows += rows.len();
            cursor = rows.last().map(backfill_cursor_from_row);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeadingGapRecoveryPlan {
    blocked_minute: Option<DateTime<Utc>>,
    history_floor_ts: DateTime<Utc>,
    warm_end_ts: Option<DateTime<Utc>>,
    replay_start_ts: DateTime<Utc>,
    continuity_end_ts: DateTime<Utc>,
    warmed_minutes: usize,
}

fn leading_gap_history_floor_ts(
    state_store: &StateStore,
    replay_start_ts: DateTime<Utc>,
    continuity_end_ts: DateTime<Utc>,
) -> DateTime<Utc> {
    match state_store.latest_continuous_trade_history_segment() {
        Some((history_start_ts, history_end_ts))
            if history_start_ts <= replay_start_ts && history_end_ts >= replay_start_ts =>
        {
            history_start_ts
        }
        _ => replay_start_ts.min(continuity_end_ts),
    }
}

async fn maybe_recover_from_leading_canonical_gap(
    state_store: &mut StateStore,
    scheduler: &mut WindowScheduler,
    next_minute: Option<DateTime<Utc>>,
) -> Option<LeadingGapRecoveryPlan> {
    if state_store.last_finalized_minute().is_some() {
        return None;
    }

    let (replay_start_ts, continuity_end_ts) = state_store.latest_continuous_canonical_segment()?;
    if let Some(blocked_minute) = next_minute {
        let blocked_presence = state_store.canonical_minute_presence(blocked_minute);
        if blocked_presence.complete_under_current_policy() || replay_start_ts <= blocked_minute {
            return None;
        }
    }

    let history_floor_ts =
        leading_gap_history_floor_ts(state_store, replay_start_ts, continuity_end_ts);
    state_store.set_effective_history_floor(Some(history_floor_ts));

    let warm_end_candidate = replay_start_ts - ChronoDuration::minutes(1);
    let mut warmed_minutes = 0usize;
    let warm_end_ts = if history_floor_ts <= warm_end_candidate {
        let mut minute = history_floor_ts;
        while minute <= warm_end_candidate {
            state_store.advance_finalized_state(minute);
            warmed_minutes += 1;
            if warmed_minutes % STARTUP_BACKFILL_YIELD_EVERY_MINUTES == 0 {
                tokio::task::yield_now().await;
            }
            minute += ChronoDuration::minutes(1);
        }
        Some(warm_end_candidate)
    } else {
        None
    };

    scheduler.mark_emitted_through(warm_end_candidate);

    Some(LeadingGapRecoveryPlan {
        blocked_minute: next_minute,
        history_floor_ts,
        warm_end_ts,
        replay_start_ts,
        continuity_end_ts,
        warmed_minutes,
    })
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
                if matches!(worker_kind, MaterializeWorkerKind::Live) {
                    metrics.set_last_persisted_ts(Some(ts_bucket.timestamp_millis()));
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

async fn run_live_materialize_compute_loop(
    ctx: Arc<AppContext>,
    dispatcher: Arc<Dispatcher>,
    runtime_options: IndicatorRuntimeOptions,
    ready_job_rx: Arc<Mutex<mpsc::Receiver<ReadyMinuteJob>>>,
    computed_job_tx: mpsc::Sender<ComputedMinuteJob>,
) -> Result<()> {
    loop {
        let job = {
            let mut ready_job_rx = ready_job_rx.lock().await;
            ready_job_rx.recv().await
        };
        let Some(job) = job else {
            break;
        };
        let artifacts = compute_window_bundle_artifacts(
            &ctx,
            dispatcher.as_ref(),
            &runtime_options,
            job.bundle,
            job.mode,
        )
        .await?;
        computed_job_tx
            .send(ComputedMinuteJob {
                ts_bucket: job.ts_bucket,
                source: job.source,
                enqueued_at: job.enqueued_at,
                artifacts,
            })
            .await
            .context("send computed live minute to ordered commit worker")?;
    }

    Ok(())
}

async fn run_live_ordered_commit_loop(
    metrics: Arc<AppMetrics>,
    dispatcher: Arc<Dispatcher>,
    mut computed_job_rx: mpsc::Receiver<ComputedMinuteJob>,
    live_ready_job_pending: Arc<AtomicUsize>,
    initial_last_persisted_ts: Option<DateTime<Utc>>,
) -> Result<()> {
    let mut next_commit_ts = initial_last_persisted_ts.map(|ts| ts + ChronoDuration::minutes(1));
    let mut pending = BTreeMap::<DateTime<Utc>, ComputedMinuteJob>::new();

    while let Some(job) = computed_job_rx.recv().await {
        pending.insert(job.ts_bucket, job);
        commit_live_jobs_in_order(
            &metrics,
            dispatcher.as_ref(),
            &live_ready_job_pending,
            &mut next_commit_ts,
            &mut pending,
        )
        .await?;
    }

    if next_commit_ts.is_none() {
        next_commit_ts = pending.keys().next().copied();
    }
    commit_live_jobs_in_order(
        &metrics,
        dispatcher.as_ref(),
        &live_ready_job_pending,
        &mut next_commit_ts,
        &mut pending,
    )
    .await?;
    if !pending.is_empty() {
        anyhow::bail!(
            "live ordered commit worker exited with {} uncommitted minute(s)",
            pending.len()
        );
    }

    Ok(())
}

async fn commit_live_jobs_in_order(
    metrics: &Arc<AppMetrics>,
    dispatcher: &Dispatcher,
    live_ready_job_pending: &Arc<AtomicUsize>,
    next_commit_ts: &mut Option<DateTime<Utc>>,
    pending: &mut BTreeMap<DateTime<Utc>, ComputedMinuteJob>,
) -> Result<()> {
    if next_commit_ts.is_none() {
        *next_commit_ts = pending.keys().next().copied();
    }
    while let Some(expected_ts) = *next_commit_ts {
        let Some(job) = pending.remove(&expected_ts) else {
            break;
        };
        let snapshots = dispatcher.persist_window_artifacts(job.artifacts).await?;
        live_ready_job_pending.fetch_sub(1, Ordering::AcqRel);
        metrics.inc_exported_window();
        metrics.set_last_persisted_ts(Some(expected_ts.timestamp_millis()));
        metrics.set_live_ready_to_bundle_ms(job.enqueued_at.elapsed().as_millis());
        log_materialized_coverage(job.source, expected_ts, &snapshots, job.enqueued_at);
        *next_commit_ts = Some(expected_ts + ChronoDuration::minutes(1));
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

async fn poll_live_materialize_handles(handles: &mut Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut idx = 0usize;
    while idx < handles.len() {
        if !handles[idx].is_finished() {
            idx += 1;
            continue;
        }
        let handle = handles.remove(idx);
        match handle.await {
            Ok(Ok(())) => anyhow::bail!("live materialize compute worker exited unexpectedly"),
            Ok(Err(err)) => {
                return Err(err).context("live materialize compute worker failed");
            }
            Err(err) => {
                return Err(err).context("live materialize compute worker join failed");
            }
        }
    }
    Ok(())
}

async fn drain_live_materialize_handles(handles: &mut Vec<JoinHandle<Result<()>>>) {
    while let Some(handle) = handles.pop() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(
                    error = %err,
                    "live materialize compute worker failed during shutdown drain"
                );
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "live materialize compute worker join failed during shutdown drain"
                );
            }
        }
    }
}

async fn abort_live_materialize_handles(handles: &mut Vec<JoinHandle<Result<()>>>) {
    while let Some(handle) = handles.pop() {
        handle.abort();
        let _ = handle.await;
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
    let artifacts =
        compute_window_bundle_artifacts(ctx, dispatcher, runtime_options, window, mode).await?;
    dispatcher.persist_window_artifacts(artifacts).await
}

async fn compute_window_bundle_artifacts(
    ctx: &Arc<AppContext>,
    dispatcher: &Dispatcher,
    runtime_options: &IndicatorRuntimeOptions,
    window: crate::runtime::state_store::WindowBundle,
    mode: DispatchMode,
) -> Result<ProcessedWindowArtifacts> {
    let minute = window.ts_bucket;
    let mut kline_history_supplement = load_kline_history_supplement(
        &ctx.db_pool,
        &ctx.config.indicator.symbol,
        &window.history_futures,
        &window.history_spot,
        runtime_options.kline_history_bars_4h,
        runtime_options.kline_history_bars_1d,
        runtime_options.kline_history_bars_3d,
        runtime_options.kline_history_bars_7d,
        runtime_options.kline_history_bars_30d,
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
        runtime_options.fvg_db_bars_3d,
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

    dispatcher.compute_window_artifacts(ictx, mode).await
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
    snapshot_path: &str,
    startup_checkpoint_path: Option<&str>,
    startup_backfill_progress: Arc<Mutex<StartupBackfillProgress>>,
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
    let mut persisted_frontier_ts = latest_progress_ts.or(latest_snapshot_table_ts);
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
            "startup catch-up window raised to 30d to satisfy long-window indicators"
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

    let mut snapshot_was_loaded = false;
    let mut checkpoint_resume_from_ts = None;
    if let Some(checkpoint) =
        try_load_startup_backfill_checkpoint(startup_checkpoint_path, &ctx.config.indicator.symbol)
    {
        from_ts = checkpoint.from_ts;
        checkpoint_resume_from_ts = Some(
            checkpoint
                .next_canonical_window_from_ts
                .unwrap_or(checkpoint.to_ts_exclusive),
        );
        persisted_frontier_ts = checkpoint.persisted_frontier_ts.or(persisted_frontier_ts);
        snapshot_was_loaded = checkpoint.snapshot_was_loaded;
        state_store.restore_from_snapshot(checkpoint.snapshot);
        info!(
            checkpoint_saved_at = %checkpoint.saved_at,
            checkpoint_from_ts = %checkpoint.from_ts,
            checkpoint_to_ts_exclusive = %checkpoint.to_ts_exclusive,
            checkpoint_resume_from_ts = ?checkpoint_resume_from_ts,
            snapshot_was_loaded,
            persisted_frontier_ts = ?persisted_frontier_ts,
            "startup backfill checkpoint loaded; resuming in-memory canonical replay state"
        );
    } else if !snapshot_path.is_empty() {
        match try_load_state_snapshot(
            snapshot_path,
            &ctx.config.indicator.symbol,
            ctx.config.indicator.snapshot_max_age_hours,
        ) {
            SnapshotLoadOutcome::Fresh(snap) => {
                let snap_ts = snap.last_finalized_ts;
                state_store.restore_from_snapshot(snap);
                snapshot_was_loaded = true;
                let snap_overlap_from_ts = floor_minute(
                    snap_ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES),
                );
                from_ts = from_ts.min(snap_overlap_from_ts);
                info!(
                    snap_ts = %snap_ts,
                    overlap_from_ts = %snap_overlap_from_ts,
                    effective_from_ts = %from_ts,
                    "State snapshot loaded successfully, running overlap repair backfill"
                );
            }
            SnapshotLoadOutcome::StaleRecoverySeed { snap, age_hours } => {
                let snap_ts = snap.last_finalized_ts;
                state_store.restore_from_snapshot(snap);
                snapshot_was_loaded = true;
                let snap_overlap_from_ts = floor_minute(
                    snap_ts - ChronoDuration::minutes(STARTUP_BACKFILL_OVERLAP_MINUTES),
                );
                from_ts = from_ts.min(snap_overlap_from_ts);
                info!(
                    snap_ts = %snap_ts,
                    age_hours,
                    overlap_from_ts = %snap_overlap_from_ts,
                    effective_from_ts = %from_ts,
                    "State snapshot exceeded age limit but passed structural checks; using it as a recovery seed"
                );
            }
            SnapshotLoadOutcome::Rejected => {
                info!("No reusable state snapshot found, rebuilding only the minimum reusable warm-history window");
                if let Some(minimum_recovery_floor) =
                    expand_startup_backfill_to_minimum_recovery_window(from_ts, to_ts)
                {
                    info!(
                        original_from_ts = %from_ts,
                        minimum_recovery_floor = %minimum_recovery_floor,
                        minimum_recovery_history_minutes = MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES,
                        "startup historical backfill expanded to rebuild minimum reusable restart history"
                    );
                    from_ts = minimum_recovery_floor;
                }
            }
        }
    } else {
        if let Some(minimum_recovery_floor) =
            expand_startup_backfill_to_minimum_recovery_window(from_ts, to_ts)
        {
            info!(
                original_from_ts = %from_ts,
                minimum_recovery_floor = %minimum_recovery_floor,
                minimum_recovery_history_minutes = MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES,
                "startup historical backfill expanded to rebuild minimum reusable restart history"
            );
            from_ts = minimum_recovery_floor;
        }
    }
    metrics.set_last_persisted_ts(persisted_frontier_ts.map(|ts| ts.timestamp_millis()));
    // Startup replay is bucket-based for canonical 1m rows. Floor the lower bound so
    // we never drop a completed ts_bucket just because the resume timestamp carried
    // non-zero seconds.
    from_ts = floor_minute(from_ts);
    let mut window_from_ts = checkpoint_resume_from_ts.unwrap_or(from_ts);
    window_from_ts = floor_minute(window_from_ts.max(from_ts));
    {
        let mut progress = startup_backfill_progress.lock().await;
        progress.record(
            from_ts,
            to_ts,
            Some(window_from_ts),
            snapshot_was_loaded,
            persisted_frontier_ts,
        );
    }
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
        backfill_source = "startup canonical replay without option_mark_greeks_5m",
        "startup historical backfill begin"
    );

    let mut total_rows = 0_u64;
    let total_minutes = (to_ts - from_ts).num_minutes().max(0);
    let total_windows = if total_minutes == 0 {
        0
    } else {
        ((total_minutes + CANONICAL_REPLAY_FETCH_WINDOW_MINUTES - 1)
            / CANONICAL_REPLAY_FETCH_WINDOW_MINUTES) as usize
    };
    let startup_backfill_started_at = Instant::now();
    let mut last_progress_log_at = Instant::now();
    let mut completed_windows = if window_from_ts > from_ts {
        ((window_from_ts - from_ts).num_minutes() / CANONICAL_REPLAY_FETCH_WINDOW_MINUTES).max(0)
            as usize
    } else {
        0
    };
    let backfill_batch_size = ctx.config.indicator.startup_backfill_batch_size.max(100);
    while window_from_ts < to_ts {
        let window_to_ts = (window_from_ts
            + ChronoDuration::minutes(CANONICAL_REPLAY_FETCH_WINDOW_MINUTES))
        .min(to_ts);
        {
            let mut progress = startup_backfill_progress.lock().await;
            progress.record(
                from_ts,
                to_ts,
                Some(window_from_ts),
                snapshot_was_loaded,
                persisted_frontier_ts,
            );
        }
        let window_started_at = Instant::now();
        info!(
            window_index = completed_windows + 1,
            windows_total = total_windows,
            window_from_ts = %window_from_ts,
            window_to_ts = %window_to_ts,
            elapsed_secs = startup_backfill_started_at.elapsed().as_secs(),
            "startup backfill replay ingest window begin"
        );
        let mut window_rows_processed = 0_u64;
        let mut window_rows_fetched = 0_u64;
        let mut cursor: Option<BackfillCursor> = None;

        loop {
            let rows = fetch_backfill_batch_excluding_option_mark_greeks(
                &ctx.db_pool,
                window_from_ts,
                window_to_ts,
                &ctx.config.indicator.symbol,
                STARTUP_BACKFILL_MARKET,
                backfill_batch_size,
                cursor.as_ref(),
            )
            .await?;
            if rows.is_empty() {
                break;
            }

            window_rows_fetched += rows.len() as u64;
            cursor = rows.last().map(backfill_cursor_from_row);

            for row in rows {
                match replay_row_to_engine_event(row) {
                    Ok(event) => {
                        metrics.inc_processed(event.event_ts.timestamp_millis());
                        state_store.ingest(event);
                        total_rows += 1;
                        window_rows_processed += 1;
                    }
                    Err(err) => {
                        warn!(error = %err, "decode startup backfill row failed");
                    }
                }

                if last_progress_log_at.elapsed()
                    >= Duration::from_secs(STARTUP_BACKFILL_PROGRESS_LOG_INTERVAL_SECS)
                {
                    let completed_minutes = (window_to_ts - from_ts).num_minutes().max(0);
                    let progress_pct = if total_minutes > 0 {
                        (completed_minutes as f64 / total_minutes as f64) * 100.0
                    } else {
                        100.0
                    };
                    info!(
                        window_index = completed_windows + 1,
                        windows_total = total_windows,
                        window_from_ts = %window_from_ts,
                        window_to_ts = %window_to_ts,
                        window_rows_processed,
                        window_rows_fetched,
                        total_rows,
                        completed_minutes,
                        total_minutes,
                        progress_pct = format_args!("{progress_pct:.1}"),
                        elapsed_secs = startup_backfill_started_at.elapsed().as_secs(),
                        "startup backfill replay ingest progress"
                    );
                    last_progress_log_at = Instant::now();
                }
            }
        }

        completed_windows += 1;
        let completed_minutes = (window_to_ts - from_ts).num_minutes().max(0);
        let progress_pct = if total_minutes > 0 {
            (completed_minutes as f64 / total_minutes as f64) * 100.0
        } else {
            100.0
        };
        let window_elapsed = window_started_at.elapsed();
        let window_elapsed_secs = window_elapsed.as_secs_f64();
        let window_minutes = (window_to_ts - window_from_ts).num_minutes().max(0) as f64;
        let rows_per_sec = if window_elapsed_secs > 0.0 {
            window_rows_fetched as f64 / window_elapsed_secs
        } else {
            0.0
        };
        let minutes_per_sec = if window_elapsed_secs > 0.0 {
            window_minutes / window_elapsed_secs
        } else {
            0.0
        };
        info!(
            window_index = completed_windows,
            windows_total = total_windows,
            window_from_ts = %window_from_ts,
            window_to_ts = %window_to_ts,
            window_rows_total = window_rows_fetched,
            total_rows,
            completed_minutes,
            total_minutes,
            progress_pct = format_args!("{progress_pct:.1}"),
            window_elapsed_ms = window_elapsed.as_millis(),
            rows_per_sec = format_args!("{rows_per_sec:.1}"),
            minutes_per_sec = format_args!("{minutes_per_sec:.3}"),
            elapsed_secs = startup_backfill_started_at.elapsed().as_secs(),
            "startup backfill replay ingest window complete"
        );
        window_from_ts = window_to_ts;
        {
            let mut progress = startup_backfill_progress.lock().await;
            progress.record(
                from_ts,
                to_ts,
                Some(window_from_ts),
                snapshot_was_loaded,
                persisted_frontier_ts,
            );
            if let (Some(path), Some(checkpoint)) = (
                startup_checkpoint_path,
                progress
                    .to_checkpoint(&ctx.config.indicator.symbol, state_store.extract_snapshot()),
            ) {
                save_startup_backfill_checkpoint(&checkpoint, path)
                    .await
                    .with_context(|| format!("save startup backfill checkpoint path={path}"))?;
            }
        }
    }
    {
        let mut progress = startup_backfill_progress.lock().await;
        progress.record(
            from_ts,
            to_ts,
            None,
            snapshot_was_loaded,
            persisted_frontier_ts,
        );
        if let (Some(path), Some(checkpoint)) = (
            startup_checkpoint_path,
            progress.to_checkpoint(&ctx.config.indicator.symbol, state_store.extract_snapshot()),
        ) {
            save_startup_backfill_checkpoint(&checkpoint, path)
                .await
                .with_context(|| format!("save startup backfill checkpoint path={path}"))?;
        }
    }

    if total_rows == 0 && state_store.latest_continuous_canonical_segment().is_none() {
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
    let can_reuse_persisted_options_surface_history =
        snapshot_was_loaded || persisted_frontier_ts.is_some();
    let options_feature_lookback_minutes = (required_options_surface_history_points()
        .saturating_sub(1) as i64)
        * OPTIONS_SURFACE_BUCKET_MINUTES;
    let options_feature_seed_from_ts = if can_reuse_persisted_options_surface_history {
        floor_timestamp_to_interval_minutes(
            repair_start_ts - ChronoDuration::minutes(options_feature_lookback_minutes),
            OPTIONS_SURFACE_BUCKET_MINUTES,
        )
    } else {
        floor_timestamp_to_interval_minutes(history_replay_start_ts, OPTIONS_SURFACE_BUCKET_MINUTES)
    };
    let options_repair_bucket_start =
        floor_timestamp_to_interval_minutes(repair_start_ts, OPTIONS_SURFACE_BUCKET_MINUTES);
    let options_replay_to_ts_exclusive = replay_end_ts + ChronoDuration::minutes(1);

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

    let (options_feature_seeded_points, options_raw_range_count, options_raw_replayed_rows) =
        hydrate_options_surface_startup_recovery(
            &ctx.db_pool,
            &ctx.config.indicator.symbol,
            state_store,
            options_feature_seed_from_ts,
            repair_start_ts,
            replay_end_ts,
        )
        .await?;
    info!(
        options_feature_seed_from_ts = %options_feature_seed_from_ts,
        options_repair_bucket_start = %options_repair_bucket_start,
        options_replay_to_ts_exclusive = %options_replay_to_ts_exclusive,
        can_reuse_persisted_options_surface_history,
        options_feature_seeded_points,
        options_raw_range_count,
        options_raw_replayed_rows,
        "startup options surface recovery prepared"
    );

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

fn build_paged_backfill_sql_internal(
    filter_market: bool,
    with_cursor: bool,
    include_option_mark_greeks: bool,
) -> String {
    const SQL_TEMPLATE: &str = r#"
    WITH events AS (
        (
            SELECT
                t.ctid::text AS row_tid_text,
                t.ts_event AS event_ts,
                'md.agg.trade.1m'::text AS msg_type,
                t.market::text AS market,
                t.symbol AS symbol,
                format('md.agg.%s.trade.1m.%s', t.market::text, lower(t.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_bucket', t.ts_bucket,
                    'chunk_start_ts', t.chunk_start_ts,
                    'chunk_end_ts', t.chunk_end_ts,
                    'source_event_count', t.source_event_count,
                    'trade_count', t.trade_count,
                    'buy_qty', t.buy_qty,
                    'sell_qty', t.sell_qty,
                    'buy_notional', t.buy_notional,
                    'sell_notional', t.sell_notional,
                    'first_price', t.first_price,
                    'last_price', t.last_price,
                    'high_price', t.high_price,
                    'low_price', t.low_price,
                    'profile_levels', t.profile_levels,
                    'whale', t.whale_json,
                    'payload_json', COALESCE(t.payload_json, '{}'::jsonb)
                ) AS data_json
            FROM md.agg_trade_1m t
            WHERE t.ts_bucket >= $1
              AND t.ts_bucket < $2
              AND t.symbol = $3
__TRADE_MARKET_FILTER__
__TRADE_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                b.ctid::text AS row_tid_text,
                b.ts_event AS event_ts,
                'md.agg.orderbook.1m'::text AS msg_type,
                b.market::text AS market,
                b.symbol AS symbol,
                format('md.agg.%s.orderbook.1m.%s', b.market::text, lower(b.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_bucket', b.ts_bucket,
                    'chunk_start_ts', b.chunk_start_ts,
                    'chunk_end_ts', b.chunk_end_ts,
                    'source_event_count', b.source_event_count,
                    'sample_count', b.sample_count,
                    'bbo_updates', b.bbo_updates,
                    'spread_sum', b.spread_sum,
                    'topk_depth_sum', b.topk_depth_sum,
                    'obi_sum', b.obi_sum,
                    'obi_l1_sum', b.obi_l1_sum,
                    'obi_k_sum', b.obi_k_sum,
                    'obi_k_dw_sum', b.obi_k_dw_sum,
                    'obi_k_dw_change_sum', b.obi_k_dw_change_sum,
                    'obi_k_dw_adj_sum', b.obi_k_dw_adj_sum,
                    'microprice_sum', b.microprice_sum,
                    'microprice_classic_sum', b.microprice_classic_sum,
                    'microprice_kappa_sum', b.microprice_kappa_sum,
                    'microprice_adj_sum', b.microprice_adj_sum,
                    'ofi_sum', b.ofi_sum,
                    'obi_k_dw_close', b.obi_k_dw_close,
                    'heatmap_levels', '[]'::jsonb,
                    'heatmap_loaded', FALSE
                ) AS data_json
            FROM md.agg_orderbook_1m b
            WHERE b.ts_bucket >= $1
              AND b.ts_bucket < $2
              AND b.symbol = $3
__ORDERBOOK_MARKET_FILTER__
__ORDERBOOK_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                l.ctid::text AS row_tid_text,
                l.ts_event AS event_ts,
                'md.agg.liq.1m'::text AS msg_type,
                l.market::text AS market,
                l.symbol AS symbol,
                format('md.agg.%s.liq.1m.%s', l.market::text, lower(l.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_bucket', l.ts_bucket,
                    'chunk_start_ts', l.chunk_start_ts,
                    'chunk_end_ts', l.chunk_end_ts,
                    'source_event_count', l.source_event_count,
                    'force_liq_levels', l.force_liq_levels
                ) AS data_json
            FROM md.agg_liq_1m l
            WHERE l.ts_bucket >= $1
              AND l.ts_bucket < $2
              AND l.symbol = $3
__LIQ_MARKET_FILTER__
__LIQ_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                f.ctid::text AS row_tid_text,
                f.ts_event AS event_ts,
                'md.agg.funding_mark.1m'::text AS msg_type,
                f.market::text AS market,
                f.symbol AS symbol,
                format('md.agg.%s.funding_mark.1m.%s', f.market::text, lower(f.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_bucket', f.ts_bucket,
                    'chunk_start_ts', f.chunk_start_ts,
                    'chunk_end_ts', f.chunk_end_ts,
                    'source_event_count', f.source_event_count,
                    'mark_points', f.mark_points,
                    'funding_points', f.funding_points
                ) AS data_json
            FROM md.agg_funding_mark_1m f
            WHERE f.ts_bucket >= $1
              AND f.ts_bucket < $2
              AND f.symbol = $3
__FUNDING_MARKET_FILTER__
__FUNDING_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                oi.ctid::text AS row_tid_text,
                oi.ts_event AS event_ts,
                'md.open_interest_current'::text AS msg_type,
                oi.market::text AS market,
                oi.symbol AS symbol,
                format('md.%s.open_interest.current.%s', oi.market::text, lower(oi.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_effective', oi.ts_event,
                    'open_interest_contracts', oi.open_interest_contracts,
                    'mark_price', oi.mark_price,
                    'open_interest_value_usdt', oi.open_interest_value_usdt
                ) AS data_json
            FROM md.open_interest_current_1m oi
            WHERE oi.ts_event >= $1
              AND oi.ts_event < $2
              AND oi.symbol = $3
__OI_CURRENT_MARKET_FILTER__
__OI_CURRENT_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                oih.ctid::text AS row_tid_text,
                oih.ts_event AS event_ts,
                'md.open_interest_hist_5m'::text AS msg_type,
                oih.market::text AS market,
                oih.symbol AS symbol,
                format('md.%s.open_interest.5m.%s', oih.market::text, lower(oih.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_effective', oih.ts_bucket,
                    'ts_bucket', oih.ts_bucket,
                    'open_interest_contracts', oih.open_interest_contracts,
                    'open_interest_value_usdt', oih.open_interest_value_usdt
                ) AS data_json
            FROM md.open_interest_hist_5m oih
            WHERE oih.ts_bucket >= $1
              AND oih.ts_bucket < $2
              AND oih.symbol = $3
__OI_HIST_MARKET_FILTER__
__OI_HIST_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )

        UNION ALL

        (
            SELECT
                lsr.ctid::text AS row_tid_text,
                lsr.ts_event AS event_ts,
                'md.long_short_ratio_5m'::text AS msg_type,
                lsr.market::text AS market,
                lsr.symbol AS symbol,
                format(
                    'md.%s.long_short_ratio.%s.5m.%s',
                    lsr.market::text,
                    lsr.ratio_type,
                    lower(lsr.symbol)
                ) AS routing_key,
                jsonb_build_object(
                    'ts_effective', lsr.ts_bucket,
                    'ts_bucket', lsr.ts_bucket,
                    'ratio_type', lsr.ratio_type,
                    'long_short_ratio', lsr.long_short_ratio,
                    'long_account_ratio', lsr.long_account_ratio,
                    'short_account_ratio', lsr.short_account_ratio
                ) AS data_json
            FROM md.long_short_ratio_5m lsr
            WHERE lsr.ts_bucket >= $1
              AND lsr.ts_bucket < $2
              AND lsr.symbol = $3
              AND lsr.ratio_type IN ('global_account', 'top_account', 'top_position')
__LONG_SHORT_RATIO_MARKET_FILTER__
__LONG_SHORT_RATIO_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )
__OPTION_MARK_GREEKS_UNION__
    )
    SELECT row_tid_text, event_ts, msg_type, market, symbol, routing_key, data_json
    FROM events
    ORDER BY event_ts ASC, msg_type ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
    LIMIT $__LIMIT_PARAM__
    "#;

    let market_param = 4usize;
    let limit_param = if filter_market { 5usize } else { 4usize };
    let cursor_ts_param = if filter_market { 6usize } else { 5usize };
    let cursor_msg_type_param = cursor_ts_param + 1;
    let cursor_market_param = cursor_ts_param + 2;
    let cursor_symbol_param = cursor_ts_param + 3;
    let cursor_routing_key_param = cursor_ts_param + 4;
    let cursor_row_tid_param = cursor_ts_param + 5;

    let mk_market_filter = |alias: &str| -> String {
        if filter_market {
            format!("              AND {alias}.market::text = ${market_param}")
        } else {
            String::new()
        }
    };

    let mk_cursor_filter = |alias: &str, msg_type_expr: &str, routing_key_expr: &str| -> String {
        if with_cursor {
            format!(
                "              AND ({alias}.ts_event, {msg_type_expr}, {alias}.market::text, {alias}.symbol, {routing_key_expr}, {alias}.ctid::text)\n                  > (${cursor_ts_param}::timestamptz, ${cursor_msg_type_param}::text, ${cursor_market_param}::text, ${cursor_symbol_param}::text, ${cursor_routing_key_param}::text, ${cursor_row_tid_param}::text)"
            )
        } else {
            String::new()
        }
    };

    let option_union = if include_option_mark_greeks {
        r#"

        UNION ALL

        (
            SELECT
                opt.ctid::text AS row_tid_text,
                opt.ts_event AS event_ts,
                'md.option_mark_greeks_5m'::text AS msg_type,
                opt.market::text AS market,
                opt.symbol AS symbol,
                format('md.futures.option_mark_greeks.5m.%s', lower(opt.symbol)) AS routing_key,
                jsonb_build_object(
                    'ts_effective', opt.ts_bucket,
                    'ts_bucket', opt.ts_bucket,
                    'option_symbol', opt.option_symbol,
                    'underlying_asset', opt.underlying_asset,
                    'expiry_ts', opt.expiry_ts,
                    'strike_price', opt.strike_price,
                    'contract_side', opt.contract_side,
                    'unit', opt.unit,
                    'index_price', opt.index_price,
                    'mark_price', opt.mark_price,
                    'bid_iv', opt.bid_iv,
                    'ask_iv', opt.ask_iv,
                    'mark_iv', opt.mark_iv,
                    'delta', opt.delta,
                    'gamma', opt.gamma,
                    'vega', opt.vega,
                    'theta', opt.theta,
                    'risk_free_interest', opt.risk_free_interest
                ) AS data_json
            FROM md.option_mark_greeks_5m opt
            WHERE opt.ts_bucket >= $1
              AND opt.ts_bucket < $2
              AND opt.symbol = $3
__OPTION_MARKET_FILTER__
__OPTION_CURSOR_FILTER__
            ORDER BY event_ts ASC, market ASC, symbol ASC, routing_key ASC, row_tid_text ASC
            LIMIT $__LIMIT_PARAM__
        )"#
        .replace("__OPTION_MARKET_FILTER__", &mk_market_filter("opt"))
        .replace(
            "__OPTION_CURSOR_FILTER__",
            &mk_cursor_filter(
                "opt",
                "'md.option_mark_greeks_5m'::text",
                "format('md.futures.option_mark_greeks.5m.%s', lower(opt.symbol))",
            ),
        )
        .replace("__LIMIT_PARAM__", &limit_param.to_string())
    } else {
        String::new()
    };

    SQL_TEMPLATE
        .replace("__TRADE_MARKET_FILTER__", &mk_market_filter("t"))
        .replace(
            "__TRADE_CURSOR_FILTER__",
            &mk_cursor_filter(
                "t",
                "'md.agg.trade.1m'::text",
                "format('md.agg.%s.trade.1m.%s', t.market::text, lower(t.symbol))",
            ),
        )
        .replace("__ORDERBOOK_MARKET_FILTER__", &mk_market_filter("b"))
        .replace(
            "__ORDERBOOK_CURSOR_FILTER__",
            &mk_cursor_filter(
                "b",
                "'md.agg.orderbook.1m'::text",
                "format('md.agg.%s.orderbook.1m.%s', b.market::text, lower(b.symbol))",
            ),
        )
        .replace("__LIQ_MARKET_FILTER__", &mk_market_filter("l"))
        .replace(
            "__LIQ_CURSOR_FILTER__",
            &mk_cursor_filter(
                "l",
                "'md.agg.liq.1m'::text",
                "format('md.agg.%s.liq.1m.%s', l.market::text, lower(l.symbol))",
            ),
        )
        .replace("__FUNDING_MARKET_FILTER__", &mk_market_filter("f"))
        .replace(
            "__FUNDING_CURSOR_FILTER__",
            &mk_cursor_filter(
                "f",
                "'md.agg.funding_mark.1m'::text",
                "format('md.agg.%s.funding_mark.1m.%s', f.market::text, lower(f.symbol))",
            ),
        )
        .replace("__OI_CURRENT_MARKET_FILTER__", &mk_market_filter("oi"))
        .replace(
            "__OI_CURRENT_CURSOR_FILTER__",
            &mk_cursor_filter(
                "oi",
                "'md.open_interest_current'::text",
                "format('md.%s.open_interest.current.%s', oi.market::text, lower(oi.symbol))",
            ),
        )
        .replace("__OI_HIST_MARKET_FILTER__", &mk_market_filter("oih"))
        .replace(
            "__OI_HIST_CURSOR_FILTER__",
            &mk_cursor_filter(
                "oih",
                "'md.open_interest_hist_5m'::text",
                "format('md.%s.open_interest.5m.%s', oih.market::text, lower(oih.symbol))",
            ),
        )
        .replace("__LONG_SHORT_RATIO_MARKET_FILTER__", &mk_market_filter("lsr"))
        .replace(
            "__LONG_SHORT_RATIO_CURSOR_FILTER__",
            &mk_cursor_filter(
                "lsr",
                "'md.long_short_ratio_5m'::text",
                "format('md.%s.long_short_ratio.%s.5m.%s', lsr.market::text, lsr.ratio_type, lower(lsr.symbol))",
            ),
        )
        .replace("__OPTION_MARK_GREEKS_UNION__", &option_union)
        .replace("__LIMIT_PARAM__", &limit_param.to_string())
}

fn build_paged_backfill_sql(filter_market: bool, with_cursor: bool) -> String {
    build_paged_backfill_sql_internal(filter_market, with_cursor, true)
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

    let mut rows = Vec::new();
    let mut cursor: Option<BackfillCursor> = None;
    loop {
        let batch = fetch_backfill_batch(
            pool,
            from_ts,
            to_ts,
            symbol,
            market,
            BACKFILL_PAGED_FETCH_DEFAULT_LIMIT,
            cursor.as_ref(),
        )
        .await?;
        if batch.is_empty() {
            break;
        }
        cursor = batch.last().map(backfill_cursor_from_row);
        rows.extend(batch);
    }

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
            row_tid_text: String::new(),
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

async fn fetch_backfill_batch_internal(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    cursor: Option<&BackfillCursor>,
    include_option_mark_greeks: bool,
) -> Result<Vec<ReplayRow>> {
    if from_ts >= to_ts {
        return Ok(Vec::new());
    }

    let filter_market = !market.eq_ignore_ascii_case("all");
    let with_cursor = cursor.is_some();
    let sql =
        build_paged_backfill_sql_internal(filter_market, with_cursor, include_option_mark_greeks);
    let symbol_upper = symbol.to_uppercase();
    let market_lower = market.to_lowercase();
    let effective_limit = limit.max(1);

    let mut query = sqlx::query(&sql)
        .bind(from_ts)
        .bind(to_ts)
        .bind(symbol_upper);
    if filter_market {
        query = query.bind(market_lower);
    }
    query = query.bind(effective_limit);
    if let Some(cursor) = cursor {
        query = query
            .bind(cursor.event_ts)
            .bind(cursor.msg_type.as_str())
            .bind(cursor.market.as_str())
            .bind(cursor.symbol.as_str())
            .bind(cursor.routing_key.as_str())
            .bind(cursor.row_tid_text.as_str());
    }

    let rows = query
        .fetch_all(pool)
        .await
        .context("fetch startup/live repair backfill batch")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(ReplayRow {
            event_ts: row.get("event_ts"),
            msg_type: row.get("msg_type"),
            market: row.get("market"),
            symbol: row.get("symbol"),
            routing_key: row.get("routing_key"),
            data_json: row.get("data_json"),
            row_tid_text: row.get("row_tid_text"),
        });
    }
    Ok(out)
}

pub async fn fetch_backfill_batch(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    cursor: Option<&BackfillCursor>,
) -> Result<Vec<ReplayRow>> {
    fetch_backfill_batch_internal(pool, from_ts, to_ts, symbol, market, limit, cursor, true).await
}

async fn fetch_backfill_batch_excluding_option_mark_greeks(
    pool: &PgPool,
    from_ts: DateTime<Utc>,
    to_ts: DateTime<Utc>,
    symbol: &str,
    market: &str,
    limit: i64,
    cursor: Option<&BackfillCursor>,
) -> Result<Vec<ReplayRow>> {
    fetch_backfill_batch_internal(pool, from_ts, to_ts, symbol, market, limit, cursor, false).await
}

fn replay_row_after_cursor(row: &ReplayRow, cursor: &BackfillCursor) -> bool {
    (
        row.event_ts,
        row.msg_type.as_str(),
        row.market.as_str(),
        row.symbol.as_str(),
        row.routing_key.as_str(),
        row.row_tid_text.as_str(),
    ) > (
        cursor.event_ts,
        cursor.msg_type.as_str(),
        cursor.market.as_str(),
        cursor.symbol.as_str(),
        cursor.routing_key.as_str(),
        cursor.row_tid_text.as_str(),
    )
}

fn backfill_cursor_from_row(row: &ReplayRow) -> BackfillCursor {
    BackfillCursor {
        event_ts: row.event_ts,
        msg_type: row.msg_type.clone(),
        market: row.market.clone(),
        symbol: row.symbol.clone(),
        routing_key: row.routing_key.clone(),
        row_tid_text: row.row_tid_text.clone(),
    }
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
        build_paged_backfill_sql, build_paged_backfill_sql_internal,
        expand_startup_backfill_to_minimum_recovery_window, find_long_null_price_run,
        handle_ingest_event, hydrate_futures_orderbook_heatmaps_for_range_with_fetch,
        live_tail_reconcile_start_ts, minimum_startup_recovery_history_floor,
        minute_exclusive_upper_bound, minute_history_is_strictly_contiguous,
        replay_heatmap_hydration_batch_end, replay_row_after_cursor,
        save_startup_backfill_checkpoint, save_state_snapshot, shutdown_ready_through_candidate,
        snapshot_has_required_history, snapshot_null_price_run_reaches_recent_tail,
        startup_backfill_checkpoint_path, try_load_startup_backfill_checkpoint,
        try_load_state_snapshot, BackfillCursor, LiveCanonicalRepairController, ReplayRow,
        SnapshotLoadOutcome, StartupBackfillCheckpoint, FUNDING_BACKFILL_WINDOW_SQL,
        LIQ_BACKFILL_WINDOW_SQL, LIVE_CANONICAL_TAIL_RECONCILE_LOOKBACK_MINUTES,
        MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES, ORDERBOOK_BACKFILL_WINDOW_SQL_SCALAR,
        ORDERBOOK_BACKFILL_WINDOW_SQL_WITH_HEATMAP, STARTUP_BACKFILL_CHECKPOINT_VERSION,
        TRADE_BACKFILL_WINDOW_SQL,
    };
    use crate::ingest::decoder::{
        AggFundingMark1mEvent, AggFundingPoint, AggHeatmapLevel, AggLiq1mEvent, AggLiqLevel,
        AggMarkPoint, AggOrderbook1mEvent, AggTrade1mEvent, AggVpinSnapshot, AggWhaleStats,
        EngineEvent, MarketKind, MdData, TradeEvent,
    };
    use crate::observability::metrics::AppMetrics;
    use crate::runtime::state_store::{
        FinalizedVpinState, FundingChange, LatestFundingState, LatestMarkState, MinuteHistory,
        StateSnapshot, StateStore, VpinState, HISTORY_LIMIT_MINUTES, STATE_SNAPSHOT_VERSION,
    };
    use crate::runtime::window_scheduler::WindowScheduler;
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use serde_json::json;
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

    fn agg_orderbook_event_spot(
        ts_bucket: chrono::DateTime<Utc>,
        heatmap_loaded: bool,
        bid_liquidity: f64,
    ) -> EngineEvent {
        let mut event = agg_orderbook_event(ts_bucket, heatmap_loaded, bid_liquidity);
        event.market = MarketKind::Spot;
        event.routing_key = "md.agg.spot.orderbook.1m.testusdt".to_string();
        event
    }

    fn agg_trade_event(
        ts_bucket: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        last_vpin: f64,
    ) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty,
                sell_qty,
                buy_notional: buy_qty * 2000.0,
                sell_notional: sell_qty * 2000.0,
                first_price: Some(2000.0),
                last_price: Some(2000.0),
                high_price: Some(2000.0),
                low_price: Some(2000.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin,
                }),
            }),
        }
    }

    fn agg_trade_event_spot(
        ts_bucket: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        last_vpin: f64,
    ) -> EngineEvent {
        let mut event = agg_trade_event(ts_bucket, buy_qty, sell_qty, last_vpin);
        event.market = MarketKind::Spot;
        event.routing_key = "md.agg.spot.trade.1m.testusdt".to_string();
        event
    }

    fn agg_liq_event(ts_bucket: chrono::DateTime<Utc>, notional: f64) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.liq.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.liq.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggLiq1m(AggLiq1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                levels: vec![AggLiqLevel {
                    price: 2000.0,
                    long_liq: notional,
                    short_liq: notional / 2.0,
                }],
            }),
        }
    }

    fn agg_funding_mark_event(
        ts_bucket: chrono::DateTime<Utc>,
        point_offset_secs: i64,
        mark_price: f64,
        funding_rate: f64,
    ) -> EngineEvent {
        let point_ts = ts_bucket + ChronoDuration::seconds(point_offset_secs);
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.funding_mark.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.funding_mark.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggFundingMark1m(AggFundingMark1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                mark_points: vec![AggMarkPoint {
                    ts: point_ts,
                    mark_price: Some(mark_price),
                    index_price: Some(mark_price + 1.0),
                    estimated_settle_price: None,
                    funding_rate: Some(funding_rate),
                    next_funding_time: None,
                }],
                funding_points: vec![AggFundingPoint {
                    ts: point_ts,
                    funding_time: Some(point_ts),
                    funding_rate,
                    mark_price: Some(mark_price),
                    next_funding_time: None,
                }],
            }),
        }
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
            option_mark_greeks_5m_buckets: Vec::new(),
            options_surface_5m: Vec::new(),
            canonical_minutes: Vec::new(),
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
    fn paged_backfill_sql_includes_extended_sources_and_row_tie_breaker() {
        let sql = build_paged_backfill_sql(false, true);
        assert!(sql.contains("FROM md.agg_trade_1m t"));
        assert!(sql.contains("FROM md.agg_orderbook_1m b"));
        assert!(sql.contains("FROM md.agg_liq_1m l"));
        assert!(sql.contains("FROM md.agg_funding_mark_1m f"));
        assert!(sql.contains("FROM md.open_interest_current_1m oi"));
        assert!(sql.contains("FROM md.open_interest_hist_5m oih"));
        assert!(sql.contains("FROM md.long_short_ratio_5m lsr"));
        assert!(sql.contains("FROM md.option_mark_greeks_5m opt"));
        assert!(sql.contains("row_tid_text"));
        assert!(sql.contains("ctid::text"));
    }

    #[test]
    fn paged_backfill_sql_can_exclude_option_mark_greeks_for_startup_seed_mode() {
        let sql = build_paged_backfill_sql_internal(false, true, false);
        assert!(sql.contains("FROM md.agg_trade_1m t"));
        assert!(sql.contains("FROM md.long_short_ratio_5m lsr"));
        assert!(!sql.contains("FROM md.option_mark_greeks_5m opt"));
    }

    #[test]
    fn replay_row_after_cursor_uses_row_tid_as_final_tie_breaker() {
        let event_ts = Utc.with_ymd_and_hms(2026, 3, 24, 0, 0, 0).single().unwrap();
        let cursor = BackfillCursor {
            event_ts,
            msg_type: "md.agg.trade.1m".to_string(),
            market: "futures".to_string(),
            symbol: "TESTUSDT".to_string(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            row_tid_text: "(0,1)".to_string(),
        };
        let same_row = ReplayRow {
            event_ts,
            msg_type: cursor.msg_type.clone(),
            market: cursor.market.clone(),
            symbol: cursor.symbol.clone(),
            routing_key: cursor.routing_key.clone(),
            data_json: json!({}),
            row_tid_text: cursor.row_tid_text.clone(),
        };
        let later_row = ReplayRow {
            row_tid_text: "(0,2)".to_string(),
            ..same_row.clone()
        };

        assert!(!replay_row_after_cursor(&same_row, &cursor));
        assert!(replay_row_after_cursor(&later_row, &cursor));
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
    fn startup_missing_snapshot_fallback_uses_minimum_reusable_history_window() {
        let to_ts = Utc
            .with_ymd_and_hms(2026, 3, 31, 21, 53, 0)
            .single()
            .unwrap();
        let recent_from_ts = to_ts - ChronoDuration::minutes(30);
        let expanded = expand_startup_backfill_to_minimum_recovery_window(recent_from_ts, to_ts)
            .expect("expected expansion to reusable floor");
        assert_eq!(expanded, minimum_startup_recovery_history_floor(to_ts));
        assert_eq!(
            expanded,
            to_ts - ChronoDuration::minutes(MIN_REUSABLE_SNAPSHOT_HISTORY_MINUTES)
        );
    }

    #[test]
    fn startup_backfill_checkpoint_path_rewrites_snapshot_suffix() {
        assert_eq!(
            startup_backfill_checkpoint_path("/tmp/indicator_engine_TESTUSDT.json.gz"),
            Some("/tmp/indicator_engine_TESTUSDT.startup_backfill.json.gz".to_string())
        );
        assert_eq!(startup_backfill_checkpoint_path(""), None);
    }

    #[tokio::test]
    async fn stale_snapshot_is_accepted_as_recovery_seed() {
        let last_finalized_ts = Utc::now() - ChronoDuration::hours(30);
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
            Some(history_start_ts),
        );
        let temp_path = std::env::temp_dir().join(format!(
            "indicator_engine_snapshot_recovery_seed_{}.json.gz",
            Uuid::new_v4()
        ));
        save_state_snapshot(&snap, temp_path.to_str().unwrap())
            .await
            .expect("save stale snapshot");

        let outcome = try_load_state_snapshot(temp_path.to_str().unwrap(), "TESTUSDT", 24);
        let _ = std::fs::remove_file(&temp_path);

        match outcome {
            SnapshotLoadOutcome::StaleRecoverySeed {
                snap: loaded,
                age_hours,
            } => {
                assert!(age_hours >= 24);
                assert_eq!(loaded.last_finalized_ts, snap.last_finalized_ts);
                assert_eq!(loaded.history_futures.len(), snap.history_futures.len());
            }
            SnapshotLoadOutcome::Fresh(_) => panic!("expected stale recovery seed, got fresh"),
            SnapshotLoadOutcome::Rejected => panic!("expected stale recovery seed, got rejected"),
        }
    }

    #[tokio::test]
    async fn startup_backfill_checkpoint_round_trips() {
        let last_finalized_ts = Utc::now() - ChronoDuration::minutes(10);
        let history_start_ts = last_finalized_ts - ChronoDuration::minutes(120);
        let history = (0..=120)
            .map(|offset| {
                priced_history_row(history_start_ts + ChronoDuration::minutes(offset), 2000.0)
            })
            .collect::<Vec<_>>();
        let snap = snapshot_fixture(
            last_finalized_ts,
            history.clone(),
            history,
            Some(history_start_ts),
        );
        let checkpoint = StartupBackfillCheckpoint {
            version: STARTUP_BACKFILL_CHECKPOINT_VERSION,
            symbol: "TESTUSDT".to_string(),
            saved_at: last_finalized_ts,
            from_ts: history_start_ts,
            to_ts_exclusive: last_finalized_ts + ChronoDuration::minutes(1),
            next_canonical_window_from_ts: None,
            snapshot_was_loaded: true,
            persisted_frontier_ts: Some(last_finalized_ts),
            snapshot: snap,
        };
        let temp_path = std::env::temp_dir().join(format!(
            "indicator_engine_startup_checkpoint_{}.json.gz",
            Uuid::new_v4()
        ));

        save_startup_backfill_checkpoint(&checkpoint, temp_path.to_str().unwrap())
            .await
            .expect("save startup checkpoint");
        let loaded = try_load_startup_backfill_checkpoint(temp_path.to_str(), "TESTUSDT")
            .expect("load startup checkpoint");
        let _ = std::fs::remove_file(&temp_path);

        assert_eq!(loaded.from_ts, checkpoint.from_ts);
        assert_eq!(loaded.to_ts_exclusive, checkpoint.to_ts_exclusive);
        assert_eq!(loaded.snapshot_was_loaded, checkpoint.snapshot_was_loaded);
        assert_eq!(
            loaded.persisted_frontier_ts,
            checkpoint.persisted_frontier_ts
        );
        assert_eq!(
            loaded.snapshot.history_futures.len(),
            checkpoint.snapshot.history_futures.len()
        );
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

    #[tokio::test]
    async fn leading_gap_recovery_skips_to_latest_continuous_segment() {
        let blocking_minute = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let replay_start_ts = blocking_minute + ChronoDuration::minutes(2);
        let continuity_end_ts = replay_start_ts + ChronoDuration::minutes(1);
        let mut state_store =
            crate::runtime::state_store::StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let mut scheduler = WindowScheduler::new(0);
        scheduler.prime_start_from(blocking_minute);

        state_store.ingest(agg_funding_mark_event(blocking_minute, 45, 2000.0, 0.01));

        for ts in [replay_start_ts, continuity_end_ts] {
            state_store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            state_store.ingest(agg_orderbook_event(ts, true, 4.0));
            state_store.ingest(agg_liq_event(ts, 10.0));
            state_store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            state_store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            state_store.ingest(agg_orderbook_event_spot(ts, true, 4.0));
        }

        let next_minute = scheduler.next_minute_to_emit();
        let plan = super::maybe_recover_from_leading_canonical_gap(
            &mut state_store,
            &mut scheduler,
            next_minute,
        )
        .await
        .expect("expected leading-gap recovery plan");

        assert_eq!(plan.blocked_minute, Some(blocking_minute));
        assert_eq!(plan.history_floor_ts, replay_start_ts);
        assert_eq!(plan.warm_end_ts, None);
        assert_eq!(plan.replay_start_ts, replay_start_ts);
        assert_eq!(plan.continuity_end_ts, continuity_end_ts);
        assert_eq!(plan.warmed_minutes, 0);
        assert_eq!(scheduler.next_minute_to_emit(), Some(replay_start_ts));
        assert_eq!(state_store.last_finalized_minute(), None);
        assert_eq!(
            state_store
                .canonical_frontier_snapshot()
                .effective_history_floor_ts,
            Some(replay_start_ts)
        );
    }

    #[tokio::test]
    async fn leading_gap_recovery_warms_trade_history_before_first_complete_minute() {
        let blocking_minute = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let warm_history_minute = blocking_minute + ChronoDuration::minutes(1);
        let replay_start_ts = blocking_minute + ChronoDuration::minutes(2);
        let continuity_end_ts = replay_start_ts + ChronoDuration::minutes(1);
        let mut state_store =
            crate::runtime::state_store::StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let mut scheduler = WindowScheduler::new(0);
        scheduler.prime_start_from(blocking_minute);

        state_store.ingest(agg_funding_mark_event(blocking_minute, 45, 2000.0, 0.01));

        for ts in [warm_history_minute, replay_start_ts, continuity_end_ts] {
            state_store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            state_store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
        }
        for ts in [replay_start_ts, continuity_end_ts] {
            state_store.ingest(agg_orderbook_event(ts, true, 4.0));
            state_store.ingest(agg_liq_event(ts, 10.0));
            state_store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            state_store.ingest(agg_orderbook_event_spot(ts, true, 4.0));
        }

        let next_minute = scheduler.next_minute_to_emit();
        let plan = super::maybe_recover_from_leading_canonical_gap(
            &mut state_store,
            &mut scheduler,
            next_minute,
        )
        .await
        .expect("expected leading-gap recovery plan");

        assert_eq!(plan.blocked_minute, Some(blocking_minute));
        assert_eq!(plan.history_floor_ts, warm_history_minute);
        assert_eq!(plan.warm_end_ts, Some(warm_history_minute));
        assert_eq!(plan.replay_start_ts, replay_start_ts);
        assert_eq!(plan.continuity_end_ts, continuity_end_ts);
        assert_eq!(plan.warmed_minutes, 1);
        assert_eq!(scheduler.next_minute_to_emit(), Some(replay_start_ts));
        assert_eq!(
            state_store.last_finalized_minute(),
            Some(warm_history_minute)
        );
        assert_eq!(state_store.history_futures_len(), 1);
        assert_eq!(
            state_store
                .canonical_frontier_snapshot()
                .effective_history_floor_ts,
            Some(warm_history_minute)
        );
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
