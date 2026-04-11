use crate::indicators::context::{
    AbsorptionEventRow, DivergenceEventRow, ExhaustionEventRow, IndicatorComputation,
    IndicatorContext, IndicatorEventRow, IndicatorLevelRow, IndicatorSnapshotRow,
    InitiationEventRow, LiquidationLevelRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::registry::build_registry;
use crate::publish::ind_publisher::{BundleOutboxMessage, IndPublisher};
use crate::storage::event_writer::EventWriter;
use crate::storage::feature_writer::FeatureWriter;
use crate::storage::level_writer::LevelWriter;
use crate::storage::snapshot_writer::SnapshotWriter;
use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

const PROCESS_WINDOW_WARN_MS: u128 = 5_000;
const PROCESS_WINDOW_STAGE_WARN_MS: u128 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    WarmStateOnly,
    ReplayMaterialize,
    Live,
    LiveCatchup,
    CutoverReplay,
    RepairReplay,
    ShutdownFlush,
}

impl DispatchMode {
    pub(crate) fn persist_outputs(self) -> bool {
        !matches!(self, Self::WarmStateOnly | Self::LiveCatchup)
    }

    pub(crate) fn persist_snapshots(self) -> bool {
        matches!(
            self,
            Self::Live | Self::CutoverReplay | Self::RepairReplay | Self::ShutdownFlush
        )
    }

    pub(crate) fn publish_outputs(self) -> bool {
        matches!(self, Self::Live | Self::RepairReplay | Self::ShutdownFlush)
    }
}

pub struct Dispatcher {
    flow_groups: Vec<IndicatorGroup>,
    deriv_indicators: Vec<Arc<dyn Indicator>>,
    orderbook_indicators: Vec<Arc<dyn Indicator>>,
    oi_ratio_patch_indicators: Vec<Arc<dyn Indicator>>,
    feature_writer: FeatureWriter,
    snapshot_writer: SnapshotWriter,
    level_writer: LevelWriter,
    event_writer: EventWriter,
    publisher: IndPublisher,
}

pub struct ProcessedWindowArtifacts {
    pub ctx: Arc<IndicatorContext>,
    pub mode: DispatchMode,
    pub started_at: Instant,
    pub compute_ms: u128,
    pub group_snapshot_counts: Vec<String>,
    pub snapshots: Vec<IndicatorSnapshotRow>,
    pub levels: Vec<IndicatorLevelRow>,
    pub events: Vec<IndicatorEventRow>,
    pub divergence_rows: Vec<DivergenceEventRow>,
    pub absorption_rows: Vec<AbsorptionEventRow>,
    pub initiation_rows: Vec<InitiationEventRow>,
    pub exhaustion_rows: Vec<ExhaustionEventRow>,
    pub liq_rows: Vec<LiquidationLevelRow>,
    pub live_messages: Option<Vec<BundleOutboxMessage>>,
}

#[derive(Clone)]
struct IndicatorGroup {
    name: &'static str,
    indicators: Vec<Arc<dyn Indicator>>,
}

#[derive(Default)]
struct GroupOutput {
    snapshots: Vec<IndicatorSnapshotRow>,
    levels: Vec<IndicatorLevelRow>,
    events: Vec<IndicatorEventRow>,
    divergence_rows: Vec<DivergenceEventRow>,
    absorption_rows: Vec<AbsorptionEventRow>,
    initiation_rows: Vec<InitiationEventRow>,
    exhaustion_rows: Vec<ExhaustionEventRow>,
    liq_rows: Vec<LiquidationLevelRow>,
}

impl Dispatcher {
    pub fn new(
        feature_writer: FeatureWriter,
        snapshot_writer: SnapshotWriter,
        level_writer: LevelWriter,
        event_writer: EventWriter,
        publisher: IndPublisher,
    ) -> Self {
        let indicators = build_registry();
        let mut flow_core_indicators = Vec::new();
        let mut flow_avwap_indicators = Vec::new();
        let mut flow_rvwap_indicators = Vec::new();
        let mut flow_high_volume_indicators = Vec::new();
        let mut flow_tpo_indicators = Vec::new();
        let mut deriv_indicators = Vec::new();
        let mut orderbook_indicators = Vec::new();
        let mut oi_ratio_patch_indicators = Vec::new();
        for indicator in indicators {
            if let Some(flow_group) = flow_group_name_for_indicator(indicator.code()) {
                match flow_group {
                    "flow_core" => flow_core_indicators.push(indicator),
                    "flow_avwap" => flow_avwap_indicators.push(indicator),
                    "flow_rvwap" => flow_rvwap_indicators.push(indicator),
                    "flow_high_volume" => flow_high_volume_indicators.push(indicator),
                    "flow_tpo" => flow_tpo_indicators.push(indicator),
                    _ => unreachable!("unknown flow group"),
                }
                continue;
            }
            match indicator.code() {
                "liquidation_density"
                | "funding_rate"
                | "open_interest"
                | "long_short_ratios"
                | "options_surface" => {
                    if matches!(indicator.code(), "open_interest" | "long_short_ratios") {
                        oi_ratio_patch_indicators.push(indicator.clone());
                    }
                    deriv_indicators.push(indicator);
                }
                _ => orderbook_indicators.push(indicator),
            }
        }

        let flow_groups = [
            ("flow_core", flow_core_indicators),
            ("flow_avwap", flow_avwap_indicators),
            ("flow_rvwap", flow_rvwap_indicators),
            ("flow_high_volume", flow_high_volume_indicators),
            ("flow_tpo", flow_tpo_indicators),
        ]
        .into_iter()
        .filter_map(|(name, indicators)| {
            if indicators.is_empty() {
                None
            } else {
                Some(IndicatorGroup { name, indicators })
            }
        })
        .collect::<Vec<_>>();

        Self {
            flow_groups,
            deriv_indicators,
            orderbook_indicators,
            oi_ratio_patch_indicators,
            feature_writer,
            snapshot_writer,
            level_writer,
            event_writer,
            publisher,
        }
    }

    pub async fn process_window(
        &self,
        ctx: Arc<IndicatorContext>,
        mode: DispatchMode,
    ) -> Result<Vec<IndicatorSnapshotRow>> {
        let artifacts = self.compute_window_artifacts(ctx, mode).await?;
        self.persist_window_artifacts(artifacts).await
    }

    pub async fn compute_window_artifacts(
        &self,
        ctx: Arc<IndicatorContext>,
        mode: DispatchMode,
    ) -> Result<ProcessedWindowArtifacts> {
        let total_started_at = Instant::now();
        let mut group_handles: Vec<(&'static str, JoinHandle<GroupOutput>)> = self
            .flow_groups
            .iter()
            .map(|group| {
                (
                    group.name,
                    spawn_group_worker(group.indicators.clone(), ctx.clone()),
                )
            })
            .collect();
        group_handles.push((
            "deriv",
            spawn_group_worker(self.deriv_indicators.clone(), ctx.clone()),
        ));
        group_handles.push((
            "orderbook_heavy",
            spawn_group_worker(self.orderbook_indicators.clone(), ctx.clone()),
        ));

        let mut group_outputs = Vec::with_capacity(group_handles.len());
        for (name, handle) in group_handles {
            group_outputs.push((name, join_group(name, handle).await?));
        }
        let compute_ms = total_started_at.elapsed().as_millis();
        let group_snapshot_counts = group_outputs
            .iter()
            .map(|(name, output)| format!("{name}={}", output.snapshots.len()))
            .collect::<Vec<_>>();

        let mut snapshots: Vec<IndicatorSnapshotRow> = Vec::new();
        let mut levels: Vec<IndicatorLevelRow> = Vec::new();
        let mut events: Vec<IndicatorEventRow> = Vec::new();
        let mut divergence_rows: Vec<DivergenceEventRow> = Vec::new();
        let mut absorption_rows: Vec<AbsorptionEventRow> = Vec::new();
        let mut initiation_rows: Vec<InitiationEventRow> = Vec::new();
        let mut exhaustion_rows: Vec<ExhaustionEventRow> = Vec::new();
        let mut liq_rows: Vec<LiquidationLevelRow> = Vec::new();

        for (_, output) in group_outputs {
            merge_group_output(
                output,
                &mut snapshots,
                &mut levels,
                &mut events,
                &mut divergence_rows,
                &mut absorption_rows,
                &mut initiation_rows,
                &mut exhaustion_rows,
                &mut liq_rows,
            );
        }
        snapshots.sort_by(|a, b| {
            a.indicator_code.cmp(b.indicator_code).then_with(|| {
                snapshot_window_rank(a.window_code).cmp(&snapshot_window_rank(b.window_code))
            })
        });

        let live_messages = if mode.publish_outputs() {
            let indicators_json_value = assemble_live_indicators_json(&snapshots);
            let mut messages = Vec::with_capacity(1);
            messages.push(self.publisher.build_minute_bundle_outbox_message(
                ctx.ts_bucket,
                &ctx.symbol,
                &indicators_json_value,
                snapshots.len(),
            )?);
            Some(messages)
        } else {
            None
        };

        let total_ms = total_started_at.elapsed().as_millis();
        debug!(
            ts_bucket = %ctx.ts_bucket,
            mode = ?mode,
            snapshot_count = snapshots.len(),
            group_snapshot_counts = %group_snapshot_counts.join(","),
            level_count = levels.len(),
            event_count = events.len(),
            divergence_count = divergence_rows.len(),
            absorption_count = absorption_rows.len(),
            initiation_count = initiation_rows.len(),
            exhaustion_count = exhaustion_rows.len(),
            liq_levels = liq_rows.len(),
            compute_ms = compute_ms,
            total_ms = total_ms,
            "indicator window processed"
        );

        Ok(ProcessedWindowArtifacts {
            ctx,
            mode,
            started_at: total_started_at,
            compute_ms,
            group_snapshot_counts,
            snapshots,
            levels,
            events,
            divergence_rows,
            absorption_rows,
            initiation_rows,
            exhaustion_rows,
            liq_rows,
            live_messages,
        })
    }

    pub async fn persist_window_artifacts(
        &self,
        artifacts: ProcessedWindowArtifacts,
    ) -> Result<Vec<IndicatorSnapshotRow>> {
        let ProcessedWindowArtifacts {
            ctx,
            mode,
            started_at,
            compute_ms,
            group_snapshot_counts: _group_snapshot_counts,
            snapshots,
            levels,
            events,
            divergence_rows,
            absorption_rows,
            initiation_rows,
            exhaustion_rows,
            liq_rows,
            live_messages,
        } = artifacts;

        if mode.persist_outputs() {
            let snapshot_write_ms = if mode.persist_snapshots() {
                let snapshot_started_at = Instant::now();
                self.snapshot_writer
                    .write_snapshots(ctx.ts_bucket, &ctx.symbol, &snapshots)
                    .await?;
                snapshot_started_at.elapsed().as_millis()
            } else {
                0
            };
            let level_started_at = Instant::now();
            self.level_writer
                .write_indicator_levels(ctx.ts_bucket, &ctx.symbol, &levels)
                .await?;
            self.level_writer
                .write_liquidation_levels(ctx.ts_bucket, &ctx.symbol, &liq_rows)
                .await?;
            let level_write_ms = level_started_at.elapsed().as_millis();
            let event_history_end_ts = ctx.ts_bucket + chrono::Duration::minutes(1);
            let event_started_at = Instant::now();
            if let Err(err) = self
                .event_writer
                .write_indicator_events(&ctx.symbol, event_history_end_ts, &events)
                .await
            {
                warn!(
                    error = %err,
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    "indicator event projection failed; continuing without blocking persisted frontier"
                );
            }
            if let Err(err) = self
                .event_writer
                .write_divergence_events(&ctx.symbol, event_history_end_ts, &divergence_rows)
                .await
            {
                warn!(
                    error = %err,
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    "divergence event projection failed; continuing without blocking persisted frontier"
                );
            }
            if let Err(err) = self
                .event_writer
                .write_absorption_events(&ctx.symbol, event_history_end_ts, &absorption_rows)
                .await
            {
                warn!(
                    error = %err,
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    "absorption event projection failed; continuing without blocking persisted frontier"
                );
            }
            if let Err(err) = self
                .event_writer
                .write_initiation_events(&ctx.symbol, event_history_end_ts, &initiation_rows)
                .await
            {
                warn!(
                    error = %err,
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    "initiation event projection failed; continuing without blocking persisted frontier"
                );
            }
            if let Err(err) = self
                .event_writer
                .write_exhaustion_events(&ctx.symbol, event_history_end_ts, &exhaustion_rows)
                .await
            {
                warn!(
                    error = %err,
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    "exhaustion event projection failed; continuing without blocking persisted frontier"
                );
            }
            let event_write_ms = event_started_at.elapsed().as_millis();
            let feature_started_at = Instant::now();
            self.feature_writer.write_all(&ctx).await?;
            let feature_write_ms = feature_started_at.elapsed().as_millis();
            let progress_started_at = Instant::now();
            if let Some(messages) = live_messages.as_ref() {
                self.snapshot_writer
                    .advance_progress_with_outbox(&ctx.symbol, ctx.ts_bucket, messages)
                    .await?;
            } else {
                self.snapshot_writer
                    .advance_progress(&ctx.symbol, ctx.ts_bucket)
                    .await?;
            }
            let progress_commit_ms = progress_started_at.elapsed().as_millis();
            let total_ms = started_at.elapsed().as_millis();

            if total_ms >= PROCESS_WINDOW_WARN_MS
                || snapshot_write_ms >= PROCESS_WINDOW_STAGE_WARN_MS
                || level_write_ms >= PROCESS_WINDOW_STAGE_WARN_MS
                || event_write_ms >= PROCESS_WINDOW_STAGE_WARN_MS
                || feature_write_ms >= PROCESS_WINDOW_STAGE_WARN_MS
                || progress_commit_ms >= PROCESS_WINDOW_STAGE_WARN_MS
            {
                warn!(
                    ts_bucket = %ctx.ts_bucket,
                    symbol = %ctx.symbol,
                    mode = ?mode,
                    snapshot_count = snapshots.len(),
                    level_count = levels.len(),
                    event_count = events.len(),
                    divergence_count = divergence_rows.len(),
                    absorption_count = absorption_rows.len(),
                    initiation_count = initiation_rows.len(),
                    exhaustion_count = exhaustion_rows.len(),
                    liq_levels = liq_rows.len(),
                    compute_ms = compute_ms,
                    snapshot_write_ms = snapshot_write_ms,
                    level_write_ms = level_write_ms,
                    event_write_ms = event_write_ms,
                    feature_write_ms = feature_write_ms,
                    progress_commit_ms = progress_commit_ms,
                    total_ms = total_ms,
                    "slow indicator minute processing"
                );
            }
        }

        Ok(snapshots)
    }

    pub async fn rewind_progress(
        &self,
        symbol: &str,
        ts_bucket: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.snapshot_writer
            .rewind_progress(symbol, ts_bucket)
            .await
    }

    pub async fn rewind_persisted_tail(
        &self,
        symbol: &str,
        repair_start_ts: chrono::DateTime<chrono::Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        self.snapshot_writer
            .rewind_persisted_tail(symbol, repair_start_ts, exchange_name)
            .await
    }

    pub async fn clear_persisted_range_for_repair(
        &self,
        symbol: &str,
        repair_start_ts: chrono::DateTime<chrono::Utc>,
        repair_end_ts: chrono::DateTime<chrono::Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        self.snapshot_writer
            .clear_persisted_range_for_repair(symbol, repair_start_ts, repair_end_ts, exchange_name)
            .await
    }

    pub async fn mark_snapshot_fanout_repair_pending(
        &self,
        symbol: &str,
        repair_start_ts: chrono::DateTime<chrono::Utc>,
        repair_end_ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.snapshot_writer
            .mark_snapshot_fanout_repair_pending(symbol, repair_start_ts, repair_end_ts)
            .await
    }

    pub async fn suppress_repair_bundle_publish_tail(
        &self,
        symbol: &str,
        repair_start_ts: chrono::DateTime<chrono::Utc>,
        exchange_name: &str,
    ) -> Result<()> {
        self.snapshot_writer
            .suppress_repair_bundle_publish_tail(symbol, repair_start_ts, exchange_name)
            .await
    }

    pub async fn set_snapshot_fanout_progress(
        &self,
        symbol: &str,
        ts_bucket: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.snapshot_writer
            .set_snapshot_fanout_progress(symbol, ts_bucket)
            .await
    }

    pub async fn process_oi_ratio_patch_window(
        &self,
        ctx: Arc<IndicatorContext>,
    ) -> Result<Vec<IndicatorSnapshotRow>> {
        let started_at = Instant::now();
        let output = evaluate_indicator_group(self.oi_ratio_patch_indicators.clone(), ctx.as_ref());
        let mut snapshots = output.snapshots;
        snapshots.sort_by(|a, b| {
            a.indicator_code.cmp(b.indicator_code).then_with(|| {
                snapshot_window_rank(a.window_code).cmp(&snapshot_window_rank(b.window_code))
            })
        });

        self.snapshot_writer
            .write_snapshots(ctx.ts_bucket, &ctx.symbol, &snapshots)
            .await?;
        self.feature_writer.write_oi_ratio_only(&ctx).await?;

        let (indicators_json, indicator_count) = self
            .snapshot_writer
            .load_minute_bundle_indicators_json(&ctx.symbol, ctx.ts_bucket)
            .await?;
        let repair_message = self
            .publisher
            .build_minute_bundle_outbox_message_with_extra_headers(
                ctx.ts_bucket,
                &ctx.symbol,
                &indicators_json,
                indicator_count,
                Some(json!({
                    "repair_reason": "oi_ratio_patch",
                    "repair_scope": "indicator_subset",
                })),
            )?;
        self.snapshot_writer
            .enqueue_bundle_repairs(&[repair_message])
            .await?;

        debug!(
            ts_bucket = %ctx.ts_bucket,
            symbol = %ctx.symbol,
            snapshot_count = snapshots.len(),
            total_ms = started_at.elapsed().as_millis(),
            "oi_ratio patch window processed"
        );

        Ok(snapshots)
    }
}

fn flow_group_name_for_indicator(indicator_code: &str) -> Option<&'static str> {
    match indicator_code {
        "price_volume_structure"
        | "footprint"
        | "divergence"
        | "cvd_pack"
        | "whale_trades"
        | "vpin"
        | "kline_history"
        | "ema_trend_regime"
        | "fvg" => Some("flow_core"),
        "avwap" => Some("flow_avwap"),
        "rvwap_sigma_bands" => Some("flow_rvwap"),
        "high_volume_pulse" => Some("flow_high_volume"),
        "tpo_market_profile" => Some("flow_tpo"),
        _ => None,
    }
}

fn spawn_group_worker(
    indicators: Vec<Arc<dyn Indicator>>,
    ctx: Arc<IndicatorContext>,
) -> JoinHandle<GroupOutput> {
    tokio::task::spawn_blocking(move || evaluate_indicator_group(indicators, ctx.as_ref()))
}

async fn join_group(group_name: &str, handle: JoinHandle<GroupOutput>) -> Result<GroupOutput> {
    handle
        .await
        .with_context(|| format!("join indicator group worker failed group={}", group_name))
}

fn evaluate_indicator_group(
    indicators: Vec<Arc<dyn Indicator>>,
    ctx: &IndicatorContext,
) -> GroupOutput {
    let mut out = GroupOutput::default();
    for indicator in &indicators {
        let comp: IndicatorComputation = indicator.evaluate(ctx);

        if let Some(s) = comp.snapshot {
            out.snapshots.push(s);
        }
        if !comp.snapshot_rows.is_empty() {
            out.snapshots.extend(comp.snapshot_rows);
        }
        if !comp.level_rows.is_empty() {
            out.levels.extend(comp.level_rows);
        }
        if !comp.event_rows.is_empty() {
            out.events.extend(comp.event_rows);
        }
        if !comp.divergence_rows.is_empty() {
            out.divergence_rows.extend(comp.divergence_rows);
        }
        if !comp.absorption_rows.is_empty() {
            out.absorption_rows.extend(comp.absorption_rows);
        }
        if !comp.initiation_rows.is_empty() {
            out.initiation_rows.extend(comp.initiation_rows);
        }
        if !comp.exhaustion_rows.is_empty() {
            out.exhaustion_rows.extend(comp.exhaustion_rows);
        }
        if !comp.liquidation_rows.is_empty() {
            out.liq_rows.extend(comp.liquidation_rows);
        }
    }
    out
}

fn merge_group_output(
    group: GroupOutput,
    snapshots: &mut Vec<IndicatorSnapshotRow>,
    levels: &mut Vec<IndicatorLevelRow>,
    events: &mut Vec<IndicatorEventRow>,
    divergence_rows: &mut Vec<DivergenceEventRow>,
    absorption_rows: &mut Vec<AbsorptionEventRow>,
    initiation_rows: &mut Vec<InitiationEventRow>,
    exhaustion_rows: &mut Vec<ExhaustionEventRow>,
    liq_rows: &mut Vec<LiquidationLevelRow>,
) {
    snapshots.extend(group.snapshots);
    levels.extend(group.levels);
    events.extend(group.events);
    divergence_rows.extend(group.divergence_rows);
    absorption_rows.extend(group.absorption_rows);
    initiation_rows.extend(group.initiation_rows);
    exhaustion_rows.extend(group.exhaustion_rows);
    liq_rows.extend(group.liq_rows);
}

fn assemble_live_indicators_json(snapshots: &[IndicatorSnapshotRow]) -> Value {
    let mut indicators_json = Map::new();
    for snapshot in snapshots {
        indicators_json
            .entry(snapshot.indicator_code.to_string())
            .or_insert_with(|| {
                json!({
                    "window_code": snapshot.window_code,
                    "payload": snapshot.payload_json.clone(),
                })
            });
    }
    Value::Object(indicators_json)
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

#[cfg(test)]
mod tests {
    use super::{assemble_live_indicators_json, flow_group_name_for_indicator, DispatchMode};
    use crate::indicators::context::IndicatorSnapshotRow;
    use serde_json::json;

    #[test]
    fn shutdown_flush_still_persists_and_publishes_outputs() {
        assert!(DispatchMode::ShutdownFlush.persist_outputs());
        assert!(DispatchMode::ShutdownFlush.persist_snapshots());
        assert!(DispatchMode::ShutdownFlush.publish_outputs());
    }

    #[test]
    fn repair_replay_still_persists_and_publishes_outputs() {
        assert!(DispatchMode::RepairReplay.persist_outputs());
        assert!(DispatchMode::RepairReplay.persist_snapshots());
        assert!(DispatchMode::RepairReplay.publish_outputs());
    }

    #[test]
    fn replay_materialize_skips_snapshot_side_effects() {
        assert!(DispatchMode::ReplayMaterialize.persist_outputs());
        assert!(!DispatchMode::ReplayMaterialize.persist_snapshots());
        assert!(!DispatchMode::ReplayMaterialize.publish_outputs());
    }

    #[test]
    fn live_catchup_stays_compute_only() {
        assert!(!DispatchMode::LiveCatchup.persist_outputs());
        assert!(!DispatchMode::LiveCatchup.persist_snapshots());
        assert!(!DispatchMode::LiveCatchup.publish_outputs());
    }

    #[test]
    fn cutover_replay_persists_without_publishing() {
        assert!(DispatchMode::CutoverReplay.persist_outputs());
        assert!(DispatchMode::CutoverReplay.persist_snapshots());
        assert!(!DispatchMode::CutoverReplay.publish_outputs());
    }

    #[test]
    fn heavy_flow_indicators_are_isolated_into_dedicated_groups() {
        assert_eq!(flow_group_name_for_indicator("avwap"), Some("flow_avwap"));
        assert_eq!(
            flow_group_name_for_indicator("rvwap_sigma_bands"),
            Some("flow_rvwap")
        );
        assert_eq!(
            flow_group_name_for_indicator("high_volume_pulse"),
            Some("flow_high_volume")
        );
        assert_eq!(
            flow_group_name_for_indicator("tpo_market_profile"),
            Some("flow_tpo")
        );
        assert_eq!(
            flow_group_name_for_indicator("price_volume_structure"),
            Some("flow_core")
        );
        assert_eq!(flow_group_name_for_indicator("funding_rate"), None);
    }

    #[test]
    fn live_bundle_assembly_preserves_avwap_by_window_payload() {
        let snapshots = vec![
            IndicatorSnapshotRow {
                indicator_code: "avwap",
                window_code: "1m",
                payload_json: json!({
                    "indicator": "avwap_dual_market",
                    "window": "1m",
                    "by_window": {
                        "4h": {
                            "window_code": "4h",
                            "is_ready": true,
                            "avwap_fut": 2050.5
                        },
                        "1d": {
                            "window_code": "1d",
                            "is_ready": false,
                            "avwap_fut": 2060.5
                        },
                        "3d": {
                            "window_code": "3d",
                            "is_ready": false,
                            "avwap_fut": 2070.5
                        },
                        "7d": {
                            "window_code": "7d",
                            "is_ready": false,
                            "avwap_fut": 2080.5
                        }
                    }
                }),
            },
            IndicatorSnapshotRow {
                indicator_code: "price_volume_structure",
                window_code: "1m",
                payload_json: json!({
                    "window": "1m",
                    "by_window": {
                        "4h": {
                            "poc_price": 2052.0
                        }
                    }
                }),
            },
        ];

        let indicators_json = assemble_live_indicators_json(&snapshots);
        assert_eq!(
            indicators_json["avwap"]["payload"]["by_window"]["4h"]["window_code"],
            json!("4h")
        );
        assert_eq!(
            indicators_json["avwap"]["payload"]["by_window"]["1d"]["avwap_fut"],
            json!(2060.5)
        );
        assert_eq!(
            indicators_json["avwap"]["payload"]["by_window"]["3d"]["avwap_fut"],
            json!(2070.5)
        );
        assert_eq!(
            indicators_json["avwap"]["payload"]["by_window"]["7d"]["avwap_fut"],
            json!(2080.5)
        );
    }
}
