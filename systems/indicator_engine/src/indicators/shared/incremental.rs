use crate::indicators::shared::funding::funding_change_json;
use crate::runtime::state_store::{
    tick_to_price, FundingChange, LatestFundingState, LatestMarkState, LevelAgg, MinuteHistory,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

const EPS: f64 = 1e-12;
const HISTORY_LIMIT_MINUTES_I64: i64 = crate::runtime::state_store::HISTORY_LIMIT_MINUTES as i64;
const AVWAP_LOOKBACK_DAYS: i64 = 7;
const AVWAP_LOOKBACK_MINUTES: i64 = AVWAP_LOOKBACK_DAYS * 24 * 60;
const AVWAP_WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const FUNDING_WINDOWS: [(&str, i64); 5] = [
    ("15m", 15),
    ("1h", 60),
    ("4h", 240),
    ("1d", 1440),
    ("3d", 4320),
];
const FUNDING_ALL_WINDOWS: [i64; 6] = [1, 15, 60, 240, 1440, 4320];

#[derive(Debug, Clone, Default)]
pub struct IncrementalIndicatorOutputs {
    pub funding_snapshot: Option<Value>,
    pub funding_feature_windows: BTreeMap<i64, FundingFeatureOutput>,
    pub avwap_snapshot: Option<Value>,
    pub avwap_feature: Option<AvwapFeatureOutput>,
    pub tpo_snapshot: Option<Value>,
    pub rvwap_snapshot: Option<Value>,
    pub high_volume_pulse_snapshot: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct AvwapFeatureOutput {
    pub anchor_ts: Option<DateTime<Utc>>,
    pub avwap_fut: Option<f64>,
    pub avwap_spot: Option<f64>,
    pub fut_last_price: Option<f64>,
    pub fut_mark_price: Option<f64>,
    pub price_minus_avwap_fut: Option<f64>,
    pub price_minus_spot_avwap_fut: Option<f64>,
    pub price_minus_spot_avwap_futmark: Option<f64>,
    pub avwap_gap_fs: Option<f64>,
    pub xmk_avwap_gap_f_minus_s: Option<f64>,
    pub zavwap_gap: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct FundingFeatureOutput {
    pub funding_current: Option<f64>,
    pub funding_current_effective_ts: Option<DateTime<Utc>>,
    pub funding_twa: Option<f64>,
    pub mark_price_last: Option<f64>,
    pub mark_price_last_ts: Option<DateTime<Utc>>,
    pub mark_price_twap: Option<f64>,
    pub index_price_last: Option<f64>,
    pub changes_json: Value,
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalIndicatorConfig {
    pub tpo_rows_nb: usize,
    pub tpo_value_area_pct: f64,
    pub tpo_session_windows: Vec<(String, i64)>,
    pub tpo_ib_minutes: i64,
    pub tpo_dev_output_windows: Vec<(String, i64)>,
    pub rvwap_windows: Vec<(String, i64)>,
    pub rvwap_output_windows: Vec<(String, i64)>,
    pub rvwap_min_samples: usize,
    pub high_volume_pulse_z_windows: Vec<(String, i64)>,
    pub high_volume_pulse_summary_windows: Vec<(String, i64)>,
    pub high_volume_pulse_min_samples: usize,
}

pub struct IncrementalIndicatorState {
    configured: bool,
    config: IncrementalIndicatorConfig,
    outputs: Arc<IncrementalIndicatorOutputs>,
    funding: FundingState,
    avwap: AvwapState,
    rvwap: RvwapState,
    high_volume: HighVolumePulseState,
    tpo: TpoState,
}

impl Default for IncrementalIndicatorState {
    fn default() -> Self {
        Self {
            configured: false,
            config: IncrementalIndicatorConfig::default(),
            outputs: Arc::new(IncrementalIndicatorOutputs::default()),
            funding: FundingState::default(),
            avwap: AvwapState::default(),
            rvwap: RvwapState::default(),
            high_volume: HighVolumePulseState::default(),
            tpo: TpoState::default(),
        }
    }
}

impl IncrementalIndicatorState {
    pub fn configure(&mut self, config: IncrementalIndicatorConfig) {
        self.configured = true;
        self.config = config;
        self.reset();
    }

    pub fn reset(&mut self) {
        self.outputs = Arc::new(IncrementalIndicatorOutputs::default());
        self.funding = FundingState::new();
        self.avwap = AvwapState::new();
        self.rvwap = RvwapState::new(
            self.config.rvwap_windows.clone(),
            self.config.rvwap_output_windows.clone(),
            self.config.rvwap_min_samples,
        );
        self.high_volume = HighVolumePulseState::new(
            self.config.high_volume_pulse_z_windows.clone(),
            self.config.high_volume_pulse_summary_windows.clone(),
            self.config.high_volume_pulse_min_samples,
        );
        self.tpo = TpoState::new(
            self.config.tpo_rows_nb,
            self.config.tpo_value_area_pct,
            self.config.tpo_session_windows.clone(),
            self.config.tpo_ib_minutes,
            self.config.tpo_dev_output_windows.clone(),
        );
    }

    pub fn outputs(&self) -> Arc<IncrementalIndicatorOutputs> {
        self.outputs.clone()
    }

    pub fn rebuild(
        &mut self,
        ts_bucket: Option<DateTime<Utc>>,
        history_futures: &[MinuteHistory],
        history_spot: &[MinuteHistory],
        latest_mark: Option<&LatestMarkState>,
        funding_changes_recent: &[FundingChange],
        _funding_recent_7d_payload: Arc<Vec<Value>>,
        funding_points_recent: &[LatestFundingState],
        mark_points_recent: &[LatestMarkState],
    ) {
        self.reset();
        if !self.configured {
            return;
        }
        let Some(ts_bucket) = ts_bucket else {
            return;
        };
        self.funding.rebuild(
            ts_bucket,
            funding_changes_recent,
            funding_points_recent,
            mark_points_recent,
        );
        self.avwap.rebuild(history_futures, history_spot);
        self.rvwap.rebuild(history_futures);
        self.high_volume.rebuild(history_futures);
        self.tpo.rebuild(history_futures, ts_bucket);
        self.outputs = Arc::new(build_incremental_outputs(
            &self.funding,
            &self.avwap,
            &self.rvwap,
            &self.high_volume,
            &self.tpo,
            ts_bucket,
            history_futures.last(),
            latest_mark,
        ));
    }

    pub fn on_finalized_minute(
        &mut self,
        ts_bucket: DateTime<Utc>,
        history_futures: &[MinuteHistory],
        history_spot: &[MinuteHistory],
        latest_mark: Option<&LatestMarkState>,
        funding_changes_recent: &[FundingChange],
        _funding_recent_7d_payload: Arc<Vec<Value>>,
        funding_points_recent: &[LatestFundingState],
        mark_points_recent: &[LatestMarkState],
    ) {
        if !self.configured {
            return;
        }

        self.funding.sync(
            ts_bucket,
            funding_changes_recent,
            funding_points_recent,
            mark_points_recent,
        );
        self.avwap.sync(history_futures, history_spot);
        self.rvwap.sync(history_futures);
        self.high_volume.sync(history_futures);
        self.tpo.sync(history_futures, ts_bucket);

        self.outputs = Arc::new(build_incremental_outputs(
            &self.funding,
            &self.avwap,
            &self.rvwap,
            &self.high_volume,
            &self.tpo,
            ts_bucket,
            history_futures.last(),
            latest_mark,
        ));
    }
}

fn build_incremental_outputs(
    funding: &FundingState,
    avwap: &AvwapState,
    rvwap: &RvwapState,
    high_volume: &HighVolumePulseState,
    tpo: &TpoState,
    ts_bucket: DateTime<Utc>,
    latest_futures: Option<&MinuteHistory>,
    latest_mark: Option<&LatestMarkState>,
) -> IncrementalIndicatorOutputs {
    let (funding_snapshot, funding_feature_windows) = funding.outputs(ts_bucket);
    let (avwap_snapshot, avwap_feature) =
        avwap.build_outputs(ts_bucket, latest_futures, latest_mark);

    IncrementalIndicatorOutputs {
        funding_snapshot,
        funding_feature_windows,
        avwap_snapshot,
        avwap_feature,
        tpo_snapshot: tpo.snapshot(ts_bucket),
        rvwap_snapshot: rvwap.snapshot(ts_bucket),
        high_volume_pulse_snapshot: high_volume.snapshot(ts_bucket),
    }
}

#[derive(Debug, Clone, Default)]
struct FundingWindowState {
    minutes: i64,
    funding_area: Option<f64>,
    mark_area: Option<f64>,
    changes_json: VecDeque<Value>,
}

#[derive(Default)]
struct FundingState {
    windows: BTreeMap<i64, FundingWindowState>,
    funding_points: VecDeque<LatestFundingState>,
    mark_points: VecDeque<LatestMarkState>,
    changes: VecDeque<FundingChange>,
    recent_7d_payload: VecDeque<(DateTime<Utc>, Value)>,
    last_ts: Option<DateTime<Utc>>,
}

impl FundingState {
    fn new() -> Self {
        let mut windows = BTreeMap::new();
        for minutes in FUNDING_ALL_WINDOWS {
            windows.insert(
                minutes,
                FundingWindowState {
                    minutes,
                    ..Default::default()
                },
            );
        }
        Self {
            windows,
            funding_points: VecDeque::new(),
            mark_points: VecDeque::new(),
            changes: VecDeque::new(),
            recent_7d_payload: VecDeque::new(),
            last_ts: None,
        }
    }

    fn rebuild(
        &mut self,
        ts_bucket: DateTime<Utc>,
        funding_changes_recent: &[FundingChange],
        funding_points_recent: &[LatestFundingState],
        mark_points_recent: &[LatestMarkState],
    ) {
        *self = Self::new();
        self.funding_points = funding_points_recent.iter().cloned().collect();
        self.mark_points = mark_points_recent.iter().cloned().collect();
        self.changes = funding_changes_recent.iter().cloned().collect();
        let recent_cutoff = ts_bucket + Duration::minutes(1) - Duration::days(7);
        self.recent_7d_payload = funding_changes_recent
            .iter()
            .filter(|change| change.ts_change >= recent_cutoff)
            .map(|change| (change.ts_change, funding_change_json(change)))
            .collect();
        self.last_ts = Some(ts_bucket);
        self.refresh_all_windows(ts_bucket);
    }

    fn sync(
        &mut self,
        ts_bucket: DateTime<Utc>,
        funding_changes_recent: &[FundingChange],
        funding_points_recent: &[LatestFundingState],
        mark_points_recent: &[LatestMarkState],
    ) {
        let Some(last_ts) = self.last_ts else {
            self.rebuild(
                ts_bucket,
                funding_changes_recent,
                funding_points_recent,
                mark_points_recent,
            );
            return;
        };
        if ts_bucket <= last_ts {
            return;
        }
        if ts_bucket != last_ts + Duration::minutes(1) {
            self.rebuild(
                ts_bucket,
                funding_changes_recent,
                funding_points_recent,
                mark_points_recent,
            );
            return;
        }

        self.append_new_funding_points(funding_points_recent);
        self.append_new_mark_points(mark_points_recent);
        let minute_changes = self.append_new_changes(funding_changes_recent, ts_bucket);
        self.prune(ts_bucket);
        self.advance_windows(last_ts, ts_bucket, &minute_changes);
        self.last_ts = Some(ts_bucket);
    }

    fn outputs(
        &self,
        ts_bucket: DateTime<Utc>,
    ) -> (Option<Value>, BTreeMap<i64, FundingFeatureOutput>) {
        let end = ts_bucket + Duration::minutes(1);
        let mut feature_windows = BTreeMap::new();
        for minutes in FUNDING_WINDOWS.map(|(_, mins)| mins) {
            feature_windows.insert(minutes, self.feature_window(minutes, end));
        }
        let current = self.feature_window(1, end);
        let mut by_window = Map::new();
        for (label, minutes) in FUNDING_WINDOWS {
            let feature = feature_windows
                .get(&minutes)
                .cloned()
                .unwrap_or_else(|| FundingFeatureOutput::default());
            let changes = feature.changes_json.as_array().cloned().unwrap_or_default();
            by_window.insert(
                label.to_string(),
                json!({
                    "window": label,
                    "funding_current": feature.funding_current,
                    "funding_current_effective_ts": feature.funding_current_effective_ts.map(|ts| ts.to_rfc3339()),
                    "funding_twa": feature.funding_twa,
                    "mark_price_last": feature.mark_price_last,
                    "mark_price_last_ts": feature.mark_price_last_ts.map(|ts| ts.to_rfc3339()),
                    "mark_price_twap": feature.mark_price_twap,
                    "change_count": changes.len(),
                    "changes": changes,
                }),
            );
        }
        let snapshot = json!({
            "funding_current": current.funding_current,
            "funding_current_effective_ts": current.funding_current_effective_ts.map(|ts| ts.to_rfc3339()),
            "funding_twa": current.funding_twa,
            "mark_price_last": current.mark_price_last,
            "mark_price_last_ts": current.mark_price_last_ts.map(|ts| ts.to_rfc3339()),
            "mark_price_twap": current.mark_price_twap,
            "recent_7d": self.recent_7d_payload.iter().map(|(_, payload)| payload.clone()).collect::<Vec<_>>(),
            "by_window": by_window,
        });
        (Some(snapshot), feature_windows)
    }

    fn append_new_funding_points(&mut self, funding_points_recent: &[LatestFundingState]) {
        let start_idx = self
            .funding_points
            .back()
            .map(|point| {
                lower_bound_funding_point_ts(
                    funding_points_recent,
                    point.ts + Duration::microseconds(1),
                )
            })
            .unwrap_or(0);
        for point in &funding_points_recent[start_idx..] {
            self.funding_points.push_back(point.clone());
        }
    }

    fn append_new_mark_points(&mut self, mark_points_recent: &[LatestMarkState]) {
        let start_idx = self
            .mark_points
            .back()
            .map(|point| {
                lower_bound_mark_point_ts(mark_points_recent, point.ts + Duration::microseconds(1))
            })
            .unwrap_or(0);
        for point in &mark_points_recent[start_idx..] {
            self.mark_points.push_back(point.clone());
        }
    }

    fn append_new_changes(
        &mut self,
        funding_changes_recent: &[FundingChange],
        ts_bucket: DateTime<Utc>,
    ) -> Vec<Value> {
        let start_idx = self
            .changes
            .back()
            .map(|change| {
                lower_bound_funding_change_ts(
                    funding_changes_recent,
                    change.ts_change + Duration::microseconds(1),
                )
            })
            .unwrap_or(0);
        let minute_start = ts_bucket;
        let minute_end = ts_bucket + Duration::minutes(1);
        let mut appended = Vec::new();
        for change in &funding_changes_recent[start_idx..] {
            self.changes.push_back(change.clone());
            let payload = funding_change_json(change);
            if change.ts_change >= minute_start && change.ts_change < minute_end {
                appended.push(payload.clone());
            }
            self.recent_7d_payload
                .push_back((change.ts_change, payload));
        }
        appended
    }

    fn prune(&mut self, ts_bucket: DateTime<Utc>) {
        let end = ts_bucket + Duration::minutes(1);
        let points_cutoff = end - Duration::days(7) - Duration::minutes(1);
        while self
            .funding_points
            .front()
            .map(|point| point.ts < points_cutoff)
            .unwrap_or(false)
        {
            self.funding_points.pop_front();
        }
        while self
            .mark_points
            .front()
            .map(|point| point.ts < points_cutoff)
            .unwrap_or(false)
        {
            self.mark_points.pop_front();
        }
        while self
            .changes
            .front()
            .map(|change| change.ts_change < end - Duration::days(7))
            .unwrap_or(false)
        {
            self.changes.pop_front();
        }
        while self
            .recent_7d_payload
            .front()
            .map(|(ts, _)| *ts < end - Duration::days(7))
            .unwrap_or(false)
        {
            self.recent_7d_payload.pop_front();
        }
    }

    fn refresh_all_windows(&mut self, ts_bucket: DateTime<Utc>) {
        let end = ts_bucket + Duration::minutes(1);
        for state in self.windows.values_mut() {
            let start = end - Duration::minutes(state.minutes);
            state.funding_area = funding_piecewise_area(start, end, &self.funding_points);
            state.mark_area = mark_piecewise_area(start, end, &self.mark_points);
            state.changes_json = self
                .changes
                .iter()
                .filter(|change| change.ts_change >= start && change.ts_change < end)
                .map(funding_change_json)
                .collect();
        }
    }

    fn advance_windows(
        &mut self,
        previous_ts: DateTime<Utc>,
        ts_bucket: DateTime<Utc>,
        minute_changes: &[Value],
    ) {
        let old_end = previous_ts + Duration::minutes(1);
        let new_end = ts_bucket + Duration::minutes(1);
        for state in self.windows.values_mut() {
            let old_start = old_end - Duration::minutes(state.minutes);
            let new_start = new_end - Duration::minutes(state.minutes);
            state.funding_area = roll_piecewise_area(
                state.funding_area,
                &self.funding_points,
                old_start,
                new_start,
                old_end,
                new_end,
                funding_piecewise_area,
            );
            state.mark_area = roll_piecewise_area(
                state.mark_area,
                &self.mark_points,
                old_start,
                new_start,
                old_end,
                new_end,
                mark_piecewise_area,
            );
            while state
                .changes_json
                .front()
                .map(|payload| {
                    payload
                        .get("change_ts")
                        .and_then(Value::as_str)
                        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                        .map(|ts| ts.with_timezone(&Utc) < new_start)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
            {
                state.changes_json.pop_front();
            }
            for payload in minute_changes {
                state.changes_json.push_back(payload.clone());
            }
        }
    }

    fn feature_window(&self, minutes: i64, end: DateTime<Utc>) -> FundingFeatureOutput {
        let state = self.windows.get(&minutes);
        let current_funding = latest_funding_at_or_before(&self.funding_points, end);
        let current_mark = latest_mark_at_or_before(&self.mark_points, end);
        FundingFeatureOutput {
            funding_current: current_funding.map(|(_, value)| value),
            funding_current_effective_ts: current_funding.map(|(ts, _)| ts),
            funding_twa: state
                .and_then(|state| state.funding_area)
                .map(|area| area / (minutes.max(1) as f64 * 60.0)),
            mark_price_last: current_mark.map(|(ts, value)| {
                let _ = ts;
                value
            }),
            mark_price_last_ts: current_mark.map(|(ts, _)| ts),
            mark_price_twap: state
                .and_then(|state| state.mark_area)
                .map(|area| area / (minutes.max(1) as f64 * 60.0)),
            index_price_last: latest_index_at_or_before(&self.mark_points, end),
            changes_json: Value::Array(
                state
                    .map(|state| state.changes_json.iter().cloned().collect())
                    .unwrap_or_default(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct AvwapMinuteContribution {
    ts_bucket: DateTime<Utc>,
    fut_notional: f64,
    fut_qty: f64,
    spot_notional: f64,
    spot_qty: f64,
    gap_minute: Option<f64>,
}

#[derive(Debug, Clone)]
struct AvwapSeriesRow {
    ts_bucket: DateTime<Utc>,
    avwap_fut: Option<f64>,
    avwap_spot: Option<f64>,
    gap: Option<f64>,
}

#[derive(Default)]
struct AvwapState {
    contributions: VecDeque<AvwapMinuteContribution>,
    sum_fut_notional: f64,
    sum_fut_qty: f64,
    sum_spot_notional: f64,
    sum_spot_qty: f64,
    gap_values: VecDeque<(DateTime<Utc>, f64)>,
    gap_sum: f64,
    gap_sumsq: f64,
    series_by_window: BTreeMap<String, VecDeque<AvwapSeriesRow>>,
    series_json_by_window: BTreeMap<String, VecDeque<Value>>,
    last_ts: Option<DateTime<Utc>>,
}

impl AvwapState {
    fn new() -> Self {
        let mut series_by_window = BTreeMap::new();
        let mut series_json_by_window = BTreeMap::new();
        for (code, _) in AVWAP_WINDOWS {
            series_by_window.insert(code.to_string(), VecDeque::new());
            series_json_by_window.insert(code.to_string(), VecDeque::new());
        }
        Self {
            series_by_window,
            series_json_by_window,
            ..Default::default()
        }
    }

    fn rebuild(&mut self, history_futures: &[MinuteHistory], history_spot: &[MinuteHistory]) {
        *self = Self::new();
        for (fut, spot) in history_futures.iter().zip(history_spot.iter()) {
            self.append_minute(fut, spot);
        }
    }

    fn sync(&mut self, history_futures: &[MinuteHistory], history_spot: &[MinuteHistory]) {
        match self.last_ts {
            None => self.rebuild(history_futures, history_spot),
            Some(last_ts) => {
                if history_futures
                    .last()
                    .map(|row| row.ts_bucket <= last_ts)
                    .unwrap_or(true)
                {
                    return;
                }
                let start_idx =
                    lower_bound_history_ts(history_futures, last_ts + Duration::minutes(1));
                let spot_start_idx =
                    lower_bound_history_ts(history_spot, last_ts + Duration::minutes(1));
                let fut_tail = &history_futures[start_idx..];
                let spot_tail = &history_spot[spot_start_idx..];
                if fut_tail.len() != spot_tail.len() {
                    self.rebuild(history_futures, history_spot);
                    return;
                }
                for (fut, spot) in fut_tail.iter().zip(spot_tail.iter()) {
                    self.append_minute(fut, spot);
                }
            }
        }
    }

    fn append_minute(&mut self, fut: &MinuteHistory, spot: &MinuteHistory) {
        let ts_bucket = fut.ts_bucket;
        let gap_minute = minute_gap(fut, spot);
        let row = AvwapMinuteContribution {
            ts_bucket,
            fut_notional: fut.total_notional,
            fut_qty: fut.total_qty,
            spot_notional: spot.total_notional,
            spot_qty: spot.total_qty,
            gap_minute,
        };
        self.sum_fut_notional += row.fut_notional;
        self.sum_fut_qty += row.fut_qty;
        self.sum_spot_notional += row.spot_notional;
        self.sum_spot_qty += row.spot_qty;
        if let Some(gap) = row.gap_minute {
            self.gap_values.push_back((ts_bucket, gap));
            self.gap_sum += gap;
            self.gap_sumsq += gap * gap;
        }
        self.contributions.push_back(row);

        let cutoff = ts_bucket - Duration::days(AVWAP_LOOKBACK_DAYS);
        while self
            .contributions
            .front()
            .map(|row| row.ts_bucket <= cutoff)
            .unwrap_or(false)
        {
            if let Some(front) = self.contributions.pop_front() {
                self.sum_fut_notional -= front.fut_notional;
                self.sum_fut_qty -= front.fut_qty;
                self.sum_spot_notional -= front.spot_notional;
                self.sum_spot_qty -= front.spot_qty;
            }
        }
        while self
            .gap_values
            .front()
            .map(|(ts, _)| *ts <= cutoff)
            .unwrap_or(false)
        {
            if let Some((_, gap)) = self.gap_values.pop_front() {
                self.gap_sum -= gap;
                self.gap_sumsq -= gap * gap;
            }
        }

        let current_fut = divide_or_none(self.sum_fut_notional, self.sum_fut_qty);
        let current_spot = divide_or_none(self.sum_spot_notional, self.sum_spot_qty);
        let current_gap = current_fut.zip(current_spot).map(|(f, s)| f - s);
        for (code, interval_mins) in AVWAP_WINDOWS {
            if ts_bucket.timestamp().rem_euclid(interval_mins * 60) != 0 {
                continue;
            }
            self.series_by_window
                .entry(code.to_string())
                .or_default()
                .push_back(AvwapSeriesRow {
                    ts_bucket,
                    avwap_fut: current_fut,
                    avwap_spot: current_spot,
                    gap: current_gap,
                });
            self.series_json_by_window
                .entry(code.to_string())
                .or_default()
                .push_back(json!({
                    "ts": ts_bucket.to_rfc3339(),
                    "avwap_fut": current_fut,
                    "avwap_spot": current_spot,
                    "xmk_avwap_gap_f_minus_s": current_gap,
                }));
            if let Some(series) = self.series_by_window.get_mut(code) {
                while series
                    .front()
                    .map(|row| row.ts_bucket <= cutoff)
                    .unwrap_or(false)
                {
                    series.pop_front();
                }
            }
            if let Some(series) = self.series_json_by_window.get_mut(code) {
                while series.len()
                    > self
                        .series_by_window
                        .get(code)
                        .map(|rows| rows.len())
                        .unwrap_or(0)
                {
                    series.pop_front();
                }
            }
        }
        self.last_ts = Some(ts_bucket);
    }

    fn build_outputs(
        &self,
        ts_bucket: DateTime<Utc>,
        latest_futures: Option<&MinuteHistory>,
        latest_mark: Option<&LatestMarkState>,
    ) -> (Option<Value>, Option<AvwapFeatureOutput>) {
        if self.last_ts.is_none() {
            return (None, None);
        }

        let avwap_fut = divide_or_none(self.sum_fut_notional, self.sum_fut_qty);
        let avwap_spot = divide_or_none(self.sum_spot_notional, self.sum_spot_qty);
        let fut_last_price = latest_futures.and_then(|row| row.last_price);
        let fut_mark_price = latest_mark.and_then(|mark| mark.mark_price);
        let price_minus_avwap_fut = fut_last_price.zip(avwap_fut).map(|(p, a)| p - a);
        let price_minus_spot_avwap_fut = fut_last_price.zip(avwap_spot).map(|(p, a)| p - a);
        let price_minus_spot_avwap_futmark = fut_mark_price.zip(avwap_spot).map(|(p, a)| p - a);
        let avwap_gap = avwap_fut.zip(avwap_spot).map(|(f, s)| f - s);
        let zavwap_gap = gap_zscore(&self.gap_values, avwap_gap);
        let lookback_start = ts_bucket - Duration::days(AVWAP_LOOKBACK_DAYS);

        let mut series_by_window = Map::new();
        for (code, _) in AVWAP_WINDOWS {
            let rows = self
                .series_json_by_window
                .get(code)
                .map(|rows| rows.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            series_by_window.insert(code.to_string(), Value::Array(rows));
        }

        let snapshot = json!({
            "indicator": "avwap_dual_market",
            "anchor_ts": lookback_start.to_rfc3339(),
            "lookback": "7d",
            "window": "1m",
            "avwap_fut": avwap_fut,
            "avwap_spot": avwap_spot,
            "fut_last_price": fut_last_price,
            "fut_mark_price": fut_mark_price,
            "price_minus_avwap_fut": price_minus_avwap_fut,
            "price_minus_spot_avwap_fut": price_minus_spot_avwap_fut,
            "price_minus_spot_avwap_futmark": price_minus_spot_avwap_futmark,
            "xmk_avwap_gap_f_minus_s": avwap_gap,
            "zavwap_gap": zavwap_gap,
            "series_by_window": series_by_window,
        });

        let feature = AvwapFeatureOutput {
            anchor_ts: Some(lookback_start),
            avwap_fut,
            avwap_spot,
            fut_last_price,
            fut_mark_price,
            price_minus_avwap_fut,
            price_minus_spot_avwap_fut,
            price_minus_spot_avwap_futmark,
            avwap_gap_fs: avwap_gap,
            xmk_avwap_gap_f_minus_s: avwap_gap,
            zavwap_gap,
        };

        (Some(snapshot), Some(feature))
    }
}

fn minute_gap(fut: &MinuteHistory, spot: &MinuteHistory) -> Option<f64> {
    let fut_price = divide_or_none(fut.total_notional, fut.total_qty)?;
    let spot_price = divide_or_none(spot.total_notional, spot.total_qty)?;
    Some(fut_price - spot_price)
}

fn gap_zscore(gap_values: &VecDeque<(DateTime<Utc>, f64)>, current: Option<f64>) -> Option<f64> {
    let current = current?;
    if gap_values.len() < 10 {
        return None;
    }
    let gaps = gap_values
        .iter()
        .rev()
        .map(|(_, gap)| *gap)
        .collect::<Vec<_>>();
    if gaps.len() < 10 {
        return None;
    }
    let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
    let variance = gaps
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / gaps.len() as f64;
    let sd = variance.sqrt();
    if sd <= EPS {
        Some(0.0)
    } else {
        Some((current - mean) / sd)
    }
}

fn divide_or_none(num: f64, den: f64) -> Option<f64> {
    if den > EPS {
        Some(num / den)
    } else {
        None
    }
}

fn roll_piecewise_area<T, F>(
    previous: Option<f64>,
    points: &VecDeque<T>,
    old_start: DateTime<Utc>,
    new_start: DateTime<Utc>,
    old_end: DateTime<Utc>,
    new_end: DateTime<Utc>,
    compute: F,
) -> Option<f64>
where
    F: Fn(DateTime<Utc>, DateTime<Utc>, &VecDeque<T>) -> Option<f64>,
{
    if previous.is_none() {
        return compute(new_start, new_end, points);
    }
    let add = compute(old_end, new_end, points)?;
    let remove = compute(old_start, new_start, points)?;
    Some(previous.unwrap_or(0.0) + add - remove)
}

fn lower_bound_funding_point_ts(values: &[LatestFundingState], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].ts < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn lower_bound_mark_point_ts(values: &[LatestMarkState], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].ts < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn lower_bound_funding_change_ts(values: &[FundingChange], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].ts_change < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn lower_bound_funding_points_deque(
    values: &VecDeque<LatestFundingState>,
    target: DateTime<Utc>,
) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].ts < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn lower_bound_mark_points_deque(
    values: &VecDeque<LatestMarkState>,
    target: DateTime<Utc>,
) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].ts < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn latest_funding_at_or_before(
    points: &VecDeque<LatestFundingState>,
    end: DateTime<Utc>,
) -> Option<(DateTime<Utc>, f64)> {
    points
        .iter()
        .rev()
        .find(|point| point.ts <= end)
        .map(|point| (point.ts, point.funding_rate))
}

fn latest_mark_at_or_before(
    points: &VecDeque<LatestMarkState>,
    end: DateTime<Utc>,
) -> Option<(DateTime<Utc>, f64)> {
    points.iter().rev().find_map(|point| {
        (point.ts <= end)
            .then_some(point.mark_price)
            .flatten()
            .map(|value| (point.ts, value))
    })
}

fn latest_index_at_or_before(
    points: &VecDeque<LatestMarkState>,
    end: DateTime<Utc>,
) -> Option<f64> {
    points
        .iter()
        .rev()
        .find_map(|point| (point.ts <= end).then_some(point.index_price).flatten())
}

fn funding_piecewise_area(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &VecDeque<LatestFundingState>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    let (_, current) = latest_funding_at_or_before(points, end)?;
    let mut last_value = latest_funding_at_or_before(points, start)
        .map(|(_, value)| value)
        .unwrap_or(current);
    let mut cursor = start;
    let mut weighted = 0.0;
    let start_idx = lower_bound_funding_points_deque(points, start);
    for idx in start_idx..points.len() {
        let point = &points[idx];
        if point.ts > end {
            break;
        }
        if point.ts <= start {
            last_value = point.funding_rate;
            continue;
        }
        let dt = (point.ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_value * dt;
        }
        cursor = point.ts;
        last_value = point.funding_rate;
    }
    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_value * dt;
    }
    Some(weighted)
}

fn mark_piecewise_area(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &VecDeque<LatestMarkState>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    let (_, current) = latest_mark_at_or_before(points, end)?;
    let mut last_value = latest_mark_at_or_before(points, start)
        .map(|(_, value)| value)
        .unwrap_or(current);
    let mut cursor = start;
    let mut weighted = 0.0;
    let start_idx = lower_bound_mark_points_deque(points, start);
    for idx in start_idx..points.len() {
        let point = &points[idx];
        if point.ts > end {
            break;
        }
        let Some(mark_price) = point.mark_price else {
            continue;
        };
        if point.ts <= start {
            last_value = mark_price;
            continue;
        }
        let dt = (point.ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_value * dt;
        }
        cursor = point.ts;
        last_value = mark_price;
    }
    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_value * dt;
    }
    Some(weighted)
}

fn build_funding_outputs(
    ts_bucket: DateTime<Utc>,
    funding_changes_recent: &[FundingChange],
    funding_recent_7d_payload: Arc<Vec<Value>>,
    funding_points_recent: &[LatestFundingState],
    mark_points_recent: &[LatestMarkState],
) -> (Option<Value>, BTreeMap<i64, FundingFeatureOutput>) {
    let mut by_window = Map::new();
    let mut feature_windows = BTreeMap::new();
    for (label, mins) in FUNDING_WINDOWS {
        let feature = compute_funding_feature_window(
            ts_bucket,
            mins,
            funding_changes_recent,
            funding_points_recent,
            mark_points_recent,
        );
        let changes = feature.changes_json.as_array().cloned().unwrap_or_default();
        by_window.insert(
            label.to_string(),
            json!({
                "window": label,
                "funding_current": feature.funding_current,
                "funding_current_effective_ts": feature.funding_current_effective_ts.map(|ts| ts.to_rfc3339()),
                "funding_twa": feature.funding_twa,
                "mark_price_last": feature.mark_price_last,
                "mark_price_last_ts": feature.mark_price_last_ts.map(|ts| ts.to_rfc3339()),
                "mark_price_twap": feature.mark_price_twap,
                "change_count": changes.len(),
                "changes": changes,
            }),
        );
        feature_windows.insert(mins, feature);
    }

    let current = compute_funding_feature_window(
        ts_bucket,
        1,
        funding_changes_recent,
        funding_points_recent,
        mark_points_recent,
    );

    let snapshot = json!({
        "funding_current": current.funding_current,
        "funding_current_effective_ts": current.funding_current_effective_ts.map(|ts| ts.to_rfc3339()),
        "funding_twa": current.funding_twa,
        "mark_price_last": current.mark_price_last,
        "mark_price_last_ts": current.mark_price_last_ts.map(|ts| ts.to_rfc3339()),
        "mark_price_twap": current.mark_price_twap,
        "recent_7d": funding_recent_7d_payload.as_ref().clone(),
        "by_window": by_window,
    });

    (Some(snapshot), feature_windows)
}

fn compute_funding_feature_window(
    ts_bucket: DateTime<Utc>,
    mins: i64,
    funding_changes_recent: &[FundingChange],
    funding_points_recent: &[LatestFundingState],
    mark_points_recent: &[LatestMarkState],
) -> FundingFeatureOutput {
    let end = ts_bucket + Duration::minutes(1);
    let start = end - Duration::minutes(mins);

    let funding_current = funding_points_recent
        .iter()
        .rev()
        .find(|point| point.ts <= end)
        .map(|point| point.funding_rate);
    let funding_current_effective_ts = funding_points_recent
        .iter()
        .rev()
        .find(|point| point.ts <= end)
        .map(|point| point.ts);
    let funding_fallback = funding_points_recent
        .iter()
        .rev()
        .find(|point| point.ts <= start)
        .map(|point| point.funding_rate)
        .or(funding_current);
    let funding_twa = piecewise_twa_funding(start, end, funding_points_recent, funding_fallback);

    let mark_last = mark_points_recent
        .iter()
        .rev()
        .find(|point| point.ts <= end && point.mark_price.is_some());
    let mark_price_last = mark_last.and_then(|point| point.mark_price);
    let mark_price_last_ts = mark_last.map(|point| point.ts);
    let mark_fallback = mark_points_recent
        .iter()
        .rev()
        .find_map(|point| (point.ts <= start).then_some(point.mark_price).flatten())
        .or(mark_price_last);
    let mark_price_twap = piecewise_twa_mark(start, end, mark_points_recent, mark_fallback);
    let index_price_last = mark_points_recent
        .iter()
        .rev()
        .find(|point| point.ts <= end && point.index_price.is_some())
        .and_then(|point| point.index_price);
    let changes_json = json!(funding_changes_recent
        .iter()
        .filter(|change| change.ts_change >= start && change.ts_change < end)
        .map(|change| {
            json!({
                "change_ts": change.ts_change.to_rfc3339(),
                "funding_prev": change.prev,
                "funding_new": change.new,
                "funding_delta": change.delta,
                "mark_price_at_change": change.mark_price_at_change,
            })
        })
        .collect::<Vec<_>>());

    FundingFeatureOutput {
        funding_current,
        funding_current_effective_ts,
        funding_twa,
        mark_price_last,
        mark_price_last_ts,
        mark_price_twap,
        index_price_last,
        changes_json,
    }
}

fn piecewise_twa_funding(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    funding_points_recent: &[LatestFundingState],
    fallback: Option<f64>,
) -> Option<f64> {
    let points = funding_points_recent
        .iter()
        .map(|point| (point.ts, point.funding_rate))
        .collect::<Vec<_>>();
    piecewise_twa_scalar(start, end, &points, fallback)
}

fn piecewise_twa_mark(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    mark_points_recent: &[LatestMarkState],
    fallback: Option<f64>,
) -> Option<f64> {
    let points = mark_points_recent
        .iter()
        .filter_map(|point| point.mark_price.map(|value| (point.ts, value)))
        .collect::<Vec<_>>();
    piecewise_twa_scalar(start, end, &points, fallback)
}

fn piecewise_twa_scalar(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    points: &[(DateTime<Utc>, f64)],
    fallback: Option<f64>,
) -> Option<f64> {
    if end <= start {
        return None;
    }
    if points.is_empty() {
        return fallback;
    }

    let mut last_value = points
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= start)
        .map(|(_, value)| *value)
        .or(fallback)
        .unwrap_or(points[0].1);
    let mut cursor = start;
    let mut weighted = 0.0;
    let mut total = 0.0;

    let start_idx = lower_bound_pair_ts(points, start);
    for (ts, value) in points.iter().skip(start_idx) {
        if *ts > end {
            break;
        }
        if *ts <= start {
            last_value = *value;
            continue;
        }
        let dt = (*ts - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        if dt > 0.0 {
            weighted += last_value * dt;
            total += dt;
        }
        cursor = *ts;
        last_value = *value;
    }

    if cursor < end {
        let dt = (end - cursor).num_milliseconds().max(0) as f64 / 1000.0;
        weighted += last_value * dt;
        total += dt;
    }

    if total <= EPS {
        fallback.or(Some(last_value))
    } else {
        Some(weighted / total)
    }
}

fn lower_bound_pair_ts(values: &[(DateTime<Utc>, f64)], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].0 < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[derive(Debug, Clone)]
struct WeightedPoint {
    ts_bucket: DateTime<Utc>,
    price: f64,
    weight: f64,
    prefix_weight: f64,
    prefix_price_weight: f64,
    prefix_price_sq_weight: f64,
    prefix_positive_samples: usize,
}

#[derive(Debug, Clone, Copy)]
struct RvwapStatsOutput {
    window_minutes: i64,
    rvwap: f64,
    sigma: f64,
    z: f64,
    sample_count: usize,
}

#[derive(Debug, Clone)]
struct RvwapSeriesRow {
    ts: DateTime<Utc>,
    by_window: BTreeMap<String, Option<RvwapStatsOutput>>,
}

#[derive(Default)]
struct RvwapState {
    rolling_windows: Vec<(String, i64)>,
    output_windows: Vec<(String, i64)>,
    min_samples: usize,
    points: VecDeque<WeightedPoint>,
    base_weight: f64,
    base_price_weight: f64,
    base_price_sq_weight: f64,
    base_positive_samples: usize,
    series_by_output_window: BTreeMap<String, VecDeque<RvwapSeriesRow>>,
    series_json_by_output_window: BTreeMap<String, VecDeque<Value>>,
    last_ts: Option<DateTime<Utc>>,
}

impl RvwapState {
    fn new(
        rolling_windows: Vec<(String, i64)>,
        output_windows: Vec<(String, i64)>,
        min_samples: usize,
    ) -> Self {
        let mut series_by_output_window = BTreeMap::new();
        let mut series_json_by_output_window = BTreeMap::new();
        for (code, _) in &output_windows {
            series_by_output_window.insert(code.clone(), VecDeque::new());
            series_json_by_output_window.insert(code.clone(), VecDeque::new());
        }
        Self {
            rolling_windows,
            output_windows,
            min_samples,
            series_by_output_window,
            series_json_by_output_window,
            ..Default::default()
        }
    }

    fn rebuild(&mut self, history_futures: &[MinuteHistory]) {
        let rolling_windows = self.rolling_windows.clone();
        let output_windows = self.output_windows.clone();
        let min_samples = self.min_samples;
        *self = Self::new(rolling_windows, output_windows, min_samples);
        for row in history_futures {
            self.append_minute(row);
        }
    }

    fn sync(&mut self, history_futures: &[MinuteHistory]) {
        match self.last_ts {
            None => self.rebuild(history_futures),
            Some(last_ts) => {
                if history_futures
                    .last()
                    .map(|row| row.ts_bucket <= last_ts)
                    .unwrap_or(true)
                {
                    return;
                }
                let start_idx =
                    lower_bound_history_ts(history_futures, last_ts + Duration::minutes(1));
                for row in &history_futures[start_idx..] {
                    self.append_minute(row);
                }
            }
        }
    }

    fn append_minute(&mut self, row: &MinuteHistory) {
        let Some(price) = typical_price(row.high_price, row.low_price, row.close_price) else {
            self.last_ts = Some(row.ts_bucket);
            return;
        };
        let weight = row.total_qty.max(0.0);
        let prev = self.points.back();
        let prev_w = prev
            .map(|point| point.prefix_weight)
            .unwrap_or(self.base_weight);
        let prev_pw = prev
            .map(|point| point.prefix_price_weight)
            .unwrap_or(self.base_price_weight);
        let prev_p2w = prev
            .map(|point| point.prefix_price_sq_weight)
            .unwrap_or(self.base_price_sq_weight);
        let prev_count = prev
            .map(|point| point.prefix_positive_samples)
            .unwrap_or(self.base_positive_samples);

        let point = WeightedPoint {
            ts_bucket: row.ts_bucket,
            price,
            weight,
            prefix_weight: prev_w + if weight > 0.0 { weight } else { 0.0 },
            prefix_price_weight: prev_pw + if weight > 0.0 { price * weight } else { 0.0 },
            prefix_price_sq_weight: prev_p2w
                + if weight > 0.0 {
                    price * price * weight
                } else {
                    0.0
                },
            prefix_positive_samples: prev_count + usize::from(weight > 0.0),
        };
        self.points.push_back(point);
        let cutoff = row.ts_bucket - Duration::minutes(HISTORY_LIMIT_MINUTES_I64);
        while self
            .points
            .front()
            .map(|point| point.ts_bucket <= cutoff)
            .unwrap_or(false)
        {
            if let Some(front) = self.points.pop_front() {
                self.base_weight = front.prefix_weight;
                self.base_price_weight = front.prefix_price_weight;
                self.base_price_sq_weight = front.prefix_price_sq_weight;
                self.base_positive_samples = front.prefix_positive_samples;
            }
        }

        let current_idx = self.points.len().saturating_sub(1);
        let current_stats = self.current_stats_by_window(current_idx);
        for (code, out_minutes) in &self.output_windows {
            let anchor = row.ts_bucket + Duration::minutes(1);
            if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
                continue;
            }
            self.series_by_output_window
                .entry(code.clone())
                .or_default()
                .push_back(RvwapSeriesRow {
                    ts: anchor,
                    by_window: current_stats.clone(),
                });
            let mut row_windows = Map::new();
            for (window_code, minutes) in &self.rolling_windows {
                row_windows.insert(
                    window_code.clone(),
                    current_stats
                        .get(window_code)
                        .and_then(|stats| *stats)
                        .map(rvwap_stats_json)
                        .unwrap_or_else(|| rvwap_null_stats_json(*minutes)),
                );
            }
            self.series_json_by_output_window
                .entry(code.clone())
                .or_default()
                .push_back(json!({
                    "ts": anchor.to_rfc3339(),
                    "by_window": row_windows,
                }));
            if let Some(series) = self.series_by_output_window.get_mut(code) {
                while series
                    .front()
                    .map(|series_row| {
                        series_row.ts <= anchor - Duration::minutes(HISTORY_LIMIT_MINUTES_I64)
                    })
                    .unwrap_or(false)
                {
                    series.pop_front();
                }
            }
            if let Some(series) = self.series_json_by_output_window.get_mut(code) {
                while series.len()
                    > self
                        .series_by_output_window
                        .get(code)
                        .map(|rows| rows.len())
                        .unwrap_or(0)
                {
                    series.pop_front();
                }
            }
        }
        self.last_ts = Some(row.ts_bucket);
    }

    fn current_stats_by_window(
        &self,
        end_idx: usize,
    ) -> BTreeMap<String, Option<RvwapStatsOutput>> {
        let mut out = BTreeMap::new();
        for (code, minutes) in &self.rolling_windows {
            out.insert(code.clone(), self.compute_stats_at(end_idx, *minutes));
        }
        out
    }

    fn compute_stats_at(&self, end_idx: usize, window_minutes: i64) -> Option<RvwapStatsOutput> {
        if self.points.is_empty() || end_idx >= self.points.len() {
            return None;
        }
        let end_ts = self.points[end_idx].ts_bucket;
        let start_ts = end_ts - Duration::minutes(window_minutes.max(1));
        let start_idx = lower_bound_points_ts(&self.points, start_ts + Duration::minutes(1));
        if start_idx > end_idx {
            return None;
        }
        let end_point = &self.points[end_idx];
        let before_point = start_idx
            .checked_sub(1)
            .and_then(|idx| self.points.get(idx));
        let before_w = before_point
            .map(|point| point.prefix_weight)
            .unwrap_or(self.base_weight);
        let before_pw = before_point
            .map(|point| point.prefix_price_weight)
            .unwrap_or(self.base_price_weight);
        let before_p2w = before_point
            .map(|point| point.prefix_price_sq_weight)
            .unwrap_or(self.base_price_sq_weight);
        let before_count = before_point
            .map(|point| point.prefix_positive_samples)
            .unwrap_or(self.base_positive_samples);

        let sum_w = end_point.prefix_weight - before_w;
        let sum_pw = end_point.prefix_price_weight - before_pw;
        let sum_p2w = end_point.prefix_price_sq_weight - before_p2w;
        let sample_count = end_point.prefix_positive_samples - before_count;

        if sample_count < self.min_samples || sum_w <= EPS {
            return None;
        }

        let rvwap = sum_pw / sum_w;
        let variance = (sum_p2w / sum_w - rvwap * rvwap).max(0.0);
        let sigma = variance.sqrt();
        let z = (end_point.price - rvwap) / (sigma + EPS);
        Some(RvwapStatsOutput {
            window_minutes,
            rvwap,
            sigma,
            z,
            sample_count,
        })
    }

    fn snapshot(&self, ts_bucket: DateTime<Utc>) -> Option<Value> {
        let last_idx = self.points.len().checked_sub(1)?;
        let by_window_stats = self.current_stats_by_window(last_idx);
        let mut by_window = Map::new();
        for (code, minutes) in &self.rolling_windows {
            by_window.insert(
                code.clone(),
                by_window_stats
                    .get(code)
                    .and_then(|stats| *stats)
                    .map(rvwap_stats_json)
                    .unwrap_or_else(|| rvwap_null_stats_json(*minutes)),
            );
        }

        let mut series_by_output_window = Map::new();
        for (code, _) in &self.output_windows {
            let rows = self
                .series_json_by_output_window
                .get(code)
                .map(|rows| rows.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            series_by_output_window.insert(code.clone(), Value::Array(rows));
        }

        Some(json!({
            "indicator": "rvwap_sigma_bands",
            "window": "1m",
            "as_of_ts": (ts_bucket + Duration::minutes(1)).to_rfc3339(),
            "source_mode": "ohlcv_approx_1m",
            "by_window": by_window,
            "series_by_output_window": series_by_output_window,
        }))
    }
}

fn rvwap_stats_json(stats: RvwapStatsOutput) -> Value {
    json!({
        "window_minutes": stats.window_minutes,
        "rvwap_w": stats.rvwap,
        "rvwap_sigma_w": stats.sigma,
        "rvwap_band_plus_1": stats.rvwap + stats.sigma,
        "rvwap_band_plus_2": stats.rvwap + 2.0 * stats.sigma,
        "rvwap_band_minus_1": stats.rvwap - stats.sigma,
        "rvwap_band_minus_2": stats.rvwap - 2.0 * stats.sigma,
        "z_price_minus_rvwap": stats.z,
        "samples_used": stats.sample_count,
    })
}

fn rvwap_null_stats_json(window_minutes: i64) -> Value {
    json!({
        "window_minutes": window_minutes,
        "rvwap_w": null,
        "rvwap_sigma_w": null,
        "rvwap_band_plus_1": null,
        "rvwap_band_plus_2": null,
        "rvwap_band_minus_1": null,
        "rvwap_band_minus_2": null,
        "z_price_minus_rvwap": null,
        "samples_used": 0,
    })
}

fn typical_price(high: Option<f64>, low: Option<f64>, close: Option<f64>) -> Option<f64> {
    Some((high? + low? + close?) / 3.0)
}

fn lower_bound_points_ts(points: &VecDeque<WeightedPoint>, target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = points.len();
    while l < r {
        let m = (l + r) / 2;
        if points[m].ts_bucket < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[derive(Debug, Clone)]
struct MinutePulsePoint {
    ts: DateTime<Utc>,
    volume: f64,
    poc_price: Option<f64>,
    poc_volume: f64,
    prefix_volume: f64,
}

#[derive(Debug, Clone)]
struct PulseHistoryWindow {
    required_samples: usize,
    previous_values: VecDeque<f64>,
    sum: f64,
    sumsq: f64,
}

impl PulseHistoryWindow {
    fn new(required_samples: usize) -> Self {
        Self {
            required_samples,
            previous_values: VecDeque::new(),
            sum: 0.0,
            sumsq: 0.0,
        }
    }

    fn current_stats(&self, window_minutes: i64, rolling_volume: f64) -> Option<Value> {
        if self.previous_values.len() < self.required_samples {
            return None;
        }
        let mean = self.sum / self.previous_values.len() as f64;
        let variance = (self.sumsq / self.previous_values.len() as f64 - mean * mean).max(0.0);
        let sigma = variance.sqrt();
        let z = (rolling_volume - mean) / (sigma + EPS);
        Some(json!({
            "window_minutes": window_minutes,
            "rolling_volume_w": rolling_volume,
            "volume_spike_z_w": z,
            "is_volume_spike_z2": z >= 2.0,
            "is_volume_spike_z3": z >= 3.0,
            "lookback_samples": self.previous_values.len(),
        }))
    }

    fn push(&mut self, value: f64) {
        self.previous_values.push_back(value);
        self.sum += value;
        self.sumsq += value * value;
        while self.previous_values.len() > self.required_samples {
            if let Some(front) = self.previous_values.pop_front() {
                self.sum -= front;
                self.sumsq -= front * front;
            }
        }
    }
}

#[derive(Default)]
struct HighVolumePulseState {
    z_windows: Vec<(String, i64)>,
    summary_windows: Vec<(String, i64)>,
    min_samples: usize,
    points: VecDeque<MinutePulsePoint>,
    base_prefix_volume: f64,
    rolling_histories: BTreeMap<String, PulseHistoryWindow>,
    summary_max: BTreeMap<String, VecDeque<MinutePulsePoint>>,
    current_by_z_window: BTreeMap<String, Value>,
    current_summary_windows: BTreeMap<String, Value>,
    current_point: Option<MinutePulsePoint>,
    last_ts: Option<DateTime<Utc>>,
}

impl HighVolumePulseState {
    fn new(
        z_windows: Vec<(String, i64)>,
        summary_windows: Vec<(String, i64)>,
        min_samples: usize,
    ) -> Self {
        let mut rolling_histories = BTreeMap::new();
        for (code, minutes) in &z_windows {
            let required = (*minutes as usize).max(min_samples.max(5));
            rolling_histories.insert(code.clone(), PulseHistoryWindow::new(required));
        }
        let mut summary_max = BTreeMap::new();
        for (code, _) in &summary_windows {
            summary_max.insert(code.clone(), VecDeque::new());
        }
        Self {
            z_windows,
            summary_windows,
            min_samples,
            rolling_histories,
            summary_max,
            ..Default::default()
        }
    }

    fn rebuild(&mut self, history_futures: &[MinuteHistory]) {
        let z_windows = self.z_windows.clone();
        let summary_windows = self.summary_windows.clone();
        let min_samples = self.min_samples;
        *self = Self::new(z_windows, summary_windows, min_samples);
        for row in history_futures {
            self.append_minute(row);
        }
    }

    fn sync(&mut self, history_futures: &[MinuteHistory]) {
        match self.last_ts {
            None => self.rebuild(history_futures),
            Some(last_ts) => {
                if history_futures
                    .last()
                    .map(|row| row.ts_bucket <= last_ts)
                    .unwrap_or(true)
                {
                    return;
                }
                let start_idx =
                    lower_bound_history_ts(history_futures, last_ts + Duration::minutes(1));
                for row in &history_futures[start_idx..] {
                    self.append_minute(row);
                }
            }
        }
    }

    fn append_minute(&mut self, row: &MinuteHistory) {
        let ts = row.ts_bucket + Duration::minutes(1);
        let (poc_price, poc_volume) = intrabar_poc_from_profile(&row.profile);
        let prefix_prev = self
            .points
            .back()
            .map(|point| point.prefix_volume)
            .unwrap_or(self.base_prefix_volume);
        let point = MinutePulsePoint {
            ts,
            volume: row.total_qty.max(0.0),
            poc_price,
            poc_volume,
            prefix_volume: prefix_prev + row.total_qty.max(0.0),
        };
        self.points.push_back(point.clone());
        self.current_point = Some(point.clone());

        let cutoff = ts - Duration::minutes(HISTORY_LIMIT_MINUTES_I64);
        while self
            .points
            .front()
            .map(|entry| entry.ts <= cutoff)
            .unwrap_or(false)
        {
            if let Some(front) = self.points.pop_front() {
                self.base_prefix_volume = front.prefix_volume;
            }
        }

        self.current_by_z_window.clear();
        for (code, minutes) in &self.z_windows {
            let rolling = self.rolling_volume(*minutes).unwrap_or(0.0);
            let value = self
                .rolling_histories
                .get(code)
                .and_then(|state| state.current_stats(*minutes, rolling))
                .unwrap_or_else(|| {
                    json!({
                        "window_minutes": minutes,
                        "rolling_volume_w": null,
                        "volume_spike_z_w": null,
                        "is_volume_spike_z2": false,
                        "is_volume_spike_z3": false,
                        "lookback_samples": 0,
                    })
                });
            self.current_by_z_window.insert(code.clone(), value);
            if let Some(state) = self.rolling_histories.get_mut(code) {
                state.push(rolling);
            }
        }

        self.current_summary_windows.clear();
        for (code, minutes) in &self.summary_windows {
            let deque = self.summary_max.entry(code.clone()).or_default();
            while deque
                .back()
                .map(|tail| tail.poc_volume <= point.poc_volume)
                .unwrap_or(false)
            {
                deque.pop_back();
            }
            deque.push_back(point.clone());
            let min_ts = ts - Duration::minutes(*minutes) + Duration::minutes(1);
            while deque
                .front()
                .map(|front| front.ts < min_ts)
                .unwrap_or(false)
            {
                deque.pop_front();
            }
            let value = deque
                .front()
                .map(|front| {
                    json!({
                        "window_minutes": minutes,
                        "ts": front.ts.to_rfc3339(),
                        "intrabar_poc_price": front.poc_price,
                        "intrabar_poc_volume": front.poc_volume,
                    })
                })
                .unwrap_or_else(|| {
                    json!({
                        "window_minutes": minutes,
                        "ts": null,
                        "intrabar_poc_price": null,
                        "intrabar_poc_volume": null,
                    })
                });
            self.current_summary_windows.insert(code.clone(), value);
        }
        self.last_ts = Some(row.ts_bucket);
    }

    fn rolling_volume(&self, window_minutes: i64) -> Option<f64> {
        let end_idx = self.points.len().checked_sub(1)?;
        let end_ts = self.points[end_idx].ts;
        let start_ts = end_ts - Duration::minutes(window_minutes.max(1));
        let start_idx = lower_bound_pulse_ts(&self.points, start_ts + Duration::minutes(1));
        if start_idx > end_idx {
            return Some(0.0);
        }
        let current = self.points[end_idx].prefix_volume;
        let before = start_idx
            .checked_sub(1)
            .and_then(|idx| self.points.get(idx).map(|point| point.prefix_volume))
            .unwrap_or(self.base_prefix_volume);
        Some(current - before)
    }

    fn snapshot(&self, ts_bucket: DateTime<Utc>) -> Option<Value> {
        let current = self.current_point.as_ref()?;
        let mut by_z_window = Map::new();
        for (code, value) in &self.current_by_z_window {
            by_z_window.insert(code.clone(), value.clone());
        }
        let mut summary = Map::new();
        for (code, value) in &self.current_summary_windows {
            summary.insert(code.clone(), value.clone());
        }
        Some(json!({
            "indicator": "high_volume_pulse",
            "window": "1m",
            "as_of_ts": (ts_bucket + Duration::minutes(1)).to_rfc3339(),
            "intrabar_poc_price": current.poc_price,
            "intrabar_poc_volume": current.poc_volume,
            "by_z_window": by_z_window,
            "intrabar_poc_max_by_window": summary,
        }))
    }
}

fn intrabar_poc_from_profile(profile: &BTreeMap<i64, LevelAgg>) -> (Option<f64>, f64) {
    profile
        .iter()
        .max_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(tick, level)| (Some(tick_to_price(*tick)), level.total()))
        .unwrap_or((None, 0.0))
}

fn lower_bound_pulse_ts(points: &VecDeque<MinutePulsePoint>, target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = points.len();
    while l < r {
        let m = (l + r) / 2;
        if points[m].ts < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[derive(Debug, Clone)]
struct MinuteRangeBar {
    ts_bucket: DateTime<Utc>,
    high: f64,
    low: f64,
}

#[derive(Debug, Clone)]
struct SessionProfileState {
    p_min: f64,
    p_max: f64,
    bin_width: f64,
    scores: Vec<i64>,
    poc_idx: usize,
    vah_idx: usize,
    val_idx: usize,
}

#[derive(Debug, Clone)]
struct TpoDevSeriesRow {
    ts: DateTime<Utc>,
    tpo_dev_poc: f64,
    tpo_dev_vah: f64,
    tpo_dev_val: f64,
}

#[derive(Debug, Clone)]
struct TpoSessionState {
    code: String,
    session_minutes: i64,
    rows_nb: usize,
    value_area_pct: f64,
    ib_minutes: i64,
    output_windows: Vec<(String, i64)>,
    session_start: Option<DateTime<Utc>>,
    session_end: Option<DateTime<Utc>>,
    lows_sorted: Vec<f64>,
    highs_sorted: Vec<f64>,
    profile: Option<SessionProfileState>,
    ib_high: Option<f64>,
    ib_low: Option<f64>,
    dev_series: BTreeMap<String, VecDeque<TpoDevSeriesRow>>,
}

impl TpoSessionState {
    fn new(
        code: String,
        session_minutes: i64,
        rows_nb: usize,
        value_area_pct: f64,
        ib_minutes: i64,
        output_windows: Vec<(String, i64)>,
    ) -> Self {
        let mut dev_series = BTreeMap::new();
        for (window_code, _) in &output_windows {
            dev_series.insert(window_code.clone(), VecDeque::new());
        }
        Self {
            code,
            session_minutes,
            rows_nb,
            value_area_pct,
            ib_minutes,
            output_windows,
            session_start: None,
            session_end: None,
            lows_sorted: Vec::new(),
            highs_sorted: Vec::new(),
            profile: None,
            ib_high: None,
            ib_low: None,
            dev_series,
        }
    }

    fn reset_for_session(&mut self, session_start: DateTime<Utc>) {
        self.session_start = Some(session_start);
        self.session_end = Some(session_start + Duration::minutes(self.session_minutes));
        self.lows_sorted.clear();
        self.highs_sorted.clear();
        self.profile = None;
        self.ib_high = None;
        self.ib_low = None;
        for rows in self.dev_series.values_mut() {
            rows.clear();
        }
    }

    fn append_history_row(&mut self, row: &MinuteHistory) {
        let session_start = floor_to_interval(row.ts_bucket, self.session_minutes);
        if self.session_start != Some(session_start) {
            self.reset_for_session(session_start);
        }
        let Some(bar) = minute_range_bar(row) else {
            return;
        };
        insert_sorted_f64(&mut self.lows_sorted, bar.low);
        insert_sorted_f64(&mut self.highs_sorted, bar.high);
        let ib_end = session_start + Duration::minutes(self.ib_minutes.max(1));
        if bar.ts_bucket >= session_start && bar.ts_bucket < ib_end {
            self.ib_high = Some(self.ib_high.map_or(bar.high, |value| value.max(bar.high)));
            self.ib_low = Some(self.ib_low.map_or(bar.low, |value| value.min(bar.low)));
        }

        self.profile = build_profile_state_from_endpoints(
            &self.lows_sorted,
            &self.highs_sorted,
            self.rows_nb.max(1),
            self.value_area_pct,
        );

        if let Some(profile) = self.profile.as_ref() {
            let anchor = bar.ts_bucket + Duration::minutes(1);
            for (window_code, out_minutes) in &self.output_windows {
                if anchor.timestamp().rem_euclid(out_minutes * 60) != 0 {
                    continue;
                }
                self.dev_series
                    .entry(window_code.clone())
                    .or_default()
                    .push_back(TpoDevSeriesRow {
                        ts: anchor,
                        tpo_dev_poc: bin_center(profile, profile.poc_idx),
                        tpo_dev_vah: bin_high(profile, profile.vah_idx),
                        tpo_dev_val: bin_low(profile, profile.val_idx),
                    });
            }
        }
    }

    fn payload(&self) -> Value {
        let session_start = self.session_start;
        let session_end = self.session_end;
        let Some(profile) = self.profile.as_ref() else {
            return json!({
                "session_window": self.code,
                "session_start": session_start.map(|ts| ts.to_rfc3339()),
                "session_end": session_end.map(|ts| ts.to_rfc3339()),
                "rows_nb": self.rows_nb,
                "value_area_pct": self.value_area_pct,
                "tpo_poc": null,
                "tpo_vah": null,
                "tpo_val": null,
                "initial_balance_high": self.ib_high,
                "initial_balance_low": self.ib_low,
                "tpo_single_print_zones": [],
                "dev_series": {},
            });
        };

        let mut dev_series = Map::new();
        for (code, rows) in &self.dev_series {
            dev_series.insert(
                code.clone(),
                Value::Array(
                    rows.iter()
                        .map(|row| {
                            json!({
                                "ts": row.ts.to_rfc3339(),
                                "tpo_dev_poc": row.tpo_dev_poc,
                                "tpo_dev_vah": row.tpo_dev_vah,
                                "tpo_dev_val": row.tpo_dev_val,
                            })
                        })
                        .collect::<Vec<_>>(),
                ),
            );
        }

        let single_print_zones = single_print_zones(profile)
            .into_iter()
            .map(|(start_idx, end_idx)| {
                json!({
                    "low": bin_low(profile, start_idx),
                    "high": bin_high(profile, end_idx),
                    "score": 1,
                })
            })
            .collect::<Vec<_>>();

        json!({
            "session_window": self.code,
            "session_start": session_start.map(|ts| ts.to_rfc3339()),
            "session_end": session_end.map(|ts| ts.to_rfc3339()),
            "rows_nb": self.rows_nb,
            "value_area_pct": self.value_area_pct,
            "tpo_poc": bin_center(profile, profile.poc_idx),
            "tpo_vah": bin_high(profile, profile.vah_idx),
            "tpo_val": bin_low(profile, profile.val_idx),
            "initial_balance_high": self.ib_high,
            "initial_balance_low": self.ib_low,
            "tpo_single_print_zones": single_print_zones,
            "dev_series": dev_series,
        })
    }
}

#[derive(Default)]
struct TpoState {
    sessions: Vec<TpoSessionState>,
    session_preference: Vec<String>,
    last_ts: Option<DateTime<Utc>>,
}

impl TpoState {
    fn new(
        rows_nb: usize,
        value_area_pct: f64,
        session_windows: Vec<(String, i64)>,
        ib_minutes: i64,
        output_windows: Vec<(String, i64)>,
    ) -> Self {
        let session_preference = session_windows
            .iter()
            .map(|(code, _)| code.clone())
            .collect();
        let sessions = session_windows
            .into_iter()
            .map(|(code, minutes)| {
                TpoSessionState::new(
                    code,
                    minutes,
                    rows_nb,
                    value_area_pct,
                    ib_minutes,
                    output_windows.clone(),
                )
            })
            .collect();
        Self {
            sessions,
            session_preference,
            last_ts: None,
        }
    }

    fn rebuild(&mut self, history_futures: &[MinuteHistory], ts_bucket: DateTime<Utc>) {
        for session in &mut self.sessions {
            session.reset_for_session(floor_to_interval(ts_bucket, session.session_minutes));
        }
        for row in history_futures {
            for session in &mut self.sessions {
                session.append_history_row(row);
            }
        }
        self.last_ts = history_futures.last().map(|row| row.ts_bucket);
    }

    fn sync(&mut self, history_futures: &[MinuteHistory], ts_bucket: DateTime<Utc>) {
        match self.last_ts {
            None => self.rebuild(history_futures, ts_bucket),
            Some(last_ts) => {
                if history_futures
                    .last()
                    .map(|row| row.ts_bucket <= last_ts)
                    .unwrap_or(true)
                {
                    return;
                }
                let start_idx =
                    lower_bound_history_ts(history_futures, last_ts + Duration::minutes(1));
                for row in &history_futures[start_idx..] {
                    for session in &mut self.sessions {
                        session.append_history_row(row);
                    }
                }
                self.last_ts = history_futures.last().map(|row| row.ts_bucket);
            }
        }
    }

    fn snapshot(&self, ts_bucket: DateTime<Utc>) -> Option<Value> {
        if self.sessions.is_empty() {
            return None;
        }
        let mut by_session = Map::new();
        for session in &self.sessions {
            by_session.insert(session.code.clone(), session.payload());
        }
        let mut out = Map::new();
        out.insert("indicator".to_string(), json!("tpo_market_profile"));
        out.insert("window".to_string(), json!("1m"));
        out.insert(
            "as_of_ts".to_string(),
            json!((ts_bucket + Duration::minutes(1)).to_rfc3339()),
        );
        out.insert(
            "rows_nb".to_string(),
            json!(self
                .sessions
                .first()
                .map(|session| session.rows_nb)
                .unwrap_or_default()),
        );
        out.insert(
            "value_area_pct".to_string(),
            json!(self
                .sessions
                .first()
                .map(|session| session.value_area_pct)
                .unwrap_or_default()),
        );
        merge_primary_session_fields(&mut out, &by_session, &self.session_preference);
        out.insert("by_session".to_string(), Value::Object(by_session));
        Some(Value::Object(out))
    }
}

fn minute_range_bar(row: &MinuteHistory) -> Option<MinuteRangeBar> {
    let high = row
        .high_price
        .or(row.close_price)
        .or(row.last_price)
        .or(row.open_price)?;
    let low = row
        .low_price
        .or(row.close_price)
        .or(row.last_price)
        .or(row.open_price)?;
    Some(MinuteRangeBar {
        ts_bucket: row.ts_bucket,
        high,
        low,
    })
}

fn build_profile_state(
    bars: &[MinuteRangeBar],
    rows_nb: usize,
    value_area_pct: f64,
) -> Option<SessionProfileState> {
    if bars.is_empty() {
        return None;
    }
    let p_min = bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min);
    let p_max = bars
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    if !p_min.is_finite() || !p_max.is_finite() {
        return None;
    }
    let bin_width = (p_max - p_min) / rows_nb as f64;
    if !bin_width.is_finite() {
        return None;
    }
    let mut profile = SessionProfileState {
        p_min,
        p_max,
        bin_width,
        scores: vec![0_i64; rows_nb],
        poc_idx: 0,
        vah_idx: 0,
        val_idx: 0,
    };
    for bar in bars {
        increment_profile(&mut profile, bar);
    }
    recompute_value_area(&mut profile, value_area_pct);
    Some(profile)
}

fn build_profile_state_from_endpoints(
    lows_sorted: &[f64],
    highs_sorted: &[f64],
    rows_nb: usize,
    value_area_pct: f64,
) -> Option<SessionProfileState> {
    if lows_sorted.is_empty() || highs_sorted.is_empty() {
        return None;
    }
    let p_min = *lows_sorted.first()?;
    let p_max = *highs_sorted.last()?;
    if !p_min.is_finite() || !p_max.is_finite() {
        return None;
    }

    let mut profile = SessionProfileState {
        p_min,
        p_max,
        bin_width: (p_max - p_min) / rows_nb.max(1) as f64,
        scores: vec![0_i64; rows_nb.max(1)],
        poc_idx: 0,
        vah_idx: 0,
        val_idx: 0,
    };

    if profile.bin_width <= EPS || !profile.bin_width.is_finite() {
        if let Some(first) = profile.scores.first_mut() {
            *first = lows_sorted.len() as i64;
        }
        recompute_value_area(&mut profile, value_area_pct);
        return Some(profile);
    }

    let bin_start_thresholds = (0..profile.scores.len())
        .map(|idx| min_price_for_bin_idx(&profile, idx))
        .collect::<Vec<_>>();

    for idx in 0..profile.scores.len() {
        let low_mapped_into_or_below = if idx + 1 == profile.scores.len() {
            lows_sorted.len()
        } else {
            lower_bound_f64(lows_sorted, bin_start_thresholds[idx + 1])
        };
        let high_lt_low = lower_bound_f64(highs_sorted, bin_start_thresholds[idx]);
        profile.scores[idx] = (low_mapped_into_or_below as i64 - high_lt_low as i64).max(0);
    }
    recompute_value_area(&mut profile, value_area_pct);
    Some(profile)
}

fn increment_profile(profile: &mut SessionProfileState, bar: &MinuteRangeBar) {
    let from_idx = price_to_bin_idx(
        bar.low,
        profile.p_min,
        profile.bin_width,
        profile.scores.len(),
    );
    let to_idx = price_to_bin_idx(
        bar.high,
        profile.p_min,
        profile.bin_width,
        profile.scores.len(),
    );
    for idx in from_idx.min(to_idx)..=from_idx.max(to_idx) {
        profile.scores[idx] += 1;
    }
}

fn recompute_value_area(profile: &mut SessionProfileState, value_area_pct: f64) {
    let poc_idx = profile
        .scores
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(&a.0)))
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    let total_score = profile.scores.iter().sum::<i64>() as f64;
    let target = (total_score * value_area_pct.clamp(0.0, 1.0)).max(1.0);
    let mut in_va = vec![false; profile.scores.len()];
    in_va[poc_idx] = true;
    let mut acc = profile.scores[poc_idx] as f64;
    let mut left = poc_idx as i64 - 1;
    let mut right = poc_idx + 1;
    while acc + EPS < target && (left >= 0 || right < profile.scores.len()) {
        let lv = if left >= 0 {
            profile.scores[left as usize] as f64
        } else {
            -1.0
        };
        let rv = if right < profile.scores.len() {
            profile.scores[right] as f64
        } else {
            -1.0
        };
        if rv >= lv && right < profile.scores.len() {
            in_va[right] = true;
            acc += rv.max(0.0);
            right += 1;
        } else if left >= 0 {
            in_va[left as usize] = true;
            acc += lv.max(0.0);
            left -= 1;
        } else {
            break;
        }
    }
    profile.poc_idx = poc_idx;
    profile.val_idx = in_va.iter().position(|flag| *flag).unwrap_or(poc_idx);
    profile.vah_idx = in_va.iter().rposition(|flag| *flag).unwrap_or(poc_idx);
}

fn single_print_zones(profile: &SessionProfileState) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (idx, score) in profile.scores.iter().enumerate() {
        if *score == 1 {
            if start.is_none() {
                start = Some(idx);
            }
        } else if let Some(s) = start.take() {
            out.push((s, idx - 1));
        }
    }
    if let Some(s) = start {
        out.push((s, profile.scores.len().saturating_sub(1)));
    }
    out
}

fn merge_primary_session_fields(
    out: &mut Map<String, Value>,
    by_session: &Map<String, Value>,
    session_preference: &[String],
) {
    let primary = session_preference
        .iter()
        .find_map(|code| by_session.get(code))
        .or_else(|| by_session.values().next());
    let Some(primary_obj) = primary.and_then(Value::as_object) else {
        return;
    };
    for key in [
        "session_window",
        "session_start",
        "session_end",
        "tpo_poc",
        "tpo_vah",
        "tpo_val",
        "initial_balance_high",
        "initial_balance_low",
        "tpo_single_print_zones",
        "dev_series",
    ] {
        if let Some(value) = primary_obj.get(key) {
            out.insert(key.to_string(), value.clone());
        }
    }
}

fn price_to_bin_idx(price: f64, p_min: f64, bin_width: f64, rows_nb: usize) -> usize {
    if rows_nb <= 1 || bin_width <= EPS || !bin_width.is_finite() {
        return 0;
    }
    let raw = ((price - p_min) / bin_width).floor();
    if !raw.is_finite() {
        return 0;
    }
    raw.max(0.0).min((rows_nb - 1) as f64) as usize
}

fn insert_sorted_f64(values: &mut Vec<f64>, value: f64) {
    let idx = upper_bound_f64(values, value);
    values.insert(idx, value);
}

fn lower_bound_f64(values: &[f64], target: f64) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].total_cmp(&target).is_lt() {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn upper_bound_f64(values: &[f64], target: f64) -> usize {
    let mut l = 0usize;
    let mut r = values.len();
    while l < r {
        let m = (l + r) / 2;
        if values[m].total_cmp(&target).is_le() {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

fn min_price_for_bin_idx(profile: &SessionProfileState, idx: usize) -> f64 {
    if idx == 0
        || profile.scores.len() <= 1
        || profile.bin_width <= EPS
        || !profile.bin_width.is_finite()
    {
        return f64::NEG_INFINITY;
    }

    let theoretical = profile.p_min + idx as f64 * profile.bin_width;
    let span = (profile.p_max - profile.p_min)
        .abs()
        .max(profile.bin_width.abs())
        .max(1.0);
    let mut lo = profile.p_min - span;
    let mut hi = profile.p_max + span;

    while price_to_bin_idx(lo, profile.p_min, profile.bin_width, profile.scores.len()) >= idx {
        lo -= span;
    }
    while price_to_bin_idx(hi, profile.p_min, profile.bin_width, profile.scores.len()) < idx {
        hi += span;
    }

    if price_to_bin_idx(
        theoretical,
        profile.p_min,
        profile.bin_width,
        profile.scores.len(),
    ) >= idx
    {
        hi = theoretical;
    } else {
        lo = theoretical;
    }

    for _ in 0..96 {
        let mid = lo + (hi - lo) * 0.5;
        if price_to_bin_idx(mid, profile.p_min, profile.bin_width, profile.scores.len()) >= idx {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

fn bin_center(profile: &SessionProfileState, idx: usize) -> f64 {
    profile.p_min + (idx as f64 + 0.5) * profile.bin_width
}

fn bin_low(profile: &SessionProfileState, idx: usize) -> f64 {
    profile.p_min + idx as f64 * profile.bin_width
}

fn bin_high(profile: &SessionProfileState, idx: usize) -> f64 {
    profile.p_min + (idx as f64 + 1.0) * profile.bin_width
}

fn floor_to_interval(ts: DateTime<Utc>, interval_minutes: i64) -> DateTime<Utc> {
    let secs = interval_minutes * 60;
    let aligned = ts.timestamp().div_euclid(secs) * secs;
    Utc.timestamp_opt(aligned, 0).single().unwrap_or(ts)
}

fn lower_bound_history_ts(history: &[MinuteHistory], target: DateTime<Utc>) -> usize {
    let mut l = 0usize;
    let mut r = history.len();
    while l < r {
        let m = (l + r) / 2;
        if history[m].ts_bucket < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

#[cfg(test)]
mod tests {
    use super::{IncrementalIndicatorConfig, IncrementalIndicatorState};
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorRuntimeOptions, KlineHistorySupplement,
        OpenInterestCurrentSidecar,
    };
    use crate::indicators::i16_funding_rate::I16FundingRate;
    use crate::indicators::i18_avwap::I18Avwap;
    use crate::indicators::i20_tpo_market_profile::I20TpoMarketProfile;
    use crate::indicators::i21_rvwap_sigma_bands::I21RvwapSigmaBands;
    use crate::indicators::i22_high_volume_pulse::I22HighVolumePulse;
    use crate::indicators::indicator_trait::Indicator;
    use crate::indicators::shared::funding::funding_change_json;
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{
        FundingChange, LatestFundingState, LatestMarkState, LiqAgg, MinuteHistory,
        MinuteWindowData, WindowBundle,
    };
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn incremental_snapshots_match_legacy_indicators() {
        let ts_start = Utc
            .with_ymd_and_hms(2026, 3, 20, 0, 0, 0)
            .single()
            .expect("valid ts");
        let history_len = 480usize;
        let mut futures_history = Vec::with_capacity(history_len);
        let mut spot_history = Vec::with_capacity(history_len);
        let mut funding_points = Vec::with_capacity(history_len);
        let mut mark_points = Vec::with_capacity(history_len);
        let mut funding_changes = Vec::new();

        let mut latest_funding = None;
        let mut latest_mark = None;
        for idx in 0..history_len {
            let ts = ts_start + Duration::minutes(idx as i64);
            let base_price = 100.0 + idx as f64 * 0.03;
            let fut_price = base_price + (idx % 11) as f64 * 0.02;
            let spot_price = base_price - (idx % 7) as f64 * 0.015;
            let fut_qty = 10.0 + (idx % 5) as f64;
            let spot_qty = 8.0 + (idx % 4) as f64;

            futures_history.push(history_row(ts, MarketKind::Futures, fut_price, fut_qty));
            spot_history.push(history_row(ts, MarketKind::Spot, spot_price, spot_qty));

            let funding_rate = -0.0001 + (idx % 9) as f64 * 0.00001;
            let mark_state = LatestMarkState {
                ts: ts + Duration::seconds(59),
                mark_price: Some(fut_price + 0.1),
                index_price: Some(spot_price + 0.05),
                funding_rate: Some(funding_rate),
                next_funding_time: None,
            };
            let funding_state = LatestFundingState {
                ts: ts + Duration::seconds(59),
                funding_rate,
                mark_price: Some(fut_price + 0.1),
                next_funding_time: None,
            };
            if latest_funding
                .as_ref()
                .map(|state: &LatestFundingState| (state.funding_rate - funding_rate).abs() > 1e-12)
                .unwrap_or(true)
            {
                let change = FundingChange {
                    ts_change: funding_state.ts,
                    prev: latest_funding.as_ref().map(|state| state.funding_rate),
                    new: funding_rate,
                    delta: latest_funding
                        .as_ref()
                        .map(|state| funding_rate - state.funding_rate),
                    mark_price_at_change: mark_state.mark_price,
                };
                funding_changes.push(change);
            }
            latest_mark = Some(mark_state.clone());
            latest_funding = Some(funding_state.clone());
            mark_points.push(mark_state);
            funding_points.push(funding_state);
        }

        let ts_bucket = futures_history.last().expect("history").ts_bucket;
        let futures = minute_window_from_history(futures_history.last().unwrap());
        let spot = minute_window_from_history(spot_history.last().unwrap());
        let funding_recent_7d_payload = Arc::new(
            funding_changes
                .iter()
                .map(funding_change_json)
                .collect::<Vec<_>>(),
        );

        let options = runtime_options();
        let bundle = WindowBundle {
            ts_bucket,
            symbol: "TESTUSDT".to_string(),
            futures,
            spot,
            history_futures: Arc::new(futures_history.clone()),
            history_spot: Arc::new(spot_history.clone()),
            trade_history_futures: futures_history.clone(),
            trade_history_spot: spot_history.clone(),
            latest_mark: latest_mark.clone(),
            latest_funding: latest_funding.clone(),
            funding_changes_in_window: funding_changes
                .iter()
                .filter(|change| {
                    change.ts_change >= ts_bucket
                        && change.ts_change < ts_bucket + Duration::minutes(1)
                })
                .cloned()
                .collect(),
            funding_points_in_window: funding_points
                .iter()
                .filter(|point| {
                    point.ts >= ts_bucket && point.ts < ts_bucket + Duration::minutes(1)
                })
                .cloned()
                .collect(),
            mark_points_in_window: mark_points
                .iter()
                .filter(|point| {
                    point.ts >= ts_bucket && point.ts < ts_bucket + Duration::minutes(1)
                })
                .cloned()
                .collect(),
            funding_changes_recent: Arc::new(funding_changes.clone()),
            funding_recent_7d_payload: funding_recent_7d_payload.clone(),
            funding_points_recent: Arc::new(funding_points.clone()),
            mark_points_recent: Arc::new(mark_points.clone()),
            liquidation_recent_7d_payload: Arc::new(
                futures_history
                    .iter()
                    .map(|row| {
                        json!({
                            "ts_bucket": row.ts_bucket.to_rfc3339(),
                            "long_liq": 0.0,
                            "short_liq": 0.0,
                        })
                    })
                    .collect(),
            ),
            divergence_all_events: Arc::new(Vec::new()),
            exhaustion_all_events: Arc::new(Vec::new()),
            latest_common_oi_ratio_bucket: None,
            current_open_interest: Some(OpenInterestCurrentSidecar {
                ts_effective: ts_bucket + Duration::minutes(1),
                open_interest_contracts: 1.0,
                mark_price: latest_mark.as_ref().and_then(|mark| mark.mark_price),
                open_interest_value_usdt: Some(1.0),
            }),
            open_interest_hist_5m: Vec::new(),
            global_account_ratio_5m: Vec::new(),
            top_account_ratio_5m: Vec::new(),
            top_position_ratio_5m: Vec::new(),
            latest_options_surface_bucket: None,
            options_surface_5m: Vec::new(),
            incremental_outputs: Arc::new(
                crate::indicators::shared::incremental::IncrementalIndicatorOutputs::default(),
            ),
        };

        let legacy_ctx = IndicatorContext::from_bundle(
            bundle.clone(),
            &options,
            KlineHistorySupplement::default(),
        );

        let mut incremental_state = IncrementalIndicatorState::default();
        incremental_state.configure(config_from_options(&options));
        incremental_state.rebuild(
            Some(ts_bucket),
            &futures_history,
            &spot_history,
            latest_mark.as_ref(),
            &funding_changes,
            funding_recent_7d_payload,
            &funding_points,
            &mark_points,
        );
        let incremental_ctx = IndicatorContext::from_bundle(
            WindowBundle {
                incremental_outputs: incremental_state.outputs(),
                ..bundle
            },
            &options,
            KlineHistorySupplement::default(),
        );

        let pairs: Vec<(&str, Value, Value)> = vec![
            (
                "funding_rate",
                I16FundingRate
                    .evaluate(&legacy_ctx)
                    .snapshot
                    .expect("legacy funding")
                    .payload_json,
                I16FundingRate
                    .evaluate(&incremental_ctx)
                    .snapshot
                    .expect("incremental funding")
                    .payload_json,
            ),
            (
                "avwap",
                I18Avwap
                    .evaluate(&legacy_ctx)
                    .snapshot
                    .expect("legacy avwap")
                    .payload_json,
                I18Avwap
                    .evaluate(&incremental_ctx)
                    .snapshot
                    .expect("incremental avwap")
                    .payload_json,
            ),
            (
                "tpo_market_profile",
                I20TpoMarketProfile
                    .evaluate(&legacy_ctx)
                    .snapshot
                    .expect("legacy tpo")
                    .payload_json,
                I20TpoMarketProfile
                    .evaluate(&incremental_ctx)
                    .snapshot
                    .expect("incremental tpo")
                    .payload_json,
            ),
            (
                "rvwap_sigma_bands",
                I21RvwapSigmaBands
                    .evaluate(&legacy_ctx)
                    .snapshot
                    .expect("legacy rvwap")
                    .payload_json,
                I21RvwapSigmaBands
                    .evaluate(&incremental_ctx)
                    .snapshot
                    .expect("incremental rvwap")
                    .payload_json,
            ),
            (
                "high_volume_pulse",
                I22HighVolumePulse
                    .evaluate(&legacy_ctx)
                    .snapshot
                    .expect("legacy high_volume")
                    .payload_json,
                I22HighVolumePulse
                    .evaluate(&incremental_ctx)
                    .snapshot
                    .expect("incremental high_volume")
                    .payload_json,
            ),
        ];

        for (code, legacy, incremental) in pairs {
            if legacy != incremental {
                let path = first_value_diff_path(&legacy, &incremental)
                    .unwrap_or_else(|| "<unknown>".to_string());
                panic!(
                    "incremental payload diverged for {code} at {path}\nlegacy={legacy}\nincremental={incremental}"
                );
            }
        }
    }

    fn first_value_diff_path(left: &Value, right: &Value) -> Option<String> {
        fn walk(left: &Value, right: &Value, path: &mut Vec<String>) -> Option<String> {
            match (left, right) {
                (Value::Object(left_map), Value::Object(right_map)) => {
                    let mut keys = left_map
                        .keys()
                        .chain(right_map.keys())
                        .cloned()
                        .collect::<Vec<_>>();
                    keys.sort();
                    keys.dedup();
                    for key in keys {
                        let in_left = left_map.get(&key);
                        let in_right = right_map.get(&key);
                        if in_left == in_right {
                            continue;
                        }
                        path.push(key);
                        let result = match (in_left, in_right) {
                            (Some(lv), Some(rv)) => {
                                walk(lv, rv, path).or_else(|| Some(path.join(".")))
                            }
                            _ => Some(path.join(".")),
                        };
                        path.pop();
                        return result;
                    }
                    None
                }
                (Value::Array(left_arr), Value::Array(right_arr)) => {
                    let len = left_arr.len().max(right_arr.len());
                    for idx in 0..len {
                        let in_left = left_arr.get(idx);
                        let in_right = right_arr.get(idx);
                        if in_left == in_right {
                            continue;
                        }
                        path.push(format!("[{idx}]"));
                        let result = match (in_left, in_right) {
                            (Some(lv), Some(rv)) => {
                                walk(lv, rv, path).or_else(|| Some(path.join(".")))
                            }
                            _ => Some(path.join(".")),
                        };
                        path.pop();
                        return result;
                    }
                    None
                }
                _ => {
                    if left == right {
                        None
                    } else {
                        Some(path.join("."))
                    }
                }
            }
        }

        walk(left, right, &mut Vec::new())
    }

    fn config_from_options(options: &IndicatorRuntimeOptions) -> IncrementalIndicatorConfig {
        IncrementalIndicatorConfig {
            tpo_rows_nb: options.tpo_rows_nb,
            tpo_value_area_pct: options.tpo_value_area_pct,
            tpo_session_windows: vec![("4h".to_string(), 240), ("1d".to_string(), 1440)],
            tpo_ib_minutes: options.tpo_ib_minutes,
            tpo_dev_output_windows: vec![("15m".to_string(), 15), ("1h".to_string(), 60)],
            rvwap_windows: vec![
                ("15m".to_string(), 15),
                ("4h".to_string(), 240),
                ("1d".to_string(), 1440),
            ],
            rvwap_output_windows: vec![("15m".to_string(), 15), ("1h".to_string(), 60)],
            rvwap_min_samples: options.rvwap_min_samples,
            high_volume_pulse_z_windows: vec![
                ("1h".to_string(), 60),
                ("4h".to_string(), 240),
                ("1d".to_string(), 1440),
            ],
            high_volume_pulse_summary_windows: vec![
                ("15m".to_string(), 15),
                ("1h".to_string(), 60),
            ],
            high_volume_pulse_min_samples: options.high_volume_pulse_min_samples,
        }
    }

    fn runtime_options() -> IndicatorRuntimeOptions {
        IndicatorRuntimeOptions {
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_bars_3d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec![
                "15m".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "3d".to_string(),
            ],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            tpo_rows_nb: 32,
            tpo_value_area_pct: 0.70,
            tpo_session_windows: vec!["4h".to_string(), "1d".to_string()],
            tpo_ib_minutes: 60,
            tpo_dev_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            rvwap_output_windows: vec!["15m".to_string(), "1h".to_string()],
            rvwap_min_samples: 5,
            high_volume_pulse_z_windows: vec!["1h".to_string(), "4h".to_string(), "1d".to_string()],
            high_volume_pulse_summary_windows: vec!["15m".to_string(), "1h".to_string()],
            high_volume_pulse_min_samples: 5,
            ema_base_periods: vec![13, 21, 34],
            ema_htf_periods: vec![100, 200],
            ema_htf_windows: vec!["4h".to_string(), "1d".to_string()],
            ema_output_windows: vec!["15m".to_string(), "1h".to_string()],
            ema_fill_from_db: true,
            ema_db_bars_4h: 256,
            ema_db_bars_1d: 256,
            ema_db_bars_3d: 256,
            divergence_sig_test_mode: DivergenceSigTestMode::Threshold,
            divergence_bootstrap_b: 200,
            divergence_bootstrap_block_len: 5,
            divergence_p_value_threshold: 0.05,
            window_codes: vec![
                "1m".to_string(),
                "15m".to_string(),
                "1h".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "3d".to_string(),
            ],
        }
    }

    fn minute_window_from_history(row: &MinuteHistory) -> MinuteWindowData {
        let mut window = MinuteWindowData::empty(row.market, row.ts_bucket);
        window.trade_count = 1;
        window.buy_qty = row.buy_qty;
        window.sell_qty = row.sell_qty;
        window.total_qty = row.total_qty;
        window.buy_notional = row.total_notional * 0.5;
        window.sell_notional = row.total_notional * 0.5;
        window.total_notional = row.total_notional;
        window.delta = row.delta;
        window.relative_delta = row.relative_delta;
        window.first_price = row.open_price;
        window.last_price = row.last_price;
        window.high_price = row.high_price;
        window.low_price = row.low_price;
        window.profile = row.profile.clone();
        window.avwap = row.avwap_minute;
        window
    }

    fn history_row(
        ts_bucket: chrono::DateTime<Utc>,
        market: MarketKind,
        price: f64,
        qty: f64,
    ) -> MinuteHistory {
        let mut profile = BTreeMap::new();
        profile.insert(
            (price * 100.0).round() as i64,
            crate::runtime::state_store::LevelAgg {
                buy_qty: qty * 0.6,
                sell_qty: qty * 0.4,
            },
        );
        MinuteHistory {
            ts_bucket,
            market,
            open_price: Some(price - 0.2),
            high_price: Some(price + 0.4),
            low_price: Some(price - 0.5),
            close_price: Some(price + 0.1),
            last_price: Some(price),
            buy_qty: qty * 0.6,
            sell_qty: qty * 0.4,
            total_qty: qty,
            total_notional: price * qty,
            delta: qty * 0.2,
            relative_delta: 0.2,
            force_liq: BTreeMap::<i64, LiqAgg>::new(),
            ofi: qty * 0.1,
            spread_twa: Some(0.02),
            topk_depth_twa: Some(1000.0),
            obi_twa: Some(0.1),
            obi_l1_twa: Some(0.1),
            obi_k_twa: Some(0.1),
            obi_k_dw_twa: Some(0.1),
            obi_k_dw_close: Some(0.1),
            obi_k_dw_change: Some(0.01),
            obi_k_dw_adj_twa: Some(0.1),
            bbo_updates: 1,
            microprice_twa: Some(price),
            microprice_classic_twa: Some(price),
            microprice_kappa_twa: Some(price),
            microprice_adj_twa: Some(price),
            cvd: qty,
            vpin: 0.2,
            avwap_minute: Some(price),
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
            profile,
        }
    }
}
