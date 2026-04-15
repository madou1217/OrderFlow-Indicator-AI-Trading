use crate::indicators::context::{
    clip01, ExhaustionEventRow, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_exhaustion_event_id;
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::collections::VecDeque;

const TICK_SIZE: f64 = 0.01;
const PIVOT_LEFT: usize = 3;
const PIVOT_RIGHT: usize = 3;
const MIN_LEG_GAP_MINUTES: i64 = 3;
const MAX_LEG_GAP_MINUTES: i64 = 180;
const EPSILON_PRICE_TICKS: f64 = 2.0;
const EPSILON_CONFIRM_TICKS: f64 = 2.0;
const EPSILON_DELTA: f64 = 20.0;
const EPSILON_RDELTA: f64 = 0.05;
const ETA_REJECT: f64 = 0.65;
const CONFIRM_BARS: usize = 5;
const LAMBDA_SPOT_PENALTY: f64 = 0.20;
const THETA_BUY_CVD: f64 = 0.0;
const THETA_BUY_WHALE: f64 = 100_000.0;
const THETA_SELL_CVD: f64 = 0.0;
const THETA_SELL_WHALE: f64 = -100_000.0;
pub(crate) const EXHAUSTION_INCREMENTAL_LOOKBACK_MINUTES: i64 =
    MAX_LEG_GAP_MINUTES + PIVOT_LEFT as i64 + PIVOT_RIGHT as i64 + CONFIRM_BARS as i64 + 5;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExhaustionEventData {
    pub direction: i16,
    pub event_type: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub pivot_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_ts_2: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_2: chrono::DateTime<chrono::Utc>,
    pub price_push_ticks: f64,
    pub delta_change: f64,
    pub rdelta_change: f64,
    pub reject_ratio: f64,
    pub confirm_speed: f64,
    pub spot_cvd_push_post_pivot: f64,
    pub spot_whale_push_post_pivot: f64,
    pub spot_continuation_risk: bool,
    pub spot_exhaustion_confirm: bool,
    pub score: f64,
    pub payload: Value,
}

#[derive(Debug, Clone)]
struct ExhaustionDerivedMinute {
    seq: usize,
    ts_bucket: DateTime<Utc>,
    high: f64,
    low: f64,
    close: f64,
    delta: f64,
    rdelta: f64,
    spot_cvd: f64,
    spot_whale_notional: f64,
}

#[derive(Debug, Clone)]
struct ExhaustionPivot {
    seq: usize,
}

#[derive(Debug, Clone)]
struct ExhaustionPendingCandidate {
    direction: i16,
    pivot_seq_1: usize,
    pivot_seq_2: usize,
}

#[derive(Debug, Clone)]
struct ExhaustionCachedEvent {
    required_start_ts: DateTime<Utc>,
    event: ExhaustionEventData,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ExhaustionEventStateMachine {
    minutes: VecDeque<ExhaustionDerivedMinute>,
    pending: VecDeque<ExhaustionPendingCandidate>,
    events: VecDeque<ExhaustionCachedEvent>,
    last_high_pivot: Option<ExhaustionPivot>,
    last_low_pivot: Option<ExhaustionPivot>,
    last_ts: Option<DateTime<Utc>>,
    next_seq: usize,
}

impl ExhaustionEventStateMachine {
    pub(crate) fn rebuild(
        &mut self,
        history_futures: &[MinuteHistory],
        history_spot: &[MinuteHistory],
    ) {
        *self = Self::default();
        let (history_futures, history_spot) = aligned_event_histories(history_futures, history_spot);
        for (fut, spot) in history_futures.iter().zip(history_spot.iter()) {
            self.append_pair(fut, spot);
        }
        self.last_ts = history_futures.last().map(|row| row.ts_bucket);
    }

    pub(crate) fn sync(
        &mut self,
        history_futures: &[MinuteHistory],
        history_spot: &[MinuteHistory],
    ) {
        let (history_futures, history_spot) = aligned_event_histories(history_futures, history_spot);
        let Some(first_ts) = history_futures.first().map(|row| row.ts_bucket) else {
            *self = Self::default();
            return;
        };
        let last_ts = history_futures.last().map(|row| row.ts_bucket).unwrap_or(first_ts);
        match self.last_ts {
            None => {
                self.rebuild(history_futures, history_spot);
                return;
            }
            Some(prev_last_ts) if prev_last_ts >= last_ts => {
                self.rebuild(history_futures, history_spot);
                return;
            }
            Some(_) => {}
        }
        self.prune_before(first_ts);
        let start_idx = lower_bound_history_ts(history_futures, self.last_ts.unwrap() + Duration::minutes(1));
        if start_idx == 0 && self.minutes.is_empty() {
            self.rebuild(history_futures, history_spot);
            return;
        }
        for (fut, spot) in history_futures[start_idx..].iter().zip(history_spot[start_idx..].iter()) {
            self.append_pair(fut, spot);
        }
        self.last_ts = Some(last_ts);
    }

    pub(crate) fn events(&self) -> Vec<ExhaustionEventData> {
        self.events.iter().map(|entry| entry.event.clone()).collect()
    }

    fn prune_before(&mut self, first_ts: DateTime<Utc>) {
        while self
            .minutes
            .front()
            .map(|row| row.ts_bucket < first_ts)
            .unwrap_or(false)
        {
            self.minutes.pop_front();
        }
        while self
            .events
            .front()
            .map(|entry| {
                entry.required_start_ts < first_ts || entry.event.start_ts < first_ts
            })
            .unwrap_or(false)
        {
            self.events.pop_front();
        }
        while self
            .pending
            .front()
            .map(|candidate| self.offset_of(candidate.pivot_seq_1).is_none())
            .unwrap_or(false)
        {
            self.pending.pop_front();
        }
        if self
            .last_high_pivot
            .as_ref()
            .map(|pivot| self.offset_of(pivot.seq).is_none())
            .unwrap_or(false)
        {
            self.last_high_pivot = None;
        }
        if self
            .last_low_pivot
            .as_ref()
            .map(|pivot| self.offset_of(pivot.seq).is_none())
            .unwrap_or(false)
        {
            self.last_low_pivot = None;
        }
    }

    fn append_pair(&mut self, fut: &MinuteHistory, spot: &MinuteHistory) {
        let high = fut.high_price.or(fut.last_price).unwrap_or(0.0);
        let low = fut.low_price.or(fut.last_price).unwrap_or(0.0);
        let close = fut
            .close_price
            .or(fut.last_price)
            .or(fut.open_price)
            .unwrap_or(0.0);
        let spot_close = spot
            .close_price
            .or(spot.last_price)
            .or(spot.open_price)
            .unwrap_or(0.0);
        let seq = self.next_seq;
        self.minutes.push_back(ExhaustionDerivedMinute {
            seq,
            ts_bucket: fut.ts_bucket,
            high,
            low,
            close,
            delta: fut.delta,
            rdelta: fut.relative_delta,
            spot_cvd: spot.cvd,
            spot_whale_notional: spot.delta * spot_close,
        });
        self.update_pending(seq);
        self.maybe_confirm_pivot(seq, true);
        self.maybe_confirm_pivot(seq, false);
        self.next_seq += 1;
    }

    fn update_pending(&mut self, current_seq: usize) {
        let Some(current_idx) = self.offset_of(current_seq) else {
            return;
        };
        let Some(current) = self.minutes.get(current_idx) else {
            return;
        };
        let mut retained = VecDeque::new();
        while let Some(candidate) = self.pending.pop_front() {
            if current_seq <= candidate.pivot_seq_2 {
                retained.push_back(candidate);
                continue;
            }
            let Some(pivot_idx) = self.offset_of(candidate.pivot_seq_2) else {
                continue;
            };
            let Some(pivot) = self.minutes.get(pivot_idx) else {
                continue;
            };
            let eps_confirm = EPSILON_CONFIRM_TICKS * TICK_SIZE;
            let confirmed = if candidate.direction < 0 {
                current.close <= pivot.low - eps_confirm
            } else {
                current.close >= pivot.high + eps_confirm
            };
            if confirmed {
                if let Some(event) = self.build_event(&candidate, current_seq) {
                    self.events.push_back(event);
                }
                continue;
            }
            if current_seq < candidate.pivot_seq_2 + CONFIRM_BARS {
                retained.push_back(candidate);
            }
        }
        self.pending = retained;
    }

    fn maybe_confirm_pivot(&mut self, current_seq: usize, is_high: bool) {
        if current_seq < PIVOT_RIGHT {
            return;
        }
        let pivot_seq = current_seq - PIVOT_RIGHT;
        let Some(pivot_idx) = self.offset_of(pivot_seq) else {
            return;
        };
        if pivot_idx < PIVOT_LEFT || pivot_idx + PIVOT_RIGHT >= self.minutes.len() {
            return;
        }
        let is_pivot = if is_high {
            let pivot_high = self.minutes[pivot_idx].high;
            let left_max = self
                .minutes
                .iter()
                .skip(pivot_idx - PIVOT_LEFT)
                .take(PIVOT_LEFT)
                .map(|row| row.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let right_max = self
                .minutes
                .iter()
                .skip(pivot_idx + 1)
                .take(PIVOT_RIGHT)
                .map(|row| row.high)
                .fold(f64::NEG_INFINITY, f64::max);
            pivot_high > left_max && pivot_high >= right_max
        } else {
            let pivot_low = self.minutes[pivot_idx].low;
            let left_min = self
                .minutes
                .iter()
                .skip(pivot_idx - PIVOT_LEFT)
                .take(PIVOT_LEFT)
                .map(|row| row.low)
                .fold(f64::INFINITY, f64::min);
            let right_min = self
                .minutes
                .iter()
                .skip(pivot_idx + 1)
                .take(PIVOT_RIGHT)
                .map(|row| row.low)
                .fold(f64::INFINITY, f64::min);
            pivot_low < left_min && pivot_low <= right_min
        };
        if !is_pivot {
            return;
        }
        let current_pivot = ExhaustionPivot { seq: pivot_seq };
        if is_high {
            if let Some(prev) = self.last_high_pivot.clone() {
                self.maybe_start_candidate(&prev, &current_pivot, true);
            }
            self.last_high_pivot = Some(current_pivot);
        } else {
            if let Some(prev) = self.last_low_pivot.clone() {
                self.maybe_start_candidate(&prev, &current_pivot, false);
            }
            self.last_low_pivot = Some(current_pivot);
        }
    }

    fn maybe_start_candidate(
        &mut self,
        pivot_1: &ExhaustionPivot,
        pivot_2: &ExhaustionPivot,
        is_high: bool,
    ) {
        let Some(idx_1) = self.offset_of(pivot_1.seq) else {
            return;
        };
        let Some(idx_2) = self.offset_of(pivot_2.seq) else {
            return;
        };
        let Some(first) = self.minutes.get(idx_1) else {
            return;
        };
        let Some(second) = self.minutes.get(idx_2) else {
            return;
        };
        let leg_minutes = (second.ts_bucket - first.ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg_minutes) {
            return;
        }
        let range = (second.high - second.low).max(TICK_SIZE);
        let eps_price = EPSILON_PRICE_TICKS * TICK_SIZE;
        let valid = if is_high {
            let reject_top = (second.high - second.close) / (range + 1e-12);
            second.high >= first.high + eps_price
                && second.delta <= first.delta - EPSILON_DELTA
                && second.rdelta <= first.rdelta - EPSILON_RDELTA
                && reject_top >= ETA_REJECT
        } else {
            let reject_bottom = (second.close - second.low) / (range + 1e-12);
            second.low <= first.low - eps_price
                && second.delta >= first.delta + EPSILON_DELTA
                && second.rdelta >= first.rdelta + EPSILON_RDELTA
                && reject_bottom >= ETA_REJECT
        };
        if !valid {
            return;
        }
        self.pending.push_back(ExhaustionPendingCandidate {
            direction: if is_high { -1 } else { 1 },
            pivot_seq_1: pivot_1.seq,
            pivot_seq_2: pivot_2.seq,
        });
    }

    fn build_event(
        &self,
        candidate: &ExhaustionPendingCandidate,
        confirm_seq: usize,
    ) -> Option<ExhaustionCachedEvent> {
        let idx_1 = self.offset_of(candidate.pivot_seq_1)?;
        let idx_2 = self.offset_of(candidate.pivot_seq_2)?;
        let confirm_idx = self.offset_of(confirm_seq)?;
        let first = self.minutes.get(idx_1)?;
        let second = self.minutes.get(idx_2)?;
        let confirm = self.minutes.get(confirm_idx)?;
        let range = (second.high - second.low).max(TICK_SIZE);
        let confirm_speed = 1.0 / ((confirm_seq - candidate.pivot_seq_2) as f64).max(1.0);
        let (event_type, pivot_price, price_push, delta_change, rdelta_change, reject_ratio, base_score) =
            if candidate.direction < 0 {
                let reject_top = (second.high - second.close) / (range + 1e-12);
                let price_push = (second.high - first.high) / TICK_SIZE;
                let delta_drop = first.delta - second.delta;
                let rdelta_drop = first.rdelta - second.rdelta;
                (
                    "buying_exhaustion",
                    second.high,
                    price_push,
                    second.delta - first.delta,
                    second.rdelta - first.rdelta,
                    reject_top,
                    0.30 * clip01(price_push / 10.0)
                        + 0.25 * clip01(delta_drop / (3.0 * EPSILON_DELTA))
                        + 0.20 * clip01(rdelta_drop / (3.0 * EPSILON_RDELTA))
                        + 0.15 * clip01((reject_top - ETA_REJECT) / (1.0 - ETA_REJECT))
                        + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64),
                )
            } else {
                let reject_bottom = (second.close - second.low) / (range + 1e-12);
                let price_push = (first.low - second.low) / TICK_SIZE;
                let delta_lift = second.delta - first.delta;
                let rdelta_lift = second.rdelta - first.rdelta;
                (
                    "selling_exhaustion",
                    second.low,
                    price_push,
                    second.delta - first.delta,
                    second.rdelta - first.rdelta,
                    reject_bottom,
                    0.30 * clip01(price_push / 10.0)
                        + 0.25 * clip01(delta_lift / (3.0 * EPSILON_DELTA))
                        + 0.20 * clip01(rdelta_lift / (3.0 * EPSILON_RDELTA))
                        + 0.15 * clip01((reject_bottom - ETA_REJECT) / (1.0 - ETA_REJECT))
                        + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64),
                )
            };
        let spot_cvd_push = confirm.spot_cvd - second.spot_cvd;
        let spot_whale_push = self
            .minutes
            .iter()
            .skip(idx_2)
            .take(confirm_idx - idx_2 + 1)
            .map(|row| row.spot_whale_notional)
            .sum::<f64>();
        let (spot_continuation_risk, spot_exhaustion_confirm) = if candidate.direction < 0 {
            (
                spot_cvd_push > THETA_BUY_CVD || spot_whale_push > THETA_BUY_WHALE,
                spot_cvd_push <= THETA_BUY_CVD && spot_whale_push <= THETA_BUY_WHALE,
            )
        } else {
            (
                spot_cvd_push < THETA_SELL_CVD || spot_whale_push < THETA_SELL_WHALE,
                spot_cvd_push >= THETA_SELL_CVD && spot_whale_push >= THETA_SELL_WHALE,
            )
        };
        let final_score = clip01(
            base_score
                * (1.0 - LAMBDA_SPOT_PENALTY * if spot_continuation_risk { 1.0 } else { 0.0 })
                + (1.0 - base_score) * 0.10 * if spot_exhaustion_confirm { 1.0 } else { 0.0 },
        );
        let start_ts = first.ts_bucket;
        let confirm_ts = confirm.ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;
        Some(ExhaustionCachedEvent {
            required_start_ts: first.ts_bucket - Duration::minutes(PIVOT_LEFT as i64),
            event: ExhaustionEventData {
                direction: candidate.direction,
                event_type: event_type.to_string(),
                start_ts,
                end_ts,
                confirm_ts,
                pivot_price,
                pivot_ts_1: first.ts_bucket,
                pivot_ts_2: second.ts_bucket,
                pivot_confirm_ts_1: first.ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
                pivot_confirm_ts_2: second.ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
                price_push_ticks: price_push,
                delta_change,
                rdelta_change,
                reject_ratio,
                confirm_speed,
                spot_cvd_push_post_pivot: spot_cvd_push,
                spot_whale_push_post_pivot: spot_whale_push,
                spot_continuation_risk,
                spot_exhaustion_confirm,
                score: final_score,
                payload: json!({
                    "event_start_ts": start_ts.to_rfc3339(),
                    "event_end_ts": end_ts.to_rfc3339(),
                    "event_available_ts": confirm_ts.to_rfc3339(),
                    "pivot_price": pivot_price,
                    "pivot_ts_1": first.ts_bucket.to_rfc3339(),
                    "pivot_ts_2": second.ts_bucket.to_rfc3339(),
                    "pivot_confirm_ts_1": (first.ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                    "pivot_confirm_ts_2": (second.ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                    "price_push_ticks": price_push,
                    "reject_ratio": reject_ratio,
                    "confirm_speed": confirm_speed,
                    "spot_cvd_push_post_pivot": spot_cvd_push,
                    "spot_whale_push_post_pivot": spot_whale_push,
                    "spot_continuation_risk": spot_continuation_risk,
                    "spot_exhaustion_confirm": spot_exhaustion_confirm,
                    "strength_score_xmk": final_score,
                    "exhaustion_quality_score": final_score,
                    "sig_pass": true
                }),
            },
        })
    }

    fn offset_of(&self, seq: usize) -> Option<usize> {
        let first_seq = self.minutes.front()?.seq;
        let idx = seq.checked_sub(first_seq)?;
        (idx < self.minutes.len()).then_some(idx)
    }
}

fn aligned_event_histories<'a>(
    history_futures: &'a [MinuteHistory],
    history_spot: &'a [MinuteHistory],
) -> (&'a [MinuteHistory], &'a [MinuteHistory]) {
    let n = history_futures.len().min(history_spot.len());
    (
        &history_futures[history_futures.len().saturating_sub(n)..],
        &history_spot[history_spot.len().saturating_sub(n)..],
    )
}

fn lower_bound_history_ts(history: &[MinuteHistory], target: DateTime<Utc>) -> usize {
    let mut lo = 0usize;
    let mut hi = history.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if history[mid].ts_bucket < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[cfg(test)]
pub(crate) fn compute_exhaustion_all_history_from_histories(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
) -> Vec<ExhaustionEventData> {
    let series = crate::indicators::context::BasicEventHistorySeries::from_histories(
        history_futures,
        history_spot,
    );
    let n = series.n;
    if n < (PIVOT_LEFT + PIVOT_RIGHT + CONFIRM_BARS + 3) {
        return Vec::new();
    }

    let fut = &history_futures[history_futures.len().saturating_sub(n)..];
    let last_idx = n - 1;

    let highs = &series.high;
    let lows = &series.low;
    let closes = &series.close;
    let deltas = &series.delta;
    let rdeltas = &series.rdelta;
    let spot_cvd = &series.spot_cvd;
    let spot_whale_notional = &series.spot_whale_notional;

    let high_pivots = confirmed_high_pivots(&highs);
    let low_pivots = confirmed_low_pivots(&lows);

    let mut out = Vec::new();
    out.extend(detect_buying_events(
        fut,
        &highs,
        &lows,
        &closes,
        &deltas,
        &rdeltas,
        &spot_cvd,
        &spot_whale_notional,
        &high_pivots,
        last_idx,
    ));
    out.extend(detect_selling_events(
        fut,
        &highs,
        &lows,
        &closes,
        &deltas,
        &rdeltas,
        &spot_cvd,
        &spot_whale_notional,
        &low_pivots,
        last_idx,
    ));
    out
}

pub(crate) fn exhaustion_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &ExhaustionEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_exhaustion_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.direction,
        event.confirm_ts,
        event.pivot_ts_1,
        event.pivot_ts_2,
    );
    let mut base = Map::new();
    base.insert("event_id".to_string(), json!(event_id));
    base.insert("type".to_string(), json!(event.event_type));
    base.insert("direction".to_string(), json!(event.direction));
    base.insert("start_ts".to_string(), json!(event.start_ts.to_rfc3339()));
    base.insert("end_ts".to_string(), json!(event.end_ts.to_rfc3339()));
    base.insert(
        "event_available_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert(
        "confirm_ts".to_string(),
        json!(event.confirm_ts.to_rfc3339()),
    );
    base.insert("score".to_string(), json!(event.score));
    base.insert("indicator_code".to_string(), json!(indicator_code));
    base.insert("pivot_price".to_string(), json!(event.pivot_price));
    match event.event_type.as_str() {
        "buying_exhaustion" => {
            base.insert("delta_drop".to_string(), json!(-event.delta_change));
            base.insert("rdelta_drop".to_string(), json!(-event.rdelta_change));
        }
        "selling_exhaustion" => {
            base.insert("delta_lift".to_string(), json!(event.delta_change));
            base.insert("rdelta_lift".to_string(), json!(event.rdelta_change));
        }
        _ => {}
    }
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

pub(crate) fn append_exhaustion_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[ExhaustionEventData],
) {
    for event in events {
        let event_id = build_exhaustion_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.direction,
            event.confirm_ts,
            event.pivot_ts_1,
            event.pivot_ts_2,
        );
        let payload_json = exhaustion_event_json(symbol, indicator_code, event).1;
        out.exhaustion_rows.push(ExhaustionEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            pivot_ts_1: Some(event.pivot_ts_1),
            pivot_ts_2: Some(event.pivot_ts_2),
            pivot_confirm_ts_1: Some(event.pivot_confirm_ts_1),
            pivot_confirm_ts_2: Some(event.pivot_confirm_ts_2),
            price_push_ticks: Some(event.price_push_ticks),
            delta_change: Some(event.delta_change),
            rdelta_change: Some(event.rdelta_change),
            reject_ratio: Some(event.reject_ratio),
            confirm_speed: Some(event.confirm_speed),
            spot_cvd_push_post_pivot: Some(event.spot_cvd_push_post_pivot),
            spot_whale_push_post_pivot: Some(event.spot_whale_push_post_pivot),
            spot_continuation_risk: Some(event.spot_continuation_risk),
            spot_exhaustion_confirm: Some(event.spot_exhaustion_confirm),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn detect_buying_events(
    fut: &[MinuteHistory],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    deltas: &[f64],
    rdeltas: &[f64],
    spot_cvd: &[f64],
    spot_whale_notional: &[f64],
    pivots: &[usize],
    last_idx: usize,
) -> Vec<ExhaustionEventData> {
    let mut events = Vec::new();
    let eps_price = EPSILON_PRICE_TICKS * TICK_SIZE;
    let eps_confirm = EPSILON_CONFIRM_TICKS * TICK_SIZE;

    for pair in pivots.windows(2) {
        let t1 = pair[0];
        let t2 = pair[1];
        if t2 + 1 >= fut.len() {
            continue;
        }

        let leg_minutes = (fut[t2].ts_bucket - fut[t1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg_minutes) {
            continue;
        }

        let range = (highs[t2] - lows[t2]).max(TICK_SIZE);
        let reject_top = (highs[t2] - closes[t2]) / (range + 1e-12);

        let cond = highs[t2] >= highs[t1] + eps_price
            && deltas[t2] <= deltas[t1] - EPSILON_DELTA
            && rdeltas[t2] <= rdeltas[t1] - EPSILON_RDELTA
            && reject_top >= ETA_REJECT;
        if !cond {
            continue;
        }

        let end = (t2 + CONFIRM_BARS).min(last_idx);
        let confirm_idx = ((t2 + 1)..=end).find(|&t| closes[t] <= lows[t2] - eps_confirm);
        let Some(tc) = confirm_idx else {
            continue;
        };

        let price_push = (highs[t2] - highs[t1]) / TICK_SIZE;
        let delta_drop = deltas[t1] - deltas[t2];
        let rdelta_drop = rdeltas[t1] - rdeltas[t2];
        let delta_change = deltas[t2] - deltas[t1];
        let rdelta_change = rdeltas[t2] - rdeltas[t1];
        let confirm_speed = 1.0 / ((tc - t2) as f64).max(1.0);
        let base_score = 0.30 * clip01(price_push / 10.0)
            + 0.25 * clip01(delta_drop / (3.0 * EPSILON_DELTA))
            + 0.20 * clip01(rdelta_drop / (3.0 * EPSILON_RDELTA))
            + 0.15 * clip01((reject_top - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64);

        let spot_cvd_push = spot_cvd[tc] - spot_cvd[t2];
        let spot_whale_push = spot_whale_notional[t2..=tc].iter().sum::<f64>();
        let spot_continuation_risk =
            spot_cvd_push > THETA_BUY_CVD || spot_whale_push > THETA_BUY_WHALE;
        let spot_exhaustion_confirm =
            spot_cvd_push <= THETA_BUY_CVD && spot_whale_push <= THETA_BUY_WHALE;
        let final_score = clip01(
            base_score
                * (1.0 - LAMBDA_SPOT_PENALTY * if spot_continuation_risk { 1.0 } else { 0.0 })
                + (1.0 - base_score) * 0.10 * if spot_exhaustion_confirm { 1.0 } else { 0.0 },
        );

        let start_ts = fut[t1].ts_bucket;
        let confirm_ts = fut[tc].ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;

        events.push(ExhaustionEventData {
            direction: -1,
            event_type: "buying_exhaustion".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: highs[t2],
            pivot_ts_1: fut[t1].ts_bucket,
            pivot_ts_2: fut[t2].ts_bucket,
            pivot_confirm_ts_1: fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            pivot_confirm_ts_2: fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            price_push_ticks: price_push,
            delta_change,
            rdelta_change,
            reject_ratio: reject_top,
            confirm_speed,
            spot_cvd_push_post_pivot: spot_cvd_push,
            spot_whale_push_post_pivot: spot_whale_push,
            spot_continuation_risk,
            spot_exhaustion_confirm,
            score: final_score,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": highs[t2],
                "pivot_ts_1": fut[t1].ts_bucket.to_rfc3339(),
                "pivot_ts_2": fut[t2].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_1": (fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "pivot_confirm_ts_2": (fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "price_push_ticks": price_push,
                "delta_drop": delta_drop,
                "rdelta_drop": rdelta_drop,
                "reject_ratio": reject_top,
                "confirm_speed": confirm_speed,
                "spot_cvd_push_post_pivot": spot_cvd_push,
                "spot_whale_push_post_pivot": spot_whale_push,
                "spot_continuation_risk": spot_continuation_risk,
                "spot_exhaustion_confirm": spot_exhaustion_confirm,
                "strength_score_xmk": final_score,
                "exhaustion_quality_score": final_score,
                "sig_pass": true
            }),
        });
    }

    events
}

#[allow(clippy::too_many_arguments)]
fn detect_selling_events(
    fut: &[MinuteHistory],
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    deltas: &[f64],
    rdeltas: &[f64],
    spot_cvd: &[f64],
    spot_whale_notional: &[f64],
    pivots: &[usize],
    last_idx: usize,
) -> Vec<ExhaustionEventData> {
    let mut events = Vec::new();
    let eps_price = EPSILON_PRICE_TICKS * TICK_SIZE;
    let eps_confirm = EPSILON_CONFIRM_TICKS * TICK_SIZE;

    for pair in pivots.windows(2) {
        let t1 = pair[0];
        let t2 = pair[1];
        if t2 + 1 >= fut.len() {
            continue;
        }

        let leg_minutes = (fut[t2].ts_bucket - fut[t1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg_minutes) {
            continue;
        }

        let range = (highs[t2] - lows[t2]).max(TICK_SIZE);
        let reject_bottom = (closes[t2] - lows[t2]) / (range + 1e-12);

        let cond = lows[t2] <= lows[t1] - eps_price
            && deltas[t2] >= deltas[t1] + EPSILON_DELTA
            && rdeltas[t2] >= rdeltas[t1] + EPSILON_RDELTA
            && reject_bottom >= ETA_REJECT;
        if !cond {
            continue;
        }

        let end = (t2 + CONFIRM_BARS).min(last_idx);
        let confirm_idx = ((t2 + 1)..=end).find(|&t| closes[t] >= highs[t2] + eps_confirm);
        let Some(tc) = confirm_idx else {
            continue;
        };

        let price_push = (lows[t1] - lows[t2]) / TICK_SIZE;
        let delta_lift = deltas[t2] - deltas[t1];
        let rdelta_lift = rdeltas[t2] - rdeltas[t1];
        let delta_change = deltas[t2] - deltas[t1];
        let rdelta_change = rdeltas[t2] - rdeltas[t1];
        let confirm_speed = 1.0 / ((tc - t2) as f64).max(1.0);
        let base_score = 0.30 * clip01(price_push / 10.0)
            + 0.25 * clip01(delta_lift / (3.0 * EPSILON_DELTA))
            + 0.20 * clip01(rdelta_lift / (3.0 * EPSILON_RDELTA))
            + 0.15 * clip01((reject_bottom - ETA_REJECT) / (1.0 - ETA_REJECT))
            + 0.10 * clip01(confirm_speed * CONFIRM_BARS as f64);

        let spot_cvd_push = spot_cvd[tc] - spot_cvd[t2];
        let spot_whale_push = spot_whale_notional[t2..=tc].iter().sum::<f64>();
        let spot_continuation_risk =
            spot_cvd_push < THETA_SELL_CVD || spot_whale_push < THETA_SELL_WHALE;
        let spot_exhaustion_confirm =
            spot_cvd_push >= THETA_SELL_CVD && spot_whale_push >= THETA_SELL_WHALE;
        let final_score = clip01(
            base_score
                * (1.0 - LAMBDA_SPOT_PENALTY * if spot_continuation_risk { 1.0 } else { 0.0 })
                + (1.0 - base_score) * 0.10 * if spot_exhaustion_confirm { 1.0 } else { 0.0 },
        );

        let start_ts = fut[t1].ts_bucket;
        let confirm_ts = fut[tc].ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;

        events.push(ExhaustionEventData {
            direction: 1,
            event_type: "selling_exhaustion".to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: lows[t2],
            pivot_ts_1: fut[t1].ts_bucket,
            pivot_ts_2: fut[t2].ts_bucket,
            pivot_confirm_ts_1: fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            pivot_confirm_ts_2: fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64),
            price_push_ticks: price_push,
            delta_change,
            rdelta_change,
            reject_ratio: reject_bottom,
            confirm_speed,
            spot_cvd_push_post_pivot: spot_cvd_push,
            spot_whale_push_post_pivot: spot_whale_push,
            spot_continuation_risk,
            spot_exhaustion_confirm,
            score: final_score,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": lows[t2],
                "pivot_ts_1": fut[t1].ts_bucket.to_rfc3339(),
                "pivot_ts_2": fut[t2].ts_bucket.to_rfc3339(),
                "pivot_confirm_ts_1": (fut[t1].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "pivot_confirm_ts_2": (fut[t2].ts_bucket + Duration::minutes(PIVOT_RIGHT as i64)).to_rfc3339(),
                "price_push_ticks": price_push,
                "delta_lift": delta_lift,
                "rdelta_lift": rdelta_lift,
                "reject_ratio": reject_bottom,
                "confirm_speed": confirm_speed,
                "spot_cvd_push_post_pivot": spot_cvd_push,
                "spot_whale_push_post_pivot": spot_whale_push,
                "spot_continuation_risk": spot_continuation_risk,
                "spot_exhaustion_confirm": spot_exhaustion_confirm,
                "strength_score_xmk": final_score,
                "exhaustion_quality_score": final_score,
                "sig_pass": true
            }),
        });
    }

    events
}

fn confirmed_high_pivots(highs: &[f64]) -> Vec<usize> {
    let mut out = Vec::new();
    if highs.len() <= PIVOT_LEFT + PIVOT_RIGHT {
        return out;
    }
    for i in PIVOT_LEFT..(highs.len() - PIVOT_RIGHT) {
        let left_max = highs[i - PIVOT_LEFT..i]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let right_max = highs[i + 1..=i + PIVOT_RIGHT]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        if highs[i] > left_max && highs[i] >= right_max {
            out.push(i);
        }
    }
    out
}

fn confirmed_low_pivots(lows: &[f64]) -> Vec<usize> {
    let mut out = Vec::new();
    if lows.len() <= PIVOT_LEFT + PIVOT_RIGHT {
        return out;
    }
    for i in PIVOT_LEFT..(lows.len() - PIVOT_RIGHT) {
        let left_min = lows[i - PIVOT_LEFT..i]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let right_min = lows[i + 1..=i + PIVOT_RIGHT]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        if lows[i] < left_min && lows[i] <= right_min {
            out.push(i);
        }
    }
    out
}

pub struct I12BuyingExhaustion;

impl Indicator for I12BuyingExhaustion {
    fn code(&self) -> &'static str {
        "buying_exhaustion"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = ctx
            .exhaustion_all_events()
            .iter()
            .filter(|e| e.event_type == "buying_exhaustion")
            .cloned()
            .collect::<Vec<_>>();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| exhaustion_event_json(&ctx.symbol, self.code(), event))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: json!({
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    )
                }),
            }),
            ..Default::default()
        };

        let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
        let current_events = all_events
            .iter()
            .filter(|event| event.confirm_ts == current_available_ts)
            .cloned()
            .collect::<Vec<_>>();
        append_exhaustion_rows(&mut out, &ctx.symbol, self.code(), &current_events);

        out
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_exhaustion_all_history_from_histories, detect_buying_events, detect_selling_events,
        exhaustion_event_json, ExhaustionEventData,
        ExhaustionEventStateMachine,
    };
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorSharedCaches,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{MinuteHistory, MinuteWindowData};
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn sample_minute(ts_bucket: chrono::DateTime<Utc>) -> MinuteHistory {
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

    fn test_ctx(
        ts_bucket: chrono::DateTime<Utc>,
        history_futures: Vec<MinuteHistory>,
        history_spot: Vec<MinuteHistory>,
    ) -> IndicatorContext {
        IndicatorContext {
            ts_bucket,
            symbol: "TESTUSDT".to_string(),
            futures: MinuteWindowData::empty(MarketKind::Futures, ts_bucket),
            spot: MinuteWindowData::empty(MarketKind::Spot, ts_bucket),
            history_futures: history_futures.into(),
            history_spot: history_spot.into(),
            trade_history_futures: Vec::new(),
            trade_history_spot: Vec::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes_in_window: Vec::new(),
            funding_points_in_window: Vec::new(),
            mark_points_in_window: Vec::new(),
            funding_changes_recent: Vec::new().into(),
            funding_points_recent: Vec::new().into(),
            mark_points_recent: Vec::new().into(),
            latest_common_oi_ratio_bucket: None,
            current_open_interest: None,
            open_interest_hist_5m: Vec::new(),
            global_account_ratio_5m: Vec::new(),
            top_account_ratio_5m: Vec::new(),
            top_position_ratio_5m: Vec::new(),
            latest_options_surface_bucket: None,
            options_surface_5m: Vec::new(),
            incremental_outputs: std::sync::Arc::new(
                crate::indicators::shared::incremental::IncrementalIndicatorOutputs::default(),
            ),
            whale_threshold_usdt: 300_000.0,
            kline_history_bars_1m: 1024,
            kline_history_bars_15m: 120,
            kline_history_bars_4h: 120,
            kline_history_bars_1d: 120,
            kline_history_bars_3d: 120,
            kline_history_bars_7d: 120,
            kline_history_bars_30d: 120,
            kline_history_fill_1d_from_db: true,
            fvg_windows: vec!["15m".to_string(), "4h".to_string(), "1d".to_string()],
            fvg_fill_from_db: true,
            fvg_db_bars_4h: 256,
            fvg_db_bars_1d: 256,
            fvg_db_bars_3d: 256,
            fvg_epsilon_gap_ticks: 2,
            fvg_atr_lookback: 14,
            fvg_min_body_ratio: 0.60,
            fvg_min_impulse_atr_ratio: 1.30,
            fvg_min_gap_atr_ratio: 0.15,
            fvg_max_gap_atr_ratio: 1.20,
            fvg_mitigated_fill_threshold: 0.80,
            fvg_invalid_close_bars: 1,
            kline_history_futures_4h_db: Vec::new(),
            kline_history_futures_1d_db: Vec::new(),
            kline_history_spot_4h_db: Vec::new(),
            kline_history_spot_1d_db: Vec::new(),
            tpo_rows_nb: 64,
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
            window_codes: vec!["1m".to_string()],
            shared_caches: Arc::new(IndicatorSharedCaches::default()),
        }
    }

    #[test]
    fn buying_exhaustion_all_history_keeps_historical_confirmed_event() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 8, 0, 0).unwrap();
        let fut = (0..15)
            .map(|i| sample_minute(base + Duration::minutes(i as i64)))
            .collect::<Vec<_>>();

        let mut highs = vec![100.0; 15];
        let mut lows = vec![99.0; 15];
        let mut closes = vec![99.5; 15];
        let mut deltas = vec![0.0; 15];
        let mut rdeltas = vec![0.0; 15];
        let spot_cvd = vec![0.0; 15];
        let spot_whale_notional = vec![0.0; 15];

        highs[3] = 101.0;
        highs[8] = 104.0;
        lows[8] = 102.0;
        closes[8] = 102.3;
        closes[9] = 102.1;
        closes[10] = 101.9;
        deltas[3] = 120.0;
        deltas[8] = 80.0;
        rdeltas[3] = 0.20;
        rdeltas[8] = 0.10;

        let events = detect_buying_events(
            &fut,
            &highs,
            &lows,
            &closes,
            &deltas,
            &rdeltas,
            &spot_cvd,
            &spot_whale_notional,
            &[3, 8],
            14,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "buying_exhaustion");
        assert_eq!(events[0].pivot_price, highs[8]);
        assert_eq!(
            events[0].confirm_ts,
            fut[10].ts_bucket + Duration::minutes(1)
        );
    }

    #[test]
    fn selling_exhaustion_all_history_keeps_historical_confirmed_event() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 10, 0, 0).unwrap();
        let fut = (0..15)
            .map(|i| sample_minute(base + Duration::minutes(i as i64)))
            .collect::<Vec<_>>();

        let mut highs = vec![101.0; 15];
        let mut lows = vec![99.0; 15];
        let mut closes = vec![100.5; 15];
        let mut deltas = vec![0.0; 15];
        let mut rdeltas = vec![0.0; 15];
        let spot_cvd = vec![0.0; 15];
        let spot_whale_notional = vec![0.0; 15];

        lows[3] = 99.0;
        highs[8] = 97.0;
        lows[8] = 96.0;
        closes[8] = 96.8;
        closes[9] = 96.9;
        closes[10] = 97.2;
        deltas[3] = -120.0;
        deltas[8] = -80.0;
        rdeltas[3] = -0.20;
        rdeltas[8] = -0.10;

        let events = detect_selling_events(
            &fut,
            &highs,
            &lows,
            &closes,
            &deltas,
            &rdeltas,
            &spot_cvd,
            &spot_whale_notional,
            &[3, 8],
            14,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "selling_exhaustion");
        assert_eq!(events[0].pivot_price, lows[8]);
        assert_eq!(
            events[0].confirm_ts,
            fut[10].ts_bucket + Duration::minutes(1)
        );
    }

    #[test]
    fn exhaustion_event_json_exposes_pivot_price() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 10, 10, 51, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 10, 11, 9, 0).unwrap();
        let event = ExhaustionEventData {
            direction: -1,
            event_type: "buying_exhaustion".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 2069.37,
            pivot_ts_1: start_ts,
            pivot_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 11, 7, 0).unwrap(),
            pivot_confirm_ts_1: Utc.with_ymd_and_hms(2026, 3, 10, 10, 54, 0).unwrap(),
            pivot_confirm_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 11, 10, 0).unwrap(),
            price_push_ticks: 337.0,
            delta_change: -1845.619,
            rdelta_change: -0.2268,
            reject_ratio: 0.7883,
            confirm_speed: 1.0,
            spot_cvd_push_post_pivot: -138.0695,
            spot_whale_push_post_pivot: 22258.808455,
            spot_continuation_risk: false,
            spot_exhaustion_confirm: true,
            score: 0.918381277425552,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": 2069.37
            }),
        };

        let (_, payload) = exhaustion_event_json("TESTUSDT", "buying_exhaustion", &event);
        assert_eq!(
            payload.get("pivot_price").and_then(|v| v.as_f64()),
            Some(2069.37)
        );
        assert_eq!(
            payload.get("delta_drop").and_then(|v| v.as_f64()),
            Some(1845.619)
        );
        assert_eq!(
            payload.get("rdelta_drop").and_then(|v| v.as_f64()),
            Some(0.2268)
        );
        assert!(payload.get("delta_change").is_none());
        assert!(payload.get("rdelta_change").is_none());
    }

    #[test]
    fn selling_exhaustion_event_json_uses_lift_field_names() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 10, 3, 38, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 10, 3, 49, 0).unwrap();
        let event = ExhaustionEventData {
            direction: 1,
            event_type: "selling_exhaustion".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 2014.52,
            pivot_ts_1: start_ts,
            pivot_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 3, 47, 0).unwrap(),
            pivot_confirm_ts_1: Utc.with_ymd_and_hms(2026, 3, 10, 3, 41, 0).unwrap(),
            pivot_confirm_ts_2: Utc.with_ymd_and_hms(2026, 3, 10, 3, 50, 0).unwrap(),
            price_push_ticks: 1471.0,
            delta_change: 248.556,
            rdelta_change: 0.2271223558,
            reject_ratio: 0.7790697674,
            confirm_speed: 1.0,
            spot_cvd_push_post_pivot: -44.1198,
            spot_whale_push_post_pivot: 283346.18833,
            spot_continuation_risk: true,
            spot_exhaustion_confirm: false,
            score: 0.7242524917,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": 2014.52,
                "delta_lift": 248.556,
                "rdelta_lift": 0.2271223558
            }),
        };

        let (_, payload) = exhaustion_event_json("TESTUSDT", "selling_exhaustion", &event);
        assert_eq!(
            payload.get("delta_lift").and_then(|v| v.as_f64()),
            Some(248.556)
        );
        assert_eq!(
            payload.get("rdelta_lift").and_then(|v| v.as_f64()),
            Some(0.2271223558)
        );
        assert!(payload.get("delta_change").is_none());
        assert!(payload.get("rdelta_change").is_none());
    }

    #[test]
    fn cached_exhaustion_all_history_matches_direct_compute() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 12, 0, 0).unwrap();
        let history_futures = (0..16)
            .map(|i| {
                let mut row = sample_minute(base + Duration::minutes(i as i64));
                row.high_price = Some(100.0 + i as f64 * 0.1);
                row.low_price = Some(99.0 + i as f64 * 0.1);
                row.close_price = Some(99.5 + i as f64 * 0.1);
                row.last_price = row.close_price;
                row.delta = i as f64;
                row.relative_delta = i as f64 / 100.0;
                row.cvd = i as f64;
                row
            })
            .collect::<Vec<_>>();
        let history_spot = (0..16)
            .map(|i| {
                let mut row = sample_minute(base + Duration::minutes(i as i64));
                row.high_price = Some(100.2 + i as f64 * 0.1);
                row.low_price = Some(99.2 + i as f64 * 0.1);
                row.close_price = Some(99.7 + i as f64 * 0.1);
                row.last_price = row.close_price;
                row.delta = (i as f64) / 2.0;
                row.relative_delta = (i as f64) / 200.0;
                row.cvd = (i as f64) * 2.0;
                row
            })
            .collect::<Vec<_>>();
        let ctx = test_ctx(base + Duration::minutes(15), history_futures, history_spot);

        let direct =
            compute_exhaustion_all_history_from_histories(&ctx.history_futures, &ctx.history_spot);
        ctx.shared_caches
            .seed_exhaustion_all_events(Arc::new(direct.clone()));
        let cached = ctx.exhaustion_all_events();
        assert_eq!(direct, *cached);

        let history_futures = ctx.history_futures.as_ref().clone();
        let history_spot = ctx.history_spot.as_ref().clone();
        let mut rebuilt_machine = ExhaustionEventStateMachine::default();
        rebuilt_machine.rebuild(&history_futures, &history_spot);
        assert_eq!(direct, rebuilt_machine.events());

        let mut streaming_machine = ExhaustionEventStateMachine::default();
        for end in 0..history_futures.len() {
            streaming_machine.sync(&history_futures[..=end], &history_spot[..=end]);
        }
        assert_eq!(direct, streaming_machine.events());
    }
}
