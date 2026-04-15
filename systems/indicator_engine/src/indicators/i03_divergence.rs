use crate::indicators::context::{
    clip01, robust_z_at, DivergenceEventRow, DivergenceSigTestMode, IndicatorComputation,
    IndicatorContext, IndicatorEventRow, IndicatorSnapshotRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::{build_divergence_event_id, build_indicator_event_id};
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use chrono::Duration;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, VecDeque};

pub struct I03Divergence;

const PIVOT_K: usize = 3;
const MIN_LEG_GAP_MINUTES: i64 = 3;
const MAX_LEG_GAP_MINUTES: i64 = 180;
const ETA_LEG: f64 = 0.5;
const DETREND_LEN_PRICE: usize = 60;
const DETREND_LEN_CVD: usize = 120;
const ROBUST_Z_LOOKBACK: usize = 120;
const ATR_LOOKBACK: usize = 60;
const EPS_CVD_Z: f64 = 0.5;
const ZP_MIN: f64 = 1.0;
const ZC_MIN: f64 = 1.0;
const BOOTSTRAP_MIN_RET_SAMPLES: usize = 32;
pub(crate) const DIVERGENCE_INCREMENTAL_LOOKBACK_MINUTES: i64 = MAX_LEG_GAP_MINUTES
    + DETREND_LEN_CVD as i64
    + ROBUST_Z_LOOKBACK as i64
    + DETREND_LEN_CVD as i64
    + ATR_LOOKBACK as i64
    + PIVOT_K as i64
    + 5;

#[derive(Debug, Clone)]
struct DivergenceCandidate {
    divergence_type: String,
    pivot_side: String,
    i1: usize,
    i2: usize,
    confirm_i1: usize,
    confirm_i2: usize,
    available_i: usize,
    price_start: f64,
    price_end: f64,
    cvd_start_fut: f64,
    cvd_end_fut: f64,
    cvd_start_spot: f64,
    cvd_end_spot: f64,
    price_diff: f64,
    cvd_diff_fut: f64,
    cvd_diff_spot: f64,
    price_effect_z: f64,
    cvd_effect_z: f64,
    sig_pass: bool,
    p_value_price: f64,
    p_value_cvd: f64,
    score: f64,
    spot_price_flow_confirm: bool,
    fut_divergence_sign: i16,
    spot_lead_score: f64,
    likely_driver: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DivergenceEventData {
    pub divergence_type: String,
    pub pivot_side: String,
    pub event_start_ts: chrono::DateTime<chrono::Utc>,
    pub event_end_ts: chrono::DateTime<chrono::Utc>,
    pub event_available_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_ts_2: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_1: chrono::DateTime<chrono::Utc>,
    pub pivot_confirm_ts_2: chrono::DateTime<chrono::Utc>,
    pub leg_minutes: i64,
    pub price_start: f64,
    pub price_end: f64,
    pub cvd_start_fut: f64,
    pub cvd_end_fut: f64,
    pub cvd_start_spot: f64,
    pub cvd_end_spot: f64,
    pub price_diff: f64,
    pub cvd_diff_fut: f64,
    pub cvd_diff_spot: f64,
    pub price_effect_z: f64,
    pub cvd_effect_z: f64,
    pub sig_pass: bool,
    pub p_value_price: f64,
    pub p_value_cvd: f64,
    pub score: f64,
    pub spot_price_flow_confirm: bool,
    pub fut_divergence_sign: i16,
    pub spot_lead_score: f64,
    pub likely_driver: String,
}

#[derive(Debug, Clone)]
struct DivergenceDerivedMinute {
    seq: usize,
    ts_bucket: chrono::DateTime<chrono::Utc>,
    close: f64,
    high: f64,
    low: f64,
    cvd_fut: f64,
    cvd_spot: f64,
    detrended_price: Option<f64>,
    z_cvd_fut: Option<f64>,
    z_cvd_spot: Option<f64>,
    atr: Option<f64>,
}

#[derive(Debug, Clone)]
struct DivergencePivot {
    seq: usize,
}

#[derive(Debug, Clone)]
struct DivergenceCachedEvent {
    required_start_ts: chrono::DateTime<chrono::Utc>,
    event: DivergenceEventData,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DivergenceEventStateMachine {
    minutes: VecDeque<DivergenceDerivedMinute>,
    close_tail: VecDeque<f64>,
    cvd_fut_tail: VecDeque<f64>,
    cvd_spot_tail: VecDeque<f64>,
    detrended_cvd_fut_tail: VecDeque<f64>,
    detrended_cvd_spot_tail: VecDeque<f64>,
    tr_tail: VecDeque<f64>,
    tr_sum: f64,
    events: VecDeque<DivergenceCachedEvent>,
    last_high_pivot: Option<DivergencePivot>,
    last_low_pivot: Option<DivergencePivot>,
    last_ts: Option<chrono::DateTime<chrono::Utc>>,
    next_seq: usize,
}

impl DivergenceEventStateMachine {
    pub(crate) fn rebuild(
        &mut self,
        history_futures: &[crate::runtime::state_store::MinuteHistory],
        history_spot: &[crate::runtime::state_store::MinuteHistory],
        sig_test_mode: DivergenceSigTestMode,
        bootstrap_b: usize,
        bootstrap_block_len: usize,
        p_value_threshold: f64,
    ) {
        *self = Self::default();
        let (history_futures, history_spot) = aligned_event_histories(history_futures, history_spot);
        for (fut, spot) in history_futures.iter().zip(history_spot.iter()) {
            self.append_pair(
                fut,
                spot,
                sig_test_mode,
                bootstrap_b,
                bootstrap_block_len,
                p_value_threshold,
            );
        }
        self.last_ts = history_futures.last().map(|row| row.ts_bucket);
    }

    pub(crate) fn sync(
        &mut self,
        history_futures: &[crate::runtime::state_store::MinuteHistory],
        history_spot: &[crate::runtime::state_store::MinuteHistory],
        sig_test_mode: DivergenceSigTestMode,
        bootstrap_b: usize,
        bootstrap_block_len: usize,
        p_value_threshold: f64,
    ) {
        let (history_futures, history_spot) = aligned_event_histories(history_futures, history_spot);
        let Some(first_ts) = history_futures.first().map(|row| row.ts_bucket) else {
            *self = Self::default();
            return;
        };
        let last_ts = history_futures.last().map(|row| row.ts_bucket).unwrap_or(first_ts);
        let front_pruned = self
            .minutes
            .front()
            .map(|row| row.ts_bucket < first_ts)
            .unwrap_or(false);
        if front_pruned && sig_test_mode == DivergenceSigTestMode::BlockBootstrap {
            self.rebuild(
                history_futures,
                history_spot,
                sig_test_mode,
                bootstrap_b,
                bootstrap_block_len,
                p_value_threshold,
            );
            return;
        }
        match self.last_ts {
            None => {
                self.rebuild(
                    history_futures,
                    history_spot,
                    sig_test_mode,
                    bootstrap_b,
                    bootstrap_block_len,
                    p_value_threshold,
                );
                return;
            }
            Some(prev_last_ts) if prev_last_ts >= last_ts => {
                self.rebuild(
                    history_futures,
                    history_spot,
                    sig_test_mode,
                    bootstrap_b,
                    bootstrap_block_len,
                    p_value_threshold,
                );
                return;
            }
            Some(_) => {}
        }
        self.prune_before(first_ts);
        let start_idx = lower_bound_history_ts(history_futures, self.last_ts.unwrap() + Duration::minutes(1));
        if start_idx == 0 && self.minutes.is_empty() {
            self.rebuild(
                history_futures,
                history_spot,
                sig_test_mode,
                bootstrap_b,
                bootstrap_block_len,
                p_value_threshold,
            );
            return;
        }
        for (fut, spot) in history_futures[start_idx..].iter().zip(history_spot[start_idx..].iter()) {
            self.append_pair(
                fut,
                spot,
                sig_test_mode,
                bootstrap_b,
                bootstrap_block_len,
                p_value_threshold,
            );
        }
        self.last_ts = Some(last_ts);
    }

    pub(crate) fn events(&self) -> Vec<DivergenceEventData> {
        self.events.iter().map(|entry| entry.event.clone()).collect()
    }

    fn prune_before(&mut self, first_ts: chrono::DateTime<chrono::Utc>) {
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
            .map(|entry| entry.required_start_ts < first_ts || entry.event.event_start_ts < first_ts)
            .unwrap_or(false)
        {
            self.events.pop_front();
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

    fn append_pair(
        &mut self,
        fut: &crate::runtime::state_store::MinuteHistory,
        spot: &crate::runtime::state_store::MinuteHistory,
        sig_test_mode: DivergenceSigTestMode,
        bootstrap_b: usize,
        bootstrap_block_len: usize,
        p_value_threshold: f64,
    ) {
        let close = fut.close_price.or(fut.last_price).unwrap_or_default();
        let high = fut.high_price.or(fut.last_price).unwrap_or_default();
        let low = fut.low_price.or(fut.last_price).unwrap_or_default();
        let cvd_fut = fut.cvd;
        let cvd_spot = spot.cvd;

        self.close_tail.push_back(close);
        if self.close_tail.len() > DETREND_LEN_PRICE {
            self.close_tail.pop_front();
        }
        self.cvd_fut_tail.push_back(cvd_fut);
        if self.cvd_fut_tail.len() > DETREND_LEN_CVD {
            self.cvd_fut_tail.pop_front();
        }
        self.cvd_spot_tail.push_back(cvd_spot);
        if self.cvd_spot_tail.len() > DETREND_LEN_CVD {
            self.cvd_spot_tail.pop_front();
        }

        let detrended_price = detrend_current(&self.close_tail, DETREND_LEN_PRICE);
        let detrended_cvd_fut = detrend_current(&self.cvd_fut_tail, DETREND_LEN_CVD);
        let detrended_cvd_spot = detrend_current(&self.cvd_spot_tail, DETREND_LEN_CVD);

        self.detrended_cvd_fut_tail
            .push_back(detrended_cvd_fut.unwrap_or(0.0));
        if self.detrended_cvd_fut_tail.len() > ROBUST_Z_LOOKBACK {
            self.detrended_cvd_fut_tail.pop_front();
        }
        self.detrended_cvd_spot_tail
            .push_back(detrended_cvd_spot.unwrap_or(0.0));
        if self.detrended_cvd_spot_tail.len() > ROBUST_Z_LOOKBACK {
            self.detrended_cvd_spot_tail.pop_front();
        }

        let z_cvd_fut = robust_z_current(&self.detrended_cvd_fut_tail, ROBUST_Z_LOOKBACK);
        let z_cvd_spot = robust_z_current(&self.detrended_cvd_spot_tail, ROBUST_Z_LOOKBACK);

        let prev_close = self
            .minutes
            .back()
            .map(|row| row.close)
            .unwrap_or(close);
        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        self.tr_tail.push_back(tr);
        self.tr_sum += tr;
        if self.tr_tail.len() > ATR_LOOKBACK {
            if let Some(removed) = self.tr_tail.pop_front() {
                self.tr_sum -= removed;
            }
        }
        let atr = (self.tr_tail.len() == ATR_LOOKBACK).then(|| self.tr_sum / ATR_LOOKBACK as f64);

        let seq = self.next_seq;
        self.minutes.push_back(DivergenceDerivedMinute {
            seq,
            ts_bucket: fut.ts_bucket,
            close,
            high,
            low,
            cvd_fut,
            cvd_spot,
            detrended_price,
            z_cvd_fut,
            z_cvd_spot,
            atr,
        });
        self.maybe_confirm_pivot(
            seq,
            true,
            sig_test_mode,
            bootstrap_b,
            bootstrap_block_len,
            p_value_threshold,
        );
        self.maybe_confirm_pivot(
            seq,
            false,
            sig_test_mode,
            bootstrap_b,
            bootstrap_block_len,
            p_value_threshold,
        );
        self.next_seq += 1;
    }

    fn maybe_confirm_pivot(
        &mut self,
        current_seq: usize,
        is_high: bool,
        sig_test_mode: DivergenceSigTestMode,
        bootstrap_b: usize,
        bootstrap_block_len: usize,
        p_value_threshold: f64,
    ) {
        if current_seq < PIVOT_K {
            return;
        }
        let pivot_seq = current_seq - PIVOT_K;
        let Some(pivot_idx) = self.offset_of(pivot_seq) else {
            return;
        };
        if pivot_idx < PIVOT_K || pivot_idx + PIVOT_K >= self.minutes.len() {
            return;
        }
        let is_pivot = if is_high {
            let pivot_high = self.minutes[pivot_idx].high;
            let left_max = self
                .minutes
                .iter()
                .skip(pivot_idx - PIVOT_K)
                .take(PIVOT_K)
                .map(|row| row.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let right_max = self
                .minutes
                .iter()
                .skip(pivot_idx + 1)
                .take(PIVOT_K)
                .map(|row| row.high)
                .fold(f64::NEG_INFINITY, f64::max);
            pivot_high > left_max && pivot_high >= right_max
        } else {
            let pivot_low = self.minutes[pivot_idx].low;
            let left_min = self
                .minutes
                .iter()
                .skip(pivot_idx - PIVOT_K)
                .take(PIVOT_K)
                .map(|row| row.low)
                .fold(f64::INFINITY, f64::min);
            let right_min = self
                .minutes
                .iter()
                .skip(pivot_idx + 1)
                .take(PIVOT_K)
                .map(|row| row.low)
                .fold(f64::INFINITY, f64::min);
            pivot_low < left_min && pivot_low <= right_min
        };
        if !is_pivot {
            return;
        }
        let current_pivot = DivergencePivot { seq: pivot_seq };
        if is_high {
            if let Some(prev) = self.last_high_pivot.clone() {
                if let Some(event) = self.build_candidate_event(
                    &prev,
                    &current_pivot,
                    true,
                    sig_test_mode,
                    bootstrap_b,
                    bootstrap_block_len,
                    p_value_threshold,
                ) {
                    self.events.push_back(event);
                }
            }
            self.last_high_pivot = Some(current_pivot);
        } else {
            if let Some(prev) = self.last_low_pivot.clone() {
                if let Some(event) = self.build_candidate_event(
                    &prev,
                    &current_pivot,
                    false,
                    sig_test_mode,
                    bootstrap_b,
                    bootstrap_block_len,
                    p_value_threshold,
                ) {
                    self.events.push_back(event);
                }
            }
            self.last_low_pivot = Some(current_pivot);
        }
    }

    fn build_candidate_event(
        &self,
        pivot_1: &DivergencePivot,
        pivot_2: &DivergencePivot,
        is_high_side: bool,
        sig_test_mode: DivergenceSigTestMode,
        bootstrap_b: usize,
        bootstrap_block_len: usize,
        p_value_threshold: f64,
    ) -> Option<DivergenceCachedEvent> {
        let idx_1 = self.offset_of(pivot_1.seq)?;
        let idx_2 = self.offset_of(pivot_2.seq)?;
        let first = self.minutes.get(idx_1)?;
        let second = self.minutes.get(idx_2)?;
        let leg = (second.ts_bucket - first.ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg) {
            return None;
        }
        let atr_v = second.atr.unwrap_or(0.0);
        if atr_v <= 1e-12 {
            return None;
        }
        let leg_eff = (second.close - first.close).abs() / (atr_v + 1e-12);
        if leg_eff < ETA_LEG {
            return None;
        }
        let (Some(zc1), Some(zc2), Some(zs1), Some(zs2)) =
            (first.z_cvd_fut, second.z_cvd_fut, first.z_cvd_spot, second.z_cvd_spot)
        else {
            return None;
        };
        let (price_start, price_end, price_diff) = if is_high_side {
            (first.high, second.high, second.high - first.high)
        } else {
            (first.low, second.low, second.low - first.low)
        };
        let cvd_diff_fut = zc2 - zc1;
        let cvd_diff_spot = zs2 - zs1;
        let eps_price = 0.5 * atr_v;
        let price_effect_z = price_diff / (atr_v + 1e-12);
        let cvd_effect_z = cvd_diff_fut;
        let divergence_type = if is_high_side {
            if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
                Some("bearish")
            } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
                Some("hidden_bearish")
            } else {
                None
            }
        } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
            Some("bullish")
        } else if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
            Some("hidden_bullish")
        } else {
            None
        }?;

        let mut p_value_price = if price_effect_z.abs() >= ZP_MIN { 0.0 } else { 1.0 };
        let mut p_value_cvd = if cvd_effect_z.abs() >= ZC_MIN { 0.0 } else { 1.0 };
        let mut sig_pass = price_effect_z.abs() >= ZP_MIN && cvd_effect_z.abs() >= ZC_MIN;

        if sig_test_mode == DivergenceSigTestMode::BlockBootstrap {
            let leg_len = (idx_2 - idx_1 + 1).max(2);
            let price_returns = build_returns_from_option_series(
                &self
                    .minutes
                    .iter()
                    .take(idx_2 + 1)
                    .map(|row| row.detrended_price)
                    .collect::<Vec<_>>(),
                idx_2,
            );
            let cvd_returns = build_returns_from_option_series(
                &self
                    .minutes
                    .iter()
                    .take(idx_2 + 1)
                    .map(|row| row.z_cvd_fut)
                    .collect::<Vec<_>>(),
                idx_2,
            );

            if price_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &price_returns,
                    leg_len,
                    price_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((idx_1 as u64) << 32) ^ (idx_2 as u64) ^ 0xA5A5_5A5A_u64,
                ) {
                    p_value_price = p;
                }
            }
            if cvd_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &cvd_returns,
                    leg_len,
                    cvd_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((idx_1 as u64) << 32) ^ (idx_2 as u64) ^ 0x5AA5_A55A_u64,
                ) {
                    p_value_cvd = p;
                }
            }
            sig_pass = p_value_price <= p_value_threshold && p_value_cvd <= p_value_threshold;
        }
        if !sig_pass {
            return None;
        }

        let delta_p_sign = price_diff.signum() as i16;
        let spot_flow_confirm = delta_p_sign == (cvd_diff_spot.signum() as i16);
        let fut_div_sign = (price_diff.signum() * cvd_diff_fut.signum()) as i16;
        let spot_lead_score = if (cvd_diff_spot.abs() + cvd_diff_fut.abs()) > 1e-12
            && (price_diff.signum() == cvd_diff_spot.signum())
            && (price_diff.signum() != cvd_diff_fut.signum())
        {
            cvd_diff_spot.abs() / (cvd_diff_spot.abs() + cvd_diff_fut.abs() + 1e-12)
        } else {
            0.0
        };
        let likely_driver = if spot_lead_score >= 0.6 {
            "spot_led"
        } else if spot_lead_score < 0.3 && fut_div_sign != -1 {
            "futures_led"
        } else {
            "mixed"
        };
        let score =
            clip01(0.5 * (price_effect_z.abs() / 3.0) + 0.5 * (cvd_effect_z.abs() / 3.0));
        let event = DivergenceEventData {
            divergence_type: divergence_type.to_string(),
            pivot_side: if is_high_side {
                "high".to_string()
            } else {
                "low".to_string()
            },
            event_start_ts: first.ts_bucket,
            event_end_ts: second.ts_bucket + Duration::minutes(1),
            event_available_ts: second.ts_bucket + Duration::minutes(PIVOT_K as i64 + 1),
            pivot_ts_1: first.ts_bucket,
            pivot_ts_2: second.ts_bucket,
            pivot_confirm_ts_1: first.ts_bucket + Duration::minutes(PIVOT_K as i64),
            pivot_confirm_ts_2: second.ts_bucket + Duration::minutes(PIVOT_K as i64),
            leg_minutes: leg,
            price_start,
            price_end,
            cvd_start_fut: first.cvd_fut,
            cvd_end_fut: second.cvd_fut,
            cvd_start_spot: first.cvd_spot,
            cvd_end_spot: second.cvd_spot,
            price_diff,
            cvd_diff_fut,
            cvd_diff_spot,
            price_effect_z,
            cvd_effect_z,
            sig_pass,
            p_value_price,
            p_value_cvd,
            score,
            spot_price_flow_confirm: spot_flow_confirm,
            fut_divergence_sign: fut_div_sign,
            spot_lead_score,
            likely_driver: likely_driver.to_string(),
        };
        let earliest_required_minutes =
            (DETREND_LEN_CVD + ROBUST_Z_LOOKBACK).saturating_sub(2) as i64;
        Some(DivergenceCachedEvent {
            required_start_ts: (second.ts_bucket - Duration::minutes(earliest_required_minutes))
                .min(first.ts_bucket - Duration::minutes(PIVOT_K as i64)),
            event,
        })
    }

    fn offset_of(&self, seq: usize) -> Option<usize> {
        let first_seq = self.minutes.front()?.seq;
        let idx = seq.checked_sub(first_seq)?;
        (idx < self.minutes.len()).then_some(idx)
    }
}

fn aligned_event_histories<'a>(
    history_futures: &'a [crate::runtime::state_store::MinuteHistory],
    history_spot: &'a [crate::runtime::state_store::MinuteHistory],
) -> (
    &'a [crate::runtime::state_store::MinuteHistory],
    &'a [crate::runtime::state_store::MinuteHistory],
) {
    let n = history_futures.len().min(history_spot.len());
    (
        &history_futures[history_futures.len().saturating_sub(n)..],
        &history_spot[history_spot.len().saturating_sub(n)..],
    )
}

fn lower_bound_history_ts(
    history: &[crate::runtime::state_store::MinuteHistory],
    target: chrono::DateTime<chrono::Utc>,
) -> usize {
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

fn detrend_current(values: &VecDeque<f64>, window: usize) -> Option<f64> {
    if values.len() < window {
        return None;
    }
    let slice = values.iter().skip(values.len() - window).copied().collect::<Vec<_>>();
    let n = slice.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = slice.iter().sum::<f64>() / n;
    let mut num = 0.0;
    let mut den = 0.0;
    for (idx, y) in slice.iter().enumerate() {
        let x = idx as f64;
        num += (x - x_mean) * (y - y_mean);
        den += (x - x_mean) * (x - x_mean);
    }
    let slope = if den > 1e-12 { num / den } else { 0.0 };
    let intercept = y_mean - slope * x_mean;
    let pred = intercept + slope * (window as f64 - 1.0);
    Some(slice[window - 1] - pred)
}

fn robust_z_current(values: &VecDeque<f64>, lookback: usize) -> Option<f64> {
    if values.len() < lookback {
        return None;
    }
    let raw = values.iter().skip(values.len() - lookback).copied().collect::<Vec<_>>();
    robust_z_at(&raw, raw.len() - 1, lookback)
}

fn candidate_to_event_data(
    candidate: &DivergenceCandidate,
    fut: &[crate::runtime::state_store::MinuteHistory],
) -> DivergenceEventData {
    DivergenceEventData {
        divergence_type: candidate.divergence_type.clone(),
        pivot_side: candidate.pivot_side.clone(),
        event_start_ts: fut[candidate.i1].ts_bucket,
        event_end_ts: fut[candidate.i2].ts_bucket + Duration::minutes(1),
        event_available_ts: fut[candidate.available_i].ts_bucket + Duration::minutes(1),
        pivot_ts_1: fut[candidate.i1].ts_bucket,
        pivot_ts_2: fut[candidate.i2].ts_bucket,
        pivot_confirm_ts_1: fut[candidate.confirm_i1].ts_bucket,
        pivot_confirm_ts_2: fut[candidate.confirm_i2].ts_bucket,
        leg_minutes: (fut[candidate.i2].ts_bucket - fut[candidate.i1].ts_bucket).num_minutes(),
        price_start: candidate.price_start,
        price_end: candidate.price_end,
        cvd_start_fut: candidate.cvd_start_fut,
        cvd_end_fut: candidate.cvd_end_fut,
        cvd_start_spot: candidate.cvd_start_spot,
        cvd_end_spot: candidate.cvd_end_spot,
        price_diff: candidate.price_diff,
        cvd_diff_fut: candidate.cvd_diff_fut,
        cvd_diff_spot: candidate.cvd_diff_spot,
        price_effect_z: candidate.price_effect_z,
        cvd_effect_z: candidate.cvd_effect_z,
        sig_pass: candidate.sig_pass,
        p_value_price: candidate.p_value_price,
        p_value_cvd: candidate.p_value_cvd,
        score: candidate.score,
        spot_price_flow_confirm: candidate.spot_price_flow_confirm,
        fut_divergence_sign: candidate.fut_divergence_sign,
        spot_lead_score: candidate.spot_lead_score,
        likely_driver: candidate.likely_driver.clone(),
    }
}

impl Indicator for I03Divergence {
    fn code(&self) -> &'static str {
        "divergence"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = ctx.divergence_all_events();
        let n = ctx.history_futures.len().min(ctx.history_spot.len());
        if n < (PIVOT_K * 4).max(ROBUST_Z_LOOKBACK + 5) {
            return IndicatorComputation {
                snapshot: Some(IndicatorSnapshotRow {
                    indicator_code: self.code(),
                    window_code: "1m",
                    payload_json: empty_snapshot_payload(ctx, "insufficient_history"),
                }),
                ..Default::default()
            };
        }

        let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
        let current_candidates = all_events
            .iter()
            .filter(|c| c.event_available_ts == current_available_ts)
            .cloned()
            .collect::<Vec<_>>();
        let latest_current = current_candidates
            .iter()
            .max_by_key(|c| c.event_available_ts)
            .cloned();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|candidate| divergence_event_json(&ctx.symbol, candidate))
                .collect(),
        );
        let lookback_covered_minutes = ctx.history_futures.len().min(ctx.history_spot.len()) as i64;
        let history_index = ctx
            .history_futures
            .iter()
            .enumerate()
            .map(|(idx, row)| (row.ts_bucket, idx))
            .collect::<HashMap<_, _>>();
        let candidates_json = all_events
            .iter()
            .map(|candidate| candidate_payload(candidate, &history_index))
            .collect::<Vec<_>>();
        let latest_payload = latest_current
            .as_ref()
            .map(|candidate| candidate_payload(candidate, &history_index));
        let signal = latest_current
            .as_ref()
            .map(|c| c.event_available_ts == current_available_ts)
            .unwrap_or(false);

        let mut out = IndicatorComputation {
            snapshot: Some(IndicatorSnapshotRow {
                indicator_code: self.code(),
                window_code: "1m",
                payload_json: Value::Null,
            }),
            ..Default::default()
        };

        if let Some(snapshot) = out.snapshot.as_mut() {
            snapshot.payload_json = if let Some(latest_ref) = latest_current.as_ref() {
                json!({
                    "signal": signal,
                    "reason": if signal { Value::Null } else { json!("candidate_not_yet_available") },
                    "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
                    "bootstrap_b": ctx.divergence_bootstrap_b,
                    "bootstrap_block_len": ctx.divergence_bootstrap_block_len,
                    "p_value_threshold": ctx.divergence_p_value_threshold,
                    "divergence_type": divergence_label(&latest_ref.divergence_type),
                    "pivot_side": latest_ref.pivot_side.clone(),
                    "signals": signal_flags(latest_ref),
                    "fut_divergence_sign": latest_ref.fut_divergence_sign,
                    "spot_price_flow_confirm": latest_ref.spot_price_flow_confirm,
                    "spot_lead_score": latest_ref.spot_lead_score,
                    "likely_driver": latest_ref.likely_driver.clone(),
                    "latest": latest_payload,
                    "candidates": candidates_json,
                    "event_count": window_view.current_events.len(),
                    "events": window_view.current_events,
                    "recent_7d": build_recent_7d_payload(
                        window_view.recent_events,
                        lookback_covered_minutes,
                        "in_memory_minute_history"
                    ),
                    "latest_7d": window_view.latest_recent
                })
            } else {
                let mut payload = empty_snapshot_payload(ctx, "no_candidate");
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("candidates".to_string(), json!(candidates_json));
                    obj.insert(
                        "event_count".to_string(),
                        json!(window_view.current_events.len()),
                    );
                    obj.insert("events".to_string(), json!(window_view.current_events));
                    obj.insert(
                        "recent_7d".to_string(),
                        build_recent_7d_payload(
                            window_view.recent_events,
                            lookback_covered_minutes,
                            "in_memory_minute_history",
                        ),
                    );
                    obj.insert("latest_7d".to_string(), json!(window_view.latest_recent));
                }
                payload
            };
        }

        for event in all_events.iter() {
            let direction = if event.divergence_type.contains("bearish") {
                -1
            } else {
                1
            };
            let payload = json!({
                "event_start_ts": event.event_start_ts.to_rfc3339(),
                "event_end_ts": event.event_end_ts.to_rfc3339(),
                "event_available_ts": event.event_available_ts.to_rfc3339(),
                "pivot_ts_1": event.pivot_ts_1.to_rfc3339(),
                "pivot_ts_2": event.pivot_ts_2.to_rfc3339(),
                "pivot_confirm_ts_1": event.pivot_confirm_ts_1.to_rfc3339(),
                "pivot_confirm_ts_2": event.pivot_confirm_ts_2.to_rfc3339(),
                "leg_minutes": event.leg_minutes,
                "price_diff": event.price_diff,
                "price_norm_diff": event.price_diff,
                "cvd_diff_fut": event.cvd_diff_fut,
                "cvd_norm_diff_fut": event.cvd_diff_fut,
                "cvd_diff_spot": event.cvd_diff_spot,
                "cvd_norm_diff_spot": event.cvd_diff_spot,
                "price_effect_z": event.price_effect_z,
                "cvd_effect_z": event.cvd_effect_z,
                "sig_pass": event.sig_pass,
                "p_value_price": event.p_value_price,
                "p_value_cvd": event.p_value_cvd,
                "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
                "spot_price_flow_confirm": event.spot_price_flow_confirm,
                "fut_divergence_sign": event.fut_divergence_sign,
                "spot_lead_score": event.spot_lead_score,
                "likely_driver": event.likely_driver.clone()
            });
            let event_id = build_indicator_event_id(
                &ctx.symbol,
                self.code(),
                &format!("{}_divergence", event.divergence_type),
                event.event_start_ts,
                Some(event.event_end_ts),
                direction,
                Some(event.pivot_ts_1),
                Some(event.pivot_ts_2),
            );
            let divergence_event_id = build_divergence_event_id(
                &ctx.symbol,
                &event.divergence_type,
                &event.pivot_side,
                event.event_start_ts,
                event.event_end_ts,
                Some(event.pivot_ts_1),
                Some(event.pivot_ts_2),
            );
            let payload = divergence_payload_json(&ctx.symbol, event, payload);

            out.event_rows.push(IndicatorEventRow::new(
                event_id,
                self.code(),
                format!("{}_divergence", event.divergence_type),
                "warn".to_string(),
                direction,
                event.event_start_ts,
                Some(event.event_end_ts),
                event.event_available_ts,
                "1m",
                Some(event.pivot_ts_1),
                Some(event.pivot_ts_2),
                Some(event.pivot_confirm_ts_1),
                Some(event.pivot_confirm_ts_2),
                Some(event.sig_pass),
                Some(event.p_value_price.max(event.p_value_cvd)),
                Some(event.score),
                Some(event.score),
                payload.clone(),
            ));

            out.divergence_rows.push(DivergenceEventRow::new(
                divergence_event_id,
                event.divergence_type.clone(),
                event.pivot_side.clone(),
                Some(event.event_available_ts),
                Some(event.pivot_ts_1),
                Some(event.pivot_ts_2),
                Some(event.pivot_confirm_ts_1),
                Some(event.pivot_confirm_ts_2),
                Some(event.leg_minutes as i32),
                event.event_start_ts,
                event.event_end_ts,
                Some(event.price_start),
                Some(event.price_end),
                Some(event.price_diff),
                Some(event.cvd_start_fut),
                Some(event.cvd_end_fut),
                Some(event.cvd_diff_fut),
                Some(event.cvd_start_spot),
                Some(event.cvd_end_spot),
                Some(event.cvd_diff_spot),
                Some(event.price_effect_z),
                Some(event.cvd_effect_z),
                Some(event.sig_pass),
                Some(event.p_value_price),
                Some(event.p_value_cvd),
                Some(event.spot_price_flow_confirm),
                Some(event.fut_divergence_sign),
                Some(event.spot_lead_score),
                Some(event.likely_driver.clone()),
                Some(event.score),
                Some(event.score),
                "1m",
                payload,
            ));
        }

        out
    }
}

fn divergence_label(kind: &str) -> &'static str {
    match kind {
        "bearish" => "bearish_divergence",
        "hidden_bearish" => "hidden_bearish_divergence",
        "bullish" => "bullish_divergence",
        "hidden_bullish" => "hidden_bullish_divergence",
        _ => "unknown",
    }
}

#[cfg(test)]
pub(crate) fn compute_divergence_all_history(
    history_futures: &[crate::runtime::state_store::MinuteHistory],
    history_spot: &[crate::runtime::state_store::MinuteHistory],
    sig_test_mode: DivergenceSigTestMode,
    bootstrap_b: usize,
    bootstrap_block_len: usize,
    p_value_threshold: f64,
) -> Vec<DivergenceEventData> {
    let series = crate::indicators::context::BasicEventHistorySeries::from_histories(
        history_futures,
        history_spot,
    );
    let n = series.n;
    if n < (PIVOT_K * 4).max(ROBUST_Z_LOOKBACK + 5) {
        return Vec::new();
    }

    let fut = &history_futures[history_futures.len().saturating_sub(n)..];
    let spot = &history_spot[history_spot.len().saturating_sub(n)..];
    let closes = &series.close;
    let highs = &series.high;
    let lows = &series.low;
    let cvd_fut = fut.iter().map(|row| row.cvd).collect::<Vec<_>>();
    let cvd_spot = &series.spot_cvd;

    let detrended_price = detrend_rolling_ols(closes, DETREND_LEN_PRICE);
    let detrended_cvd_fut = detrend_rolling_ols(&cvd_fut, DETREND_LEN_CVD);
    let detrended_cvd_spot = detrend_rolling_ols(cvd_spot, DETREND_LEN_CVD);
    let z_cvd_fut = robust_z_series(&detrended_cvd_fut, ROBUST_Z_LOOKBACK);
    let z_cvd_spot = robust_z_series(&detrended_cvd_spot, ROBUST_Z_LOOKBACK);
    let high_pivots = confirmed_high_pivots(highs, PIVOT_K, n - 1);
    let low_pivots = confirmed_low_pivots(lows, PIVOT_K, n - 1);
    let atr = rolling_atr(highs, lows, closes, ATR_LOOKBACK);

    let mut candidates = Vec::new();
    candidates.extend(all_candidates(
        true,
        &high_pivots,
        fut,
        spot,
        closes,
        highs,
        lows,
        &detrended_price,
        &z_cvd_fut,
        &z_cvd_spot,
        &atr,
        sig_test_mode,
        bootstrap_b,
        bootstrap_block_len,
        p_value_threshold,
    ));
    candidates.extend(all_candidates(
        false,
        &low_pivots,
        fut,
        spot,
        closes,
        highs,
        lows,
        &detrended_price,
        &z_cvd_fut,
        &z_cvd_spot,
        &atr,
        sig_test_mode,
        bootstrap_b,
        bootstrap_block_len,
        p_value_threshold,
    ));
    candidates.sort_by_key(|candidate| (candidate.available_i, candidate.i1, candidate.i2));

    candidates
        .iter()
        .map(|candidate| candidate_to_event_data(candidate, fut))
        .collect()
}

fn signal_flags(candidate: &DivergenceEventData) -> serde_json::Value {
    let label = divergence_label(&candidate.divergence_type);
    json!({
        "bearish_divergence": label == "bearish_divergence",
        "hidden_bearish_divergence": label == "hidden_bearish_divergence",
        "bullish_divergence": label == "bullish_divergence",
        "hidden_bullish_divergence": label == "hidden_bullish_divergence"
    })
}

fn candidate_index_json(
    history_index: &HashMap<chrono::DateTime<chrono::Utc>, usize>,
    ts: chrono::DateTime<chrono::Utc>,
) -> Value {
    history_index
        .get(&ts)
        .map(|idx| json!(idx))
        .unwrap_or(Value::Null)
}

fn candidate_payload(
    candidate: &DivergenceEventData,
    history_index: &HashMap<chrono::DateTime<chrono::Utc>, usize>,
) -> serde_json::Value {
    json!({
        "type": divergence_label(&candidate.divergence_type),
        "pivot_side": candidate.pivot_side.clone(),
        "i1": candidate_index_json(history_index, candidate.pivot_ts_1),
        "i2": candidate_index_json(history_index, candidate.pivot_ts_2),
        "confirm_i1": candidate_index_json(history_index, candidate.pivot_confirm_ts_1),
        "confirm_i2": candidate_index_json(history_index, candidate.pivot_confirm_ts_2),
        "available_i": candidate_index_json(
            history_index,
            candidate.event_available_ts - Duration::minutes(1)
        ),
        "event_start_ts": candidate.event_start_ts.to_rfc3339(),
        "event_end_ts": candidate.event_end_ts.to_rfc3339(),
        "event_available_ts": candidate.event_available_ts.to_rfc3339(),
        "pivot_ts_1": candidate.pivot_ts_1.to_rfc3339(),
        "pivot_ts_2": candidate.pivot_ts_2.to_rfc3339(),
        "pivot_confirm_ts_1": candidate.pivot_confirm_ts_1.to_rfc3339(),
        "pivot_confirm_ts_2": candidate.pivot_confirm_ts_2.to_rfc3339(),
        "leg_minutes": candidate.leg_minutes,
        "price_start": candidate.price_start,
        "price_end": candidate.price_end,
        "cvd_start_fut": candidate.cvd_start_fut,
        "cvd_end_fut": candidate.cvd_end_fut,
        "cvd_start_spot": candidate.cvd_start_spot,
        "cvd_end_spot": candidate.cvd_end_spot,
        "price_diff": candidate.price_diff,
        "cvd_diff_fut": candidate.cvd_diff_fut,
        "cvd_diff_spot": candidate.cvd_diff_spot,
        "price_effect_z": candidate.price_effect_z,
        "cvd_effect_z": candidate.cvd_effect_z,
        "sig_pass": candidate.sig_pass,
        "p_value_price": candidate.p_value_price,
        "p_value_cvd": candidate.p_value_cvd,
        "score": candidate.score,
        "spot_price_flow_confirm": candidate.spot_price_flow_confirm,
        "fut_divergence_sign": candidate.fut_divergence_sign,
        "spot_lead_score": candidate.spot_lead_score,
        "likely_driver": candidate.likely_driver.clone(),
        "signals": signal_flags(candidate)
    })
}

fn divergence_payload_json(symbol: &str, candidate: &DivergenceEventData, payload: Value) -> Value {
    let event_id = build_divergence_event_id(
        symbol,
        &candidate.divergence_type,
        &candidate.pivot_side,
        candidate.event_start_ts,
        candidate.event_end_ts,
        Some(candidate.pivot_ts_1),
        Some(candidate.pivot_ts_2),
    );
    let mut base = Map::new();
    base.insert("event_id".to_string(), json!(event_id));
    base.insert(
        "type".to_string(),
        json!(format!("{}_divergence", candidate.divergence_type)),
    );
    base.insert("pivot_side".to_string(), json!(candidate.pivot_side));
    base.insert(
        "start_ts".to_string(),
        json!(candidate.event_start_ts.to_rfc3339()),
    );
    base.insert(
        "end_ts".to_string(),
        json!(candidate.event_end_ts.to_rfc3339()),
    );
    base.insert(
        "event_available_ts".to_string(),
        json!(candidate.event_available_ts.to_rfc3339()),
    );
    base.insert("score".to_string(), json!(candidate.score));
    merge_payload_fields(base, &payload)
}

fn divergence_event_json(
    symbol: &str,
    candidate: &DivergenceEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let start_ts = candidate.event_start_ts;
    let end_ts = candidate.event_end_ts;
    let available_ts = candidate.event_available_ts;
    let payload = json!({
        "event_start_ts": start_ts.to_rfc3339(),
        "event_end_ts": end_ts.to_rfc3339(),
        "event_available_ts": available_ts.to_rfc3339(),
        "pivot_ts_1": candidate.pivot_ts_1.to_rfc3339(),
        "pivot_ts_2": candidate.pivot_ts_2.to_rfc3339(),
        "pivot_confirm_ts_1": candidate.pivot_confirm_ts_1.to_rfc3339(),
        "pivot_confirm_ts_2": candidate.pivot_confirm_ts_2.to_rfc3339(),
        "leg_minutes": candidate.leg_minutes,
        "price_diff": candidate.price_diff,
        "price_norm_diff": candidate.price_diff,
        "cvd_diff_fut": candidate.cvd_diff_fut,
        "cvd_norm_diff_fut": candidate.cvd_diff_fut,
        "cvd_diff_spot": candidate.cvd_diff_spot,
        "cvd_norm_diff_spot": candidate.cvd_diff_spot,
        "price_effect_z": candidate.price_effect_z,
        "cvd_effect_z": candidate.cvd_effect_z,
        "sig_pass": candidate.sig_pass,
        "p_value_price": candidate.p_value_price,
        "p_value_cvd": candidate.p_value_cvd,
        "sig_test_mode": "derived",
        "spot_price_flow_confirm": candidate.spot_price_flow_confirm,
        "fut_divergence_sign": candidate.fut_divergence_sign,
        "spot_lead_score": candidate.spot_lead_score,
        "likely_driver": candidate.likely_driver.clone()
    });
    (
        available_ts,
        divergence_payload_json(symbol, candidate, payload),
    )
}

fn empty_snapshot_payload(ctx: &IndicatorContext, reason: &str) -> serde_json::Value {
    json!({
        "signal": false,
        "reason": reason,
        "sig_test_mode": ctx.divergence_sig_test_mode.as_str(),
        "bootstrap_b": ctx.divergence_bootstrap_b,
        "bootstrap_block_len": ctx.divergence_bootstrap_block_len,
        "p_value_threshold": ctx.divergence_p_value_threshold,
        "divergence_type": Value::Null,
        "pivot_side": Value::Null,
        "signals": {
            "bearish_divergence": false,
            "hidden_bearish_divergence": false,
            "bullish_divergence": false,
            "hidden_bullish_divergence": false
        },
        "fut_divergence_sign": Value::Null,
        "spot_price_flow_confirm": Value::Null,
        "spot_lead_score": Value::Null,
        "likely_driver": Value::Null,
        "latest": Value::Null,
        "candidates": []
    })
}

fn detrend_rolling_ols(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    for i in 0..values.len() {
        if i + 1 < window {
            continue;
        }
        let start = i + 1 - window;
        let slice = &values[start..=i];
        let n = slice.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = slice.iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (idx, y) in slice.iter().enumerate() {
            let x = idx as f64;
            num += (x - x_mean) * (y - y_mean);
            den += (x - x_mean) * (x - x_mean);
        }
        let slope = if den > 1e-12 { num / den } else { 0.0 };
        let intercept = y_mean - slope * x_mean;
        let pred = intercept + slope * (window as f64 - 1.0);
        out[i] = Some(values[i] - pred);
    }
    out
}

fn robust_z_series(values: &[Option<f64>], lookback: usize) -> Vec<Option<f64>> {
    let mut raw = Vec::with_capacity(values.len());
    let mut out = vec![None; values.len()];
    for (i, v) in values.iter().enumerate() {
        raw.push(v.unwrap_or(0.0));
        out[i] = robust_z_at(&raw, i, lookback);
    }
    out
}

fn confirmed_high_pivots(highs: &[f64], k: usize, last_idx: usize) -> Vec<(usize, usize)> {
    let mut pivots = Vec::new();
    if highs.len() <= k * 2 {
        return pivots;
    }
    for i in k..(highs.len() - k) {
        if i + k > last_idx {
            break;
        }
        let left_max = highs[i - k..i]
            .iter()
            .fold(f64::NEG_INFINITY, |a, b| a.max(*b));
        let right_max = highs[i + 1..=i + k]
            .iter()
            .fold(f64::NEG_INFINITY, |a, b| a.max(*b));
        if highs[i] > left_max && highs[i] >= right_max {
            pivots.push((i, i + k));
        }
    }
    pivots
}

fn confirmed_low_pivots(lows: &[f64], k: usize, last_idx: usize) -> Vec<(usize, usize)> {
    let mut pivots = Vec::new();
    if lows.len() <= k * 2 {
        return pivots;
    }
    for i in k..(lows.len() - k) {
        if i + k > last_idx {
            break;
        }
        let left_min = lows[i - k..i].iter().fold(f64::INFINITY, |a, b| a.min(*b));
        let right_min = lows[i + 1..=i + k]
            .iter()
            .fold(f64::INFINITY, |a, b| a.min(*b));
        if lows[i] < left_min && lows[i] <= right_min {
            pivots.push((i, i + k));
        }
    }
    pivots
}

fn rolling_atr(highs: &[f64], lows: &[f64], closes: &[f64], lookback: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; highs.len()];
    if highs.is_empty() || lows.len() != highs.len() || closes.len() != highs.len() {
        return out;
    }
    let mut trs = Vec::with_capacity(highs.len());
    for i in 0..highs.len() {
        let prev_close = if i > 0 { closes[i - 1] } else { closes[i] };
        let tr = (highs[i] - lows[i])
            .max((highs[i] - prev_close).abs())
            .max((lows[i] - prev_close).abs());
        trs.push(tr);
        if i + 1 >= lookback {
            let start = i + 1 - lookback;
            let m = trs[start..=i].iter().sum::<f64>() / lookback as f64;
            out[i] = Some(m);
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn all_candidates(
    is_high_side: bool,
    pivots: &[(usize, usize)],
    fut: &[crate::runtime::state_store::MinuteHistory],
    spot: &[crate::runtime::state_store::MinuteHistory],
    _closes: &[f64],
    highs: &[f64],
    lows: &[f64],
    detrended_price: &[Option<f64>],
    z_cvd_fut: &[Option<f64>],
    z_cvd_spot: &[Option<f64>],
    atr: &[Option<f64>],
    sig_test_mode: DivergenceSigTestMode,
    bootstrap_b: usize,
    bootstrap_block_len: usize,
    p_value_threshold: f64,
) -> Vec<DivergenceCandidate> {
    if pivots.len() < 2 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for pair in pivots.windows(2) {
        let (i1, c1) = pair[0];
        let (i2, c2) = pair[1];
        let leg = (fut[i2].ts_bucket - fut[i1].ts_bucket).num_minutes();
        if !(MIN_LEG_GAP_MINUTES..=MAX_LEG_GAP_MINUTES).contains(&leg) {
            continue;
        }
        let atr_v = atr[i2].unwrap_or(0.0);
        if atr_v <= 1e-12 {
            continue;
        }
        let close1 = fut[i1]
            .close_price
            .or(fut[i1].last_price)
            .unwrap_or_default();
        let close2 = fut[i2]
            .close_price
            .or(fut[i2].last_price)
            .unwrap_or_default();
        let leg_eff = (close2 - close1).abs() / (atr_v + 1e-12);
        if leg_eff < ETA_LEG {
            continue;
        }
        let (Some(zc1), Some(zc2), Some(zs1), Some(zs2)) =
            (z_cvd_fut[i1], z_cvd_fut[i2], z_cvd_spot[i1], z_cvd_spot[i2])
        else {
            continue;
        };

        let (price_start, price_end, price_diff) = if is_high_side {
            (highs[i1], highs[i2], highs[i2] - highs[i1])
        } else {
            (lows[i1], lows[i2], lows[i2] - lows[i1])
        };
        let cvd_diff_fut = zc2 - zc1;
        let cvd_diff_spot = zs2 - zs1;
        let eps_price = 0.5 * atr_v;
        let price_effect_z = price_diff / (atr_v + 1e-12);
        let cvd_effect_z = cvd_diff_fut;
        let divergence_type = if is_high_side {
            if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
                Some("bearish")
            } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
                Some("hidden_bearish")
            } else {
                None
            }
        } else if price_diff <= -eps_price && cvd_diff_fut >= EPS_CVD_Z {
            Some("bullish")
        } else if price_diff >= eps_price && cvd_diff_fut <= -EPS_CVD_Z {
            Some("hidden_bullish")
        } else {
            None
        };
        let Some(divergence_type) = divergence_type else {
            continue;
        };

        let mut p_value_price = if price_effect_z.abs() >= ZP_MIN {
            0.0
        } else {
            1.0
        };
        let mut p_value_cvd = if cvd_effect_z.abs() >= ZC_MIN {
            0.0
        } else {
            1.0
        };
        let mut sig_pass = price_effect_z.abs() >= ZP_MIN && cvd_effect_z.abs() >= ZC_MIN;

        if sig_test_mode == DivergenceSigTestMode::BlockBootstrap {
            let leg_len = (i2 - i1 + 1).max(2);
            let price_returns = build_returns_from_option_series(detrended_price, i2);
            let cvd_returns = build_returns_from_option_series(z_cvd_fut, i2);

            if price_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &price_returns,
                    leg_len,
                    price_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((i1 as u64) << 32) ^ (i2 as u64) ^ 0xA5A5_5A5A_u64,
                ) {
                    p_value_price = p;
                }
            }

            if cvd_returns.len() >= BOOTSTRAP_MIN_RET_SAMPLES {
                if let Some(p) = block_bootstrap_pvalue(
                    &cvd_returns,
                    leg_len,
                    cvd_effect_z.abs(),
                    bootstrap_b,
                    bootstrap_block_len,
                    ((i1 as u64) << 32) ^ (i2 as u64) ^ 0x5AA5_A55A_u64,
                ) {
                    p_value_cvd = p;
                }
            }

            sig_pass = p_value_price <= p_value_threshold && p_value_cvd <= p_value_threshold;
        }

        if !sig_pass {
            continue;
        }

        let delta_p_sign = price_diff.signum() as i16;
        let spot_flow_confirm = delta_p_sign == (cvd_diff_spot.signum() as i16);
        let fut_div_sign = (price_diff.signum() * cvd_diff_fut.signum()) as i16;
        let spot_lead_score = if (cvd_diff_spot.abs() + cvd_diff_fut.abs()) > 1e-12
            && (price_diff.signum() == cvd_diff_spot.signum())
            && (price_diff.signum() != cvd_diff_fut.signum())
        {
            cvd_diff_spot.abs() / (cvd_diff_spot.abs() + cvd_diff_fut.abs() + 1e-12)
        } else {
            0.0
        };
        let likely_driver = if spot_lead_score >= 0.6 {
            "spot_led"
        } else if spot_lead_score < 0.3 && fut_div_sign != -1 {
            "futures_led"
        } else {
            "mixed"
        };
        let score =
            clip01(1.0 * (0.5 * (price_effect_z.abs() / 3.0) + 0.5 * (cvd_effect_z.abs() / 3.0)));
        let available_i = c1.max(c2);

        out.push(DivergenceCandidate {
            divergence_type: divergence_type.to_string(),
            pivot_side: if is_high_side {
                "high".to_string()
            } else {
                "low".to_string()
            },
            i1,
            i2,
            confirm_i1: c1,
            confirm_i2: c2,
            available_i,
            price_start,
            price_end,
            cvd_start_fut: fut[i1].cvd,
            cvd_end_fut: fut[i2].cvd,
            cvd_start_spot: spot[i1].cvd,
            cvd_end_spot: spot[i2].cvd,
            price_diff,
            cvd_diff_fut,
            cvd_diff_spot,
            price_effect_z,
            cvd_effect_z,
            sig_pass,
            p_value_price,
            p_value_cvd,
            score,
            spot_price_flow_confirm: spot_flow_confirm,
            fut_divergence_sign: fut_div_sign,
            spot_lead_score,
            likely_driver: likely_driver.to_string(),
        });
    }
    out
}

fn build_returns_from_option_series(series: &[Option<f64>], end_idx: usize) -> Vec<f64> {
    if end_idx == 0 || series.is_empty() {
        return Vec::new();
    }
    let upper = end_idx.min(series.len() - 1);
    let mut vals = Vec::new();
    for v in series.iter().take(upper + 1).flatten() {
        vals.push(*v);
    }
    if vals.len() < 3 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(vals.len().saturating_sub(1));
    for i in 1..vals.len() {
        out.push(vals[i] - vals[i - 1]);
    }
    out
}

fn block_bootstrap_pvalue(
    returns: &[f64],
    leg_len: usize,
    observed_abs_z: f64,
    bootstrap_b: usize,
    block_len: usize,
    seed: u64,
) -> Option<f64> {
    if returns.len() < BOOTSTRAP_MIN_RET_SAMPLES || leg_len < 2 || bootstrap_b == 0 {
        return None;
    }
    let std = stddev_slice(returns)?;
    if std <= 1e-12 {
        return Some(1.0);
    }

    let block = block_len.clamp(1, returns.len().saturating_sub(1).max(1));
    let mut rng = XorShift64::new(seed.max(1));
    let mut hit = 0usize;

    for _ in 0..bootstrap_b {
        let mut sampled = Vec::with_capacity(leg_len);
        while sampled.len() < leg_len {
            let max_start = returns.len().saturating_sub(block);
            let start = if max_start == 0 {
                0
            } else {
                rng.next_usize(max_start + 1)
            };
            for j in 0..block {
                if sampled.len() >= leg_len {
                    break;
                }
                sampled.push(returns[start + j]);
            }
        }

        let sum = sampled.iter().sum::<f64>();
        let z = sum.abs() / (std * (leg_len as f64).sqrt() + 1e-12);
        if z >= observed_abs_z {
            hit += 1;
        }
    }

    Some((hit as f64 + 1.0) / (bootstrap_b as f64 + 1.0))
}

fn stddev_slice(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    Some(var.sqrt())
}

#[derive(Clone, Copy)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            0
        } else {
            (self.next_u64() as usize) % bound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_divergence_all_history, DivergenceEventStateMachine, DivergenceSigTestMode,
    };
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LevelAgg, MinuteHistory};
    use chrono::{Duration, TimeZone, Utc};
    use std::collections::BTreeMap;

    #[test]
    fn streaming_divergence_matches_direct_compute() {
        let base = Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).unwrap();
        let mut history_futures = Vec::new();
        let mut history_spot = Vec::new();
        for idx in 0..80 {
            let ts = base + Duration::minutes(idx as i64);
            let angle = idx as f64 / 4.0;
            let price = 100.0 + angle.sin() * 2.0 + idx as f64 * 0.03;
            let spot_price = 99.8 + angle.sin() * 1.7 + idx as f64 * 0.025;
            history_futures.push(sample_minute(
                ts,
                MarketKind::Futures,
                price,
                10.0 + angle.cos(),
                30.0 + angle.sin() * -5.0 + idx as f64 * 0.2,
            ));
            history_spot.push(sample_minute(
                ts,
                MarketKind::Spot,
                spot_price,
                9.0 + angle.sin(),
                25.0 + angle.sin() * 4.0 + idx as f64 * 0.15,
            ));
        }

        let direct = compute_divergence_all_history(
            &history_futures,
            &history_spot,
            DivergenceSigTestMode::Threshold,
            200,
            5,
            0.05,
        );

        let mut rebuilt_machine = DivergenceEventStateMachine::default();
        rebuilt_machine.rebuild(
            &history_futures,
            &history_spot,
            DivergenceSigTestMode::Threshold,
            200,
            5,
            0.05,
        );
        assert_eq!(direct, rebuilt_machine.events());

        let mut streaming_machine = DivergenceEventStateMachine::default();
        for end in 0..history_futures.len() {
            streaming_machine.sync(
                &history_futures[..=end],
                &history_spot[..=end],
                DivergenceSigTestMode::Threshold,
                200,
                5,
                0.05,
            );
        }
        assert_eq!(direct, streaming_machine.events());
    }

    fn sample_minute(
        ts_bucket: chrono::DateTime<chrono::Utc>,
        market: MarketKind,
        price: f64,
        delta: f64,
        cvd: f64,
    ) -> MinuteHistory {
        let mut profile = BTreeMap::new();
        profile.insert(
            (price * 100.0).round() as i64,
            LevelAgg {
                buy_qty: delta.max(0.0),
                sell_qty: (-delta).max(0.0),
            },
        );
        MinuteHistory {
            ts_bucket,
            market,
            open_price: Some(price - 0.2),
            high_price: Some(price + 0.5),
            low_price: Some(price - 0.5),
            close_price: Some(price + 0.1),
            last_price: Some(price + 0.1),
            buy_qty: delta.max(0.0) + 1.0,
            sell_qty: (-delta).max(0.0) + 1.0,
            total_qty: delta.abs() + 2.0,
            total_notional: price * (delta.abs() + 2.0),
            delta,
            relative_delta: delta / 10.0,
            force_liq: BTreeMap::new(),
            ofi: delta / 2.0,
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
            cvd,
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
