use crate::indicators::context::{
    clip01, IndicatorComputation, IndicatorContext, IndicatorSnapshotRow, InitiationEventRow,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::event_ids::build_initiation_event_id;
use crate::indicators::shared::event_views::{
    build_event_window_view, build_recent_7d_payload, merge_payload_fields,
};
use crate::indicators::shared::market_structure::{
    stacked_imbalance_flags, value_area_key_levels_ticks,
};
use crate::runtime::state_store::{tick_to_price, MinuteHistory};
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};
use std::collections::VecDeque;

const TICK_SIZE: f64 = 0.01;
const EPSILON_BREAK_TICKS: f64 = 2.0;
const ZDELTA_LOOKBACK: usize = 60;
const ZDELTA_MIN: f64 = 1.5;
const RDELTA_MIN: f64 = 0.20;
const MIN_FOLLOW_MINUTES: usize = 5;
const HOLD_BREAK_TICKS: f64 = 1.0;
pub(crate) const INITIATION_INCREMENTAL_LOOKBACK_MINUTES: i64 =
    ZDELTA_LOOKBACK as i64 + MIN_FOLLOW_MINUTES as i64 + 5;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InitiationEventData {
    pub direction: i16,
    pub event_type: String,
    pub start_ts: chrono::DateTime<chrono::Utc>,
    pub end_ts: chrono::DateTime<chrono::Utc>,
    pub confirm_ts: chrono::DateTime<chrono::Utc>,
    pub pivot_price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub z_delta: f64,
    pub rdelta_mean: f64,
    pub break_mag_ticks: f64,
    pub min_follow_required_minutes: i32,
    pub follow_through_minutes: i32,
    pub follow_through_end_ts: chrono::DateTime<chrono::Utc>,
    pub follow_through_delta_sum: f64,
    pub follow_through_hold_ok: bool,
    pub follow_through_max_adverse_excursion_ticks: f64,
    pub spot_break_confirm: bool,
    pub spot_rdelta_1m_mean: f64,
    pub spot_cvd_change: f64,
    pub spot_whale_break_confirm: bool,
    pub score: f64,
    pub payload: Value,
}

#[derive(Debug, Clone)]
struct InitiationDerivedMinute {
    seq: usize,
    ts_bucket: DateTime<Utc>,
    high: f64,
    low: f64,
    close: f64,
    delta: f64,
    rdelta: f64,
    spot_rdelta: f64,
    spot_cvd: f64,
    spot_whale_notional: f64,
    vah: f64,
    val: f64,
    stacked_buy: bool,
    stacked_sell: bool,
    zdelta: f64,
}

#[derive(Debug, Clone)]
struct InitiationPendingCandidate {
    direction: i16,
    start_seq: usize,
    pivot_price: f64,
    z_delta: f64,
    follow_through_delta_sum: f64,
    min_close_post_break: f64,
    max_close_post_break: f64,
    min_low: f64,
    max_high: f64,
    event_price_low: f64,
    event_price_high: f64,
}

#[derive(Debug, Clone)]
struct InitiationCachedEvent {
    required_start_ts: DateTime<Utc>,
    event: InitiationEventData,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InitiationEventStateMachine {
    minutes: VecDeque<InitiationDerivedMinute>,
    delta_window: VecDeque<(usize, f64)>,
    delta_window_sum: f64,
    delta_window_sumsq: f64,
    pending: VecDeque<InitiationPendingCandidate>,
    events: VecDeque<InitiationCachedEvent>,
    last_ts: Option<DateTime<Utc>>,
    vah_ffill: Option<f64>,
    val_ffill: Option<f64>,
    next_seq: usize,
}

impl InitiationEventStateMachine {
    pub(crate) fn rebuild(
        &mut self,
        history_futures: &[MinuteHistory],
        history_spot: &[MinuteHistory],
    ) {
        *self = Self::default();
        let (history_futures, history_spot) =
            aligned_event_histories(history_futures, history_spot);
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
        let (history_futures, history_spot) =
            aligned_event_histories(history_futures, history_spot);
        let Some(first_ts) = history_futures.first().map(|row| row.ts_bucket) else {
            *self = Self::default();
            return;
        };
        let last_ts = history_futures
            .last()
            .map(|row| row.ts_bucket)
            .unwrap_or(first_ts);
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
        let start_idx = lower_bound_history_ts(
            history_futures,
            self.last_ts.unwrap() + Duration::minutes(1),
        );
        if start_idx == 0 && self.minutes.is_empty() {
            self.rebuild(history_futures, history_spot);
            return;
        }
        for (fut, spot) in history_futures[start_idx..]
            .iter()
            .zip(history_spot[start_idx..].iter())
        {
            self.append_pair(fut, spot);
        }
        self.last_ts = Some(last_ts);
    }

    pub(crate) fn events(&self) -> Vec<InitiationEventData> {
        self.events
            .iter()
            .map(|entry| entry.event.clone())
            .collect()
    }

    fn prune_before(&mut self, first_ts: DateTime<Utc>) {
        while self
            .minutes
            .front()
            .map(|row| row.ts_bucket < first_ts)
            .unwrap_or(false)
        {
            if let Some(removed) = self.minutes.pop_front() {
                if self
                    .delta_window
                    .front()
                    .map(|(seq, _)| *seq == removed.seq)
                    .unwrap_or(false)
                {
                    self.delta_window.pop_front();
                    self.delta_window_sum -= removed.delta;
                    self.delta_window_sumsq -= removed.delta * removed.delta;
                }
            }
        }
        while self
            .events
            .front()
            .map(|entry| entry.required_start_ts < first_ts)
            .unwrap_or(false)
        {
            self.events.pop_front();
        }
        while self
            .pending
            .front()
            .map(|candidate| {
                self.offset_of(candidate.start_seq)
                    .and_then(|idx| self.minutes.get(idx))
                    .map(|row| row.ts_bucket < first_ts)
                    .unwrap_or(true)
            })
            .unwrap_or(false)
        {
            self.pending.pop_front();
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
        if let Some((val_tick, vah_tick, _)) = value_area_key_levels_ticks(&fut.profile) {
            self.val_ffill = Some(tick_to_price(val_tick));
            self.vah_ffill = Some(tick_to_price(vah_tick));
        }
        let (stacked_buy, stacked_sell) = stacked_imbalance_flags(&fut.profile);

        self.delta_window.push_back((self.next_seq, fut.delta));
        self.delta_window_sum += fut.delta;
        self.delta_window_sumsq += fut.delta * fut.delta;
        if self.delta_window.len() > ZDELTA_LOOKBACK {
            if let Some((_, removed)) = self.delta_window.pop_front() {
                self.delta_window_sum -= removed;
                self.delta_window_sumsq -= removed * removed;
            }
        }
        let zdelta = if self.delta_window.len() == ZDELTA_LOOKBACK {
            let mean = self.delta_window_sum / ZDELTA_LOOKBACK as f64;
            let var = (self.delta_window_sumsq / ZDELTA_LOOKBACK as f64 - mean * mean).max(0.0);
            (fut.delta - mean) / (var.sqrt() + 1e-12)
        } else {
            0.0
        };

        let spot_close = spot
            .close_price
            .or(spot.last_price)
            .or(spot.open_price)
            .unwrap_or(0.0);
        let minute = InitiationDerivedMinute {
            seq: self.next_seq,
            ts_bucket: fut.ts_bucket,
            high,
            low,
            close,
            delta: fut.delta,
            rdelta: fut.relative_delta,
            spot_rdelta: spot.relative_delta,
            spot_cvd: spot.cvd,
            spot_whale_notional: spot.delta * spot_close,
            vah: self.vah_ffill.unwrap_or(high),
            val: self.val_ffill.unwrap_or(low),
            stacked_buy,
            stacked_sell,
            zdelta,
        };
        self.next_seq += 1;
        self.minutes.push_back(minute);
        let seq = self.minutes.back().map(|row| row.seq).unwrap_or_default();
        self.update_pending(seq);
        self.maybe_start_candidate(seq);
    }

    fn update_pending(&mut self, current_seq: usize) {
        let Some(current_idx) = self.offset_of(current_seq) else {
            return;
        };
        let Some(current) = self.minutes.get(current_idx).cloned() else {
            return;
        };
        let mut retained = VecDeque::new();
        while let Some(mut candidate) = self.pending.pop_front() {
            if current_seq <= candidate.start_seq {
                retained.push_back(candidate);
                continue;
            }
            candidate.follow_through_delta_sum += current.delta;
            candidate.min_close_post_break = candidate.min_close_post_break.min(current.close);
            candidate.max_close_post_break = candidate.max_close_post_break.max(current.close);
            candidate.min_low = candidate.min_low.min(current.low);
            candidate.max_high = candidate.max_high.max(current.high);
            candidate.event_price_low = candidate.event_price_low.min(current.low);
            candidate.event_price_high = candidate.event_price_high.max(current.high);

            if current_seq < candidate.start_seq + MIN_FOLLOW_MINUTES {
                retained.push_back(candidate);
                continue;
            }
            if let Some(event) = self.build_event(&candidate, current_seq) {
                self.events.push_back(event);
            }
        }
        self.pending = retained
            .into_iter()
            .filter(|candidate| current_seq < candidate.start_seq + MIN_FOLLOW_MINUTES)
            .collect();
    }

    fn maybe_start_candidate(&mut self, seq: usize) {
        let Some(idx) = self.offset_of(seq) else {
            return;
        };
        let Some(current) = self.minutes.get(idx) else {
            return;
        };
        if idx == 0 {
            return;
        };
        let Some(prev) = self.minutes.get(idx - 1) else {
            return;
        };
        if self.delta_window.len() < ZDELTA_LOOKBACK {
            return;
        }

        let range = (current.high - current.low).max(TICK_SIZE);
        let clv_bull = (current.close - current.low) / (range + 1e-12);
        let clv_bear = (current.high - current.close) / (range + 1e-12);
        let eps_break = EPSILON_BREAK_TICKS * TICK_SIZE;

        let cand_bull = current.close > current.vah + eps_break
            && prev.close <= prev.vah + eps_break
            && current.zdelta >= ZDELTA_MIN
            && current.rdelta >= RDELTA_MIN
            && current.stacked_buy
            && clv_bull >= 0.70;
        let cand_bear = current.close < current.val - eps_break
            && prev.close >= prev.val - eps_break
            && current.zdelta <= -ZDELTA_MIN
            && current.rdelta <= -RDELTA_MIN
            && current.stacked_sell
            && clv_bear >= 0.70;
        let direction = match (cand_bull, cand_bear) {
            (true, false) => 1,
            (false, true) => -1,
            _ => 0,
        };
        if direction == 0 {
            return;
        }
        self.pending.push_back(InitiationPendingCandidate {
            direction,
            start_seq: seq,
            pivot_price: if direction > 0 {
                current.vah
            } else {
                current.val
            },
            z_delta: current.zdelta,
            follow_through_delta_sum: current.delta,
            min_close_post_break: f64::INFINITY,
            max_close_post_break: f64::NEG_INFINITY,
            min_low: current.low,
            max_high: current.high,
            event_price_low: current.low,
            event_price_high: current.high,
        });
    }

    fn build_event(
        &self,
        candidate: &InitiationPendingCandidate,
        confirm_seq: usize,
    ) -> Option<InitiationCachedEvent> {
        let start_idx = self.offset_of(candidate.start_seq)?;
        let confirm_idx = self.offset_of(confirm_seq)?;
        let start = self.minutes.get(start_idx)?;
        let confirm = self.minutes.get(confirm_idx)?;
        if confirm_seq != candidate.start_seq + MIN_FOLLOW_MINUTES {
            return None;
        }
        let eps_hold = HOLD_BREAK_TICKS * TICK_SIZE;
        let follow_through_hold_ok = if candidate.direction > 0 {
            candidate.min_close_post_break >= start.vah - eps_hold
                && candidate.follow_through_delta_sum > 0.0
        } else {
            candidate.max_close_post_break <= start.val + eps_hold
                && candidate.follow_through_delta_sum < 0.0
        };
        if !follow_through_hold_ok {
            return None;
        }

        let group = self
            .minutes
            .iter()
            .skip(start_idx)
            .take(confirm_idx - start_idx + 1)
            .collect::<Vec<_>>();
        let n_g = group.len() as f64;
        let rd_mean = group.iter().map(|row| row.rdelta).sum::<f64>() / n_g;
        let break_mag = if candidate.direction > 0 {
            (start.close - start.vah) / TICK_SIZE
        } else {
            (start.val - start.close) / TICK_SIZE
        };
        let follow_through_max_adverse_excursion_ticks = if candidate.direction > 0 {
            ((start.vah - candidate.min_low).max(0.0)) / TICK_SIZE
        } else {
            ((candidate.max_high - start.val).max(0.0)) / TICK_SIZE
        };
        let spot_rd_mean = group.iter().map(|row| row.spot_rdelta).sum::<f64>() / n_g;
        let spot_cvd_change = confirm.spot_cvd - start.spot_cvd;
        let spot_break_confirm = (candidate.direction as f64 * spot_rd_mean) >= 0.05
            && (candidate.direction as f64 * spot_cvd_change) >= 0.0;
        let spot_whale_confirm = group.iter().map(|row| row.spot_whale_notional).sum::<f64>()
            * candidate.direction as f64
            > 0.0;
        let score = 0.30 * clip01(candidate.z_delta.abs() / 3.0)
            + 0.30 * clip01(rd_mean.abs() / 0.5)
            + 0.20 * clip01(break_mag.abs() / 6.0)
            + 0.20 * clip01(n_g / (MIN_FOLLOW_MINUTES as f64 + 1.0));

        let event_type = if candidate.direction > 0 {
            "bullish_initiation"
        } else {
            "bearish_initiation"
        };
        let start_ts = start.ts_bucket;
        let confirm_ts = confirm.ts_bucket + Duration::minutes(1);
        let end_ts = confirm_ts;
        Some(InitiationCachedEvent {
            required_start_ts: start_ts - Duration::minutes((ZDELTA_LOOKBACK - 1) as i64),
            event: InitiationEventData {
                direction: candidate.direction,
                event_type: event_type.to_string(),
                start_ts,
                end_ts,
                confirm_ts,
                pivot_price: candidate.pivot_price,
                price_low: candidate.event_price_low,
                price_high: candidate.event_price_high,
                z_delta: candidate.z_delta,
                rdelta_mean: rd_mean,
                break_mag_ticks: break_mag,
                min_follow_required_minutes: MIN_FOLLOW_MINUTES as i32,
                follow_through_minutes: (confirm_seq - candidate.start_seq) as i32,
                follow_through_end_ts: confirm.ts_bucket + Duration::minutes(1),
                follow_through_delta_sum: candidate.follow_through_delta_sum,
                follow_through_hold_ok,
                follow_through_max_adverse_excursion_ticks,
                spot_break_confirm,
                spot_rdelta_1m_mean: spot_rd_mean,
                spot_cvd_change,
                spot_whale_break_confirm: spot_whale_confirm,
                score,
                payload: json!({
                    "event_start_ts": start_ts.to_rfc3339(),
                    "event_end_ts": end_ts.to_rfc3339(),
                    "event_available_ts": confirm_ts.to_rfc3339(),
                    "pivot_price": candidate.pivot_price,
                    "price_low": candidate.event_price_low,
                    "price_high": candidate.event_price_high,
                    "z_delta": candidate.z_delta,
                    "rdelta_mean": rd_mean,
                    "break_mag_ticks": break_mag,
                    "min_follow_required_minutes": MIN_FOLLOW_MINUTES,
                    "follow_through_minutes": confirm_seq - candidate.start_seq,
                    "follow_through_end_ts": (confirm.ts_bucket + Duration::minutes(1)).to_rfc3339(),
                    "follow_through_delta_sum": candidate.follow_through_delta_sum,
                    "follow_through_hold_ok": follow_through_hold_ok,
                    "follow_through_max_adverse_excursion_ticks": follow_through_max_adverse_excursion_ticks,
                    "spot_break_confirm": spot_break_confirm,
                    "spot_rdelta_1m_mean": spot_rd_mean,
                    "spot_cvd_change": spot_cvd_change,
                    "spot_whale_break_confirm": spot_whale_confirm,
                    "strength_score_xmk": 0.80 * score
                        + 0.15 * clip01(candidate.direction as f64 * spot_rd_mean)
                        + 0.05 * if spot_whale_confirm { 1.0 } else { 0.0 },
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
pub(crate) fn compute_initiation_all_history_from_histories(
    history_futures: &[MinuteHistory],
    history_spot: &[MinuteHistory],
) -> Vec<InitiationEventData> {
    let series = crate::indicators::context::BasicEventHistorySeries::from_histories(
        history_futures,
        history_spot,
    );
    let n = series.n;
    if n < ZDELTA_LOOKBACK + MIN_FOLLOW_MINUTES + 3 {
        return Vec::new();
    }
    let fut = &history_futures[history_futures.len().saturating_sub(n)..];
    let high = &series.high;
    let low = &series.low;
    let close = &series.close;
    let delta = &series.delta;
    let rdelta = &series.rdelta;
    let spot_rdelta = &series.spot_rdelta;
    let spot_cvd = &series.spot_cvd;

    let mut zdelta = vec![None; n];
    for i in (ZDELTA_LOOKBACK - 1)..n {
        let win = &delta[i + 1 - ZDELTA_LOOKBACK..=i];
        let mean = win.iter().sum::<f64>() / ZDELTA_LOOKBACK as f64;
        let var = win
            .iter()
            .map(|v| {
                let d = v - mean;
                d * d
            })
            .sum::<f64>()
            / ZDELTA_LOOKBACK as f64;
        let sd = var.sqrt() + 1e-12;
        zdelta[i] = Some((delta[i] - mean) / sd);
    }

    let mut vah = vec![0.0; n];
    let mut val = vec![0.0; n];
    let mut stacked_buy_flags = vec![false; n];
    let mut stacked_sell_flags = vec![false; n];
    let mut vah_ffill: Option<f64> = None;
    let mut val_ffill: Option<f64> = None;
    for i in 0..n {
        if let Some((val_tick, vah_tick, _)) = value_area_key_levels_ticks(&fut[i].profile) {
            val_ffill = Some(tick_to_price(val_tick));
            vah_ffill = Some(tick_to_price(vah_tick));
        }
        let (stacked_buy, stacked_sell) = stacked_imbalance_flags(&fut[i].profile);
        stacked_buy_flags[i] = stacked_buy;
        stacked_sell_flags[i] = stacked_sell;
        vah[i] = vah_ffill.unwrap_or(high[i]);
        val[i] = val_ffill.unwrap_or(low[i]);
    }

    let eps_break = EPSILON_BREAK_TICKS * TICK_SIZE;
    let eps_hold = HOLD_BREAK_TICKS * TICK_SIZE;

    let mut out = Vec::new();
    for i in 1..n {
        if i + MIN_FOLLOW_MINUTES >= n {
            break;
        }
        let range = (high[i] - low[i]).max(TICK_SIZE);
        let clv_bull = (close[i] - low[i]) / (range + 1e-12);
        let clv_bear = (high[i] - close[i]) / (range + 1e-12);
        let z = zdelta[i].unwrap_or(0.0);

        let cand_bull = close[i] > vah[i] + eps_break
            && close[i - 1] <= vah[i - 1] + eps_break
            && z >= ZDELTA_MIN
            && rdelta[i] >= RDELTA_MIN
            && stacked_buy_flags[i]
            && clv_bull >= 0.70;
        let cand_bear = close[i] < val[i] - eps_break
            && close[i - 1] >= val[i - 1] - eps_break
            && z <= -ZDELTA_MIN
            && rdelta[i] <= -RDELTA_MIN
            && stacked_sell_flags[i]
            && clv_bear >= 0.70;
        let dir = match (cand_bull, cand_bear) {
            (true, false) => 1_i16,
            (false, true) => -1_i16,
            _ => 0_i16,
        };
        if dir == 0 {
            continue;
        }

        let follow_through_end_idx = i + MIN_FOLLOW_MINUTES;
        let follow_through_delta_sum = delta[i..=follow_through_end_idx].iter().sum::<f64>();
        let follow_through_hold_ok = if dir > 0 {
            let min_close = close[i + 1..=i + MIN_FOLLOW_MINUTES]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            min_close >= vah[i] - eps_hold && follow_through_delta_sum > 0.0
        } else {
            let max_close = close[i + 1..=i + MIN_FOLLOW_MINUTES]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            max_close <= val[i] + eps_hold && follow_through_delta_sum < 0.0
        };
        if !follow_through_hold_ok {
            continue;
        }

        let confirm_idx = follow_through_end_idx;
        let end_idx = confirm_idx;
        let event_price_low = low[i..=end_idx]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let event_price_high = high[i..=end_idx]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let follow_through_max_adverse_excursion_ticks = if dir > 0 {
            let min_low = low[i..=confirm_idx]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            ((vah[i] - min_low).max(0.0)) / TICK_SIZE
        } else {
            let max_high = high[i..=confirm_idx]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            ((max_high - val[i]).max(0.0)) / TICK_SIZE
        };

        let n_g = (end_idx - i + 1) as f64;
        let rd_mean = rdelta[i..=end_idx].iter().sum::<f64>() / n_g;
        let break_mag = if dir > 0 {
            (close[i] - vah[i]) / TICK_SIZE
        } else {
            (val[i] - close[i]) / TICK_SIZE
        };
        let score = 0.30 * clip01(z.abs() / 3.0)
            + 0.30 * clip01(rd_mean.abs() / 0.5)
            + 0.20 * clip01(break_mag.abs() / 6.0)
            + 0.20 * clip01(n_g / (MIN_FOLLOW_MINUTES as f64 + 1.0));

        let spot_rd_mean = spot_rdelta[i..=end_idx].iter().sum::<f64>() / n_g;
        let spot_cvd_change = spot_cvd[end_idx] - spot_cvd[i];
        let spot_break_confirm =
            (dir as f64 * spot_rd_mean) >= 0.05 && (dir as f64 * spot_cvd_change) >= 0.0;
        let spot_whale_confirm =
            series.spot_whale_notional[i..=end_idx].iter().sum::<f64>() * dir as f64 > 0.0;
        let strength_score_xmk = 0.80 * score
            + 0.15 * clip01(dir as f64 * spot_rd_mean)
            + 0.05 * if spot_whale_confirm { 1.0 } else { 0.0 };

        let start_ts = fut[i].ts_bucket;
        let confirm_ts = fut[confirm_idx].ts_bucket + Duration::minutes(1);
        let end_ts = fut[end_idx].ts_bucket + Duration::minutes(1);
        let event_type = if dir > 0 {
            "bullish_initiation"
        } else {
            "bearish_initiation"
        };

        out.push(InitiationEventData {
            direction: dir,
            event_type: event_type.to_string(),
            start_ts,
            end_ts,
            confirm_ts,
            pivot_price: if dir > 0 { vah[i] } else { val[i] },
            price_low: event_price_low,
            price_high: event_price_high,
            z_delta: z,
            rdelta_mean: rd_mean,
            break_mag_ticks: break_mag,
            min_follow_required_minutes: MIN_FOLLOW_MINUTES as i32,
            follow_through_minutes: (confirm_idx - i) as i32,
            follow_through_end_ts: fut[confirm_idx].ts_bucket + Duration::minutes(1),
            follow_through_delta_sum,
            follow_through_hold_ok,
            follow_through_max_adverse_excursion_ticks,
            spot_break_confirm,
            spot_rdelta_1m_mean: spot_rd_mean,
            spot_cvd_change,
            spot_whale_break_confirm: spot_whale_confirm,
            score: strength_score_xmk,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": end_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "pivot_price": if dir > 0 { vah[i] } else { val[i] },
                "price_low": event_price_low,
                "price_high": event_price_high,
                "z_delta": z,
                "rdelta_mean": rd_mean,
                "break_mag_ticks": break_mag,
                "min_follow_required_minutes": MIN_FOLLOW_MINUTES as i32,
                "follow_through_minutes": (confirm_idx - i) as i32,
                "follow_through_delta_sum": follow_through_delta_sum,
                "follow_through_hold_ok": follow_through_hold_ok,
                "follow_through_max_adverse_excursion_ticks": follow_through_max_adverse_excursion_ticks,
                "spot_break_confirm": spot_break_confirm,
                "spot_rdelta_mean": spot_rd_mean,
                "spot_rdelta_1m_mean": spot_rd_mean,
                "spot_cvd_change": spot_cvd_change,
                "spot_whale_break_confirm": spot_whale_confirm,
                "strength_score_xmk": strength_score_xmk,
                "sig_pass": true
            }),
        });
    }

    out
}

pub(crate) fn detect_initiation_events(ctx: &IndicatorContext) -> Vec<InitiationEventData> {
    let current_available_ts = ctx.ts_bucket + Duration::minutes(1);
    ctx.initiation_all_events()
        .iter()
        .filter(|event| event.confirm_ts == current_available_ts)
        .cloned()
        .collect()
}

pub(crate) fn initiation_event_json(
    symbol: &str,
    indicator_code: &'static str,
    event: &InitiationEventData,
) -> (chrono::DateTime<chrono::Utc>, Value) {
    let event_id = build_initiation_event_id(
        symbol,
        indicator_code,
        &event.event_type,
        event.direction,
        event.confirm_ts,
        event.start_ts,
        event.end_ts,
        event.pivot_price,
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
    base.insert(
        "event_start_ts".to_string(),
        json!(event.start_ts.to_rfc3339()),
    );
    base.insert("event_end_ts".to_string(), json!(event.end_ts.to_rfc3339()));
    base.insert("pivot_price".to_string(), json!(event.pivot_price));
    base.insert("price_low".to_string(), json!(event.price_low));
    base.insert("price_high".to_string(), json!(event.price_high));
    base.insert("break_mag_ticks".to_string(), json!(event.break_mag_ticks));
    base.insert("z_delta".to_string(), json!(event.z_delta));
    base.insert("rdelta_mean".to_string(), json!(event.rdelta_mean));
    base.insert(
        "min_follow_required_minutes".to_string(),
        json!(event.min_follow_required_minutes),
    );
    base.insert(
        "follow_through_minutes".to_string(),
        json!(event.follow_through_minutes),
    );
    base.insert(
        "follow_through_delta_sum".to_string(),
        json!(event.follow_through_delta_sum),
    );
    base.insert(
        "follow_through_hold_ok".to_string(),
        json!(event.follow_through_hold_ok),
    );
    base.insert(
        "follow_through_max_adverse_excursion_ticks".to_string(),
        json!(event.follow_through_max_adverse_excursion_ticks),
    );
    base.insert(
        "spot_break_confirm".to_string(),
        json!(event.spot_break_confirm),
    );
    base.insert(
        "spot_rdelta_mean".to_string(),
        json!(event.spot_rdelta_1m_mean),
    );
    base.insert(
        "spot_rdelta_1m_mean".to_string(),
        json!(event.spot_rdelta_1m_mean),
    );
    base.insert("spot_cvd_change".to_string(), json!(event.spot_cvd_change));
    base.insert(
        "spot_whale_break_confirm".to_string(),
        json!(event.spot_whale_break_confirm),
    );
    (event.confirm_ts, merge_payload_fields(base, &event.payload))
}

fn append_initiation_rows(
    out: &mut IndicatorComputation,
    symbol: &str,
    indicator_code: &'static str,
    events: &[InitiationEventData],
) {
    for event in events {
        let event_id = build_initiation_event_id(
            symbol,
            indicator_code,
            &event.event_type,
            event.direction,
            event.confirm_ts,
            event.start_ts,
            event.end_ts,
            event.pivot_price,
        );
        let payload_json = initiation_event_json(symbol, indicator_code, event).1;
        out.initiation_rows.push(InitiationEventRow {
            event_id,
            event_type: event.event_type.clone(),
            direction: event.direction,
            ts_event_start: event.start_ts,
            ts_event_end: event.end_ts,
            confirm_ts: event.confirm_ts,
            event_available_ts: event.confirm_ts,
            pivot_price: Some(event.pivot_price),
            break_mag_ticks: Some(event.break_mag_ticks),
            z_delta: Some(event.z_delta),
            rdelta_mean: Some(event.rdelta_mean),
            min_follow_required_minutes: Some(event.min_follow_required_minutes),
            follow_through_minutes: Some(event.follow_through_minutes),
            follow_through_end_ts: Some(event.follow_through_end_ts),
            follow_through_delta_sum: Some(event.follow_through_delta_sum),
            follow_through_hold_ok: Some(event.follow_through_hold_ok),
            follow_through_max_adverse_excursion_ticks: Some(
                event.follow_through_max_adverse_excursion_ticks,
            ),
            spot_break_confirm: Some(event.spot_break_confirm),
            spot_rdelta_1m_mean: Some(event.spot_rdelta_1m_mean),
            spot_cvd_change: Some(event.spot_cvd_change),
            spot_whale_break_confirm: Some(event.spot_whale_break_confirm),
            score: Some(event.score),
            confidence: Some(event.score),
            window_code: "1m",
            payload_json,
        });
    }
}

pub struct I07Initiation;

impl Indicator for I07Initiation {
    fn code(&self) -> &'static str {
        "initiation"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation {
        let all_events = ctx.initiation_all_events();
        let window_view = build_event_window_view(
            ctx.ts_bucket,
            all_events
                .iter()
                .map(|event| initiation_event_json(&ctx.symbol, self.code(), event))
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

        let current_events = detect_initiation_events(ctx);
        append_initiation_rows(
            &mut out,
            &ctx.symbol,
            self.code(),
            current_events.as_slice(),
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_initiation_all_history_from_histories, detect_initiation_events,
        initiation_event_json, InitiationEventData, InitiationEventStateMachine,
    };
    use crate::indicators::context::{
        DivergenceSigTestMode, IndicatorContext, IndicatorSharedCaches,
    };
    use crate::indicators::shared::event_ids::build_initiation_event_id;
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::{LevelAgg, MinuteHistory, MinuteWindowData};
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn sample_minute(
        ts_bucket: chrono::DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        buy_qty: f64,
        sell_qty: f64,
        cvd: f64,
    ) -> MinuteHistory {
        let mut profile = BTreeMap::new();
        profile.insert(
            100,
            LevelAgg {
                buy_qty: 12.0,
                sell_qty: 1.0,
            },
        );
        profile.insert(
            101,
            LevelAgg {
                buy_qty: 12.0,
                sell_qty: 1.0,
            },
        );
        profile.insert(
            102,
            LevelAgg {
                buy_qty: 12.0,
                sell_qty: 1.0,
            },
        );
        profile.insert(
            103,
            LevelAgg {
                buy_qty: 1.0,
                sell_qty: 12.0,
            },
        );
        MinuteHistory {
            ts_bucket,
            market: MarketKind::Futures,
            open_price: Some(open),
            high_price: Some(high),
            low_price: Some(low),
            close_price: Some(close),
            last_price: Some(close),
            buy_qty,
            sell_qty,
            total_qty: buy_qty + sell_qty,
            total_notional: (buy_qty + sell_qty) * close,
            delta: buy_qty - sell_qty,
            relative_delta: if (buy_qty + sell_qty) > 0.0 {
                (buy_qty - sell_qty) / (buy_qty + sell_qty)
            } else {
                0.0
            },
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
            cvd,
            vpin: 0.0,
            avwap_minute: Some(close),
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
    fn initiation_event_json_exposes_follow_through_fields() {
        let start_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 10, 0).unwrap();
        let confirm_ts = Utc.with_ymd_and_hms(2026, 3, 9, 1, 16, 0).unwrap();
        let event = InitiationEventData {
            direction: -1,
            event_type: "bearish_initiation".to_string(),
            start_ts,
            end_ts: confirm_ts,
            confirm_ts,
            pivot_price: 1950.4,
            price_low: 1948.8,
            price_high: 1953.1,
            z_delta: -2.18,
            rdelta_mean: -0.29,
            break_mag_ticks: 14.0,
            min_follow_required_minutes: 5,
            follow_through_minutes: 5,
            follow_through_end_ts: confirm_ts,
            follow_through_delta_sum: -312.8,
            follow_through_hold_ok: true,
            follow_through_max_adverse_excursion_ticks: 1.5,
            spot_break_confirm: true,
            spot_rdelta_1m_mean: -0.11,
            spot_cvd_change: -201.8,
            spot_whale_break_confirm: false,
            score: 0.81,
            payload: json!({
                "event_start_ts": start_ts.to_rfc3339(),
                "event_end_ts": confirm_ts.to_rfc3339(),
                "event_available_ts": confirm_ts.to_rfc3339(),
                "price_low": 1948.8,
                "price_high": 1953.1,
                "min_follow_required_minutes": 5,
                "follow_through_minutes": 5,
                "follow_through_delta_sum": -312.8,
                "follow_through_hold_ok": true,
                "follow_through_max_adverse_excursion_ticks": 1.5,
                "score": 0.81
            }),
        };

        let (_, payload) = initiation_event_json("TESTUSDT", "initiation", &event);
        let expected_event_id = build_initiation_event_id(
            "TESTUSDT",
            "initiation",
            "bearish_initiation",
            -1,
            confirm_ts,
            start_ts,
            confirm_ts,
            1950.4,
        );
        assert_eq!(
            payload.get("event_id").and_then(|v| v.as_str()),
            Some(expected_event_id.as_str())
        );
        assert_eq!(
            payload
                .get("follow_through_delta_sum")
                .and_then(|v| v.as_f64()),
            Some(-312.8)
        );
        assert_eq!(
            payload
                .get("min_follow_required_minutes")
                .and_then(|v| v.as_i64()),
            Some(5)
        );
        assert_eq!(
            payload.get("price_low").and_then(|v| v.as_f64()),
            Some(1948.8)
        );
        assert_eq!(
            payload.get("price_high").and_then(|v| v.as_f64()),
            Some(1953.1)
        );
        assert_eq!(
            payload
                .get("follow_through_hold_ok")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            payload
                .get("follow_through_max_adverse_excursion_ticks")
                .and_then(|v| v.as_f64()),
            Some(1.5)
        );
    }

    #[test]
    fn cached_initiation_all_history_matches_direct_compute() {
        let base = Utc.with_ymd_and_hms(2026, 3, 9, 2, 0, 0).unwrap();
        let history_futures = (0..12)
            .map(|i| {
                let ts = base + chrono::Duration::minutes(i as i64);
                sample_minute(
                    ts,
                    100.0 + i as f64 * 0.05,
                    101.0 + i as f64 * 0.05,
                    99.0 + i as f64 * 0.05,
                    100.3 + i as f64 * 0.05,
                    6.0 + i as f64,
                    3.0 + (i % 2) as f64,
                    i as f64,
                )
            })
            .collect::<Vec<_>>();
        let history_spot = (0..12)
            .map(|i| {
                let ts = base + chrono::Duration::minutes(i as i64);
                sample_minute(
                    ts,
                    99.8 + i as f64 * 0.05,
                    100.8 + i as f64 * 0.05,
                    98.8 + i as f64 * 0.05,
                    100.1 + i as f64 * 0.05,
                    4.0 + i as f64,
                    2.5 + (i % 3) as f64,
                    (i * 2) as f64,
                )
            })
            .collect::<Vec<_>>();
        let ctx = test_ctx(
            base + chrono::Duration::minutes(11),
            history_futures,
            history_spot,
        );

        let direct =
            compute_initiation_all_history_from_histories(&ctx.history_futures, &ctx.history_spot);
        ctx.shared_caches
            .seed_initiation_all_events(Arc::new(direct.clone()));
        let cached = ctx.initiation_all_events();
        assert_eq!(direct, *cached);

        let history_futures = ctx.history_futures.as_ref().clone();
        let history_spot = ctx.history_spot.as_ref().clone();

        let mut rebuilt_machine = InitiationEventStateMachine::default();
        rebuilt_machine.rebuild(&history_futures, &history_spot);
        assert_eq!(direct, rebuilt_machine.events());

        let mut streaming_machine = InitiationEventStateMachine::default();
        for end in 0..history_futures.len() {
            streaming_machine.sync(&history_futures[..=end], &history_spot[..=end]);
        }
        assert_eq!(direct, streaming_machine.events());

        let expected_current = direct
            .iter()
            .filter(|event| event.confirm_ts == ctx.ts_bucket + chrono::Duration::minutes(1))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(detect_initiation_events(&ctx), expected_current);
    }
}
