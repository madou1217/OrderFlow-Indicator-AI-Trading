use std::time::{Duration, Instant};

pub const SOURCE_LAG_WARN_SECS: i64 = 5;
pub const SOURCE_LAG_FORCE_RECONNECT_SECS: i64 = 20;
pub const SOURCE_LAG_FORCE_RECONNECT_STREAK: u64 = 200;
pub const SOURCE_LAG_LOG_EVERY: u64 = 100;

const SOURCE_LAG_INITIAL_LOG_STREAK: u64 = 8;
const SOURCE_LAG_INITIAL_LOG_DWELL_MS: u64 = 750;
pub const SOURCE_LAG_FORCE_RECONNECT_DWELL_SECS: u64 = 3;

#[derive(Debug, Clone, Copy, Default)]
pub struct SourceLagObservation {
    pub source_lag_secs: i64,
    pub stale_source_streak: u64,
    pub stale_source_seen: u64,
    pub stale_duration_ms: u128,
    pub log_stale: bool,
    pub log_recovered: bool,
    pub force_reconnect: bool,
}

#[derive(Debug, Default)]
pub struct SourceLagTracker {
    stale_source_streak: u64,
    stale_source_seen: u64,
    stale_started_at: Option<Instant>,
    stale_logged: bool,
}

impl SourceLagTracker {
    pub fn observe(
        &mut self,
        source_lag_secs: i64,
        backfill_in_progress: bool,
    ) -> SourceLagObservation {
        self.observe_at(source_lag_secs, backfill_in_progress, Instant::now())
    }

    fn observe_at(
        &mut self,
        source_lag_secs: i64,
        backfill_in_progress: bool,
        now: Instant,
    ) -> SourceLagObservation {
        if backfill_in_progress || source_lag_secs <= SOURCE_LAG_WARN_SECS {
            let observation = SourceLagObservation {
                source_lag_secs,
                stale_source_streak: self.stale_source_streak,
                stale_source_seen: self.stale_source_seen,
                stale_duration_ms: self.stale_duration_ms(now),
                log_recovered: self.stale_logged,
                ..SourceLagObservation::default()
            };
            self.reset();
            return observation;
        }

        self.stale_source_streak = self.stale_source_streak.saturating_add(1);
        self.stale_source_seen = self.stale_source_seen.saturating_add(1);
        let started_at = self.stale_started_at.get_or_insert(now);
        let stale_duration_ms = now.duration_since(*started_at).as_millis();

        let log_stale = if !self.stale_logged {
            self.stale_source_streak >= SOURCE_LAG_INITIAL_LOG_STREAK
                || stale_duration_ms >= u128::from(SOURCE_LAG_INITIAL_LOG_DWELL_MS)
        } else {
            self.stale_source_streak % SOURCE_LAG_LOG_EVERY == 0
        };
        if log_stale {
            self.stale_logged = true;
        }

        let force_reconnect = source_lag_secs >= SOURCE_LAG_FORCE_RECONNECT_SECS
            && self.stale_source_streak >= SOURCE_LAG_FORCE_RECONNECT_STREAK
            && stale_duration_ms
                >= (Duration::from_secs(SOURCE_LAG_FORCE_RECONNECT_DWELL_SECS).as_millis());

        SourceLagObservation {
            source_lag_secs,
            stale_source_streak: self.stale_source_streak,
            stale_source_seen: self.stale_source_seen,
            stale_duration_ms,
            log_stale,
            log_recovered: false,
            force_reconnect,
        }
    }

    pub fn stale_source_streak(&self) -> u64 {
        self.stale_source_streak
    }

    pub fn stale_source_seen(&self) -> u64 {
        self.stale_source_seen
    }

    pub fn has_logged_stale(&self) -> bool {
        self.stale_logged
    }

    fn stale_duration_ms(&self, now: Instant) -> u128 {
        self.stale_started_at
            .map(|started| now.duration_since(started).as_millis())
            .unwrap_or(0)
    }

    fn reset(&mut self) {
        self.stale_source_streak = 0;
        self.stale_source_seen = 0;
        self.stale_started_at = None;
        self.stale_logged = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SourceLagTracker, SOURCE_LAG_FORCE_RECONNECT_DWELL_SECS, SOURCE_LAG_FORCE_RECONNECT_SECS,
        SOURCE_LAG_FORCE_RECONNECT_STREAK,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn isolated_stale_event_does_not_log_or_recover() {
        let base = Instant::now();
        let mut tracker = SourceLagTracker::default();

        let stale = tracker.observe_at(12, false, base);
        assert!(!stale.log_stale);
        assert!(!stale.force_reconnect);

        let recovered = tracker.observe_at(0, false, base + Duration::from_millis(5));
        assert!(!recovered.log_recovered);
    }

    #[test]
    fn sustained_stale_logs_once_and_recovery_is_debounced() {
        let base = Instant::now();
        let mut tracker = SourceLagTracker::default();

        for idx in 0..7 {
            let obs = tracker.observe_at(10, false, base + Duration::from_millis(idx * 100));
            assert!(!obs.log_stale);
        }

        let logged = tracker.observe_at(10, false, base + Duration::from_millis(800));
        assert!(logged.log_stale);
        assert_eq!(logged.stale_source_streak, 8);

        let recovered = tracker.observe_at(0, false, base + Duration::from_millis(900));
        assert!(recovered.log_recovered);
        assert_eq!(recovered.stale_source_streak, 8);
    }

    #[test]
    fn reconnect_requires_streak_and_dwell() {
        let base = Instant::now();
        let mut tracker = SourceLagTracker::default();

        for idx in 0..SOURCE_LAG_FORCE_RECONNECT_STREAK {
            let obs = tracker.observe_at(
                SOURCE_LAG_FORCE_RECONNECT_SECS,
                false,
                base + Duration::from_millis(idx),
            );
            assert!(!obs.force_reconnect);
        }

        let obs = tracker.observe_at(
            SOURCE_LAG_FORCE_RECONNECT_SECS,
            false,
            base + Duration::from_secs(SOURCE_LAG_FORCE_RECONNECT_DWELL_SECS),
        );
        assert!(obs.force_reconnect);
    }
}
