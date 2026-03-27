use crate::workflow::schema::{RecentBar, ZoneState};
use chrono::{DateTime, Utc};

pub fn zone_acceptance_above(state: &ZoneState) -> bool {
    state.acceptance_state == "accepted_above"
}

pub fn zone_acceptance_below(state: &ZoneState) -> bool {
    state.acceptance_state == "accepted_below"
}

pub fn reaccept_inside_value(state: &ZoneState) -> bool {
    state.position_relative == "inside"
}

pub fn failed_auction_confirmed(state: &ZoneState) -> bool {
    state.failed_auction_state == "failed_above" || state.failed_auction_state == "failed_below"
}

pub fn price_above_on_close(bars: &[RecentBar], level: f64) -> bool {
    bars.last().is_some_and(|bar| bar.close > level)
}

pub fn price_below_on_close(bars: &[RecentBar], level: f64) -> bool {
    bars.last().is_some_and(|bar| bar.close < level)
}

pub fn event_after_precondition(event_ts: DateTime<Utc>, precondition_ts: DateTime<Utc>) -> bool {
    event_ts >= precondition_ts
}

#[cfg(test)]
mod tests {
    use super::{
        event_after_precondition, failed_auction_confirmed, price_above_on_close,
        price_below_on_close, reaccept_inside_value, zone_acceptance_above, zone_acceptance_below,
    };
    use crate::workflow::schema::{RecentBar, ZoneState};
    use chrono::{Duration, Utc};

    fn sample_bar(close: f64) -> RecentBar {
        let now = Utc::now();
        RecentBar {
            open_time: now - Duration::minutes(15),
            close_time: now,
            open: close - 1.0,
            high: close + 1.0,
            low: close - 2.0,
            close,
            is_closed: true,
        }
    }

    #[test]
    fn price_close_predicates_use_latest_bar() {
        let bars = vec![sample_bar(100.0), sample_bar(105.0)];
        assert!(price_above_on_close(&bars, 104.0));
        assert!(price_below_on_close(&bars, 106.0));
        assert!(!price_above_on_close(&bars, 106.0));
    }

    #[test]
    fn zone_state_predicates_are_deterministic() {
        let state = ZoneState {
            zone_id: "z".to_string(),
            position_relative: "inside".to_string(),
            acceptance_state: "accepted_above".to_string(),
            failed_auction_state: "failed_below".to_string(),
            last_close: Some(100.0),
            confirmed_at: Some(Utc::now()),
        };
        assert!(zone_acceptance_above(&state));
        assert!(!zone_acceptance_below(&state));
        assert!(reaccept_inside_value(&state));
        assert!(failed_auction_confirmed(&state));
    }

    #[test]
    fn ordering_predicates_match_contract() {
        let now = Utc::now();
        assert!(event_after_precondition(now, now - Duration::minutes(1)));
        assert!(!event_after_precondition(now - Duration::minutes(2), now));
    }
}
