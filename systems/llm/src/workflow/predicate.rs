use crate::workflow::schema::ZoneState;

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

#[cfg(test)]
mod tests {
    use super::{
        failed_auction_confirmed, reaccept_inside_value, zone_acceptance_above,
        zone_acceptance_below,
    };
    use crate::workflow::schema::ZoneState;
    use chrono::Utc;

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
}
