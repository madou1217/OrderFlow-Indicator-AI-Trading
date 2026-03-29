use crate::runtime::state_store::{tick_to_price, LiqAgg, MinuteHistory};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

const LONG_PEAK_PROMINENCE_FRACTION: f64 = 0.03;
const SHORT_PEAK_PROMINENCE_FRACTION: f64 = 0.03;

pub fn build_recent_7d_entry(row: &MinuteHistory) -> Value {
    let mut rows = row
        .force_liq
        .iter()
        .map(|(tick, agg)| {
            let long = agg.long_liq;
            let short = agg.short_liq;
            let net = short - long;
            (*tick, long, short, net)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(tick, _, _, _)| *tick);

    let total_long = rows.iter().map(|(_, long, _, _)| *long).sum::<f64>();
    let total_short = rows.iter().map(|(_, _, short, _)| *short).sum::<f64>();
    let long_peak_threshold = total_long * LONG_PEAK_PROMINENCE_FRACTION;
    let short_peak_threshold = total_short * SHORT_PEAK_PROMINENCE_FRACTION;

    let top_long = rows
        .iter()
        .map(|(_, long, _, _)| *long)
        .fold(0.0_f64, |a, b| a.max(b));
    let top_short = rows
        .iter()
        .map(|(_, _, short, _)| *short)
        .fold(0.0_f64, |a, b| a.max(b));

    let long_peaks = detect_local_peaks(&rows, true, long_peak_threshold);
    let short_peaks = detect_local_peaks(&rows, false, short_peak_threshold);

    let peak_levels = rows
        .iter()
        .filter(|(tick, _, _, _)| long_peaks.contains(tick) || short_peaks.contains(tick))
        .map(|(tick, long, short, net)| {
            json!({
                "price": tick_to_price(*tick),
                "long": *long,
                "short": *short,
                "net": *net,
                "is_long_peak": long_peaks.contains(tick),
                "is_short_peak": short_peaks.contains(tick),
                "long_peak_score": if top_long > 0.0 { Some(*long / top_long) } else { None },
                "short_peak_score": if top_short > 0.0 { Some(*short / top_short) } else { None },
            })
        })
        .collect::<Vec<_>>();

    json!({
        "ts_snapshot": row.ts_bucket.to_rfc3339(),
        "levels_count": rows.len(),
        "long_total": total_long,
        "short_total": total_short,
        "peak_levels": peak_levels,
    })
}

fn detect_local_peaks(
    rows: &[(i64, f64, f64, f64)],
    is_long: bool,
    threshold: f64,
) -> HashSet<i64> {
    let mut peaks = HashSet::new();
    if rows.is_empty() {
        return peaks;
    }

    for idx in 0..rows.len() {
        let (tick, long, short, _) = rows[idx];
        let value = if is_long { long } else { short };
        if value <= threshold {
            continue;
        }

        let left = idx
            .checked_sub(1)
            .and_then(|i| rows.get(i))
            .map(|(_, l, s, _)| if is_long { *l } else { *s })
            .unwrap_or(f64::NEG_INFINITY);
        let right = rows
            .get(idx + 1)
            .map(|(_, l, s, _)| if is_long { *l } else { *s })
            .unwrap_or(f64::NEG_INFINITY);

        if value >= left && value >= right {
            peaks.insert(tick);
        }
    }

    peaks
}

pub fn aggregate_force_liq_rows(rows: &[&MinuteHistory]) -> BTreeMap<i64, LiqAgg> {
    let mut out = BTreeMap::new();
    for row in rows {
        for (tick, agg) in &row.force_liq {
            let slot = out.entry(*tick).or_insert_with(LiqAgg::default);
            slot.long_liq += agg.long_liq;
            slot.short_liq += agg.short_liq;
        }
    }
    out
}

pub fn sort_desc_by_value_then_tick(rows: &mut [(i64, f64)]) {
    rows.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
}
