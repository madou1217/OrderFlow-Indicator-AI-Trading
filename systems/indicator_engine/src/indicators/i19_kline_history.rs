use crate::indicators::context::{
    daily_window_days, window_code_minutes, IndicatorContext, KlineHistoryBar,
};
use crate::indicators::indicator_trait::Indicator;
use crate::indicators::shared::output_mapper::snapshot_only;
use crate::runtime::state_store::MinuteHistory;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use std::collections::BTreeMap;

pub struct I19KlineHistory;

impl Indicator for I19KlineHistory {
    fn code(&self) -> &'static str {
        "kline_history"
    }

    fn evaluate(&self, ctx: &IndicatorContext) -> crate::indicators::context::IndicatorComputation {
        let current_minute_close = ctx.ts_bucket + Duration::minutes(1);
        let interval_specs = [
            ("1m", ctx.kline_history_bars_1m),
            ("15m", ctx.kline_history_bars_15m),
            ("4h", ctx.kline_history_bars_4h),
            ("1d", ctx.kline_history_bars_1d),
            ("3d", ctx.kline_history_bars_3d),
            ("7d", ctx.kline_history_bars_7d),
            ("30d", ctx.kline_history_bars_30d),
        ];

        let futures_1d_records = merge_interval_bar_records(
            &ctx.kline_history_futures_1d_db,
            &build_interval_bar_records(
                &ctx.history_futures,
                1440,
                usize::MAX,
                current_minute_close,
            ),
        );
        let spot_1d_records = merge_interval_bar_records(
            &ctx.kline_history_spot_1d_db,
            &build_interval_bar_records(&ctx.history_spot, 1440, usize::MAX, current_minute_close),
        );

        let mut intervals = serde_json::Map::new();
        for (interval_code, limit) in interval_specs {
            let Some(interval_minutes) = window_code_minutes(interval_code) else {
                continue;
            };
            let futures_bars = match interval_code {
                code if daily_window_days(code).unwrap_or(0) > 1 => {
                    build_interval_bars_from_records(
                        &futures_1d_records,
                        interval_minutes,
                        limit,
                        current_minute_close,
                    )
                }
                "4h" => build_interval_bars_with_db(
                    &ctx.history_futures,
                    &ctx.kline_history_futures_4h_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                "1d" if ctx.kline_history_fill_1d_from_db => build_interval_bars_with_db(
                    &ctx.history_futures,
                    &ctx.kline_history_futures_1d_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                _ => build_interval_bars(
                    &ctx.history_futures,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
            };
            let spot_bars = match interval_code {
                code if daily_window_days(code).unwrap_or(0) > 1 => {
                    build_interval_bars_from_records(
                        &spot_1d_records,
                        interval_minutes,
                        limit,
                        current_minute_close,
                    )
                }
                "4h" => build_interval_bars_with_db(
                    &ctx.history_spot,
                    &ctx.kline_history_spot_4h_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                "1d" if ctx.kline_history_fill_1d_from_db => build_interval_bars_with_db(
                    &ctx.history_spot,
                    &ctx.kline_history_spot_1d_db,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
                _ => build_interval_bars(
                    &ctx.history_spot,
                    interval_minutes,
                    limit,
                    current_minute_close,
                ),
            };

            intervals.insert(
                interval_code.to_string(),
                json!({
                    "interval_code": interval_code,
                    "requested_count": limit,
                    "markets": {
                        "futures": {
                            "returned_count": futures_bars.len(),
                            "bars": futures_bars,
                        },
                        "spot": {
                            "returned_count": spot_bars.len(),
                            "bars": spot_bars,
                        }
                    }
                }),
            );
        }

        snapshot_only(
            self.code(),
            json!({
                "indicator": "kline_history",
                "window": "1m",
                "as_of_ts": current_minute_close.to_rfc3339(),
                "intervals": intervals,
            }),
        )
    }
}

#[derive(Clone)]
struct BarAccumulator {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    expected_minutes: i64,
    covered_minutes: i64,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume_base: f64,
    volume_quote: f64,
}

impl BarAccumulator {
    fn new(open_time: DateTime<Utc>, interval_minutes: i64) -> Self {
        Self {
            open_time,
            close_time: open_time + Duration::minutes(interval_minutes),
            expected_minutes: interval_minutes,
            covered_minutes: 0,
            open: None,
            high: None,
            low: None,
            close: None,
            volume_base: 0.0,
            volume_quote: 0.0,
        }
    }

    fn apply(&mut self, bar: &MinuteHistory) {
        let open = bar.open_price.or(bar.last_price).or(bar.close_price);
        let high = bar
            .high_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(open);
        let low = bar
            .low_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(open);
        let close = bar.close_price.or(bar.last_price).or(bar.open_price);

        if self.open.is_none() {
            self.open = open;
        }
        if let Some(value) = high {
            self.high = Some(self.high.map_or(value, |prev| prev.max(value)));
        }
        if let Some(value) = low {
            self.low = Some(self.low.map_or(value, |prev| prev.min(value)));
        }
        if close.is_some() {
            self.close = close;
        }

        self.covered_minutes += 1;
        self.volume_base += bar.total_qty;
        self.volume_quote += bar.total_notional;
    }

    fn to_bar(self, current_minute_close: DateTime<Utc>) -> KlineHistoryBar {
        let is_closed = bar_is_closed(
            self.close_time,
            current_minute_close,
            self.covered_minutes,
            self.expected_minutes,
        );
        KlineHistoryBar {
            open_time: self.open_time,
            close_time: self.close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume_base: self.volume_base,
            volume_quote: self.volume_quote,
            is_closed,
            minutes_covered: self.covered_minutes,
            expected_minutes: self.expected_minutes,
        }
    }
}

pub fn build_interval_bar_records(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<KlineHistoryBar> {
    if interval_minutes <= 0 || limit == 0 || history.is_empty() {
        return Vec::new();
    }

    if interval_minutes == 1 {
        return history
            .iter()
            .rev()
            .take(limit)
            .map(minute_bar_to_record)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
    }

    let mut grouped = BTreeMap::<DateTime<Utc>, BarAccumulator>::new();
    for minute in history {
        let open_time = floor_to_interval(minute.ts_bucket, interval_minutes);
        let entry = grouped
            .entry(open_time)
            .or_insert_with(|| BarAccumulator::new(open_time, interval_minutes));
        entry.apply(minute);
    }

    let mut bars = grouped
        .into_values()
        .map(|bar| bar.to_bar(current_minute_close))
        .collect::<Vec<_>>();
    if bars.len() > limit {
        bars = bars.split_off(bars.len() - limit);
    }
    bars
}

pub fn oldest_retained_bar_open_time(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    build_interval_bar_records(history, interval_minutes, limit, current_minute_close)
        .first()
        .map(|bar| bar.open_time)
}

fn build_interval_bars(
    history: &[MinuteHistory],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<serde_json::Value> {
    build_interval_bar_records(history, interval_minutes, limit, current_minute_close)
        .into_iter()
        .map(bar_to_json)
        .collect()
}

pub fn build_interval_bar_records_from_records(
    bars: &[KlineHistoryBar],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<KlineHistoryBar> {
    if interval_minutes <= 0 || limit == 0 || bars.is_empty() {
        return Vec::new();
    }

    let mut grouped = BTreeMap::<DateTime<Utc>, KlineHistoryBar>::new();
    for bar in bars {
        let open_time = floor_to_interval(bar.open_time, interval_minutes);
        let entry = grouped.entry(open_time).or_insert_with(|| KlineHistoryBar {
            open_time,
            close_time: open_time + Duration::minutes(interval_minutes),
            open: None,
            high: None,
            low: None,
            close: None,
            volume_base: 0.0,
            volume_quote: 0.0,
            is_closed: false,
            minutes_covered: 0,
            expected_minutes: interval_minutes,
        });
        apply_record_to_bar(entry, bar);
    }

    let mut records = grouped.into_values().collect::<Vec<_>>();
    for record in &mut records {
        record.is_closed = bar_is_closed(
            record.close_time,
            current_minute_close,
            record.minutes_covered,
            record.expected_minutes,
        );
    }
    if records.len() > limit {
        records = records.split_off(records.len() - limit);
    }
    records
}

fn build_interval_bars_from_records(
    bars: &[KlineHistoryBar],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<serde_json::Value> {
    build_interval_bar_records_from_records(bars, interval_minutes, limit, current_minute_close)
        .into_iter()
        .map(bar_to_json)
        .collect()
}

fn build_interval_bars_with_db(
    history: &[MinuteHistory],
    db_bars: &[KlineHistoryBar],
    interval_minutes: i64,
    limit: usize,
    current_minute_close: DateTime<Utc>,
) -> Vec<serde_json::Value> {
    let mut merged = BTreeMap::<DateTime<Utc>, KlineHistoryBar>::new();
    for bar in db_bars {
        merged.insert(bar.open_time, bar.clone());
    }
    for bar in
        build_interval_bar_records(history, interval_minutes, usize::MAX, current_minute_close)
    {
        let preserve_existing_db_bar = merged
            .get(&bar.open_time)
            .map(|existing| should_preserve_existing_bar(existing, &bar))
            .unwrap_or(false);
        if !preserve_existing_db_bar {
            merged.insert(bar.open_time, bar);
        }
    }

    let mut bars = merged.into_values().map(bar_to_json).collect::<Vec<_>>();
    if bars.len() > limit {
        bars = bars.split_off(bars.len() - limit);
    }
    bars
}

fn merge_interval_bar_records(
    db_bars: &[KlineHistoryBar],
    in_mem_bars: &[KlineHistoryBar],
) -> Vec<KlineHistoryBar> {
    let mut merged = BTreeMap::<DateTime<Utc>, KlineHistoryBar>::new();
    for bar in db_bars {
        merged.insert(bar.open_time, bar.clone());
    }
    for bar in in_mem_bars {
        let preserve_existing_db_bar = merged
            .get(&bar.open_time)
            .map(|existing| should_preserve_existing_bar(existing, bar))
            .unwrap_or(false);
        if !preserve_existing_db_bar {
            merged.insert(bar.open_time, bar.clone());
        }
    }
    merged.into_values().collect()
}

fn minute_bar_to_record(bar: &MinuteHistory) -> KlineHistoryBar {
    let open_time = bar.ts_bucket;
    let close_time = open_time + Duration::minutes(1);
    KlineHistoryBar {
        open_time,
        close_time,
        open: bar.open_price.or(bar.last_price).or(bar.close_price),
        high: bar
            .high_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(bar.open_price),
        low: bar
            .low_price
            .or(bar.close_price)
            .or(bar.last_price)
            .or(bar.open_price),
        close: bar.close_price.or(bar.last_price).or(bar.open_price),
        volume_base: bar.total_qty,
        volume_quote: bar.total_notional,
        is_closed: true,
        minutes_covered: 1,
        expected_minutes: 1,
    }
}

fn bar_has_any_price(bar: &KlineHistoryBar) -> bool {
    bar.open.is_some() || bar.high.is_some() || bar.low.is_some() || bar.close.is_some()
}

fn bar_has_full_coverage(bar: &KlineHistoryBar) -> bool {
    bar.minutes_covered >= bar.expected_minutes.max(1)
}

fn bar_is_closed(
    close_time: DateTime<Utc>,
    current_minute_close: DateTime<Utc>,
    minutes_covered: i64,
    expected_minutes: i64,
) -> bool {
    close_time <= current_minute_close && minutes_covered >= expected_minutes.max(1)
}

fn should_preserve_existing_bar(existing: &KlineHistoryBar, candidate: &KlineHistoryBar) -> bool {
    if !bar_has_any_price(existing) {
        return false;
    }
    let existing_complete = bar_has_full_coverage(existing);
    let candidate_complete = bar_has_full_coverage(candidate);
    (!candidate_complete && existing_complete)
        || existing.minutes_covered > candidate.minutes_covered
}

fn apply_record_to_bar(target: &mut KlineHistoryBar, source: &KlineHistoryBar) {
    if target.open.is_none() {
        target.open = source.open;
    }
    if let Some(value) = source.high {
        target.high = Some(target.high.map_or(value, |prev| prev.max(value)));
    }
    if let Some(value) = source.low {
        target.low = Some(target.low.map_or(value, |prev| prev.min(value)));
    }
    if source.close.is_some() {
        target.close = source.close;
    }
    target.volume_base += source.volume_base;
    target.volume_quote += source.volume_quote;
    target.minutes_covered += source.minutes_covered;
}

fn bar_to_json(bar: KlineHistoryBar) -> serde_json::Value {
    json!({
        "open_time": bar.open_time.to_rfc3339(),
        "close_time": bar.close_time.to_rfc3339(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume_base": bar.volume_base,
        "volume_quote": bar.volume_quote,
        "is_closed": bar.is_closed,
        "minutes_covered": bar.minutes_covered,
        "expected_minutes": bar.expected_minutes,
    })
}

fn floor_to_interval(ts: DateTime<Utc>, interval_minutes: i64) -> DateTime<Utc> {
    let secs = interval_minutes * 60;
    let aligned = ts.timestamp().div_euclid(secs) * secs;
    DateTime::<Utc>::from_timestamp(aligned, 0).unwrap_or(ts)
}

#[cfg(test)]
mod tests {
    use super::{
        build_interval_bar_records, build_interval_bar_records_from_records,
        build_interval_bars_with_db, floor_to_interval,
    };
    use crate::indicators::context::KlineHistoryBar;
    use crate::ingest::decoder::MarketKind;
    use crate::runtime::state_store::MinuteHistory;
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn empty_minute(ts_bucket: chrono::DateTime<Utc>) -> MinuteHistory {
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

    fn priced_minute(ts_bucket: chrono::DateTime<Utc>, price: f64) -> MinuteHistory {
        MinuteHistory {
            open_price: Some(price),
            high_price: Some(price),
            low_price: Some(price),
            close_price: Some(price),
            last_price: Some(price),
            total_qty: 1.0,
            total_notional: price,
            ..empty_minute(ts_bucket)
        }
    }

    fn db_bar(
        open_time: chrono::DateTime<Utc>,
        price: f64,
        interval_minutes: i64,
    ) -> KlineHistoryBar {
        KlineHistoryBar {
            open_time,
            close_time: open_time + Duration::minutes(interval_minutes),
            open: Some(price),
            high: Some(price + 10.0),
            low: Some(price - 10.0),
            close: Some(price + 1.0),
            volume_base: 100.0,
            volume_quote: 1000.0,
            is_closed: true,
            minutes_covered: interval_minutes,
            expected_minutes: interval_minutes,
        }
    }

    #[test]
    fn empty_in_memory_bar_does_not_override_db_bar() {
        let minute = Utc
            .with_ymd_and_hms(2026, 3, 10, 16, 5, 0)
            .single()
            .unwrap();
        let open_time = floor_to_interval(minute, 240);
        let bars = build_interval_bars_with_db(
            &[empty_minute(minute)],
            &[db_bar(open_time, 2000.0, 240)],
            240,
            10,
            open_time + Duration::minutes(240),
        );

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0]["open"], json!(2000.0));
        assert_eq!(bars[0]["close"], json!(2001.0));
        assert_eq!(bars[0]["minutes_covered"], json!(240));
    }

    #[test]
    fn incomplete_priced_in_memory_bar_does_not_override_complete_db_bar() {
        let minute = Utc
            .with_ymd_and_hms(2026, 3, 10, 16, 5, 0)
            .single()
            .unwrap();
        let open_time = floor_to_interval(minute, 240);
        let bars = build_interval_bars_with_db(
            &[priced_minute(minute, 2100.0)],
            &[db_bar(open_time, 2000.0, 240)],
            240,
            10,
            open_time + Duration::minutes(240),
        );

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0]["open"], json!(2000.0));
        assert_eq!(bars[0]["close"], json!(2001.0));
        assert_eq!(bars[0]["minutes_covered"], json!(240));
    }

    #[test]
    fn truncated_in_memory_interval_is_not_marked_closed() {
        let start = Utc
            .with_ymd_and_hms(2026, 3, 10, 16, 0, 0)
            .single()
            .unwrap();
        let bars = build_interval_bar_records(
            &[
                priced_minute(start, 100.0),
                priced_minute(start + Duration::minutes(1), 101.0),
            ],
            240,
            10,
            start + Duration::minutes(240),
        );

        assert_eq!(bars.len(), 1);
        assert!(!bars[0].is_closed);
        assert_eq!(bars[0].minutes_covered, 2);
        assert_eq!(bars[0].expected_minutes, 240);
    }

    #[test]
    fn daily_records_can_be_aggregated_into_3d_bars() {
        let current_close = Utc.with_ymd_and_hms(2026, 3, 7, 0, 0, 0).single().unwrap();
        let start = floor_to_interval(current_close - Duration::days(6), 4320);
        let bars = vec![
            db_bar(start, 100.0, 1440),
            db_bar(start + Duration::days(1), 110.0, 1440),
            db_bar(start + Duration::days(2), 120.0, 1440),
            db_bar(start + Duration::days(3), 90.0, 1440),
            db_bar(start + Duration::days(4), 80.0, 1440),
            db_bar(start + Duration::days(5), 70.0, 1440),
        ];

        let aggregated = build_interval_bar_records_from_records(&bars, 4320, 10, current_close);

        assert_eq!(aggregated.len(), 2);
        assert_eq!(aggregated[0].open, Some(100.0));
        assert_eq!(aggregated[0].close, Some(121.0));
        assert_eq!(aggregated[0].minutes_covered, 4320);
        assert!(aggregated[0].is_closed);
        assert_eq!(aggregated[1].open, Some(90.0));
        assert_eq!(aggregated[1].close, Some(71.0));
        assert_eq!(aggregated[1].minutes_covered, 4320);
        assert!(aggregated[1].is_closed);
    }

    #[test]
    fn incomplete_daily_records_do_not_produce_closed_multiday_bar() {
        let current_close = Utc.with_ymd_and_hms(2026, 3, 10, 0, 0, 0).single().unwrap();
        let start = floor_to_interval(current_close - Duration::days(6), 10_080);
        let bars = vec![
            db_bar(start, 100.0, 1440),
            db_bar(start + Duration::days(1), 110.0, 1440),
            db_bar(start + Duration::days(2), 120.0, 1440),
        ];

        let aggregated = build_interval_bar_records_from_records(&bars, 10_080, 10, current_close);

        assert_eq!(aggregated.len(), 1);
        assert!(!aggregated[0].is_closed);
        assert_eq!(aggregated[0].minutes_covered, 4320);
        assert_eq!(aggregated[0].expected_minutes, 10_080);
    }
}
