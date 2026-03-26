use crate::exchange::binance::rest::open_interest::BinanceOpenInterest;
use crate::exchange::binance::rest::open_interest_hist::BinanceOpenInterestHistRecord;
use crate::normalize::{utc_from_millis, NormalizedMdEvent};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde_json::json;

pub fn normalize_current_rest(
    market: &str,
    symbol: &str,
    record: &BinanceOpenInterest,
    mark_price: Option<f64>,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let event_ts = utc_from_millis(record.time)?;
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let open_interest_contracts = record
        .open_interest
        .parse::<f64>()
        .with_context(|| format!("parse open interest contracts {}", record.open_interest))?;
    let open_interest_value_usdt = mark_price.map(|px| open_interest_contracts * px);

    Ok(NormalizedMdEvent {
        msg_type: "md.open_interest_current".to_string(),
        market: market.to_string(),
        symbol: symbol_up,
        source_kind: "rest".to_string(),
        backfill_in_progress,
        routing_key: format!("md.{}.open_interest.current.{}", market, symbol_low),
        stream_name: "fapi/v1/openInterest".to_string(),
        event_ts,
        data: json!({
            "stream_name": "fapi/v1/openInterest",
            "ts_recv": Utc::now().to_rfc3339(),
            "ts_effective": event_ts.to_rfc3339(),
            "open_interest_contracts": open_interest_contracts,
            "mark_price": mark_price,
            "open_interest_value_usdt": open_interest_value_usdt,
            "payload_json": {},
        }),
    })
}

pub fn normalize_hist_5m_rest(
    market: &str,
    symbol: &str,
    record: &BinanceOpenInterestHistRecord,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let ts_bucket = utc_from_millis(record.timestamp)?;
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let open_interest_contracts = record
        .sum_open_interest
        .parse::<f64>()
        .with_context(|| format!("parse sum open interest {}", record.sum_open_interest))?;
    let open_interest_value_usdt =
        record
            .sum_open_interest_value
            .parse::<f64>()
            .with_context(|| {
                format!(
                    "parse sum open interest value {}",
                    record.sum_open_interest_value
                )
            })?;
    let reference_price = if open_interest_contracts.abs() > f64::EPSILON {
        Some(open_interest_value_usdt / open_interest_contracts)
    } else {
        return Err(anyhow!(
            "open interest contracts is zero for symbol={} ts_bucket={}",
            symbol_up,
            ts_bucket
        ));
    };

    Ok(NormalizedMdEvent {
        msg_type: "md.open_interest_hist_5m".to_string(),
        market: market.to_string(),
        symbol: symbol_up,
        source_kind: "rest".to_string(),
        backfill_in_progress,
        routing_key: format!("md.{}.open_interest.5m.{}", market, symbol_low),
        stream_name: "futures/data/openInterestHist".to_string(),
        event_ts: ts_bucket,
        data: json!({
            "stream_name": "futures/data/openInterestHist",
            "ts_recv": Utc::now().to_rfc3339(),
            "ts_bucket": ts_bucket.to_rfc3339(),
            "ts_effective": ts_bucket.to_rfc3339(),
            "open_interest_contracts": open_interest_contracts,
            "open_interest_value_usdt": open_interest_value_usdt,
            "reference_price": reference_price,
            "payload_json": {
                "cmc_circulating_supply": record.cmc_circulating_supply,
            },
        }),
    })
}
