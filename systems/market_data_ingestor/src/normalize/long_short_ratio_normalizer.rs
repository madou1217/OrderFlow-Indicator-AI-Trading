use crate::exchange::binance::rest::long_short_ratio::BinanceLongShortRatioRecord;
use crate::normalize::{utc_from_millis, NormalizedMdEvent};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::json;

pub fn normalize_5m_rest(
    market: &str,
    symbol: &str,
    ratio_type: &str,
    stream_name: &str,
    record: &BinanceLongShortRatioRecord,
    backfill_in_progress: bool,
) -> Result<NormalizedMdEvent> {
    let ts_bucket = utc_from_millis(record.timestamp)?;
    let symbol_up = symbol.to_uppercase();
    let symbol_low = symbol.to_lowercase();
    let long_short_ratio = record
        .long_short_ratio
        .parse::<f64>()
        .with_context(|| format!("parse long_short_ratio {}", record.long_short_ratio))?;
    let long_account_ratio = record
        .long_account
        .parse::<f64>()
        .with_context(|| format!("parse long_account {}", record.long_account))?;
    let short_account_ratio = record
        .short_account
        .parse::<f64>()
        .with_context(|| format!("parse short_account {}", record.short_account))?;

    Ok(NormalizedMdEvent {
        msg_type: "md.long_short_ratio_5m".to_string(),
        market: market.to_string(),
        symbol: symbol_up,
        source_kind: "rest".to_string(),
        backfill_in_progress,
        routing_key: format!(
            "md.{}.long_short_ratio.{}.5m.{}",
            market, ratio_type, symbol_low
        ),
        stream_name: stream_name.to_string(),
        event_ts: ts_bucket,
        data: json!({
            "stream_name": stream_name,
            "ts_recv": Utc::now().to_rfc3339(),
            "ts_bucket": ts_bucket.to_rfc3339(),
            "ts_effective": ts_bucket.to_rfc3339(),
            "ratio_type": ratio_type,
            "long_short_ratio": long_short_ratio,
            "long_account_ratio": long_account_ratio,
            "short_account_ratio": short_account_ratio,
            "payload_json": {},
        }),
    })
}
