use crate::exchange::binance::rest::options_exchange_info::BinanceOptionSymbolInfo;
use crate::exchange::binance::rest::options_mark::BinanceOptionMarkRecord;
use crate::normalize::{utc_from_millis, NormalizedMdEvent};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

pub fn normalize_mark_greeks_5m_rest(
    underlying_symbol: &str,
    ts_bucket: DateTime<Utc>,
    meta: &BinanceOptionSymbolInfo,
    index_price: Option<f64>,
    mark: &BinanceOptionMarkRecord,
) -> Result<NormalizedMdEvent> {
    let symbol_up = underlying_symbol.to_ascii_uppercase();
    let symbol_low = symbol_up.to_ascii_lowercase();
    let expiry_ts = utc_from_millis(meta.expiry_date)?;
    let strike_price = parse_value_f64(&meta.strike_price)
        .with_context(|| format!("parse strike_price for {}", meta.symbol))?;
    let unit = meta.unit.as_ref().map(parse_value_f64).transpose()?;
    let underlying_asset = underlying_asset_from_symbol(&meta.underlying);

    Ok(NormalizedMdEvent {
        msg_type: "md.option_mark_greeks_5m".to_string(),
        market: "futures".to_string(),
        symbol: symbol_up,
        source_kind: "rest".to_string(),
        backfill_in_progress: false,
        routing_key: format!("md.futures.option_mark_greeks.5m.{}", symbol_low),
        stream_name: "eapi/v1/mark".to_string(),
        event_ts: ts_bucket,
        data: json!({
            "stream_name": "eapi/v1/mark",
            "ts_recv": Utc::now().to_rfc3339(),
            "ts_bucket": ts_bucket.to_rfc3339(),
            "ts_effective": ts_bucket.to_rfc3339(),
            "option_symbol": meta.symbol,
            "underlying_asset": underlying_asset,
            "expiry_ts": expiry_ts.to_rfc3339(),
            "strike_price": strike_price,
            "contract_side": meta.side,
            "unit": unit,
            "index_price": index_price,
            "mark_price": parse_optional_num(mark.mark_price.as_deref())?,
            "bid_iv": parse_optional_iv(mark.bid_iv.as_deref())?,
            "ask_iv": parse_optional_iv(mark.ask_iv.as_deref())?,
            "mark_iv": parse_optional_iv(mark.mark_iv.as_deref())?,
            "delta": parse_optional_num(mark.delta.as_deref())?,
            "gamma": parse_optional_num(mark.gamma.as_deref())?,
            "vega": parse_optional_num(mark.vega.as_deref())?,
            "theta": parse_optional_num(mark.theta.as_deref())?,
            "risk_free_interest": parse_optional_num(mark.risk_free_interest.as_deref())?,
            "payload_json": {},
        }),
    })
}

fn parse_value_f64(value: &Value) -> Result<f64> {
    match value {
        Value::String(v) => v
            .parse::<f64>()
            .with_context(|| format!("parse numeric string {}", v)),
        Value::Number(v) => v
            .as_f64()
            .ok_or_else(|| anyhow::anyhow!("invalid numeric value {}", v)),
        _ => anyhow::bail!("unsupported numeric value {:?}", value),
    }
}

fn parse_optional_num(value: Option<&str>) -> Result<Option<f64>> {
    value
        .map(|raw| raw.parse::<f64>().with_context(|| format!("parse numeric {}", raw)))
        .transpose()
}

fn parse_optional_iv(value: Option<&str>) -> Result<Option<f64>> {
    match parse_optional_num(value)? {
        Some(v) if v < 0.0 => Ok(None),
        other => Ok(other),
    }
}

fn underlying_asset_from_symbol(underlying: &str) -> String {
    underlying
        .strip_suffix("USDT")
        .or_else(|| underlying.strip_suffix("USD"))
        .unwrap_or(underlying)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_optional_iv;

    #[test]
    fn negative_iv_sentinel_is_treated_as_missing() {
        assert_eq!(parse_optional_iv(Some("-1.0")).unwrap(), None);
        assert_eq!(parse_optional_iv(Some("1.131")).unwrap(), Some(1.131));
        assert_eq!(parse_optional_iv(None).unwrap(), None);
    }
}
