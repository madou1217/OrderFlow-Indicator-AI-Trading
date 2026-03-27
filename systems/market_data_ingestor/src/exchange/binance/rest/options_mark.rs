use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOptionMarkRecord {
    pub symbol: String,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default, rename = "bidIV")]
    pub bid_iv: Option<String>,
    #[serde(default, rename = "askIV")]
    pub ask_iv: Option<String>,
    #[serde(default, rename = "markIV")]
    pub mark_iv: Option<String>,
    #[serde(default)]
    pub delta: Option<String>,
    #[serde(default)]
    pub gamma: Option<String>,
    #[serde(default)]
    pub vega: Option<String>,
    #[serde(default)]
    pub theta: Option<String>,
    #[serde(default)]
    pub risk_free_interest: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::BinanceOptionMarkRecord;

    #[test]
    fn deserialize_iv_fields_from_binance_acronym_keys() {
        let payload = r#"{
            "symbol":"BTC-260327-100000-C",
            "markPrice":"0.001",
            "bidIV":"-1.0",
            "askIV":"8.73296738",
            "markIV":"1.131",
            "delta":"0.0",
            "gamma":"0.0",
            "vega":"0.0",
            "theta":"0.0",
            "riskFreeInterest":"0.7561"
        }"#;

        let row: BinanceOptionMarkRecord = serde_json::from_str(payload).unwrap();
        assert_eq!(row.bid_iv.as_deref(), Some("-1.0"));
        assert_eq!(row.ask_iv.as_deref(), Some("8.73296738"));
        assert_eq!(row.mark_iv.as_deref(), Some("1.131"));
        assert_eq!(row.risk_free_interest.as_deref(), Some("0.7561"));
    }
}
