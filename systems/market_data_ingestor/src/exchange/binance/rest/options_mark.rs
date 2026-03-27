use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOptionMarkRecord {
    pub symbol: String,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub bid_iv: Option<String>,
    #[serde(default)]
    pub ask_iv: Option<String>,
    #[serde(default)]
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
