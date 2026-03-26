use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOpenInterest {
    pub symbol: String,
    pub open_interest: String,
    pub time: i64,
}
