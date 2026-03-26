use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceLongShortRatioRecord {
    pub symbol: String,
    pub long_account: String,
    pub short_account: String,
    pub long_short_ratio: String,
    pub timestamp: i64,
}
