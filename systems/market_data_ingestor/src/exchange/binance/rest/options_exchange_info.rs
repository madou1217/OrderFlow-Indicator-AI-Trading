use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOptionsExchangeInfo {
    #[serde(default)]
    pub option_symbols: Vec<BinanceOptionSymbolInfo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOptionSymbolInfo {
    pub symbol: String,
    pub underlying: String,
    pub expiry_date: i64,
    pub side: String,
    pub strike_price: Value,
    #[serde(default)]
    pub unit: Option<Value>,
    #[serde(default)]
    pub status: Option<String>,
}
