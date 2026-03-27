use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOptionIndexPrice {
    pub index_price: String,
    pub time: i64,
}
