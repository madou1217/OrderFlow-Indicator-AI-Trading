use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOpenInterestHistRecord {
    pub symbol: String,
    pub sum_open_interest: String,
    pub sum_open_interest_value: String,
    #[serde(rename = "CMCCirculatingSupply")]
    pub cmc_circulating_supply: Option<String>,
    pub timestamp: i64,
}
