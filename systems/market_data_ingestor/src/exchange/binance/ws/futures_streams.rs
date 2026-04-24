pub fn public_streams(symbol: &str) -> Vec<String> {
    let s = symbol.to_lowercase();
    vec![format!("{}@depth@100ms", s), format!("{}@bookTicker", s)]
}

pub fn market_streams(symbol: &str) -> Vec<String> {
    let s = symbol.to_lowercase();
    vec![
        format!("{}@aggTrade", s),
        format!("{}@markPrice@1s", s),
        format!("{}@forceOrder", s),
        format!("{}@kline_1m", s),
        format!("{}@kline_15m", s),
        format!("{}@kline_1h", s),
        format!("{}@kline_4h", s),
        format!("{}@kline_1d", s),
    ]
}

pub fn required_streams(symbol: &str) -> Vec<String> {
    let mut streams = public_streams(symbol);
    streams.extend(market_streams(symbol));
    streams
}
