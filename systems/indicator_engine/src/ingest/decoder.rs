use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MarketKind {
    Spot,
    Futures,
}

impl MarketKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Futures => "futures",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineEvent {
    pub schema_version: i32,
    pub msg_type: String,
    pub message_id: Uuid,
    pub trace_id: Uuid,
    pub routing_key: String,
    pub market: MarketKind,
    pub symbol: String,
    pub source_kind: String,
    pub backfill_in_progress: bool,
    pub event_ts: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub data: MdData,
}

#[derive(Debug, Clone)]
pub struct EngineEventEnvelope {
    pub schema_version: i32,
    pub msg_type: String,
    pub message_id: Uuid,
    pub trace_id: Uuid,
    pub routing_key: String,
    pub market: String,
    pub symbol: String,
    pub source_kind: String,
    pub backfill_in_progress: bool,
    pub event_ts: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum MdData {
    Trade(TradeEvent),
    Depth(DepthDeltaEvent),
    OrderbookSnapshot(OrderbookSnapshotEvent),
    Bbo(BboEvent),
    Kline(KlineEvent),
    MarkPrice(MarkPriceEvent),
    FundingRate(FundingRateEvent),
    ForceOrder(ForceOrderEvent),
    AggTrade1m(AggTrade1mEvent),
    AggOrderbook1m(AggOrderbook1mEvent),
    AggLiq1m(AggLiq1mEvent),
    AggFundingMark1m(AggFundingMark1mEvent),
    OpenInterestCurrent(OpenInterestCurrentEvent),
    OpenInterestHist5m(OpenInterestHist5mEvent),
    LongShortRatio5m(LongShortRatio5mEvent),
    OptionMarkGreeks5m(OptionMarkGreeks5mEvent),
}

#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub price: f64,
    pub qty_eth: f64,
    pub notional_usdt: f64,
    pub aggressor_side: i8,
}

#[derive(Debug, Clone)]
pub struct DepthDeltaEvent {
    pub first_update_id: Option<i64>,
    pub final_update_id: Option<i64>,
    pub prev_final_update_id: Option<i64>,
    pub bids_delta: Vec<(f64, f64)>,
    pub asks_delta: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct OrderbookSnapshotEvent {
    pub depth_levels: i64,
    pub last_update_id: Option<i64>,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct BboEvent {
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
    pub sample_count: i64,
}

#[derive(Debug, Clone)]
pub struct KlineEvent {
    pub interval_code: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume_base: Option<f64>,
    pub quote_volume: Option<f64>,
    pub trade_count: Option<i64>,
    pub is_closed: bool,
}

#[derive(Debug, Clone)]
pub struct MarkPriceEvent {
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub estimated_settle_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct FundingRateEvent {
    pub funding_time: DateTime<Utc>,
    pub funding_rate: f64,
    pub mark_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ForceOrderEvent {
    pub order_side: String,
    pub price: Option<f64>,
    pub filled_qty: Option<f64>,
    pub notional_usdt: Option<f64>,
    pub liq_side: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggWhaleStats {
    pub trade_count: i64,
    pub buy_count: i64,
    pub sell_count: i64,
    pub notional_total: f64,
    pub notional_buy: f64,
    pub notional_sell: f64,
    pub qty_eth_total: f64,
    pub qty_eth_buy: f64,
    pub qty_eth_sell: f64,
    pub max_single_notional: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggVpinSnapshot {
    pub current_buy: f64,
    pub current_sell: f64,
    pub current_fill: f64,
    pub imbalances: Vec<f64>,
    pub imbalance_sum: f64,
    pub last_vpin: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggProfileLevel {
    pub price: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggTrade1mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub chunk_start_ts: DateTime<Utc>,
    pub chunk_end_ts: DateTime<Utc>,
    pub source_event_count: i64,
    pub trade_count: i64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub first_price: Option<f64>,
    pub last_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub profile_levels: Vec<AggProfileLevel>,
    pub whale: AggWhaleStats,
    pub vpin_snapshot: Option<AggVpinSnapshot>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggHeatmapLevel {
    pub price: f64,
    pub bid_liquidity: f64,
    pub ask_liquidity: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggOrderbook1mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub chunk_start_ts: DateTime<Utc>,
    pub chunk_end_ts: DateTime<Utc>,
    pub source_event_count: i64,
    pub sample_count: i64,
    pub bbo_updates: i64,
    pub spread_sum: f64,
    pub topk_depth_sum: f64,
    pub obi_sum: f64,
    pub obi_l1_sum: f64,
    pub obi_k_sum: f64,
    pub obi_k_dw_sum: f64,
    pub obi_k_dw_change_sum: f64,
    pub obi_k_dw_adj_sum: f64,
    pub microprice_sum: f64,
    pub microprice_classic_sum: f64,
    pub microprice_kappa_sum: f64,
    pub microprice_adj_sum: f64,
    pub ofi_sum: f64,
    pub obi_k_dw_close: Option<f64>,
    pub heatmap_levels: Vec<AggHeatmapLevel>,
    pub heatmap_loaded: bool,
}

impl AggOrderbook1mEvent {
    pub fn scalar_eq(&self, other: &Self) -> bool {
        self.ts_bucket == other.ts_bucket
            && self.chunk_start_ts == other.chunk_start_ts
            && self.chunk_end_ts == other.chunk_end_ts
            && self.source_event_count == other.source_event_count
            && self.sample_count == other.sample_count
            && self.bbo_updates == other.bbo_updates
            && self.spread_sum == other.spread_sum
            && self.topk_depth_sum == other.topk_depth_sum
            && self.obi_sum == other.obi_sum
            && self.obi_l1_sum == other.obi_l1_sum
            && self.obi_k_sum == other.obi_k_sum
            && self.obi_k_dw_sum == other.obi_k_dw_sum
            && self.obi_k_dw_change_sum == other.obi_k_dw_change_sum
            && self.obi_k_dw_adj_sum == other.obi_k_dw_adj_sum
            && self.microprice_sum == other.microprice_sum
            && self.microprice_classic_sum == other.microprice_classic_sum
            && self.microprice_kappa_sum == other.microprice_kappa_sum
            && self.microprice_adj_sum == other.microprice_adj_sum
            && self.ofi_sum == other.ofi_sum
            && self.obi_k_dw_close == other.obi_k_dw_close
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggLiqLevel {
    pub price: f64,
    pub long_liq: f64,
    pub short_liq: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggLiq1mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub chunk_start_ts: DateTime<Utc>,
    pub chunk_end_ts: DateTime<Utc>,
    pub source_event_count: i64,
    pub levels: Vec<AggLiqLevel>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggMarkPoint {
    pub ts: DateTime<Utc>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub estimated_settle_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggFundingPoint {
    pub ts: DateTime<Utc>,
    pub funding_time: Option<DateTime<Utc>>,
    pub funding_rate: f64,
    pub mark_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggFundingMark1mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub chunk_start_ts: DateTime<Utc>,
    pub chunk_end_ts: DateTime<Utc>,
    pub source_event_count: i64,
    pub mark_points: Vec<AggMarkPoint>,
    pub funding_points: Vec<AggFundingPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenInterestCurrentEvent {
    pub ts_effective: DateTime<Utc>,
    pub open_interest_contracts: f64,
    pub mark_price: Option<f64>,
    pub open_interest_value_usdt: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenInterestHist5mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub open_interest_contracts: f64,
    pub open_interest_value_usdt: f64,
    pub reference_price: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum LongShortRatioType {
    GlobalAccount,
    TopAccount,
    TopPosition,
}

impl LongShortRatioType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GlobalAccount => "global_account",
            Self::TopAccount => "top_account",
            Self::TopPosition => "top_position",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LongShortRatio5mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub ratio_type: LongShortRatioType,
    pub long_short_ratio: f64,
    pub long_account_ratio: Option<f64>,
    pub short_account_ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionMarkGreeks5mEvent {
    pub ts_bucket: DateTime<Utc>,
    pub option_symbol: String,
    pub underlying_asset: String,
    pub expiry_ts: DateTime<Utc>,
    pub strike_price: f64,
    pub contract_side: String,
    pub unit: Option<f64>,
    pub index_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub mark_iv: Option<f64>,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub vega: Option<f64>,
    pub theta: Option<f64>,
    pub risk_free_interest: Option<f64>,
}

fn parse_md_data(msg_type: &str, data_obj: &Value) -> Result<MdData> {
    let data = match msg_type {
        "md.trade" => MdData::Trade(parse_trade(&data_obj)?),
        "md.depth" => MdData::Depth(parse_depth(&data_obj)?),
        "md.orderbook_snapshot_l2" => MdData::OrderbookSnapshot(parse_snapshot(&data_obj)?),
        "md.bbo" => MdData::Bbo(parse_bbo(&data_obj)?),
        "md.kline" => MdData::Kline(parse_kline(&data_obj)?),
        "md.mark_price" => MdData::MarkPrice(parse_mark_price(&data_obj)?),
        "md.funding_rate" => MdData::FundingRate(parse_funding_rate(&data_obj)?),
        "md.force_order" => MdData::ForceOrder(parse_force_order(&data_obj)?),
        "md.agg.trade.1m" => MdData::AggTrade1m(parse_agg_trade_1m(&data_obj)?),
        "md.agg.orderbook.1m" => MdData::AggOrderbook1m(parse_agg_orderbook_1m(&data_obj)?),
        "md.agg.liq.1m" => MdData::AggLiq1m(parse_agg_liq_1m(&data_obj)?),
        "md.agg.funding_mark.1m" => MdData::AggFundingMark1m(parse_agg_funding_mark_1m(&data_obj)?),
        "md.open_interest_current" => {
            MdData::OpenInterestCurrent(parse_open_interest_current(&data_obj)?)
        }
        "md.open_interest_hist_5m" => {
            MdData::OpenInterestHist5m(parse_open_interest_hist_5m(&data_obj)?)
        }
        "md.long_short_ratio_5m" => MdData::LongShortRatio5m(parse_long_short_ratio_5m(&data_obj)?),
        "md.option_mark_greeks_5m" => {
            MdData::OptionMarkGreeks5m(parse_option_mark_greeks_5m(&data_obj)?)
        }
        other => return Err(anyhow!("unsupported msg_type: {}", other)),
    };

    Ok(data)
}

pub fn build_engine_event(envelope: EngineEventEnvelope, data_obj: Value) -> Result<EngineEvent> {
    let EngineEventEnvelope {
        schema_version,
        msg_type,
        message_id,
        trace_id,
        routing_key,
        market,
        symbol,
        source_kind,
        backfill_in_progress,
        event_ts,
        published_at,
    } = envelope;
    let market = parse_market(market)?;
    let symbol = symbol.to_uppercase();
    let data = parse_md_data(&msg_type, &data_obj)?;

    Ok(EngineEvent {
        schema_version,
        msg_type,
        message_id,
        trace_id,
        routing_key,
        market,
        symbol,
        source_kind,
        backfill_in_progress,
        event_ts,
        published_at,
        data,
    })
}

pub fn decode_contract_body(payload: &[u8]) -> Result<EngineEvent> {
    let root: Value = serde_json::from_slice(payload).context("decode mq body json")?;

    let envelope = EngineEventEnvelope {
        schema_version: required_i64(&root, "schema_version")? as i32,
        msg_type: required_str(&root, "msg_type")?,
        message_id: parse_uuid(required_str(&root, "message_id")?, "message_id")?,
        trace_id: parse_uuid(required_str(&root, "trace_id")?, "trace_id")?,
        routing_key: required_str(&root, "routing_key")?,
        market: required_str(&root, "market")?,
        symbol: required_str(&root, "symbol")?,
        source_kind: required_str(&root, "source_kind")?,
        backfill_in_progress: required_bool(&root, "backfill_in_progress")?,
        event_ts: parse_ts(required_str(&root, "event_ts")?, "event_ts")?,
        published_at: parse_ts(required_str(&root, "published_at")?, "published_at")?,
    };

    let data_obj = root
        .get("data")
        .ok_or_else(|| anyhow!("missing data"))?
        .clone();

    build_engine_event(envelope, data_obj)
}

fn parse_trade(data: &Value) -> Result<TradeEvent> {
    let price = required_f64(data, "price")?;
    let qty_eth = optional_f64(data, "qty_base")?
        .or(optional_f64(data, "qty_eth")?)
        .unwrap_or_else(|| required_f64(data, "qty_raw").unwrap_or(0.0));
    let notional_usdt = data
        .get("notional_usdt")
        .map(to_f64)
        .transpose()?
        .unwrap_or(price * qty_eth);
    let aggressor_side = optional_i64(data, "aggressor_side")
        .map(|v| v as i8)
        .or_else(|| {
            data.get("is_buyer_maker")
                .and_then(Value::as_bool)
                .map(|v| if v { -1 } else { 1 })
        })
        .ok_or_else(|| anyhow!("missing int field: aggressor_side"))?;

    Ok(TradeEvent {
        price,
        qty_eth,
        notional_usdt,
        aggressor_side,
    })
}

fn parse_depth(data: &Value) -> Result<DepthDeltaEvent> {
    Ok(DepthDeltaEvent {
        first_update_id: optional_i64(data, "first_update_id"),
        final_update_id: optional_i64(data, "final_update_id"),
        prev_final_update_id: optional_i64(data, "prev_final_update_id"),
        bids_delta: parse_levels(required_value(data, "bids_delta")?)?,
        asks_delta: parse_levels(required_value(data, "asks_delta")?)?,
    })
}

fn parse_snapshot(data: &Value) -> Result<OrderbookSnapshotEvent> {
    Ok(OrderbookSnapshotEvent {
        depth_levels: required_i64(data, "depth_levels")?,
        last_update_id: optional_i64(data, "last_update_id"),
        bids: parse_levels(required_value(data, "bids")?)?,
        asks: parse_levels(required_value(data, "asks")?)?,
    })
}

fn parse_bbo(data: &Value) -> Result<BboEvent> {
    let sample_count = optional_i64(data, "event_count").unwrap_or(1).max(1);
    Ok(BboEvent {
        bid_price: required_f64(data, "bid_price")?,
        bid_qty: required_f64(data, "bid_qty")?,
        ask_price: required_f64(data, "ask_price")?,
        ask_qty: required_f64(data, "ask_qty")?,
        sample_count,
    })
}

fn parse_kline(data: &Value) -> Result<KlineEvent> {
    Ok(KlineEvent {
        interval_code: required_str(data, "interval_code")?,
        open_time: parse_ts(required_str(data, "open_time")?, "open_time")?,
        close_time: parse_ts(required_str(data, "close_time")?, "close_time")?,
        open_price: required_f64(data, "open_price")?,
        high_price: required_f64(data, "high_price")?,
        low_price: required_f64(data, "low_price")?,
        close_price: required_f64(data, "close_price")?,
        volume_base: optional_f64(data, "volume_base")?,
        quote_volume: optional_f64(data, "quote_volume")?,
        trade_count: optional_i64(data, "trade_count"),
        is_closed: required_bool(data, "is_closed")?,
    })
}

fn parse_mark_price(data: &Value) -> Result<MarkPriceEvent> {
    Ok(MarkPriceEvent {
        mark_price: optional_f64(data, "mark_price")?,
        index_price: optional_f64(data, "index_price")?,
        estimated_settle_price: optional_f64(data, "estimated_settle_price")?,
        funding_rate: optional_f64(data, "funding_rate")?,
        next_funding_time: optional_ts(data, "next_funding_time")?,
    })
}

fn parse_funding_rate(data: &Value) -> Result<FundingRateEvent> {
    Ok(FundingRateEvent {
        funding_time: parse_ts(required_str(data, "funding_time")?, "funding_time")?,
        funding_rate: required_f64(data, "funding_rate")?,
        mark_price: optional_f64(data, "mark_price")?,
        next_funding_time: optional_ts(data, "next_funding_time")?,
    })
}

fn parse_force_order(data: &Value) -> Result<ForceOrderEvent> {
    Ok(ForceOrderEvent {
        order_side: required_str(data, "order_side")?,
        price: optional_f64(data, "price")?,
        filled_qty: optional_f64(data, "filled_qty")?,
        notional_usdt: optional_f64(data, "notional_usdt")?,
        liq_side: required_str(data, "liq_side")?,
    })
}

fn parse_agg_trade_1m(data: &Value) -> Result<AggTrade1mEvent> {
    let whale = required_value(data, "whale")?;
    let qty_eth_total = optional_f64(&whale, "qty_base_total")?
        .or(optional_f64(&whale, "qty_eth_total")?)
        .unwrap_or(0.0);
    let qty_eth_buy_opt =
        optional_f64(&whale, "qty_base_buy")?.or(optional_f64(&whale, "qty_eth_buy")?);
    let qty_eth_sell_opt =
        optional_f64(&whale, "qty_base_sell")?.or(optional_f64(&whale, "qty_eth_sell")?);
    let delta_qty_opt =
        optional_f64(&whale, "delta_qty_base")?.or(optional_f64(&whale, "delta_qty_eth")?);
    let (qty_eth_buy, qty_eth_sell, _delta_qty_eth) = match (qty_eth_buy_opt, qty_eth_sell_opt) {
        (Some(buy), Some(sell)) => (buy, sell, buy - sell),
        _ => {
            if let Some(delta_qty) = delta_qty_opt {
                // Backward-compatible reconstruction for historical payloads that only carried
                // total + signed delta.
                let buy = ((qty_eth_total + delta_qty) / 2.0).max(0.0);
                let sell = ((qty_eth_total - delta_qty) / 2.0).max(0.0);
                (buy, sell, delta_qty)
            } else {
                (0.0, 0.0, 0.0)
            }
        }
    };
    Ok(AggTrade1mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_bucket")?, "ts_bucket")?,
        chunk_start_ts: parse_ts(required_str(data, "chunk_start_ts")?, "chunk_start_ts")?,
        chunk_end_ts: parse_ts(required_str(data, "chunk_end_ts")?, "chunk_end_ts")?,
        source_event_count: optional_i64(data, "source_event_count").unwrap_or(0),
        trade_count: optional_i64(data, "trade_count").unwrap_or(0),
        buy_qty: optional_f64(data, "buy_qty")?.unwrap_or(0.0),
        sell_qty: optional_f64(data, "sell_qty")?.unwrap_or(0.0),
        buy_notional: optional_f64(data, "buy_notional")?.unwrap_or(0.0),
        sell_notional: optional_f64(data, "sell_notional")?.unwrap_or(0.0),
        first_price: optional_f64(data, "first_price")?,
        last_price: optional_f64(data, "last_price")?,
        high_price: optional_f64(data, "high_price")?,
        low_price: optional_f64(data, "low_price")?,
        profile_levels: parse_profile_levels(required_value(data, "profile_levels")?)?,
        whale: AggWhaleStats {
            trade_count: optional_i64(&whale, "trade_count").unwrap_or(0),
            buy_count: optional_i64(&whale, "buy_count").unwrap_or(0),
            sell_count: optional_i64(&whale, "sell_count").unwrap_or(0),
            notional_total: optional_f64(&whale, "notional_total")?.unwrap_or(0.0),
            notional_buy: optional_f64(&whale, "notional_buy")?.unwrap_or(0.0),
            notional_sell: optional_f64(&whale, "notional_sell")?.unwrap_or(0.0),
            qty_eth_total,
            qty_eth_buy,
            qty_eth_sell,
            max_single_notional: optional_f64(&whale, "max_single_notional")?.unwrap_or(0.0),
        },
        vpin_snapshot: parse_optional_vpin_snapshot(
            data.get("payload_json")
                .and_then(|payload| payload.get("vpin_state")),
        )?,
    })
}

fn parse_optional_vpin_snapshot(value: Option<&Value>) -> Result<Option<AggVpinSnapshot>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let imbalances = value
        .get("imbalances")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| to_f64(item).ok())
        .collect::<Vec<_>>();
    Ok(Some(AggVpinSnapshot {
        current_buy: optional_f64(value, "current_buy")?.unwrap_or(0.0),
        current_sell: optional_f64(value, "current_sell")?.unwrap_or(0.0),
        current_fill: optional_f64(value, "current_fill")?.unwrap_or(0.0),
        imbalances,
        imbalance_sum: optional_f64(value, "imbalance_sum")?.unwrap_or(0.0),
        last_vpin: optional_f64(value, "last_vpin")?.unwrap_or(0.0),
    }))
}

fn parse_agg_orderbook_1m(data: &Value) -> Result<AggOrderbook1mEvent> {
    Ok(AggOrderbook1mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_bucket")?, "ts_bucket")?,
        chunk_start_ts: parse_ts(required_str(data, "chunk_start_ts")?, "chunk_start_ts")?,
        chunk_end_ts: parse_ts(required_str(data, "chunk_end_ts")?, "chunk_end_ts")?,
        source_event_count: optional_i64(data, "source_event_count").unwrap_or(0),
        sample_count: optional_i64(data, "sample_count").unwrap_or(0),
        bbo_updates: optional_i64(data, "bbo_updates").unwrap_or(0),
        spread_sum: optional_f64(data, "spread_sum")?.unwrap_or(0.0),
        topk_depth_sum: optional_f64(data, "topk_depth_sum")?.unwrap_or(0.0),
        obi_sum: optional_f64(data, "obi_sum")?.unwrap_or(0.0),
        obi_l1_sum: optional_f64(data, "obi_l1_sum")?.unwrap_or(0.0),
        obi_k_sum: optional_f64(data, "obi_k_sum")?.unwrap_or(0.0),
        obi_k_dw_sum: optional_f64(data, "obi_k_dw_sum")?.unwrap_or(0.0),
        obi_k_dw_change_sum: optional_f64(data, "obi_k_dw_change_sum")?.unwrap_or(0.0),
        obi_k_dw_adj_sum: optional_f64(data, "obi_k_dw_adj_sum")?.unwrap_or(0.0),
        microprice_sum: optional_f64(data, "microprice_sum")?.unwrap_or(0.0),
        microprice_classic_sum: optional_f64(data, "microprice_classic_sum")?.unwrap_or(0.0),
        microprice_kappa_sum: optional_f64(data, "microprice_kappa_sum")?.unwrap_or(0.0),
        microprice_adj_sum: optional_f64(data, "microprice_adj_sum")?.unwrap_or(0.0),
        ofi_sum: optional_f64(data, "ofi_sum")?.unwrap_or(0.0),
        obi_k_dw_close: optional_f64(data, "obi_k_dw_close")?,
        heatmap_levels: parse_heatmap_levels(required_value(data, "heatmap_levels")?)?,
        heatmap_loaded: optional_bool(data, "heatmap_loaded").unwrap_or(true),
    })
}

fn parse_agg_liq_1m(data: &Value) -> Result<AggLiq1mEvent> {
    Ok(AggLiq1mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_bucket")?, "ts_bucket")?,
        chunk_start_ts: parse_ts(required_str(data, "chunk_start_ts")?, "chunk_start_ts")?,
        chunk_end_ts: parse_ts(required_str(data, "chunk_end_ts")?, "chunk_end_ts")?,
        source_event_count: optional_i64(data, "source_event_count").unwrap_or(0),
        levels: parse_liq_levels(required_value(data, "force_liq_levels")?)?,
    })
}

fn parse_agg_funding_mark_1m(data: &Value) -> Result<AggFundingMark1mEvent> {
    Ok(AggFundingMark1mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_bucket")?, "ts_bucket")?,
        chunk_start_ts: parse_ts(required_str(data, "chunk_start_ts")?, "chunk_start_ts")?,
        chunk_end_ts: parse_ts(required_str(data, "chunk_end_ts")?, "chunk_end_ts")?,
        source_event_count: optional_i64(data, "source_event_count").unwrap_or(0),
        mark_points: parse_mark_points(required_value(data, "mark_points")?)?,
        funding_points: parse_funding_points(required_value(data, "funding_points")?)?,
    })
}

fn parse_open_interest_current(data: &Value) -> Result<OpenInterestCurrentEvent> {
    Ok(OpenInterestCurrentEvent {
        ts_effective: parse_ts(required_str(data, "ts_effective")?, "ts_effective")?,
        open_interest_contracts: required_f64(data, "open_interest_contracts")?,
        mark_price: optional_f64(data, "mark_price")?,
        open_interest_value_usdt: optional_f64(data, "open_interest_value_usdt")?,
    })
}

fn parse_open_interest_hist_5m(data: &Value) -> Result<OpenInterestHist5mEvent> {
    let open_interest_contracts = required_f64(data, "open_interest_contracts")?;
    let open_interest_value_usdt = required_f64(data, "open_interest_value_usdt")?;
    let reference_price = if open_interest_contracts.abs() > f64::EPSILON {
        Some(open_interest_value_usdt / open_interest_contracts)
    } else {
        None
    };
    Ok(OpenInterestHist5mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_effective")?, "ts_effective")?,
        open_interest_contracts,
        open_interest_value_usdt,
        reference_price,
    })
}

fn parse_long_short_ratio_5m(data: &Value) -> Result<LongShortRatio5mEvent> {
    Ok(LongShortRatio5mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_effective")?, "ts_effective")?,
        ratio_type: parse_long_short_ratio_type(&required_str(data, "ratio_type")?)?,
        long_short_ratio: required_f64(data, "long_short_ratio")?,
        long_account_ratio: optional_f64(data, "long_account_ratio")?,
        short_account_ratio: optional_f64(data, "short_account_ratio")?,
    })
}

fn parse_option_mark_greeks_5m(data: &Value) -> Result<OptionMarkGreeks5mEvent> {
    Ok(OptionMarkGreeks5mEvent {
        ts_bucket: parse_ts(required_str(data, "ts_bucket")?, "ts_bucket")?,
        option_symbol: required_str(data, "option_symbol")?,
        underlying_asset: required_str(data, "underlying_asset")?,
        expiry_ts: parse_ts(required_str(data, "expiry_ts")?, "expiry_ts")?,
        strike_price: required_f64(data, "strike_price")?,
        contract_side: required_str(data, "contract_side")?,
        unit: optional_f64(data, "unit")?,
        index_price: optional_f64(data, "index_price")?,
        mark_price: optional_f64(data, "mark_price")?,
        bid_iv: optional_f64(data, "bid_iv")?,
        ask_iv: optional_f64(data, "ask_iv")?,
        mark_iv: optional_f64(data, "mark_iv")?,
        delta: optional_f64(data, "delta")?,
        gamma: optional_f64(data, "gamma")?,
        vega: optional_f64(data, "vega")?,
        theta: optional_f64(data, "theta")?,
        risk_free_interest: optional_f64(data, "risk_free_interest")?,
    })
}

fn parse_long_short_ratio_type(value: &str) -> Result<LongShortRatioType> {
    match value {
        "global_account" => Ok(LongShortRatioType::GlobalAccount),
        "top_account" => Ok(LongShortRatioType::TopAccount),
        "top_position" => Ok(LongShortRatioType::TopPosition),
        other => Err(anyhow!("unsupported long short ratio type: {}", other)),
    }
}

fn parse_profile_levels(value: Value) -> Result<Vec<AggProfileLevel>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("profile_levels must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("profile level row must be [price,buy_qty,sell_qty]"))?;
        if level.len() < 3 {
            continue;
        }
        out.push(AggProfileLevel {
            price: to_f64(&level[0])?,
            buy_qty: to_f64(&level[1])?,
            sell_qty: to_f64(&level[2])?,
        });
    }
    Ok(out)
}

fn parse_heatmap_levels(value: Value) -> Result<Vec<AggHeatmapLevel>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("heatmap_levels must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("heatmap level row must be [price,bid,ask]"))?;
        if level.len() < 3 {
            continue;
        }
        out.push(AggHeatmapLevel {
            price: to_f64(&level[0])?,
            bid_liquidity: to_f64(&level[1])?,
            ask_liquidity: to_f64(&level[2])?,
        });
    }
    Ok(out)
}

fn parse_liq_levels(value: Value) -> Result<Vec<AggLiqLevel>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("force_liq_levels must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("liq level row must be [price,long,short]"))?;
        if level.len() < 3 {
            continue;
        }
        out.push(AggLiqLevel {
            price: to_f64(&level[0])?,
            long_liq: to_f64(&level[1])?,
            short_liq: to_f64(&level[2])?,
        });
    }
    Ok(out)
}

fn parse_mark_points(value: Value) -> Result<Vec<AggMarkPoint>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("mark_points must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let obj = row
            .as_object()
            .ok_or_else(|| anyhow!("mark point row must be object"))?;
        let ts = obj
            .get("ts")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("mark point missing ts"))?;
        out.push(AggMarkPoint {
            ts: parse_ts(ts.to_owned(), "mark_points.ts")?,
            mark_price: value_to_opt_f64(obj.get("mark_price"))?,
            index_price: value_to_opt_f64(obj.get("index_price"))?,
            estimated_settle_price: value_to_opt_f64(obj.get("estimated_settle_price"))?,
            funding_rate: value_to_opt_f64(obj.get("funding_rate"))?,
            next_funding_time: value_to_opt_ts(obj.get("next_funding_time"))?,
        });
    }
    Ok(out)
}

fn parse_funding_points(value: Value) -> Result<Vec<AggFundingPoint>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("funding_points must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let obj = row
            .as_object()
            .ok_or_else(|| anyhow!("funding point row must be object"))?;
        let ts = obj
            .get("ts")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("funding point missing ts"))?;
        let funding_rate = obj
            .get("funding_rate")
            .ok_or_else(|| anyhow!("funding point missing funding_rate"))
            .and_then(to_f64)?;
        out.push(AggFundingPoint {
            ts: parse_ts(ts.to_owned(), "funding_points.ts")?,
            funding_time: value_to_opt_ts(obj.get("funding_time"))?,
            funding_rate,
            mark_price: value_to_opt_f64(obj.get("mark_price"))?,
            next_funding_time: value_to_opt_ts(obj.get("next_funding_time"))?,
        });
    }
    Ok(out)
}

fn parse_levels(value: Value) -> Result<Vec<(f64, f64)>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("levels must be array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let level = row
            .as_array()
            .ok_or_else(|| anyhow!("level row must be [price,qty]"))?;
        if level.len() < 2 {
            return Err(anyhow!("level row length < 2"));
        }
        let price = to_f64(&level[0])?;
        let qty = to_f64(&level[1])?;
        out.push((price, qty));
    }
    Ok(out)
}

fn parse_uuid(text: String, field: &str) -> Result<Uuid> {
    Uuid::parse_str(&text).with_context(|| format!("parse {} uuid", field))
}

fn parse_market(text: String) -> Result<MarketKind> {
    match text.as_str() {
        "spot" => Ok(MarketKind::Spot),
        "futures" => Ok(MarketKind::Futures),
        _ => Err(anyhow!("invalid market: {}", text)),
    }
}

fn parse_ts(text: String, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&text)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("parse {}", field))
}

fn required_value(value: &Value, key: &str) -> Result<Value> {
    value
        .get(key)
        .cloned()
        .ok_or_else(|| anyhow!("missing field: {}", key))
}

fn required_str(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing string field: {}", key))
}

fn required_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing bool field: {}", key))
}

fn required_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing int field: {}", key))
}

fn optional_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

fn optional_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn required_f64(value: &Value, key: &str) -> Result<f64> {
    let raw = value
        .get(key)
        .ok_or_else(|| anyhow!("missing decimal field: {}", key))?;
    to_f64(raw).with_context(|| format!("parse decimal field {}", key))
}

fn optional_f64(value: &Value, key: &str) -> Result<Option<f64>> {
    match value.get(key) {
        Some(Value::Null) | None => Ok(None),
        Some(v) => Ok(Some(to_f64(v)?)),
    }
}

fn optional_ts(value: &Value, key: &str) -> Result<Option<DateTime<Utc>>> {
    match value.get(key) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(s)) => DateTime::parse_from_rfc3339(s)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .with_context(|| format!("parse timestamp field {}", key)),
        _ => Err(anyhow!("invalid timestamp field type: {}", key)),
    }
}

fn value_to_opt_f64(value: Option<&Value>) -> Result<Option<f64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(to_f64(v)?)),
    }
}

fn value_to_opt_ts(value: Option<&Value>) -> Result<Option<DateTime<Utc>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => DateTime::parse_from_rfc3339(s)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .context("parse optional timestamp value"),
        Some(_) => Err(anyhow!("invalid optional timestamp value type")),
    }
}

fn to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::String(s) => s
            .parse::<f64>()
            .with_context(|| format!("invalid decimal string: {}", s)),
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| anyhow!("cannot convert number to f64: {}", n)),
        _ => Err(anyhow!("unsupported decimal type")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_engine_event, decode_contract_body, EngineEventEnvelope, MarketKind, MdData,
    };
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn sample_envelope(msg_type: &str, data: serde_json::Value) -> serde_json::Value {
        json!({
            "schema_version": 1,
            "msg_type": msg_type,
            "message_id": "11111111-1111-1111-1111-111111111111",
            "trace_id": "22222222-2222-2222-2222-222222222222",
            "routing_key": "md.futures.trade.1m.btcusdt",
            "market": "futures",
            "symbol": "btcusdt",
            "source_kind": "replay",
            "backfill_in_progress": true,
            "event_ts": "2026-03-30T00:21:00Z",
            "published_at": "2026-03-30T00:21:05Z",
            "data": data
        })
    }

    fn sample_direct_envelope(msg_type: &str) -> EngineEventEnvelope {
        EngineEventEnvelope {
            schema_version: 1,
            msg_type: msg_type.to_string(),
            message_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            trace_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
            routing_key: "md.futures.trade.1m.btcusdt".to_string(),
            market: "futures".to_string(),
            symbol: "btcusdt".to_string(),
            source_kind: "replay".to_string(),
            backfill_in_progress: true,
            event_ts: Utc.with_ymd_and_hms(2026, 3, 30, 0, 21, 0).unwrap(),
            published_at: Utc.with_ymd_and_hms(2026, 3, 30, 0, 21, 5).unwrap(),
        }
    }

    #[test]
    fn build_engine_event_matches_contract_decode_for_agg_trade_backfill_rows() {
        let data = json!({
            "ts_bucket": "2026-03-30T00:21:00Z",
            "chunk_start_ts": "2026-03-30T00:21:00Z",
            "chunk_end_ts": "2026-03-30T00:21:59Z",
            "source_event_count": 120,
            "trade_count": 50,
            "buy_qty": 12.5,
            "sell_qty": 9.25,
            "buy_notional": 104321.5,
            "sell_notional": 77221.0,
            "first_price": 82500.0,
            "last_price": 82525.5,
            "high_price": 82555.0,
            "low_price": 82480.0,
            "profile_levels": [],
            "whale": {
                "trade_count": 2,
                "buy_count": 1,
                "sell_count": 1,
                "notional_total": 400000.0,
                "notional_buy": 250000.0,
                "notional_sell": 150000.0,
                "qty_base_total": 4.0,
                "delta_qty_base": 1.5,
                "max_single_notional": 260000.0
            },
            "payload_json": {
                "vpin_state": {
                    "current_buy": 1.0,
                    "current_sell": 0.5,
                    "current_fill": 0.25,
                    "imbalances": [0.2, 0.4],
                    "imbalance_sum": 0.6,
                    "last_vpin": 0.3
                }
            }
        });

        let decoded = decode_contract_body(
            serde_json::to_vec(&sample_envelope("md.agg.trade.1m", data.clone()))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let direct = build_engine_event(sample_direct_envelope("md.agg.trade.1m"), data).unwrap();

        assert_eq!(direct.schema_version, decoded.schema_version);
        assert_eq!(direct.msg_type, decoded.msg_type);
        assert_eq!(direct.message_id, decoded.message_id);
        assert_eq!(direct.trace_id, decoded.trace_id);
        assert_eq!(direct.routing_key, decoded.routing_key);
        assert_eq!(direct.market, MarketKind::Futures);
        assert_eq!(direct.market, decoded.market);
        assert_eq!(direct.symbol, "BTCUSDT");
        assert_eq!(direct.symbol, decoded.symbol);
        assert_eq!(direct.source_kind, decoded.source_kind);
        assert_eq!(direct.backfill_in_progress, decoded.backfill_in_progress);
        assert_eq!(direct.event_ts, decoded.event_ts);
        assert_eq!(direct.published_at, decoded.published_at);

        match (&direct.data, &decoded.data) {
            (MdData::AggTrade1m(left), MdData::AggTrade1m(right)) => assert_eq!(left, right),
            other => panic!("unexpected variants: {:?}", other),
        }
    }
}
