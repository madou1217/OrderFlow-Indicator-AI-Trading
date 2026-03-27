use crate::indicators::context::{
    LongShortRatioPoint, OpenInterestCurrentSidecar, OpenInterestHistPoint, OptionMarkGreeksPoint,
    OptionsSurfacePoint,
};
use crate::ingest::decoder::{
    AggFundingMark1mEvent, AggLiq1mEvent, AggOrderbook1mEvent, AggTrade1mEvent, AggVpinSnapshot,
    BboEvent, DepthDeltaEvent, EngineEvent, ForceOrderEvent, LongShortRatio5mEvent,
    LongShortRatioType, MarketKind, MdData, OpenInterestCurrentEvent, OpenInterestHist5mEvent,
    OptionMarkGreeks5mEvent, OrderbookSnapshotEvent, TradeEvent,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::collections::{BTreeMap, HashMap, VecDeque};
use tracing::debug;

pub const HISTORY_LIMIT_MINUTES: usize = 60 * 24 * 9; // keep 9 days
const PRICE_SCALE: f64 = 100.0; // 0.01 tick bin for level-based outputs
const DEPTH_CONFLATION_ENABLED: bool = false;
const DEPTH_CONFLATION_MS: i64 = 100;
const ORDERBOOK_TOPK: usize = 5;
const ORDERBOOK_DW_LAMBDA: f64 = 0.35;
const MICROPRICE_KAPPA: f64 = 0.35;
const MICROPRICE_LAMBDA_OBI: f64 = 0.15;
const SIZE_FLOOR: f64 = 1e-3;
const TICK_SIZE: f64 = 1.0 / PRICE_SCALE;
const VPIN_BUCKET_SIZE_BASE: f64 = 50.0;
const VPIN_ROLLING_BUCKETS: usize = 50;
const VPIN_EPS: f64 = 1e-12;
// Keep one full day of finalized canonical 1m inputs so late trade corrections
// can still recompute indicators that depend on 1h/4h/1d rolling trade history.
// This retention also keeps the paired VPIN snapshots long enough to restore the
// correct trade-state when reopening an older finalized minute.
const CANONICAL_REPLAY_KEEP_MINUTES: i64 = 60 * 24;
const OI_RATIO_HISTORY_KEEP_5M_BUCKETS: usize = 12 * 24 * 10; // 10 days
const OI_CURRENT_HISTORY_KEEP_MINUTES: usize = 60 * 24;
const OPTIONS_SURFACE_HISTORY_KEEP_5M_BUCKETS: usize = 12 * 24 * 5; // 5 days

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LevelAgg {
    pub buy_qty: f64,
    pub sell_qty: f64,
}

impl LevelAgg {
    pub fn total(&self) -> f64 {
        self.buy_qty + self.sell_qty
    }

    pub fn delta(&self) -> f64 {
        self.buy_qty - self.sell_qty
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LiqAgg {
    pub long_liq: f64,
    pub short_liq: f64,
}

impl LiqAgg {
    pub fn net(&self) -> f64 {
        self.long_liq - self.short_liq
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BookLevelAgg {
    pub bid_liquidity: f64,
    pub ask_liquidity: f64,
}

impl BookLevelAgg {
    pub fn total(&self) -> f64 {
        self.bid_liquidity + self.ask_liquidity
    }

    pub fn net(&self) -> f64 {
        self.bid_liquidity - self.ask_liquidity
    }

    pub fn imbalance(&self) -> f64 {
        let total = self.total();
        if total <= 1e-12 {
            0.0
        } else {
            self.net() / total
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WhaleStats {
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

#[derive(Debug, Clone)]
pub struct MinuteWindowData {
    pub market: MarketKind,
    pub ts_bucket: DateTime<Utc>,
    pub trade_count: i64,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub total_qty: f64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub total_notional: f64,
    pub delta: f64,
    pub relative_delta: f64,
    pub first_price: Option<f64>,
    pub last_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub profile: BTreeMap<i64, LevelAgg>,
    pub force_liq: BTreeMap<i64, LiqAgg>,
    pub heatmap: BTreeMap<i64, BookLevelAgg>,
    pub depth_k: usize,
    pub spread_twa: Option<f64>,
    pub topk_depth_twa: Option<f64>,
    pub obi_twa: Option<f64>,
    pub obi_l1_twa: Option<f64>,
    pub obi_k_twa: Option<f64>,
    pub obi_k_dw_twa: Option<f64>,
    pub obi_k_dw_close: Option<f64>,
    pub obi_k_dw_change: Option<f64>,
    pub obi_k_dw_adj_twa: Option<f64>,
    pub ofi: f64,
    pub bbo_updates: i64,
    pub microprice_twa: Option<f64>,
    pub microprice_classic_twa: Option<f64>,
    pub microprice_kappa_twa: Option<f64>,
    pub microprice_adj_twa: Option<f64>,
    pub avwap: Option<f64>,
    pub whale: WhaleStats,
}

impl MinuteWindowData {
    pub fn empty(market: MarketKind, ts_bucket: DateTime<Utc>) -> Self {
        Self {
            market,
            ts_bucket,
            trade_count: 0,
            buy_qty: 0.0,
            sell_qty: 0.0,
            total_qty: 0.0,
            buy_notional: 0.0,
            sell_notional: 0.0,
            total_notional: 0.0,
            delta: 0.0,
            relative_delta: 0.0,
            first_price: None,
            last_price: None,
            high_price: None,
            low_price: None,
            profile: BTreeMap::new(),
            force_liq: BTreeMap::new(),
            heatmap: BTreeMap::new(),
            depth_k: ORDERBOOK_TOPK,
            spread_twa: None,
            topk_depth_twa: None,
            obi_twa: None,
            obi_l1_twa: None,
            obi_k_twa: None,
            obi_k_dw_twa: None,
            obi_k_dw_close: None,
            obi_k_dw_change: None,
            obi_k_dw_adj_twa: None,
            ofi: 0.0,
            bbo_updates: 0,
            microprice_twa: None,
            microprice_classic_twa: None,
            microprice_kappa_twa: None,
            microprice_adj_twa: None,
            avwap: None,
            whale: WhaleStats::default(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MinuteHistory {
    pub ts_bucket: DateTime<Utc>,
    pub market: MarketKind,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub last_price: Option<f64>,
    pub buy_qty: f64,
    pub sell_qty: f64,
    pub total_qty: f64,
    pub total_notional: f64,
    pub delta: f64,
    pub relative_delta: f64,
    pub force_liq: BTreeMap<i64, LiqAgg>,
    pub ofi: f64,
    pub spread_twa: Option<f64>,
    pub topk_depth_twa: Option<f64>,
    pub obi_twa: Option<f64>,
    pub obi_l1_twa: Option<f64>,
    pub obi_k_twa: Option<f64>,
    pub obi_k_dw_twa: Option<f64>,
    pub obi_k_dw_close: Option<f64>,
    pub obi_k_dw_change: Option<f64>,
    pub obi_k_dw_adj_twa: Option<f64>,
    pub bbo_updates: i64,
    pub microprice_twa: Option<f64>,
    pub microprice_classic_twa: Option<f64>,
    pub microprice_kappa_twa: Option<f64>,
    pub microprice_adj_twa: Option<f64>,
    pub cvd: f64,
    pub vpin: f64,
    pub avwap_minute: Option<f64>,
    pub whale_trade_count: i64,
    pub whale_buy_count: i64,
    pub whale_sell_count: i64,
    pub whale_notional_total: f64,
    pub whale_notional_buy: f64,
    pub whale_notional_sell: f64,
    pub whale_qty_eth_total: f64,
    pub whale_qty_eth_buy: f64,
    pub whale_qty_eth_sell: f64,
    pub whale_max_single_notional: f64,
    /// Per-tick trade profile for this closed bar.  Used by footprint multi-window aggregation.
    pub profile: BTreeMap<i64, LevelAgg>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LatestMarkState {
    pub ts: DateTime<Utc>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LatestFundingState {
    pub ts: DateTime<Utc>,
    pub funding_rate: f64,
    pub mark_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FundingChange {
    pub ts_change: DateTime<Utc>,
    pub prev: Option<f64>,
    pub new: f64,
    pub delta: Option<f64>,
    pub mark_price_at_change: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct WindowBundle {
    pub ts_bucket: DateTime<Utc>,
    pub symbol: String,
    pub futures: MinuteWindowData,
    pub spot: MinuteWindowData,
    pub history_futures: Vec<MinuteHistory>,
    pub history_spot: Vec<MinuteHistory>,
    pub trade_history_futures: Vec<MinuteHistory>,
    pub trade_history_spot: Vec<MinuteHistory>,
    pub latest_mark: Option<LatestMarkState>,
    pub latest_funding: Option<LatestFundingState>,
    pub funding_changes_in_window: Vec<FundingChange>,
    pub funding_points_in_window: Vec<LatestFundingState>,
    pub mark_points_in_window: Vec<LatestMarkState>,
    pub funding_changes_recent: Vec<FundingChange>,
    pub funding_points_recent: Vec<LatestFundingState>,
    pub mark_points_recent: Vec<LatestMarkState>,
    pub latest_common_oi_ratio_bucket: Option<DateTime<Utc>>,
    pub current_open_interest: Option<OpenInterestCurrentSidecar>,
    pub open_interest_hist_5m: Vec<OpenInterestHistPoint>,
    pub global_account_ratio_5m: Vec<LongShortRatioPoint>,
    pub top_account_ratio_5m: Vec<LongShortRatioPoint>,
    pub top_position_ratio_5m: Vec<LongShortRatioPoint>,
    pub latest_options_surface_bucket: Option<DateTime<Utc>>,
    pub options_surface_5m: Vec<OptionsSurfacePoint>,
}

#[derive(Debug, Clone, Default)]
struct CanonicalMinuteInputs {
    trade: Option<AggTrade1mEvent>,
    orderbook: Option<AggOrderbook1mEvent>,
    liq: Option<AggLiq1mEvent>,
    funding_mark: Option<AggFundingMark1mEvent>,
}

#[derive(Debug, Clone, Default)]
struct CanonicalMinuteByMarket {
    futures: CanonicalMinuteInputs,
    spot: CanonicalMinuteInputs,
}

impl CanonicalMinuteByMarket {
    fn for_market(&self, market: MarketKind) -> &CanonicalMinuteInputs {
        match market {
            MarketKind::Futures => &self.futures,
            MarketKind::Spot => &self.spot,
        }
    }

    fn for_market_mut(&mut self, market: MarketKind) -> &mut CanonicalMinuteInputs {
        match market {
            MarketKind::Futures => &mut self.futures,
            MarketKind::Spot => &mut self.spot,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CanonicalMinutePresence {
    pub minute_present: bool,
    pub trade_futures: bool,
    pub trade_spot: bool,
    pub orderbook_futures: bool,
    pub orderbook_spot: bool,
    pub liq_futures: bool,
    pub funding_futures: bool,
}

impl CanonicalMinutePresence {
    pub fn complete_under_current_policy(&self) -> bool {
        self.trade_futures
            && self.orderbook_futures
            && self.funding_futures
            && self.trade_spot
            && self.orderbook_spot
    }

    pub fn missing_required_sources(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.trade_futures {
            missing.push("trade_futures");
        }
        if !self.orderbook_futures {
            missing.push("orderbook_futures");
        }
        if !self.funding_futures {
            missing.push("funding_futures");
        }
        if !self.trade_spot {
            missing.push("trade_spot");
        }
        if !self.orderbook_spot {
            missing.push("orderbook_spot");
        }
        missing
    }
}

#[derive(Debug, Clone, Default)]
pub struct CanonicalFrontierSnapshot {
    pub trade_futures_ts: Option<DateTime<Utc>>,
    pub trade_spot_ts: Option<DateTime<Utc>>,
    pub orderbook_futures_ts: Option<DateTime<Utc>>,
    pub orderbook_spot_ts: Option<DateTime<Utc>>,
    pub liq_futures_ts: Option<DateTime<Utc>>,
    pub funding_futures_ts: Option<DateTime<Utc>>,
    pub last_canonical_minute_ts: Option<DateTime<Utc>>,
    pub latest_contiguous_complete_minute_ts: Option<DateTime<Utc>>,
    pub last_finalized_minute_ts: Option<DateTime<Utc>>,
    pub effective_history_floor_ts: Option<DateTime<Utc>>,
    pub dirty_recompute_from_ts: Option<DateTime<Utc>>,
    pub dirty_recompute_end_ts: Option<DateTime<Utc>>,
    pub oi_ratio_patch_from_ts: Option<DateTime<Utc>>,
    pub oi_ratio_patch_end_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct OiRatioWindowView {
    latest_common_bucket: Option<DateTime<Utc>>,
    current_open_interest: Option<OpenInterestCurrentSidecar>,
    open_interest_hist_5m: Vec<OpenInterestHistPoint>,
    global_account_ratio_5m: Vec<LongShortRatioPoint>,
    top_account_ratio_5m: Vec<LongShortRatioPoint>,
    top_position_ratio_5m: Vec<LongShortRatioPoint>,
}

#[derive(Debug, Clone, Default)]
struct OptionsSurfaceWindowView {
    latest_bucket: Option<DateTime<Utc>>,
    points: Vec<OptionsSurfacePoint>,
}

fn canonical_minute_presence(minute: Option<&CanonicalMinuteByMarket>) -> CanonicalMinutePresence {
    let Some(minute) = minute else {
        return CanonicalMinutePresence::default();
    };

    CanonicalMinutePresence {
        minute_present: true,
        trade_futures: minute.futures.trade.is_some(),
        trade_spot: minute.spot.trade.is_some(),
        orderbook_futures: minute.futures.orderbook.is_some(),
        orderbook_spot: minute.spot.orderbook.is_some(),
        liq_futures: minute.futures.liq.is_some(),
        funding_futures: minute.futures.funding_mark.is_some(),
    }
}

fn canonical_futures_minute_is_complete(inputs: &CanonicalMinuteInputs) -> bool {
    inputs.trade.is_some() && inputs.orderbook.is_some() && inputs.funding_mark.is_some()
}

fn canonical_spot_minute_is_complete(inputs: &CanonicalMinuteInputs) -> bool {
    inputs.trade.is_some() && inputs.orderbook.is_some()
}

fn canonical_minute_is_complete(minute: &CanonicalMinuteByMarket) -> bool {
    canonical_futures_minute_is_complete(&minute.futures)
        && canonical_spot_minute_is_complete(&minute.spot)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FinalizedVpinState {
    ts_bucket: DateTime<Utc>,
    snapshot: AggVpinSnapshot,
}

#[derive(Debug, Clone, Default)]
struct BucketOrderbookAgg {
    sample_count: i64,
    bbo_updates: i64,
    spread_sum: f64,
    topk_depth_sum: f64,
    obi_sum: f64,
    obi_l1_sum: f64,
    obi_k_sum: f64,
    obi_k_dw_sum: f64,
    obi_k_dw_change_sum: f64,
    obi_k_dw_adj_sum: f64,
    microprice_sum: f64,
    microprice_classic_sum: f64,
    microprice_kappa_sum: f64,
    microprice_adj_sum: f64,
    ofi_sum: f64,
    obi_k_dw_close: Option<f64>,
    heatmap: BTreeMap<i64, BookLevelAgg>,
}

#[derive(Debug, Clone)]
struct MinuteBucket {
    market: MarketKind,
    ts_bucket: DateTime<Utc>,
    trade_count: i64,
    buy_qty: f64,
    sell_qty: f64,
    buy_notional: f64,
    sell_notional: f64,
    first_price: Option<f64>,
    last_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    profile: BTreeMap<i64, LevelAgg>,
    force_liq: BTreeMap<i64, LiqAgg>,
    book_agg: BucketOrderbookAgg,
    vpin_close: Option<f64>,
    avwap_num: f64,
    avwap_den: f64,
    whale: WhaleStats,
}

impl MinuteBucket {
    fn new(market: MarketKind, ts_bucket: DateTime<Utc>) -> Self {
        Self {
            market,
            ts_bucket,
            trade_count: 0,
            buy_qty: 0.0,
            sell_qty: 0.0,
            buy_notional: 0.0,
            sell_notional: 0.0,
            first_price: None,
            last_price: None,
            high_price: None,
            low_price: None,
            profile: BTreeMap::new(),
            force_liq: BTreeMap::new(),
            book_agg: BucketOrderbookAgg::default(),
            vpin_close: None,
            avwap_num: 0.0,
            avwap_den: 0.0,
            whale: WhaleStats::default(),
        }
    }

    fn apply_trade(&mut self, trade: &TradeEvent, whale_threshold_usdt: f64) {
        self.trade_count += 1;
        if trade.aggressor_side >= 0 {
            self.buy_qty += trade.qty_eth;
            self.buy_notional += trade.notional_usdt;
        } else {
            self.sell_qty += trade.qty_eth;
            self.sell_notional += trade.notional_usdt;
        }

        self.first_price.get_or_insert(trade.price);
        self.last_price = Some(trade.price);
        self.high_price = Some(self.high_price.map_or(trade.price, |v| v.max(trade.price)));
        self.low_price = Some(self.low_price.map_or(trade.price, |v| v.min(trade.price)));

        let tick = price_to_tick(trade.price);
        let level = self.profile.entry(tick).or_default();
        if trade.aggressor_side >= 0 {
            level.buy_qty += trade.qty_eth;
        } else {
            level.sell_qty += trade.qty_eth;
        }

        self.avwap_num += trade.price * trade.qty_eth;
        self.avwap_den += trade.qty_eth;

        if trade.notional_usdt >= whale_threshold_usdt {
            self.whale.trade_count += 1;
            self.whale.notional_total += trade.notional_usdt;
            self.whale.qty_eth_total += trade.qty_eth;
            if trade.aggressor_side >= 0 {
                self.whale.buy_count += 1;
                self.whale.notional_buy += trade.notional_usdt;
                self.whale.qty_eth_buy += trade.qty_eth;
            } else {
                self.whale.sell_count += 1;
                self.whale.notional_sell += trade.notional_usdt;
                self.whale.qty_eth_sell += trade.qty_eth;
            }
            if trade.notional_usdt > self.whale.max_single_notional {
                self.whale.max_single_notional = trade.notional_usdt;
            }
        }
    }

    fn apply_force_order(&mut self, event: &ForceOrderEvent) {
        if let Some(price) = event.price {
            let tick = price_to_tick(price);
            let level = self.force_liq.entry(tick).or_default();
            let qty = event.notional_usdt.unwrap_or_else(|| {
                event
                    .filled_qty
                    .map(|q| q * price)
                    .unwrap_or_default()
                    .abs()
            });
            if event.liq_side == "long_liq" {
                level.long_liq += qty;
            } else if event.liq_side == "short_liq" {
                level.short_liq += qty;
            }
        }
    }

    fn apply_trade_agg(&mut self, event: &AggTrade1mEvent) {
        self.trade_count += event.trade_count.max(0);
        self.buy_qty += event.buy_qty;
        self.sell_qty += event.sell_qty;
        self.buy_notional += event.buy_notional;
        self.sell_notional += event.sell_notional;

        if let Some(first_price) = event.first_price {
            self.first_price.get_or_insert(first_price);
        }
        if let Some(last_price) = event.last_price {
            self.last_price = Some(last_price);
        }
        if let Some(high_price) = event.high_price {
            self.high_price = Some(self.high_price.map_or(high_price, |v| v.max(high_price)));
        }
        if let Some(low_price) = event.low_price {
            self.low_price = Some(self.low_price.map_or(low_price, |v| v.min(low_price)));
        }

        for level in &event.profile_levels {
            let tick = price_to_tick(level.price);
            let dst = self.profile.entry(tick).or_default();
            dst.buy_qty += level.buy_qty;
            dst.sell_qty += level.sell_qty;
        }

        let total_qty = (event.buy_qty + event.sell_qty).max(0.0);
        if total_qty > 0.0 {
            self.avwap_num += event.buy_notional + event.sell_notional;
            self.avwap_den += total_qty;
        }

        self.whale.trade_count += event.whale.trade_count.max(0);
        self.whale.buy_count += event.whale.buy_count.max(0);
        self.whale.sell_count += event.whale.sell_count.max(0);
        self.whale.notional_total += event.whale.notional_total;
        self.whale.notional_buy += event.whale.notional_buy;
        self.whale.notional_sell += event.whale.notional_sell;
        self.whale.qty_eth_total += event.whale.qty_eth_total;
        self.whale.qty_eth_buy += event.whale.qty_eth_buy;
        self.whale.qty_eth_sell += event.whale.qty_eth_sell;
        self.whale.max_single_notional = self
            .whale
            .max_single_notional
            .max(event.whale.max_single_notional);
    }

    fn replace_trade_agg(&mut self, event: &AggTrade1mEvent) {
        self.trade_count = 0;
        self.buy_qty = 0.0;
        self.sell_qty = 0.0;
        self.buy_notional = 0.0;
        self.sell_notional = 0.0;
        self.first_price = None;
        self.last_price = None;
        self.high_price = None;
        self.low_price = None;
        self.profile.clear();
        self.vpin_close = None;
        self.avwap_num = 0.0;
        self.avwap_den = 0.0;
        self.whale = WhaleStats::default();
        self.apply_trade_agg(event);
    }

    fn apply_orderbook_agg(&mut self, event: &AggOrderbook1mEvent) {
        self.book_agg.sample_count += event.sample_count.max(0);
        self.book_agg.bbo_updates += event.bbo_updates.max(0);
        self.book_agg.spread_sum += event.spread_sum;
        self.book_agg.topk_depth_sum += event.topk_depth_sum;
        self.book_agg.obi_sum += event.obi_sum;
        self.book_agg.obi_l1_sum += event.obi_l1_sum;
        self.book_agg.obi_k_sum += event.obi_k_sum;
        self.book_agg.obi_k_dw_sum += event.obi_k_dw_sum;
        self.book_agg.obi_k_dw_change_sum += event.obi_k_dw_change_sum;
        self.book_agg.obi_k_dw_adj_sum += event.obi_k_dw_adj_sum;
        self.book_agg.microprice_sum += event.microprice_sum;
        self.book_agg.microprice_classic_sum += event.microprice_classic_sum;
        self.book_agg.microprice_kappa_sum += event.microprice_kappa_sum;
        self.book_agg.microprice_adj_sum += event.microprice_adj_sum;
        self.book_agg.ofi_sum += event.ofi_sum;
        if let Some(v) = event.obi_k_dw_close {
            self.book_agg.obi_k_dw_close = Some(v);
        }
        for level in &event.heatmap_levels {
            let tick = price_to_tick(level.price);
            let dst = self.book_agg.heatmap.entry(tick).or_default();
            dst.bid_liquidity += level.bid_liquidity;
            dst.ask_liquidity += level.ask_liquidity;
        }
    }

    fn replace_orderbook_agg(&mut self, event: &AggOrderbook1mEvent) {
        self.book_agg = BucketOrderbookAgg::default();
        self.apply_orderbook_agg(event);
    }

    fn apply_liq_agg(&mut self, event: &AggLiq1mEvent) {
        for level in &event.levels {
            let tick = price_to_tick(level.price);
            let dst = self.force_liq.entry(tick).or_default();
            dst.long_liq += level.long_liq;
            dst.short_liq += level.short_liq;
        }
    }

    fn replace_liq_agg(&mut self, event: &AggLiq1mEvent) {
        self.force_liq.clear();
        self.apply_liq_agg(event);
    }

    fn apply_orderbook_sample(&mut self, sample: OrderbookSample) {
        self.apply_orderbook_sample_weighted(sample, 1);
    }

    fn apply_orderbook_sample_weighted(&mut self, sample: OrderbookSample, sample_count: i64) {
        let samples = sample_count.max(1);
        let weight = samples as f64;
        self.book_agg.sample_count += samples;
        self.book_agg.bbo_updates += sample.bbo_updates * samples;
        self.book_agg.spread_sum += sample.spread * weight;
        self.book_agg.topk_depth_sum += sample.topk_depth * weight;
        self.book_agg.obi_sum += sample.obi * weight;
        self.book_agg.obi_l1_sum += sample.obi_l1 * weight;
        self.book_agg.obi_k_sum += sample.obi_k * weight;
        self.book_agg.obi_k_dw_sum += sample.obi_k_dw * weight;
        self.book_agg.obi_k_dw_change_sum += sample.obi_k_dw_change * weight;
        self.book_agg.obi_k_dw_adj_sum += sample.obi_k_dw_adj * weight;
        self.book_agg.microprice_sum += sample.microprice * weight;
        self.book_agg.microprice_classic_sum += sample.microprice_classic * weight;
        self.book_agg.microprice_kappa_sum += sample.microprice_kappa * weight;
        self.book_agg.microprice_adj_sum += sample.microprice_adj * weight;
        self.book_agg.ofi_sum += sample.ofi * weight;
        self.book_agg.obi_k_dw_close = Some(sample.obi_k_dw);
        for level in sample.heatmap_levels {
            let agg = self.book_agg.heatmap.entry(level.tick).or_default();
            agg.bid_liquidity += level.bid_qty * weight;
            agg.ask_liquidity += level.ask_qty * weight;
        }
    }

    fn finalize(self, cvd_prev: f64, vpin: f64) -> (MinuteWindowData, MinuteHistory, f64) {
        let total_qty = self.buy_qty + self.sell_qty;
        let total_notional = self.buy_notional + self.sell_notional;
        let delta = self.buy_qty - self.sell_qty;
        let relative_delta = if total_qty > 0.0 {
            delta / total_qty
        } else {
            0.0
        };
        let cvd = cvd_prev + delta;

        let sample_count = self.book_agg.sample_count.max(0) as f64;
        let spread_twa = if sample_count > 0.0 {
            Some(self.book_agg.spread_sum / sample_count)
        } else {
            None
        };
        let topk_depth_twa = if sample_count > 0.0 {
            Some(self.book_agg.topk_depth_sum / sample_count)
        } else {
            None
        };
        let obi_twa = if sample_count > 0.0 {
            Some(self.book_agg.obi_sum / sample_count)
        } else {
            None
        };
        let obi_l1_twa = if sample_count > 0.0 {
            Some(self.book_agg.obi_l1_sum / sample_count)
        } else {
            None
        };
        let obi_k_twa = if sample_count > 0.0 {
            Some(self.book_agg.obi_k_sum / sample_count)
        } else {
            None
        };
        let obi_k_dw_twa = if sample_count > 0.0 {
            Some(self.book_agg.obi_k_dw_sum / sample_count)
        } else {
            None
        };
        let obi_k_dw_change = if sample_count > 0.0 {
            Some(self.book_agg.obi_k_dw_change_sum / sample_count)
        } else {
            None
        };
        let obi_k_dw_adj_twa = if sample_count > 0.0 {
            Some(self.book_agg.obi_k_dw_adj_sum / sample_count)
        } else {
            None
        };
        let microprice_twa = if sample_count > 0.0 {
            Some(self.book_agg.microprice_sum / sample_count)
        } else {
            None
        };
        let microprice_classic_twa = if sample_count > 0.0 {
            Some(self.book_agg.microprice_classic_sum / sample_count)
        } else {
            None
        };
        let microprice_kappa_twa = if sample_count > 0.0 {
            Some(self.book_agg.microprice_kappa_sum / sample_count)
        } else {
            None
        };
        let microprice_adj_twa = if sample_count > 0.0 {
            Some(self.book_agg.microprice_adj_sum / sample_count)
        } else {
            None
        };
        let heatmap = if sample_count > 0.0 {
            self.book_agg
                .heatmap
                .into_iter()
                .map(|(tick, agg)| {
                    (
                        tick,
                        BookLevelAgg {
                            bid_liquidity: agg.bid_liquidity / sample_count,
                            ask_liquidity: agg.ask_liquidity / sample_count,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };

        let avwap = if self.avwap_den > 0.0 {
            Some(self.avwap_num / self.avwap_den)
        } else {
            None
        };

        let window = MinuteWindowData {
            market: self.market,
            ts_bucket: self.ts_bucket,
            trade_count: self.trade_count,
            buy_qty: self.buy_qty,
            sell_qty: self.sell_qty,
            total_qty,
            buy_notional: self.buy_notional,
            sell_notional: self.sell_notional,
            total_notional,
            delta,
            relative_delta,
            first_price: self.first_price,
            last_price: self.last_price,
            high_price: self.high_price,
            low_price: self.low_price,
            profile: self.profile,
            force_liq: self.force_liq,
            heatmap,
            depth_k: ORDERBOOK_TOPK,
            spread_twa,
            topk_depth_twa,
            obi_twa,
            obi_l1_twa,
            obi_k_twa,
            obi_k_dw_twa,
            obi_k_dw_close: self.book_agg.obi_k_dw_close,
            obi_k_dw_change,
            obi_k_dw_adj_twa,
            ofi: self.book_agg.ofi_sum,
            bbo_updates: self.book_agg.bbo_updates,
            microprice_twa,
            microprice_classic_twa,
            microprice_kappa_twa,
            microprice_adj_twa,
            avwap,
            whale: self.whale,
        };

        let history = MinuteHistory {
            ts_bucket: window.ts_bucket,
            market: window.market,
            open_price: window.first_price,
            high_price: window.high_price,
            low_price: window.low_price,
            close_price: window.last_price,
            last_price: window.last_price,
            buy_qty: window.buy_qty,
            sell_qty: window.sell_qty,
            total_qty: window.total_qty,
            total_notional: window.total_notional,
            delta: window.delta,
            relative_delta: window.relative_delta,
            force_liq: window.force_liq.clone(),
            ofi: window.ofi,
            spread_twa: window.spread_twa,
            topk_depth_twa: window.topk_depth_twa,
            obi_twa: window.obi_twa,
            obi_l1_twa: window.obi_l1_twa,
            obi_k_twa: window.obi_k_twa,
            obi_k_dw_twa: window.obi_k_dw_twa,
            obi_k_dw_close: window.obi_k_dw_close,
            obi_k_dw_change: window.obi_k_dw_change,
            obi_k_dw_adj_twa: window.obi_k_dw_adj_twa,
            bbo_updates: window.bbo_updates,
            microprice_twa: window.microprice_twa,
            microprice_classic_twa: window.microprice_classic_twa,
            microprice_kappa_twa: window.microprice_kappa_twa,
            microprice_adj_twa: window.microprice_adj_twa,
            cvd,
            vpin,
            avwap_minute: window.avwap,
            whale_trade_count: window.whale.trade_count,
            whale_buy_count: window.whale.buy_count,
            whale_sell_count: window.whale.sell_count,
            whale_notional_total: window.whale.notional_total,
            whale_notional_buy: window.whale.notional_buy,
            whale_notional_sell: window.whale.notional_sell,
            whale_qty_eth_total: window.whale.qty_eth_total,
            whale_qty_eth_buy: window.whale.qty_eth_buy,
            whale_qty_eth_sell: window.whale.qty_eth_sell,
            whale_max_single_notional: window.whale.max_single_notional,
            profile: window.profile.clone(),
        };

        (window, history, cvd)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BboState {
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone)]
struct OrderbookState {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_bbo: Option<BboState>,
    last_obi_k_dw: Option<f64>,
}

impl Default for OrderbookState {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_bbo: None,
            last_obi_k_dw: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HeatmapLevelSample {
    tick: i64,
    bid_qty: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone, Default)]
struct OrderbookSample {
    spread: f64,
    topk_depth: f64,
    obi: f64,
    obi_l1: f64,
    obi_k: f64,
    obi_k_dw: f64,
    obi_k_dw_change: f64,
    obi_k_dw_adj: f64,
    microprice: f64,
    microprice_classic: f64,
    microprice_kappa: f64,
    microprice_adj: f64,
    ofi: f64,
    bbo_updates: i64,
    heatmap_levels: Vec<HeatmapLevelSample>,
}

#[derive(Debug, Clone)]
struct DepthConflationState {
    current_slot_100ms: i64,
    latest_sample: Option<OrderbookSample>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VpinState {
    bucket_size: f64,
    rolling_bucket_count: usize,
    current_buy: f64,
    current_sell: f64,
    current_fill: f64,
    imbalances: VecDeque<f64>,
    imbalance_sum: f64,
    last_vpin: f64,
}

impl VpinState {
    pub(crate) fn new(bucket_size: f64, rolling_bucket_count: usize) -> Self {
        Self {
            bucket_size,
            rolling_bucket_count,
            current_buy: 0.0,
            current_sell: 0.0,
            current_fill: 0.0,
            imbalances: VecDeque::new(),
            imbalance_sum: 0.0,
            last_vpin: 0.0,
        }
    }

    fn ingest_flow(&mut self, buy_qty: f64, sell_qty: f64) -> f64 {
        let mut remain_buy = buy_qty.max(0.0);
        let mut remain_sell = sell_qty.max(0.0);

        while remain_buy + remain_sell > VPIN_EPS {
            if self.current_fill + VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }

            let remain_total = remain_buy + remain_sell;
            let capacity = (self.bucket_size - self.current_fill).max(0.0);
            if capacity <= VPIN_EPS {
                break;
            }

            let take = remain_total.min(capacity);
            let buy_part = take * (remain_buy / remain_total);
            let sell_part = take - buy_part;

            self.current_buy += buy_part;
            self.current_sell += sell_part;
            self.current_fill += take;

            remain_buy -= buy_part;
            remain_sell -= sell_part;

            if self.current_fill + VPIN_EPS >= self.bucket_size {
                self.close_bucket();
            }
        }

        self.last_vpin
    }

    fn current_vpin(&self) -> f64 {
        self.last_vpin
    }

    fn snapshot(&self) -> AggVpinSnapshot {
        AggVpinSnapshot {
            current_buy: self.current_buy,
            current_sell: self.current_sell,
            current_fill: self.current_fill,
            imbalances: self.imbalances.iter().copied().collect(),
            imbalance_sum: self.imbalance_sum,
            last_vpin: self.last_vpin,
        }
    }

    fn restore(&mut self, snapshot: &AggVpinSnapshot) {
        self.current_buy = snapshot.current_buy;
        self.current_sell = snapshot.current_sell;
        self.current_fill = snapshot.current_fill;
        self.imbalances = snapshot.imbalances.iter().copied().collect();
        self.imbalance_sum = snapshot.imbalance_sum;
        self.last_vpin = snapshot.last_vpin;
    }

    fn close_bucket(&mut self) {
        if self.current_fill <= VPIN_EPS {
            self.current_buy = 0.0;
            self.current_sell = 0.0;
            self.current_fill = 0.0;
            return;
        }

        let imbalance = (self.current_buy - self.current_sell).abs();
        self.imbalances.push_back(imbalance);
        self.imbalance_sum += imbalance;

        while self.imbalances.len() > self.rolling_bucket_count {
            if let Some(old) = self.imbalances.pop_front() {
                self.imbalance_sum -= old;
            }
        }

        if self.imbalances.len() >= self.rolling_bucket_count && self.bucket_size > VPIN_EPS {
            let denom = self.rolling_bucket_count as f64 * self.bucket_size;
            self.last_vpin = (self.imbalance_sum / denom).clamp(0.0, 1.0);
        } else {
            // Warmup: before enough complete buckets exist, keep VPIN neutral.
            self.last_vpin = 0.0;
        }

        self.current_buy = 0.0;
        self.current_sell = 0.0;
        self.current_fill = 0.0;
    }
}

impl OrderbookState {
    fn apply_snapshot(&mut self, event: &OrderbookSnapshotEvent) {
        self.bids.clear();
        self.asks.clear();
        for (price, qty) in &event.bids {
            self.bids.insert(price_to_tick(*price), *qty);
        }
        for (price, qty) in &event.asks {
            self.asks.insert(price_to_tick(*price), *qty);
        }
    }

    fn apply_depth_delta(&mut self, event: &DepthDeltaEvent) {
        for (price, qty) in &event.bids_delta {
            let tick = price_to_tick(*price);
            if *qty <= 0.0 {
                self.bids.remove(&tick);
            } else {
                self.bids.insert(tick, *qty);
            }
        }
        for (price, qty) in &event.asks_delta {
            let tick = price_to_tick(*price);
            if *qty <= 0.0 {
                self.asks.remove(&tick);
            } else {
                self.asks.insert(tick, *qty);
            }
        }
    }

    fn apply_bbo(&mut self, bbo: &BboEvent) -> OrderbookSample {
        let current = BboState {
            bid_price: bbo.bid_price,
            bid_qty: bbo.bid_qty,
            ask_price: bbo.ask_price,
            ask_qty: bbo.ask_qty,
        };

        self.bids.insert(price_to_tick(bbo.bid_price), bbo.bid_qty);
        self.asks.insert(price_to_tick(bbo.ask_price), bbo.ask_qty);

        let ofi = if let Some(prev) = self.last_bbo {
            (current.bid_qty - prev.bid_qty) - (current.ask_qty - prev.ask_qty)
        } else {
            0.0
        };
        self.last_bbo = Some(current);

        let mut sample = self.make_sample(ofi);
        sample.bbo_updates = 1;
        sample
    }

    fn sample_from_depth(&mut self) -> OrderbookSample {
        self.make_sample(0.0)
    }

    fn make_sample(&mut self, ofi: f64) -> OrderbookSample {
        let best_bid = self
            .bids
            .iter()
            .next_back()
            .map(|(p, q)| (tick_to_price(*p), *q));
        let best_ask = self
            .asks
            .iter()
            .next()
            .map(|(p, q)| (tick_to_price(*p), *q));

        let (spread, microprice_classic, microprice_kappa, microprice_adj, obi_l1) =
            match (best_bid, best_ask) {
                (Some((bp, bq)), Some((ap, aq))) => {
                    let spread = (ap - bp).max(0.0);
                    let denom = (bq + aq).max(1e-12);
                    let mid = (ap + bp) / 2.0;
                    let micro_classic = (ap * bq + bp * aq) / denom;
                    let bid_floor = bq.max(SIZE_FLOOR);
                    let ask_floor = aq.max(SIZE_FLOOR);
                    let bid_pow = (bid_floor + 1e-12).powf(MICROPRICE_KAPPA);
                    let ask_pow = (ask_floor + 1e-12).powf(MICROPRICE_KAPPA);
                    let micro_kappa = (bid_pow * ap + ask_pow * bp) / (bid_pow + ask_pow + 1e-12);
                    let obi_l1 = (bq - aq) / denom;
                    // Backfill / replay can temporarily expose a crossed book (bid > ask).
                    // Clamp against ordered bounds so snapshot generation stays tolerant.
                    let (lo, hi) = if bp <= ap { (bp, ap) } else { (ap, bp) };
                    let micro_adj =
                        (mid + MICROPRICE_LAMBDA_OBI * obi_l1 * (TICK_SIZE / 2.0)).clamp(lo, hi);
                    (spread, micro_classic, micro_kappa, micro_adj, obi_l1)
                }
                _ => (0.0, 0.0, 0.0, 0.0, 0.0),
            };

        let sum_bid: f64 = self
            .bids
            .iter()
            .rev()
            .take(ORDERBOOK_TOPK)
            .map(|(_, q)| *q)
            .sum();
        let sum_ask: f64 = self.asks.iter().take(ORDERBOOK_TOPK).map(|(_, q)| *q).sum();
        let topk_depth = sum_bid + sum_ask;
        let obi_k = if topk_depth > 0.0 {
            (sum_bid - sum_ask) / topk_depth
        } else {
            0.0
        };
        let (dw_bid, dw_ask) = weighted_topk_depth(&self.bids, &self.asks, ORDERBOOK_TOPK);
        let dw_total = dw_bid + dw_ask;
        let obi_k_dw = if dw_total > 0.0 {
            (dw_bid - dw_ask) / dw_total
        } else {
            0.0
        };
        let obi_k_dw_change = self
            .last_obi_k_dw
            .map(|prev| obi_k_dw - prev)
            .unwrap_or(0.0);
        self.last_obi_k_dw = Some(obi_k_dw);
        let depth_factor = (topk_depth / (topk_depth + 1.0)).clamp(0.0, 1.0);
        let spread_factor = if spread <= 1e-12 {
            1.0
        } else {
            (TICK_SIZE / spread).clamp(0.0, 1.0)
        };
        let obi_k_dw_adj = obi_k_dw * depth_factor * spread_factor;
        let heatmap_levels = collect_heatmap_levels(&self.bids, &self.asks);

        OrderbookSample {
            spread,
            topk_depth,
            obi: obi_k,
            obi_l1,
            obi_k,
            obi_k_dw,
            obi_k_dw_change,
            obi_k_dw_adj,
            microprice: microprice_kappa,
            microprice_classic,
            microprice_kappa,
            microprice_adj,
            ofi,
            bbo_updates: 0,
            heatmap_levels,
        }
    }
}

pub struct StateStore {
    symbol: String,
    whale_threshold_usdt: f64,
    effective_history_floor_ts: Option<DateTime<Utc>>,
    buckets: HashMap<(MarketKind, i64), MinuteBucket>,
    canonical_minutes: BTreeMap<i64, CanonicalMinuteByMarket>,
    history_futures: VecDeque<MinuteHistory>,
    history_spot: VecDeque<MinuteHistory>,
    finalized_vpin_futures: VecDeque<FinalizedVpinState>,
    finalized_vpin_spot: VecDeque<FinalizedVpinState>,
    cvd_futures: f64,
    cvd_spot: f64,
    vpin_futures: VpinState,
    vpin_spot: VpinState,
    orderbooks: HashMap<MarketKind, OrderbookState>,
    depth_conflation: HashMap<(MarketKind, i64), DepthConflationState>,
    latest_mark: Option<LatestMarkState>,
    latest_funding: Option<LatestFundingState>,
    funding_changes: VecDeque<FundingChange>,
    mark_timeline: VecDeque<LatestMarkState>,
    funding_timeline: VecDeque<LatestFundingState>,
    current_open_interest_timeline: VecDeque<OpenInterestCurrentSidecar>,
    open_interest_hist_5m: VecDeque<OpenInterestHistPoint>,
    global_account_ratio_5m: VecDeque<LongShortRatioPoint>,
    top_account_ratio_5m: VecDeque<LongShortRatioPoint>,
    top_position_ratio_5m: VecDeque<LongShortRatioPoint>,
    option_mark_greeks_5m: VecDeque<OptionMarkGreeksPoint>,
    dirty_recompute_from: Option<DateTime<Utc>>,
    // Fixed target end for the current dirty-recompute batch series.
    // Set when dirty is first triggered; extended if later minutes become dirty.
    // Cleared when the full recompute completes.
    dirty_recompute_end: Option<DateTime<Utc>>,
    // Whether truncate_finalized_suffix has already been called for the current
    // dirty_recompute_from. Reset to false whenever dirty_from moves to an earlier
    // minute (requiring a fresh truncation).
    dirty_recompute_truncated: bool,
    oi_ratio_patch_from: Option<DateTime<Utc>>,
    oi_ratio_patch_end: Option<DateTime<Utc>>,
    oi_ratio_patch_mark_total: u64,
    oi_ratio_patch_extends_backward_total: u64,
    last_finalized_minute: Option<DateTime<Utc>>,
}

impl StateStore {
    pub fn new(symbol: String, whale_threshold_usdt: f64) -> Self {
        let mut orderbooks = HashMap::new();
        orderbooks.insert(MarketKind::Spot, OrderbookState::default());
        orderbooks.insert(MarketKind::Futures, OrderbookState::default());

        Self {
            symbol,
            whale_threshold_usdt,
            effective_history_floor_ts: None,
            buckets: HashMap::new(),
            canonical_minutes: BTreeMap::new(),
            history_futures: VecDeque::new(),
            history_spot: VecDeque::new(),
            finalized_vpin_futures: VecDeque::new(),
            finalized_vpin_spot: VecDeque::new(),
            cvd_futures: 0.0,
            cvd_spot: 0.0,
            vpin_futures: VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS),
            vpin_spot: VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS),
            orderbooks,
            depth_conflation: HashMap::new(),
            latest_mark: None,
            latest_funding: None,
            funding_changes: VecDeque::new(),
            mark_timeline: VecDeque::new(),
            funding_timeline: VecDeque::new(),
            current_open_interest_timeline: VecDeque::new(),
            open_interest_hist_5m: VecDeque::new(),
            global_account_ratio_5m: VecDeque::new(),
            top_account_ratio_5m: VecDeque::new(),
            top_position_ratio_5m: VecDeque::new(),
            option_mark_greeks_5m: VecDeque::new(),
            dirty_recompute_from: None,
            dirty_recompute_end: None,
            dirty_recompute_truncated: false,
            oi_ratio_patch_from: None,
            oi_ratio_patch_end: None,
            oi_ratio_patch_mark_total: 0,
            oi_ratio_patch_extends_backward_total: 0,
            last_finalized_minute: None,
        }
    }

    pub fn set_effective_history_floor(&mut self, ts: Option<DateTime<Utc>>) {
        self.effective_history_floor_ts = ts;
    }

    pub fn last_finalized_minute(&self) -> Option<DateTime<Utc>> {
        self.last_finalized_minute
    }

    pub fn reset_finalized_state(&mut self) {
        self.history_futures.clear();
        self.history_spot.clear();
        self.finalized_vpin_futures.clear();
        self.finalized_vpin_spot.clear();
        self.cvd_futures = 0.0;
        self.cvd_spot = 0.0;
        self.vpin_futures = VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS);
        self.vpin_spot = VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS);
        self.latest_mark = None;
        self.latest_funding = None;
        self.funding_changes.clear();
        self.mark_timeline.clear();
        self.funding_timeline.clear();
        self.current_open_interest_timeline.clear();
        self.open_interest_hist_5m.clear();
        self.global_account_ratio_5m.clear();
        self.top_account_ratio_5m.clear();
        self.top_position_ratio_5m.clear();
        self.option_mark_greeks_5m.clear();
        self.dirty_recompute_from = None;
        self.dirty_recompute_end = None;
        self.dirty_recompute_truncated = false;
        self.oi_ratio_patch_from = None;
        self.oi_ratio_patch_end = None;
        self.oi_ratio_patch_mark_total = 0;
        self.oi_ratio_patch_extends_backward_total = 0;
        self.last_finalized_minute = None;
    }

    pub fn rewind_finalized_state_from(&mut self, start: DateTime<Utc>) {
        self.truncate_finalized_suffix(start);
    }

    pub fn clear_dirty_recompute_state(&mut self) {
        self.dirty_recompute_from = None;
        self.dirty_recompute_end = None;
        self.dirty_recompute_truncated = false;
    }

    pub fn clear_oi_ratio_patch_state(&mut self) {
        self.oi_ratio_patch_from = None;
        self.oi_ratio_patch_end = None;
    }

    pub fn reset_for_new_continuous_segment(&mut self, start: DateTime<Utc>) {
        self.buckets.clear();
        self.canonical_minutes.clear();
        self.depth_conflation.clear();
        self.orderbooks.clear();
        self.orderbooks
            .insert(MarketKind::Spot, OrderbookState::default());
        self.orderbooks
            .insert(MarketKind::Futures, OrderbookState::default());
        self.reset_finalized_state();
        self.effective_history_floor_ts = Some(start);
    }

    pub fn latest_contiguous_complete_canonical_minute_from(
        &self,
        start: DateTime<Utc>,
        max_ts: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        if start > max_ts {
            return None;
        }

        let mut cur = start;
        let mut latest_ready = None;
        while cur <= max_ts {
            let Some(minute) = self.canonical_minutes.get(&cur.timestamp()) else {
                break;
            };
            if !canonical_minute_is_complete(minute) {
                break;
            }
            latest_ready = Some(cur);
            cur += Duration::minutes(1);
        }

        latest_ready
    }

    pub fn canonical_minute_presence(&self, ts_bucket: DateTime<Utc>) -> CanonicalMinutePresence {
        canonical_minute_presence(self.canonical_minutes.get(&ts_bucket.timestamp()))
    }

    pub fn canonical_frontier_snapshot(&self) -> CanonicalFrontierSnapshot {
        let mut snapshot = CanonicalFrontierSnapshot {
            latest_contiguous_complete_minute_ts: self
                .latest_continuous_canonical_segment()
                .map(|(_, end)| end),
            last_finalized_minute_ts: self.last_finalized_minute,
            effective_history_floor_ts: self.effective_history_floor_ts,
            dirty_recompute_from_ts: self.dirty_recompute_from,
            dirty_recompute_end_ts: self.dirty_recompute_end,
            oi_ratio_patch_from_ts: self.oi_ratio_patch_from,
            oi_ratio_patch_end_ts: self.oi_ratio_patch_end,
            ..CanonicalFrontierSnapshot::default()
        };

        for (minute_sec, minute) in &self.canonical_minutes {
            let Some(ts_bucket) = Utc.timestamp_opt(*minute_sec, 0).single() else {
                continue;
            };
            let presence = canonical_minute_presence(Some(minute));
            snapshot.last_canonical_minute_ts = Some(ts_bucket);
            if presence.trade_futures {
                snapshot.trade_futures_ts = Some(ts_bucket);
            }
            if presence.trade_spot {
                snapshot.trade_spot_ts = Some(ts_bucket);
            }
            if presence.orderbook_futures {
                snapshot.orderbook_futures_ts = Some(ts_bucket);
            }
            if presence.orderbook_spot {
                snapshot.orderbook_spot_ts = Some(ts_bucket);
            }
            if presence.liq_futures {
                snapshot.liq_futures_ts = Some(ts_bucket);
            }
            if presence.funding_futures {
                snapshot.funding_futures_ts = Some(ts_bucket);
            }
        }

        snapshot
    }

    pub fn latest_continuous_canonical_segment(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let mut current_start: Option<DateTime<Utc>> = None;
        let mut prev_complete: Option<DateTime<Utc>> = None;
        let mut latest_segment: Option<(DateTime<Utc>, DateTime<Utc>)> = None;

        for minute_sec in self.canonical_minutes.keys() {
            let Some(ts_bucket) = Utc.timestamp_opt(*minute_sec, 0).single() else {
                continue;
            };
            let minute = &self.canonical_minutes[minute_sec];
            if !canonical_minute_is_complete(minute) {
                current_start = None;
                prev_complete = None;
                continue;
            }

            let starts_new_segment = prev_complete
                .map(|prev| ts_bucket != prev + Duration::minutes(1))
                .unwrap_or(true);
            if starts_new_segment {
                current_start = Some(ts_bucket);
            }

            let segment_start = current_start.unwrap_or(ts_bucket);
            latest_segment = Some((segment_start, ts_bucket));
            prev_complete = Some(ts_bucket);
        }

        latest_segment
    }

    pub fn ingest(&mut self, event: EngineEvent) {
        let bucket_ts = floor_minute(event.event_ts);
        let whale_threshold_usdt = self.whale_threshold_usdt;

        match event.data {
            MdData::Trade(trade) => {
                self.bucket_mut(event.market, bucket_ts)
                    .apply_trade(&trade, whale_threshold_usdt);
                let vpin = {
                    let state = self.vpin_state_mut(event.market);
                    if trade.aggressor_side >= 0 {
                        state.ingest_flow(trade.qty_eth, 0.0);
                    } else {
                        state.ingest_flow(0.0, trade.qty_eth);
                    }
                    state.current_vpin()
                };
                self.bucket_mut(event.market, bucket_ts).vpin_close = Some(vpin);
            }
            MdData::AggTrade1m(trade) => {
                self.store_canonical_trade(event.market, trade);
            }
            MdData::Depth(depth) => {
                let sample = if let Some(book) = self.orderbooks.get_mut(&event.market) {
                    book.apply_depth_delta(&depth);
                    Some(book.sample_from_depth())
                } else {
                    None
                };
                if let Some(sample) = sample {
                    if DEPTH_CONFLATION_ENABLED {
                        self.ingest_depth_conflated(
                            event.market,
                            bucket_ts,
                            event.event_ts,
                            sample,
                        );
                    } else {
                        let bucket = self.bucket_mut(event.market, bucket_ts);
                        bucket.apply_orderbook_sample(sample);
                    }
                }
            }
            MdData::OrderbookSnapshot(snapshot) => {
                let sample = if let Some(book) = self.orderbooks.get_mut(&event.market) {
                    book.apply_snapshot(&snapshot);
                    Some(book.sample_from_depth())
                } else {
                    None
                };
                if let Some(sample) = sample {
                    let bucket = self.bucket_mut(event.market, bucket_ts);
                    bucket.apply_orderbook_sample(sample);
                }
            }
            MdData::Bbo(bbo) => {
                let sample = if let Some(book) = self.orderbooks.get_mut(&event.market) {
                    Some(book.apply_bbo(&bbo))
                } else {
                    None
                };
                if let Some(sample) = sample {
                    let bucket = self.bucket_mut(event.market, bucket_ts);
                    bucket.apply_orderbook_sample_weighted(sample, bbo.sample_count);
                }
            }
            MdData::AggOrderbook1m(orderbook) => {
                self.store_canonical_orderbook(event.market, orderbook);
            }
            MdData::Kline(kline) => {
                // Only 1m klines update the current-minute OHLC.
                // Multi-interval klines (15m, 1h, …) span multiple 1m buckets and would
                // contaminate this bucket's high/low/open with the full multi-bar range.
                if kline.interval_code == "1m" {
                    let bucket = self.bucket_mut(event.market, floor_minute(kline.open_time));
                    bucket.last_price = Some(kline.close_price);
                    bucket.high_price = Some(
                        bucket
                            .high_price
                            .map_or(kline.high_price, |v| v.max(kline.high_price)),
                    );
                    bucket.low_price = Some(
                        bucket
                            .low_price
                            .map_or(kline.low_price, |v| v.min(kline.low_price)),
                    );
                    if bucket.first_price.is_none() {
                        bucket.first_price = Some(kline.open_price);
                    }
                }
            }
            MdData::MarkPrice(mark) => {
                let state = LatestMarkState {
                    ts: event.event_ts,
                    mark_price: mark.mark_price,
                    index_price: mark.index_price,
                    funding_rate: mark.funding_rate,
                    next_funding_time: mark.next_funding_time,
                };
                self.latest_mark = Some(state.clone());
                self.mark_timeline.push_back(state);
                while self.mark_timeline.len() > HISTORY_LIMIT_MINUTES * 2 {
                    self.mark_timeline.pop_front();
                }
                if let Some(funding_rate) = mark.funding_rate {
                    self.record_funding_state(
                        event.event_ts,
                        funding_rate,
                        mark.mark_price,
                        mark.next_funding_time,
                        event.event_ts,
                    );
                }
            }
            MdData::FundingRate(funding) => {
                self.record_funding_state(
                    event.event_ts,
                    funding.funding_rate,
                    funding.mark_price,
                    funding.next_funding_time,
                    funding.funding_time,
                );
            }
            MdData::ForceOrder(force_order) => {
                let bucket = self.bucket_mut(event.market, bucket_ts);
                bucket.apply_force_order(&force_order);
            }
            MdData::AggLiq1m(liq) => {
                self.store_canonical_liq(event.market, liq);
            }
            MdData::AggFundingMark1m(funding_mark) => {
                self.store_canonical_funding_mark(event.market, funding_mark);
            }
            MdData::OpenInterestCurrent(open_interest) => {
                self.store_current_open_interest(open_interest);
            }
            MdData::OpenInterestHist5m(open_interest_hist) => {
                self.store_open_interest_hist_5m(open_interest_hist);
            }
            MdData::LongShortRatio5m(long_short_ratio) => {
                self.store_long_short_ratio_5m(long_short_ratio);
            }
            MdData::OptionMarkGreeks5m(option_mark) => {
                self.store_option_mark_greeks_5m(option_mark);
            }
        }
    }

    pub fn finalize_minute(&mut self, ts_bucket: DateTime<Utc>) -> WindowBundle {
        let futures = self.finalize_market(MarketKind::Futures, ts_bucket);
        let spot = self.finalize_market(MarketKind::Spot, ts_bucket);
        self.apply_canonical_funding_minute(ts_bucket);
        self.last_finalized_minute = Some(ts_bucket);
        self.trim_replay_retention(ts_bucket);
        self.build_window_bundle(ts_bucket, futures, spot)
    }

    /// Process at most `max_batch` dirty-recompute windows per call, yielding
    /// control back to the caller between batches.  Call repeatedly (on each
    /// tick) until an empty Vec is returned, which signals completion.
    ///
    /// Splitting into batches lets the async select loop stay responsive to
    /// SIGTERM / Ctrl-C and to new live events between batches, even when
    /// hundreds of minutes need recomputing after a canonical-data correction.
    pub fn recompute_dirty_finalized_minutes(&mut self, max_batch: usize) -> Vec<WindowBundle> {
        let Some(start) = self.dirty_recompute_from else {
            return Vec::new();
        };

        // Lazily capture the target end on the first batch of a new dirty series.
        if self.dirty_recompute_end.is_none() {
            match self.last_finalized_minute {
                Some(lf) => self.dirty_recompute_end = Some(lf),
                None => {
                    self.dirty_recompute_from = None;
                    return Vec::new();
                }
            }
        }
        let end = self.dirty_recompute_end.unwrap();

        if start > end {
            self.dirty_recompute_from = None;
            self.dirty_recompute_end = None;
            self.dirty_recompute_truncated = false;
            return Vec::new();
        }

        // Truncate history suffix once per dirty series (or whenever dirty_from
        // moved to an earlier minute, requiring a fresh start).
        if !self.dirty_recompute_truncated {
            self.truncate_finalized_suffix(start);
            self.dirty_recompute_truncated = true;
        }

        let batch_span_minutes = i64::try_from(max_batch.saturating_sub(1)).unwrap_or(i64::MAX);
        let batch_end = end.min(start + Duration::minutes(batch_span_minutes));

        if batch_end >= end {
            // Final batch — clear all dirty state.
            self.dirty_recompute_from = None;
            self.dirty_recompute_end = None;
            self.dirty_recompute_truncated = false;
        } else {
            // More batches remain; advance the start pointer for the next call.
            self.dirty_recompute_from = Some(batch_end + Duration::minutes(1));
        }

        let mut out = Vec::new();
        let mut minute = start;
        while minute <= batch_end {
            out.push(self.finalize_minute(minute));
            minute += Duration::minutes(1);
        }
        out
    }

    pub fn has_pending_dirty_recompute(&self) -> bool {
        self.dirty_recompute_from.is_some()
    }

    pub fn has_pending_oi_ratio_patch(&self) -> bool {
        self.oi_ratio_patch_from.is_some()
    }

    pub fn pending_dirty_recompute_batch_range(
        &self,
        max_batch: usize,
    ) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let start = self.dirty_recompute_from?;
        let end = self.dirty_recompute_end.or(self.last_finalized_minute)?;
        if start > end {
            return None;
        }
        let batch_span_minutes = i64::try_from(max_batch.saturating_sub(1)).unwrap_or(i64::MAX);
        Some((
            start,
            end.min(start + Duration::minutes(batch_span_minutes)),
        ))
    }

    pub fn pending_oi_ratio_patch_batch_range(
        &self,
        max_batch: usize,
    ) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let start = self.oi_ratio_patch_from?;
        let end = self.oi_ratio_patch_end.or(self.last_finalized_minute)?;
        if start > end {
            return None;
        }
        let batch_span_minutes = i64::try_from(max_batch.saturating_sub(1)).unwrap_or(i64::MAX);
        Some((
            start,
            end.min(start + Duration::minutes(batch_span_minutes)),
        ))
    }

    pub fn take_oi_ratio_patch_batch(&mut self, max_batch: usize) -> Vec<DateTime<Utc>> {
        let Some(start) = self.oi_ratio_patch_from else {
            return Vec::new();
        };
        let Some(end) = self.oi_ratio_patch_end.or(self.last_finalized_minute) else {
            self.clear_oi_ratio_patch_state();
            return Vec::new();
        };
        if start > end {
            self.clear_oi_ratio_patch_state();
            return Vec::new();
        }

        let batch_span_minutes = i64::try_from(max_batch.saturating_sub(1)).unwrap_or(i64::MAX);
        let batch_end = end.min(start + Duration::minutes(batch_span_minutes));
        if batch_end >= end {
            self.clear_oi_ratio_patch_state();
        } else {
            self.oi_ratio_patch_from = Some(batch_end + Duration::minutes(1));
        }

        let mut out = Vec::new();
        let mut minute = start;
        while minute <= batch_end {
            out.push(minute);
            minute += Duration::minutes(1);
        }
        out
    }

    pub fn has_unhydrated_futures_orderbook_heatmap_in_range(
        &self,
        from_ts: DateTime<Utc>,
        to_ts_inclusive: DateTime<Utc>,
    ) -> bool {
        if from_ts > to_ts_inclusive {
            return false;
        }
        self.canonical_minutes
            .range(from_ts.timestamp()..=to_ts_inclusive.timestamp())
            .any(|(_, minute)| {
                minute
                    .futures
                    .orderbook
                    .as_ref()
                    .map(|orderbook| !orderbook.heatmap_loaded)
                    .unwrap_or(false)
            })
    }

    fn finalize_market(
        &mut self,
        market: MarketKind,
        ts_bucket: DateTime<Utc>,
    ) -> MinuteWindowData {
        if DEPTH_CONFLATION_ENABLED {
            self.flush_depth_conflation_for_bucket(market, ts_bucket);
        }
        let bucket = self.build_bucket_for_minute(market, ts_bucket);
        let carry_forward_vpin = if market == MarketKind::Futures {
            self.history_futures.back().map(|h| h.vpin).unwrap_or(0.0)
        } else {
            self.history_spot.back().map(|h| h.vpin).unwrap_or(0.0)
        };
        let minute_vpin = bucket.vpin_close.unwrap_or(carry_forward_vpin);

        let (window, history, cvd_new) = if market == MarketKind::Futures {
            bucket.finalize(self.cvd_futures, minute_vpin)
        } else {
            bucket.finalize(self.cvd_spot, minute_vpin)
        };

        if market == MarketKind::Futures {
            self.cvd_futures = cvd_new;
            self.history_futures.push_back(history);
            while self.history_futures.len() > HISTORY_LIMIT_MINUTES {
                self.history_futures.pop_front();
            }
            self.push_finalized_vpin_snapshot(market, ts_bucket);
        } else {
            self.cvd_spot = cvd_new;
            self.history_spot.push_back(history);
            while self.history_spot.len() > HISTORY_LIMIT_MINUTES {
                self.history_spot.pop_front();
            }
            self.push_finalized_vpin_snapshot(market, ts_bucket);
        }

        window
    }

    fn bucket_mut(&mut self, market: MarketKind, ts_bucket: DateTime<Utc>) -> &mut MinuteBucket {
        let key = (market, ts_bucket.timestamp());
        self.buckets
            .entry(key)
            .or_insert_with(|| MinuteBucket::new(market, ts_bucket))
    }

    fn vpin_state_mut(&mut self, market: MarketKind) -> &mut VpinState {
        match market {
            MarketKind::Spot => &mut self.vpin_spot,
            MarketKind::Futures => &mut self.vpin_futures,
        }
    }

    fn store_canonical_trade(&mut self, market: MarketKind, trade: AggTrade1mEvent) {
        let changed = {
            let slot = self.canonical_slot_mut(market, trade.ts_bucket);
            slot.trade.as_ref() != Some(&trade)
        };
        if changed {
            self.canonical_slot_mut(market, trade.ts_bucket).trade = Some(trade.clone());
            self.mark_dirty_recompute_if_finalized(trade.ts_bucket);
        }
    }

    fn store_canonical_orderbook(&mut self, market: MarketKind, orderbook: AggOrderbook1mEvent) {
        let changed = {
            let slot = self.canonical_slot_mut(market, orderbook.ts_bucket);
            match slot.orderbook.as_ref() {
                Some(existing)
                    if existing.heatmap_loaded
                        && !orderbook.heatmap_loaded
                        && existing.scalar_eq(&orderbook) =>
                {
                    false
                }
                Some(existing) => existing != &orderbook,
                None => true,
            }
        };
        if changed {
            self.canonical_slot_mut(market, orderbook.ts_bucket)
                .orderbook = Some(orderbook.clone());
            self.mark_dirty_recompute_if_finalized(orderbook.ts_bucket);
        }
    }

    fn store_canonical_liq(&mut self, market: MarketKind, liq: AggLiq1mEvent) {
        let changed = {
            let slot = self.canonical_slot_mut(market, liq.ts_bucket);
            slot.liq.as_ref() != Some(&liq)
        };
        if changed {
            self.canonical_slot_mut(market, liq.ts_bucket).liq = Some(liq.clone());
            self.mark_dirty_recompute_if_finalized(liq.ts_bucket);
        }
    }

    fn store_canonical_funding_mark(
        &mut self,
        market: MarketKind,
        funding_mark: AggFundingMark1mEvent,
    ) {
        let changed = {
            let slot = self.canonical_slot_mut(market, funding_mark.ts_bucket);
            slot.funding_mark.as_ref() != Some(&funding_mark)
        };
        if changed {
            self.canonical_slot_mut(market, funding_mark.ts_bucket)
                .funding_mark = Some(funding_mark.clone());
            self.mark_dirty_recompute_if_finalized(funding_mark.ts_bucket);
        }
    }

    fn store_current_open_interest(&mut self, open_interest: OpenInterestCurrentEvent) {
        let point = OpenInterestCurrentSidecar {
            ts_effective: open_interest.ts_effective,
            open_interest_contracts: open_interest.open_interest_contracts,
            mark_price: open_interest.mark_price,
            open_interest_value_usdt: open_interest.open_interest_value_usdt,
        };
        upsert_sorted_point(
            &mut self.current_open_interest_timeline,
            point.ts_effective,
            point,
            OI_CURRENT_HISTORY_KEEP_MINUTES,
            |item| item.ts_effective,
        );
    }

    fn store_open_interest_hist_5m(&mut self, open_interest_hist: OpenInterestHist5mEvent) {
        let point = OpenInterestHistPoint {
            ts_bucket: open_interest_hist.ts_bucket,
            open_interest_contracts: open_interest_hist.open_interest_contracts,
            open_interest_value_usdt: open_interest_hist.open_interest_value_usdt,
            reference_price: open_interest_hist.reference_price,
        };
        let changed = upsert_sorted_point(
            &mut self.open_interest_hist_5m,
            point.ts_bucket,
            point,
            OI_RATIO_HISTORY_KEEP_5M_BUCKETS,
            |item| item.ts_bucket,
        );
        if changed {
            self.mark_oi_ratio_patch_if_finalized(
                open_interest_hist.ts_bucket,
                "open_interest_hist_5m_changed",
            );
        }
    }

    fn store_long_short_ratio_5m(&mut self, long_short_ratio: LongShortRatio5mEvent) {
        let point = LongShortRatioPoint {
            ts_bucket: long_short_ratio.ts_bucket,
            long_short_ratio: long_short_ratio.long_short_ratio,
            long_account_ratio: long_short_ratio.long_account_ratio,
            short_account_ratio: long_short_ratio.short_account_ratio,
        };
        let changed = match long_short_ratio.ratio_type {
            LongShortRatioType::GlobalAccount => upsert_sorted_point(
                &mut self.global_account_ratio_5m,
                point.ts_bucket,
                point,
                OI_RATIO_HISTORY_KEEP_5M_BUCKETS,
                |item| item.ts_bucket,
            ),
            LongShortRatioType::TopAccount => upsert_sorted_point(
                &mut self.top_account_ratio_5m,
                point.ts_bucket,
                point,
                OI_RATIO_HISTORY_KEEP_5M_BUCKETS,
                |item| item.ts_bucket,
            ),
            LongShortRatioType::TopPosition => upsert_sorted_point(
                &mut self.top_position_ratio_5m,
                point.ts_bucket,
                point,
                OI_RATIO_HISTORY_KEEP_5M_BUCKETS,
                |item| item.ts_bucket,
            ),
        };
        if changed {
            self.mark_oi_ratio_patch_if_finalized(
                long_short_ratio.ts_bucket,
                "long_short_ratio_5m_changed",
            );
        }
    }

    fn store_option_mark_greeks_5m(&mut self, event: OptionMarkGreeks5mEvent) {
        let point = OptionMarkGreeksPoint {
            ts_bucket: event.ts_bucket,
            option_symbol: event.option_symbol,
            underlying_asset: event.underlying_asset,
            expiry_ts: event.expiry_ts,
            strike_price: event.strike_price,
            contract_side: event.contract_side,
            unit: event.unit,
            index_price: event.index_price,
            mark_price: event.mark_price,
            bid_iv: event.bid_iv,
            ask_iv: event.ask_iv,
            mark_iv: event.mark_iv,
            delta: event.delta,
            gamma: event.gamma,
            vega: event.vega,
            theta: event.theta,
            risk_free_interest: event.risk_free_interest,
        };
        upsert_sorted_point_by(
            &mut self.option_mark_greeks_5m,
            point,
            OPTIONS_SURFACE_HISTORY_KEEP_5M_BUCKETS * 512,
            |item| (item.ts_bucket, item.option_symbol.clone()),
        );
    }

    fn mark_oi_ratio_patch_if_finalized(&mut self, ts_bucket: DateTime<Utc>, reason: &'static str) {
        if self
            .last_finalized_minute
            .map(|last| ts_bucket <= last)
            .unwrap_or(false)
        {
            let old_from = self.oi_ratio_patch_from;
            let extends_backward = old_from.map(|prev| ts_bucket < prev).unwrap_or(true);
            self.oi_ratio_patch_from = Some(
                self.oi_ratio_patch_from
                    .map(|prev| prev.min(ts_bucket))
                    .unwrap_or(ts_bucket),
            );
            if let Some(last) = self.last_finalized_minute {
                self.oi_ratio_patch_end = Some(
                    self.oi_ratio_patch_end
                        .map(|prev| prev.max(last))
                        .unwrap_or(last),
                );
            }
            self.oi_ratio_patch_mark_total += 1;
            if extends_backward {
                self.oi_ratio_patch_extends_backward_total += 1;
            }
            debug!(
                oi_ratio_patch_mark_total = self.oi_ratio_patch_mark_total,
                oi_ratio_patch_mark_minute = %ts_bucket,
                oi_ratio_patch_mark_reason = reason,
                oi_ratio_patch_extends_backward = extends_backward,
                oi_ratio_patch_extends_backward_total = self.oi_ratio_patch_extends_backward_total,
                oi_ratio_patch_from = ?self.oi_ratio_patch_from,
                oi_ratio_patch_end = ?self.oi_ratio_patch_end,
                last_finalized_minute = ?self.last_finalized_minute,
                "oi_ratio patch minute marked"
            );
        }
    }

    fn canonical_slot_mut(
        &mut self,
        market: MarketKind,
        ts_bucket: DateTime<Utc>,
    ) -> &mut CanonicalMinuteInputs {
        self.canonical_minutes
            .entry(ts_bucket.timestamp())
            .or_default()
            .for_market_mut(market)
    }

    fn canonical_slot(
        &self,
        market: MarketKind,
        ts_bucket: DateTime<Utc>,
    ) -> Option<&CanonicalMinuteInputs> {
        self.canonical_minutes
            .get(&ts_bucket.timestamp())
            .map(|v| v.for_market(market))
    }

    fn mark_dirty_recompute_if_finalized(&mut self, ts_bucket: DateTime<Utc>) {
        if self
            .last_finalized_minute
            .map(|last| ts_bucket <= last)
            .unwrap_or(false)
        {
            // If the new dirty point is earlier than the current batch start,
            // the history truncation must be redone from the new start.
            let extends_backward = self
                .dirty_recompute_from
                .map(|prev| ts_bucket < prev)
                .unwrap_or(true); // first dirty trigger is always a "fresh start"

            self.dirty_recompute_from = Some(
                self.dirty_recompute_from
                    .map(|prev| prev.min(ts_bucket))
                    .unwrap_or(ts_bucket),
            );

            if extends_backward {
                // Reset truncation flag so the next batch call re-truncates
                // from the new (earlier) start position.
                self.dirty_recompute_truncated = false;
                // Also clear the cached end so it is recaptured on next batch call.
                self.dirty_recompute_end = None;
            } else {
                // Start hasn't moved earlier; just extend the target end if needed.
                if let Some(last) = self.last_finalized_minute {
                    self.dirty_recompute_end = Some(
                        self.dirty_recompute_end
                            .map(|e| e.max(last))
                            .unwrap_or(last),
                    );
                }
            }
        }
    }

    fn build_bucket_for_minute(
        &mut self,
        market: MarketKind,
        ts_bucket: DateTime<Utc>,
    ) -> MinuteBucket {
        let key = (market, ts_bucket.timestamp());
        let mut bucket = self
            .buckets
            .remove(&key)
            .unwrap_or_else(|| MinuteBucket::new(market, ts_bucket));

        if let Some(canonical) = self.canonical_slot(market, ts_bucket).cloned() {
            if let Some(trade) = canonical.trade.as_ref() {
                bucket.replace_trade_agg(trade);
                let vpin = {
                    let state = self.vpin_state_mut(market);
                    if let Some(snapshot) = &trade.vpin_snapshot {
                        state.restore(snapshot);
                    } else {
                        state.ingest_flow(trade.buy_qty, trade.sell_qty);
                    }
                    state.current_vpin()
                };
                bucket.vpin_close = Some(vpin);
            }
            if let Some(orderbook) = canonical.orderbook.as_ref() {
                bucket.replace_orderbook_agg(orderbook);
            }
            if let Some(liq) = canonical.liq.as_ref() {
                bucket.replace_liq_agg(liq);
            }
        }

        bucket
    }

    fn apply_canonical_funding_minute(&mut self, ts_bucket: DateTime<Utc>) {
        for market in [MarketKind::Futures, MarketKind::Spot] {
            if let Some(funding_mark) = self
                .canonical_slot(market, ts_bucket)
                .and_then(|slot| slot.funding_mark.clone())
            {
                self.apply_funding_mark_agg(&funding_mark);
            }
        }
    }

    fn apply_funding_mark_agg(&mut self, funding_mark: &AggFundingMark1mEvent) {
        for point in &funding_mark.mark_points {
            let state = LatestMarkState {
                ts: point.ts,
                mark_price: point.mark_price,
                index_price: point.index_price,
                funding_rate: point.funding_rate,
                next_funding_time: point.next_funding_time,
            };
            self.latest_mark = Some(state.clone());
            self.mark_timeline.push_back(state);
            while self.mark_timeline.len() > HISTORY_LIMIT_MINUTES * 2 {
                self.mark_timeline.pop_front();
            }
            if let Some(funding_rate) = point.funding_rate {
                self.record_funding_state(
                    point.ts,
                    funding_rate,
                    point.mark_price,
                    point.next_funding_time,
                    point.ts,
                );
            }
        }

        for point in &funding_mark.funding_points {
            self.record_funding_state(
                point.ts,
                point.funding_rate,
                point.mark_price,
                point.next_funding_time,
                point.funding_time.unwrap_or(point.ts),
            );
        }
    }

    fn build_window_bundle(
        &self,
        ts_bucket: DateTime<Utc>,
        futures: MinuteWindowData,
        spot: MinuteWindowData,
    ) -> WindowBundle {
        let start = ts_bucket;
        let end = ts_bucket + Duration::minutes(1);
        let funding_changes_in_window = self
            .funding_changes
            .iter()
            .filter(|c| c.ts_change >= start && c.ts_change < end)
            .cloned()
            .collect::<Vec<_>>();

        let funding_points_in_window = self
            .funding_timeline
            .iter()
            .filter(|v| v.ts >= start && v.ts < end)
            .cloned()
            .collect::<Vec<_>>();

        let mark_points_in_window = self
            .mark_timeline
            .iter()
            .filter(|v| v.ts >= start && v.ts < end)
            .cloned()
            .collect::<Vec<_>>();

        let oi_ratio_view = self.build_oi_ratio_view_for_minute(ts_bucket);
        let options_surface_view = self.build_options_surface_view_for_minute(ts_bucket);

        WindowBundle {
            ts_bucket,
            symbol: self.symbol.clone(),
            futures,
            spot,
            history_futures: self.history_futures.iter().cloned().collect(),
            history_spot: self.history_spot.iter().cloned().collect(),
            trade_history_futures: self
                .build_trade_history_with_canonical_backfill(MarketKind::Futures),
            trade_history_spot: self.build_trade_history_with_canonical_backfill(MarketKind::Spot),
            latest_mark: self.latest_mark.clone(),
            latest_funding: self.latest_funding.clone(),
            funding_changes_in_window,
            funding_points_in_window,
            mark_points_in_window,
            funding_changes_recent: self.funding_changes.iter().cloned().collect(),
            funding_points_recent: self.funding_timeline.iter().cloned().collect(),
            mark_points_recent: self.mark_timeline.iter().cloned().collect(),
            latest_common_oi_ratio_bucket: oi_ratio_view.latest_common_bucket,
            current_open_interest: oi_ratio_view.current_open_interest,
            open_interest_hist_5m: oi_ratio_view.open_interest_hist_5m,
            global_account_ratio_5m: oi_ratio_view.global_account_ratio_5m,
            top_account_ratio_5m: oi_ratio_view.top_account_ratio_5m,
            top_position_ratio_5m: oi_ratio_view.top_position_ratio_5m,
            latest_options_surface_bucket: options_surface_view.latest_bucket,
            options_surface_5m: options_surface_view.points,
        }
    }

    pub fn build_oi_ratio_patch_bundle_for_minute(&self, ts_bucket: DateTime<Utc>) -> WindowBundle {
        let as_of_ts = ts_bucket + Duration::minutes(1);
        let oi_ratio_view = self.build_oi_ratio_view_for_minute(ts_bucket);
        let options_surface_view = self.build_options_surface_view_for_minute(ts_bucket);
        let futures = self
            .history_row(MarketKind::Futures, ts_bucket)
            .map(minute_window_from_history_row)
            .unwrap_or_else(|| MinuteWindowData::empty(MarketKind::Futures, ts_bucket));
        let spot = self
            .history_row(MarketKind::Spot, ts_bucket)
            .map(minute_window_from_history_row)
            .unwrap_or_else(|| MinuteWindowData::empty(MarketKind::Spot, ts_bucket));
        WindowBundle {
            ts_bucket,
            symbol: self.symbol.clone(),
            futures,
            spot,
            history_futures: self.history_prefix(MarketKind::Futures, ts_bucket),
            history_spot: self.history_prefix(MarketKind::Spot, ts_bucket),
            trade_history_futures: self
                .build_trade_history_with_canonical_backfill_until(MarketKind::Futures, ts_bucket),
            trade_history_spot: self
                .build_trade_history_with_canonical_backfill_until(MarketKind::Spot, ts_bucket),
            latest_mark: self.latest_mark_as_of(as_of_ts),
            latest_funding: self.latest_funding_as_of(as_of_ts),
            funding_changes_in_window: self.funding_changes_between(ts_bucket, as_of_ts),
            funding_points_in_window: self.funding_points_between(ts_bucket, as_of_ts),
            mark_points_in_window: self.mark_points_between(ts_bucket, as_of_ts),
            funding_changes_recent: self.funding_changes_until(as_of_ts),
            funding_points_recent: self.funding_points_until(as_of_ts),
            mark_points_recent: self.mark_points_until(as_of_ts),
            latest_common_oi_ratio_bucket: oi_ratio_view.latest_common_bucket,
            current_open_interest: oi_ratio_view.current_open_interest,
            open_interest_hist_5m: oi_ratio_view.open_interest_hist_5m,
            global_account_ratio_5m: oi_ratio_view.global_account_ratio_5m,
            top_account_ratio_5m: oi_ratio_view.top_account_ratio_5m,
            top_position_ratio_5m: oi_ratio_view.top_position_ratio_5m,
            latest_options_surface_bucket: options_surface_view.latest_bucket,
            options_surface_5m: options_surface_view.points,
        }
    }

    fn build_oi_ratio_view_for_minute(&self, ts_bucket: DateTime<Utc>) -> OiRatioWindowView {
        let as_of_ts = ts_bucket + Duration::minutes(1);
        let current_open_interest = self
            .current_open_interest_timeline
            .iter()
            .rev()
            .find(|point| point.ts_effective <= as_of_ts)
            .cloned();

        let mut open_interest_hist_5m = self
            .open_interest_hist_5m
            .iter()
            .filter(|point| point.ts_bucket <= as_of_ts)
            .cloned()
            .collect::<Vec<_>>();
        let mut global_account_ratio_5m = self
            .global_account_ratio_5m
            .iter()
            .filter(|point| point.ts_bucket <= as_of_ts)
            .cloned()
            .collect::<Vec<_>>();
        let mut top_account_ratio_5m = self
            .top_account_ratio_5m
            .iter()
            .filter(|point| point.ts_bucket <= as_of_ts)
            .cloned()
            .collect::<Vec<_>>();
        let mut top_position_ratio_5m = self
            .top_position_ratio_5m
            .iter()
            .filter(|point| point.ts_bucket <= as_of_ts)
            .cloned()
            .collect::<Vec<_>>();

        let latest_common_bucket = match (
            open_interest_hist_5m.last().map(|point| point.ts_bucket),
            global_account_ratio_5m.last().map(|point| point.ts_bucket),
            top_account_ratio_5m.last().map(|point| point.ts_bucket),
            top_position_ratio_5m.last().map(|point| point.ts_bucket),
        ) {
            (Some(oi), Some(global), Some(top_account), Some(top_position)) => {
                Some(oi.min(global).min(top_account).min(top_position))
            }
            _ => None,
        };

        let Some(common_bucket) = latest_common_bucket else {
            return OiRatioWindowView {
                current_open_interest,
                ..OiRatioWindowView::default()
            };
        };

        open_interest_hist_5m.retain(|point| point.ts_bucket <= common_bucket);
        global_account_ratio_5m.retain(|point| point.ts_bucket <= common_bucket);
        top_account_ratio_5m.retain(|point| point.ts_bucket <= common_bucket);
        top_position_ratio_5m.retain(|point| point.ts_bucket <= common_bucket);

        OiRatioWindowView {
            latest_common_bucket: Some(common_bucket),
            current_open_interest,
            open_interest_hist_5m,
            global_account_ratio_5m,
            top_account_ratio_5m,
            top_position_ratio_5m,
        }
    }

    fn build_options_surface_view_for_minute(
        &self,
        ts_bucket: DateTime<Utc>,
    ) -> OptionsSurfaceWindowView {
        let as_of_ts = ts_bucket + Duration::minutes(1);
        let mut by_bucket = BTreeMap::<DateTime<Utc>, Vec<&OptionMarkGreeksPoint>>::new();
        for point in &self.option_mark_greeks_5m {
            if point.ts_bucket <= as_of_ts {
                by_bucket.entry(point.ts_bucket).or_default().push(point);
            }
        }

        let mut points = Vec::with_capacity(by_bucket.len());
        for (bucket, rows) in by_bucket {
            if let Some(point) = aggregate_options_surface_bucket(bucket, &rows) {
                points.push(point);
            }
        }
        let latest_bucket = points.last().map(|point| point.ts_bucket);
        OptionsSurfaceWindowView {
            latest_bucket,
            points,
        }
    }

    fn build_trade_history_with_canonical_backfill(
        &self,
        market: MarketKind,
    ) -> Vec<MinuteHistory> {
        let up_to_ts = match market {
            MarketKind::Futures => self.history_futures.back().map(|h| h.ts_bucket),
            MarketKind::Spot => self.history_spot.back().map(|h| h.ts_bucket),
        };
        self.build_trade_history_with_canonical_backfill_until_opt(market, up_to_ts)
    }

    fn build_trade_history_with_canonical_backfill_until(
        &self,
        market: MarketKind,
        up_to_ts: DateTime<Utc>,
    ) -> Vec<MinuteHistory> {
        self.build_trade_history_with_canonical_backfill_until_opt(market, Some(up_to_ts))
    }

    fn build_trade_history_with_canonical_backfill_until_opt(
        &self,
        market: MarketKind,
        up_to_ts: Option<DateTime<Utc>>,
    ) -> Vec<MinuteHistory> {
        let history = match market {
            MarketKind::Futures => &self.history_futures,
            MarketKind::Spot => &self.history_spot,
        };
        let Some(mut minute) = history.front().map(|h| h.ts_bucket) else {
            return Vec::new();
        };
        let Some(last_minute) = history
            .back()
            .map(|h| h.ts_bucket)
            .map(|last| up_to_ts.map(|bound| last.min(bound)).unwrap_or(last))
        else {
            return Vec::new();
        };

        let existing = history
            .iter()
            .cloned()
            .map(|row| (row.ts_bucket.timestamp(), row))
            .collect::<BTreeMap<_, _>>();

        let mut out = Vec::with_capacity(history.len());
        let mut prev_cvd = 0.0;
        let mut prev_vpin = 0.0;
        while minute <= last_minute {
            let existing_row = existing.get(&minute.timestamp());
            let row = self
                .canonical_slot(market, minute)
                .and_then(|slot| slot.trade.as_ref())
                .map(|trade| {
                    build_trade_history_row_from_canonical(
                        market,
                        trade,
                        existing_row,
                        prev_cvd,
                        prev_vpin,
                    )
                })
                .or_else(|| existing_row.cloned());

            if let Some(row) = row {
                prev_cvd = row.cvd;
                prev_vpin = row.vpin;
                out.push(row);
            }

            minute += Duration::minutes(1);
        }

        out
    }

    fn history_row(&self, market: MarketKind, ts_bucket: DateTime<Utc>) -> Option<&MinuteHistory> {
        let history = match market {
            MarketKind::Futures => &self.history_futures,
            MarketKind::Spot => &self.history_spot,
        };
        history.iter().find(|row| row.ts_bucket == ts_bucket)
    }

    fn history_prefix(&self, market: MarketKind, up_to_ts: DateTime<Utc>) -> Vec<MinuteHistory> {
        let history = match market {
            MarketKind::Futures => &self.history_futures,
            MarketKind::Spot => &self.history_spot,
        };
        history
            .iter()
            .filter(|row| row.ts_bucket <= up_to_ts)
            .cloned()
            .collect()
    }

    fn latest_mark_as_of(&self, as_of_ts: DateTime<Utc>) -> Option<LatestMarkState> {
        self.mark_timeline
            .iter()
            .rev()
            .find(|point| point.ts <= as_of_ts)
            .cloned()
    }

    fn latest_funding_as_of(&self, as_of_ts: DateTime<Utc>) -> Option<LatestFundingState> {
        self.funding_timeline
            .iter()
            .rev()
            .find(|point| point.ts <= as_of_ts)
            .cloned()
    }

    fn funding_changes_between(
        &self,
        start_ts: DateTime<Utc>,
        end_ts: DateTime<Utc>,
    ) -> Vec<FundingChange> {
        self.funding_changes
            .iter()
            .filter(|point| point.ts_change >= start_ts && point.ts_change < end_ts)
            .cloned()
            .collect()
    }

    fn funding_points_between(
        &self,
        start_ts: DateTime<Utc>,
        end_ts: DateTime<Utc>,
    ) -> Vec<LatestFundingState> {
        self.funding_timeline
            .iter()
            .filter(|point| point.ts >= start_ts && point.ts < end_ts)
            .cloned()
            .collect()
    }

    fn mark_points_between(
        &self,
        start_ts: DateTime<Utc>,
        end_ts: DateTime<Utc>,
    ) -> Vec<LatestMarkState> {
        self.mark_timeline
            .iter()
            .filter(|point| point.ts >= start_ts && point.ts < end_ts)
            .cloned()
            .collect()
    }

    fn funding_changes_until(&self, as_of_ts: DateTime<Utc>) -> Vec<FundingChange> {
        self.funding_changes
            .iter()
            .filter(|point| point.ts_change <= as_of_ts)
            .cloned()
            .collect()
    }

    fn funding_points_until(&self, as_of_ts: DateTime<Utc>) -> Vec<LatestFundingState> {
        self.funding_timeline
            .iter()
            .filter(|point| point.ts <= as_of_ts)
            .cloned()
            .collect()
    }

    fn mark_points_until(&self, as_of_ts: DateTime<Utc>) -> Vec<LatestMarkState> {
        self.mark_timeline
            .iter()
            .filter(|point| point.ts <= as_of_ts)
            .cloned()
            .collect()
    }

    fn push_finalized_vpin_snapshot(&mut self, market: MarketKind, ts_bucket: DateTime<Utc>) {
        let snapshot = self.vpin_state_mut(market).snapshot();
        let target = match market {
            MarketKind::Futures => &mut self.finalized_vpin_futures,
            MarketKind::Spot => &mut self.finalized_vpin_spot,
        };
        target.push_back(FinalizedVpinState {
            ts_bucket,
            snapshot,
        });
    }

    fn trim_replay_retention(&mut self, reference_minute: DateTime<Utc>) {
        let canonical_cutoff = reference_minute - Duration::minutes(CANONICAL_REPLAY_KEEP_MINUTES);
        self.canonical_minutes
            .retain(|minute_sec, _| *minute_sec >= canonical_cutoff.timestamp());

        let vpin_cutoff = reference_minute - Duration::minutes(CANONICAL_REPLAY_KEEP_MINUTES + 1);
        while self
            .finalized_vpin_futures
            .front()
            .map(|v| v.ts_bucket < vpin_cutoff)
            .unwrap_or(false)
        {
            self.finalized_vpin_futures.pop_front();
        }
        while self
            .finalized_vpin_spot
            .front()
            .map(|v| v.ts_bucket < vpin_cutoff)
            .unwrap_or(false)
        {
            self.finalized_vpin_spot.pop_front();
        }
    }

    fn truncate_finalized_suffix(&mut self, start: DateTime<Utc>) {
        while self
            .history_futures
            .back()
            .map(|h| h.ts_bucket >= start)
            .unwrap_or(false)
        {
            self.history_futures.pop_back();
        }
        while self
            .history_spot
            .back()
            .map(|h| h.ts_bucket >= start)
            .unwrap_or(false)
        {
            self.history_spot.pop_back();
        }
        while self
            .finalized_vpin_futures
            .back()
            .map(|v| v.ts_bucket >= start)
            .unwrap_or(false)
        {
            self.finalized_vpin_futures.pop_back();
        }
        while self
            .finalized_vpin_spot
            .back()
            .map(|v| v.ts_bucket >= start)
            .unwrap_or(false)
        {
            self.finalized_vpin_spot.pop_back();
        }

        self.cvd_futures = self.history_futures.back().map(|h| h.cvd).unwrap_or(0.0);
        self.cvd_spot = self.history_spot.back().map(|h| h.cvd).unwrap_or(0.0);

        if let Some(snapshot) = self
            .finalized_vpin_futures
            .back()
            .map(|v| v.snapshot.clone())
        {
            self.vpin_futures.restore(&snapshot);
        } else {
            self.vpin_futures = VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS);
        }
        if let Some(snapshot) = self.finalized_vpin_spot.back().map(|v| v.snapshot.clone()) {
            self.vpin_spot.restore(&snapshot);
        } else {
            self.vpin_spot = VpinState::new(VPIN_BUCKET_SIZE_BASE, VPIN_ROLLING_BUCKETS);
        }

        while self
            .mark_timeline
            .back()
            .map(|v| v.ts >= start)
            .unwrap_or(false)
        {
            self.mark_timeline.pop_back();
        }
        while self
            .funding_timeline
            .back()
            .map(|v| v.ts >= start)
            .unwrap_or(false)
        {
            self.funding_timeline.pop_back();
        }
        while self
            .funding_changes
            .back()
            .map(|v| v.ts_change >= start)
            .unwrap_or(false)
        {
            self.funding_changes.pop_back();
        }

        self.latest_mark = self.mark_timeline.back().cloned();
        self.latest_funding = self.funding_timeline.back().cloned();
        self.last_finalized_minute = self
            .history_futures
            .back()
            .map(|h| h.ts_bucket)
            .or_else(|| self.history_spot.back().map(|h| h.ts_bucket));
    }

    fn record_funding_state(
        &mut self,
        state_ts: DateTime<Utc>,
        funding_rate: f64,
        mark_price: Option<f64>,
        next_funding_time: Option<DateTime<Utc>>,
        change_ts: DateTime<Utc>,
    ) {
        let prev = self.latest_funding.as_ref().map(|f| f.funding_rate);
        let delta = prev.map(|p| funding_rate - p);
        let state = LatestFundingState {
            ts: state_ts,
            funding_rate,
            mark_price,
            next_funding_time,
        };
        self.latest_funding = Some(state.clone());
        self.funding_timeline.push_back(state);
        while self.funding_timeline.len() > HISTORY_LIMIT_MINUTES * 2 {
            self.funding_timeline.pop_front();
        }

        let changed = prev
            .map(|p| (p - funding_rate).abs() > 1e-12)
            .unwrap_or(true);
        if changed {
            self.funding_changes.push_back(FundingChange {
                ts_change: change_ts,
                prev,
                new: funding_rate,
                delta,
                mark_price_at_change: mark_price,
            });
            while self.funding_changes.len() > HISTORY_LIMIT_MINUTES {
                self.funding_changes.pop_front();
            }
        }
    }

    fn ingest_depth_conflated(
        &mut self,
        market: MarketKind,
        bucket_ts: DateTime<Utc>,
        event_ts: DateTime<Utc>,
        sample: OrderbookSample,
    ) {
        let bucket_key = (market, bucket_ts.timestamp());
        let slot_100ms = event_ts.timestamp_millis().div_euclid(DEPTH_CONFLATION_MS);
        let mut flush_prev_sample: Option<OrderbookSample> = None;

        {
            let state =
                self.depth_conflation
                    .entry(bucket_key)
                    .or_insert_with(|| DepthConflationState {
                        current_slot_100ms: slot_100ms,
                        latest_sample: None,
                    });
            if state.current_slot_100ms != slot_100ms {
                flush_prev_sample = state.latest_sample.take();
                state.current_slot_100ms = slot_100ms;
            }
            state.latest_sample = Some(sample);
        }

        if let Some(prev) = flush_prev_sample {
            let bucket = self.bucket_mut(market, bucket_ts);
            bucket.apply_orderbook_sample(prev);
        }
    }

    fn flush_depth_conflation_for_bucket(&mut self, market: MarketKind, ts_bucket: DateTime<Utc>) {
        let key = (market, ts_bucket.timestamp());
        let pending = if let Some(state) = self.depth_conflation.get_mut(&key) {
            state.latest_sample.take()
        } else {
            None
        };
        if let Some(sample) = pending {
            let bucket = self.bucket_mut(market, ts_bucket);
            bucket.apply_orderbook_sample(sample);
        }
        self.depth_conflation.remove(&key);
    }

    pub fn history_futures_len(&self) -> usize {
        self.history_futures.len()
    }

    pub fn extract_snapshot(&self) -> StateSnapshot {
        let last_finalized_ts = self
            .last_finalized_minute
            .unwrap_or_else(|| Utc::now() - chrono::Duration::minutes(1));
        StateSnapshot {
            version: STATE_SNAPSHOT_VERSION,
            symbol: self.symbol.clone(),
            last_finalized_ts,
            saved_at: Utc::now(),
            effective_history_floor_ts: self.effective_history_floor_ts,
            cvd_futures: self.cvd_futures,
            cvd_spot: self.cvd_spot,
            vpin_futures: self.vpin_futures.clone(),
            vpin_spot: self.vpin_spot.clone(),
            finalized_vpin_futures: self.finalized_vpin_futures.iter().cloned().collect(),
            finalized_vpin_spot: self.finalized_vpin_spot.iter().cloned().collect(),
            history_futures: self.history_futures.iter().cloned().collect(),
            history_spot: self.history_spot.iter().cloned().collect(),
            latest_mark: self.latest_mark.clone(),
            latest_funding: self.latest_funding.clone(),
            funding_changes: self.funding_changes.iter().cloned().collect(),
            mark_timeline: self.mark_timeline.iter().cloned().collect(),
            funding_timeline: self.funding_timeline.iter().cloned().collect(),
            current_open_interest_timeline: self
                .current_open_interest_timeline
                .iter()
                .cloned()
                .collect(),
            open_interest_hist_5m: self.open_interest_hist_5m.iter().cloned().collect(),
            global_account_ratio_5m: self.global_account_ratio_5m.iter().cloned().collect(),
            top_account_ratio_5m: self.top_account_ratio_5m.iter().cloned().collect(),
            top_position_ratio_5m: self.top_position_ratio_5m.iter().cloned().collect(),
            option_mark_greeks_5m: self.option_mark_greeks_5m.iter().cloned().collect(),
        }
    }

    pub fn restore_from_snapshot(&mut self, snap: StateSnapshot) {
        self.effective_history_floor_ts = snap.effective_history_floor_ts;
        self.vpin_futures = snap.vpin_futures;
        self.vpin_spot = snap.vpin_spot;
        self.finalized_vpin_futures = snap.finalized_vpin_futures.into_iter().collect();
        self.finalized_vpin_spot = snap.finalized_vpin_spot.into_iter().collect();
        self.history_futures = snap.history_futures.into_iter().collect();
        self.history_spot = snap.history_spot.into_iter().collect();
        self.latest_mark = snap.latest_mark;
        self.latest_funding = snap.latest_funding;
        self.funding_changes = snap.funding_changes.into_iter().collect();
        self.mark_timeline = snap.mark_timeline.into_iter().collect();
        self.funding_timeline = snap.funding_timeline.into_iter().collect();
        self.current_open_interest_timeline =
            snap.current_open_interest_timeline.into_iter().collect();
        self.open_interest_hist_5m = snap.open_interest_hist_5m.into_iter().collect();
        self.global_account_ratio_5m = snap.global_account_ratio_5m.into_iter().collect();
        self.top_account_ratio_5m = snap.top_account_ratio_5m.into_iter().collect();
        self.top_position_ratio_5m = snap.top_position_ratio_5m.into_iter().collect();
        self.option_mark_greeks_5m = snap.option_mark_greeks_5m.into_iter().collect();
        // CVD must be derived from history tail (not stored value) to ensure accuracy.
        self.cvd_futures = self.history_futures.back().map(|h| h.cvd).unwrap_or(0.0);
        self.cvd_spot = self.history_spot.back().map(|h| h.cvd).unwrap_or(0.0);
        // last_finalized_minute also derived from history tail.
        self.last_finalized_minute = self.history_futures.back().map(|h| h.ts_bucket);
    }
}

#[derive(Debug, Clone)]
struct DerivedExpirySurface {
    expiry_ts: DateTime<Utc>,
    atm_strike: Option<f64>,
    atm_iv: Option<f64>,
    rr_25d: Option<f64>,
}

fn aggregate_options_surface_bucket(
    ts_bucket: DateTime<Utc>,
    rows: &[&OptionMarkGreeksPoint],
) -> Option<OptionsSurfacePoint> {
    let live_rows = rows
        .iter()
        .copied()
        .filter(|row| row.expiry_ts > ts_bucket)
        .collect::<Vec<_>>();
    if live_rows.is_empty() {
        return None;
    }

    let mut by_expiry = BTreeMap::<DateTime<Utc>, Vec<&OptionMarkGreeksPoint>>::new();
    for row in live_rows {
        by_expiry.entry(row.expiry_ts).or_default().push(row);
    }

    let mut surfaces = by_expiry
        .into_iter()
        .map(|(expiry_ts, expiry_rows)| derive_expiry_surface(ts_bucket, expiry_ts, &expiry_rows))
        .collect::<Vec<_>>();
    surfaces.sort_by_key(|surface| surface.expiry_ts);

    let front = surfaces.first();
    let second = surfaces.get(1);
    let atm_iv_30d_proxy = derive_atm_iv_30d_proxy(ts_bucket, &surfaces);

    Some(OptionsSurfacePoint {
        ts_bucket,
        front_expiry_ts: front.map(|surface| surface.expiry_ts),
        second_expiry_ts: second.map(|surface| surface.expiry_ts),
        atm_strike_front: front.and_then(|surface| surface.atm_strike),
        atm_iv_front: front.and_then(|surface| surface.atm_iv),
        atm_iv_second: second.and_then(|surface| surface.atm_iv),
        atm_iv_30d_proxy,
        rr_25d_front: front.and_then(|surface| surface.rr_25d),
        rr_25d_second: second.and_then(|surface| surface.rr_25d),
        skew_state: classify_skew_state(front.and_then(|surface| surface.rr_25d)).to_string(),
        term_structure_state: classify_term_structure_state(
            front.and_then(|surface| surface.atm_iv),
            second.and_then(|surface| surface.atm_iv),
        )
        .to_string(),
    })
}

fn derive_expiry_surface(
    ts_bucket: DateTime<Utc>,
    expiry_ts: DateTime<Utc>,
    rows: &[&OptionMarkGreeksPoint],
) -> DerivedExpirySurface {
    let index_price = rows.iter().find_map(|row| row.index_price);
    let atm_strike = index_price.and_then(|px| choose_nearest_strike(rows, px));
    let atm_iv = atm_strike.and_then(|strike| compute_atm_iv(rows, strike));
    let rr_25d = compute_rr_25d(rows);

    let _ = ts_bucket;
    DerivedExpirySurface {
        expiry_ts,
        atm_strike,
        atm_iv,
        rr_25d,
    }
}

fn choose_nearest_strike(rows: &[&OptionMarkGreeksPoint], index_price: f64) -> Option<f64> {
    rows.iter()
        .filter_map(|row| {
            let iv_score = if row.mark_iv.is_some() { 0 } else { 1 };
            Some(((row.strike_price - index_price).abs(), iv_score, row.strike_price))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
        .map(|tuple| tuple.2)
}

fn compute_atm_iv(rows: &[&OptionMarkGreeksPoint], strike: f64) -> Option<f64> {
    let strike_rows = rows
        .iter()
        .copied()
        .filter(|row| (row.strike_price - strike).abs() <= 1e-9)
        .collect::<Vec<_>>();
    let call_iv = strike_rows
        .iter()
        .find(|row| row.contract_side.eq_ignore_ascii_case("call"))
        .and_then(|row| row.mark_iv.or(row.bid_iv).or(row.ask_iv));
    let put_iv = strike_rows
        .iter()
        .find(|row| row.contract_side.eq_ignore_ascii_case("put"))
        .and_then(|row| row.mark_iv.or(row.bid_iv).or(row.ask_iv));

    match (call_iv, put_iv) {
        (Some(call), Some(put)) => Some((call + put) / 2.0),
        (Some(call), None) => Some(call),
        (None, Some(put)) => Some(put),
        (None, None) => None,
    }
}

fn compute_rr_25d(rows: &[&OptionMarkGreeksPoint]) -> Option<f64> {
    let call = rows
        .iter()
        .filter(|row| row.contract_side.eq_ignore_ascii_case("call"))
        .filter_map(|row| {
            Some((
                (row.delta? - 0.25).abs(),
                row.mark_iv.or(row.bid_iv).or(row.ask_iv)?,
            ))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|tuple| tuple.1);
    let put = rows
        .iter()
        .filter(|row| row.contract_side.eq_ignore_ascii_case("put"))
        .filter_map(|row| {
            Some((
                (row.delta? + 0.25).abs(),
                row.mark_iv.or(row.bid_iv).or(row.ask_iv)?,
            ))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|tuple| tuple.1);

    call.zip(put).map(|(call_iv, put_iv)| call_iv - put_iv)
}

fn derive_atm_iv_30d_proxy(
    ts_bucket: DateTime<Utc>,
    surfaces: &[DerivedExpirySurface],
) -> Option<f64> {
    let mut points = surfaces
        .iter()
        .filter_map(|surface| {
            let atm_iv = surface.atm_iv?;
            let days = (surface.expiry_ts - ts_bucket).num_seconds() as f64 / 86_400.0;
            if days <= 0.0 {
                None
            } else {
                Some((days, atm_iv))
            }
        })
        .collect::<Vec<_>>();
    if points.is_empty() {
        return None;
    }
    points.sort_by(|a, b| a.0.total_cmp(&b.0));

    let target = 30.0;
    for window in points.windows(2) {
        let (d0, iv0) = window[0];
        let (d1, iv1) = window[1];
        if d0 <= target && target <= d1 && (d1 - d0).abs() > f64::EPSILON {
            let w = (target - d0) / (d1 - d0);
            return Some(iv0 + ((iv1 - iv0) * w));
        }
    }

    points
        .into_iter()
        .min_by(|a, b| (a.0 - target).abs().total_cmp(&(b.0 - target).abs()))
        .map(|tuple| tuple.1)
}

fn classify_skew_state(rr_25d_front: Option<f64>) -> &'static str {
    match rr_25d_front {
        Some(value) if value <= -0.02 => "put_skewed",
        Some(value) if value >= 0.02 => "call_skewed",
        Some(_) => "neutral",
        None => "unclear",
    }
}

fn classify_term_structure_state(
    front_iv: Option<f64>,
    second_iv: Option<f64>,
) -> &'static str {
    match front_iv.zip(second_iv) {
        Some((front, second)) if (front - second) >= 0.01 => "front_rich",
        Some((front, second)) if (second - front) >= 0.01 => "back_rich",
        Some(_) => "flat",
        None => "unclear",
    }
}

pub const STATE_SNAPSHOT_VERSION: u32 = 3;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StateSnapshot {
    pub version: u32,
    pub symbol: String,
    pub last_finalized_ts: DateTime<Utc>,
    pub saved_at: DateTime<Utc>,
    #[serde(default)]
    pub effective_history_floor_ts: Option<DateTime<Utc>>,
    /// Informational only; CVD is re-derived from history tail on restore.
    pub cvd_futures: f64,
    pub cvd_spot: f64,
    pub vpin_futures: VpinState,
    pub vpin_spot: VpinState,
    pub finalized_vpin_futures: Vec<FinalizedVpinState>,
    pub finalized_vpin_spot: Vec<FinalizedVpinState>,
    /// VecDeque serialized as Vec.
    pub history_futures: Vec<MinuteHistory>,
    pub history_spot: Vec<MinuteHistory>,
    pub latest_mark: Option<LatestMarkState>,
    pub latest_funding: Option<LatestFundingState>,
    pub funding_changes: Vec<FundingChange>,
    pub mark_timeline: Vec<LatestMarkState>,
    pub funding_timeline: Vec<LatestFundingState>,
    #[serde(default)]
    pub current_open_interest_timeline: Vec<OpenInterestCurrentSidecar>,
    #[serde(default)]
    pub open_interest_hist_5m: Vec<OpenInterestHistPoint>,
    #[serde(default)]
    pub global_account_ratio_5m: Vec<LongShortRatioPoint>,
    #[serde(default)]
    pub top_account_ratio_5m: Vec<LongShortRatioPoint>,
    #[serde(default)]
    pub top_position_ratio_5m: Vec<LongShortRatioPoint>,
    #[serde(default)]
    pub option_mark_greeks_5m: Vec<OptionMarkGreeksPoint>,
}

pub fn floor_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    let sec = ts.timestamp();
    let floored = sec - sec.rem_euclid(60);
    Utc.timestamp_opt(floored, 0).single().unwrap_or(ts)
}

fn upsert_sorted_point<T, F>(
    deque: &mut VecDeque<T>,
    point_ts: DateTime<Utc>,
    point: T,
    keep_limit: usize,
    ts_of: F,
) -> bool
where
    T: Clone + PartialEq,
    F: Fn(&T) -> DateTime<Utc>,
{
    if let Some(existing_idx) = deque.iter().position(|item| ts_of(item) == point_ts) {
        if deque[existing_idx] == point {
            return false;
        }
        deque[existing_idx] = point;
        return true;
    }

    match deque
        .back()
        .map(|item| ts_of(item) <= point_ts)
        .unwrap_or(true)
    {
        true => deque.push_back(point),
        false => {
            let insert_idx = deque
                .iter()
                .position(|item| ts_of(item) > point_ts)
                .unwrap_or(deque.len());
            deque.insert(insert_idx, point);
        }
    }

    while deque.len() > keep_limit {
        deque.pop_front();
    }
    true
}

fn upsert_sorted_point_by<T, K, F>(
    deque: &mut VecDeque<T>,
    point: T,
    keep_limit: usize,
    key_of: F,
) -> bool
where
    T: Clone + PartialEq,
    K: Ord,
    F: Fn(&T) -> K,
{
    let point_key = key_of(&point);
    if let Some(existing_idx) = deque.iter().position(|item| key_of(item) == point_key) {
        if deque[existing_idx] == point {
            return false;
        }
        deque[existing_idx] = point;
        return true;
    }

    let insert_idx = deque
        .iter()
        .position(|item| key_of(item) > point_key)
        .unwrap_or(deque.len());
    deque.insert(insert_idx, point);

    while deque.len() > keep_limit {
        deque.pop_front();
    }
    true
}

pub fn price_to_tick(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

pub fn tick_to_price(tick: i64) -> f64 {
    tick as f64 / PRICE_SCALE
}

fn weighted_topk_depth(
    bids: &BTreeMap<i64, f64>,
    asks: &BTreeMap<i64, f64>,
    topk: usize,
) -> (f64, f64) {
    let weighted_bid = bids
        .iter()
        .rev()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(ORDERBOOK_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    let weighted_ask = asks
        .iter()
        .take(topk)
        .enumerate()
        .map(|(idx, (_, qty))| (-(ORDERBOOK_DW_LAMBDA) * idx as f64).exp() * *qty)
        .sum::<f64>();
    (weighted_bid, weighted_ask)
}

fn build_trade_history_row_from_canonical(
    market: MarketKind,
    trade: &AggTrade1mEvent,
    existing: Option<&MinuteHistory>,
    prev_cvd: f64,
    prev_vpin: f64,
) -> MinuteHistory {
    let profile = trade
        .profile_levels
        .iter()
        .map(|level| {
            (
                price_to_tick(level.price),
                LevelAgg {
                    buy_qty: level.buy_qty,
                    sell_qty: level.sell_qty,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let total_qty = (trade.buy_qty + trade.sell_qty).max(0.0);
    let total_notional = trade.buy_notional + trade.sell_notional;
    let delta = trade.buy_qty - trade.sell_qty;
    let relative_delta = if total_qty > 0.0 {
        delta / total_qty
    } else {
        0.0
    };
    let avwap_minute = (total_qty > 0.0)
        .then_some(total_notional / total_qty)
        .or_else(|| existing.and_then(|row| row.avwap_minute));
    let vpin = trade
        .vpin_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_vpin)
        .or_else(|| existing.map(|row| row.vpin))
        .unwrap_or(prev_vpin);

    MinuteHistory {
        ts_bucket: trade.ts_bucket,
        market,
        open_price: trade
            .first_price
            .or(existing.and_then(|row| row.open_price)),
        high_price: trade.high_price.or(existing.and_then(|row| row.high_price)),
        low_price: trade.low_price.or(existing.and_then(|row| row.low_price)),
        close_price: trade
            .last_price
            .or(existing.and_then(|row| row.close_price)),
        last_price: trade.last_price.or(existing.and_then(|row| row.last_price)),
        buy_qty: trade.buy_qty,
        sell_qty: trade.sell_qty,
        total_qty,
        total_notional,
        delta,
        relative_delta,
        force_liq: existing
            .map(|row| row.force_liq.clone())
            .unwrap_or_default(),
        ofi: existing.map(|row| row.ofi).unwrap_or(0.0),
        spread_twa: existing.and_then(|row| row.spread_twa),
        topk_depth_twa: existing.and_then(|row| row.topk_depth_twa),
        obi_twa: existing.and_then(|row| row.obi_twa),
        obi_l1_twa: existing.and_then(|row| row.obi_l1_twa),
        obi_k_twa: existing.and_then(|row| row.obi_k_twa),
        obi_k_dw_twa: existing.and_then(|row| row.obi_k_dw_twa),
        obi_k_dw_close: existing.and_then(|row| row.obi_k_dw_close),
        obi_k_dw_change: existing.and_then(|row| row.obi_k_dw_change),
        obi_k_dw_adj_twa: existing.and_then(|row| row.obi_k_dw_adj_twa),
        bbo_updates: existing.map(|row| row.bbo_updates).unwrap_or(0),
        microprice_twa: existing.and_then(|row| row.microprice_twa),
        microprice_classic_twa: existing.and_then(|row| row.microprice_classic_twa),
        microprice_kappa_twa: existing.and_then(|row| row.microprice_kappa_twa),
        microprice_adj_twa: existing.and_then(|row| row.microprice_adj_twa),
        cvd: prev_cvd + delta,
        vpin,
        avwap_minute,
        whale_trade_count: trade.whale.trade_count,
        whale_buy_count: trade.whale.buy_count,
        whale_sell_count: trade.whale.sell_count,
        whale_notional_total: trade.whale.notional_total,
        whale_notional_buy: trade.whale.notional_buy,
        whale_notional_sell: trade.whale.notional_sell,
        whale_qty_eth_total: trade.whale.qty_eth_total,
        whale_qty_eth_buy: trade.whale.qty_eth_buy,
        whale_qty_eth_sell: trade.whale.qty_eth_sell,
        whale_max_single_notional: trade.whale.max_single_notional,
        profile,
    }
}

fn minute_window_from_history_row(row: &MinuteHistory) -> MinuteWindowData {
    MinuteWindowData {
        market: row.market,
        ts_bucket: row.ts_bucket,
        trade_count: 0,
        buy_qty: row.buy_qty,
        sell_qty: row.sell_qty,
        total_qty: row.total_qty,
        buy_notional: 0.0,
        sell_notional: 0.0,
        total_notional: row.total_notional,
        delta: row.delta,
        relative_delta: row.relative_delta,
        first_price: row.open_price,
        last_price: row.last_price.or(row.close_price),
        high_price: row.high_price,
        low_price: row.low_price,
        profile: row.profile.clone(),
        force_liq: row.force_liq.clone(),
        heatmap: BTreeMap::new(),
        depth_k: ORDERBOOK_TOPK,
        spread_twa: row.spread_twa,
        topk_depth_twa: row.topk_depth_twa,
        obi_twa: row.obi_twa,
        obi_l1_twa: row.obi_l1_twa,
        obi_k_twa: row.obi_k_twa,
        obi_k_dw_twa: row.obi_k_dw_twa,
        obi_k_dw_close: row.obi_k_dw_close,
        obi_k_dw_change: row.obi_k_dw_change,
        obi_k_dw_adj_twa: row.obi_k_dw_adj_twa,
        ofi: row.ofi,
        bbo_updates: row.bbo_updates,
        microprice_twa: row.microprice_twa,
        microprice_classic_twa: row.microprice_classic_twa,
        microprice_kappa_twa: row.microprice_kappa_twa,
        microprice_adj_twa: row.microprice_adj_twa,
        avwap: row.avwap_minute,
        whale: WhaleStats {
            trade_count: row.whale_trade_count,
            buy_count: row.whale_buy_count,
            sell_count: row.whale_sell_count,
            notional_total: row.whale_notional_total,
            notional_buy: row.whale_notional_buy,
            notional_sell: row.whale_notional_sell,
            qty_eth_total: row.whale_qty_eth_total,
            qty_eth_buy: row.whale_qty_eth_buy,
            qty_eth_sell: row.whale_qty_eth_sell,
            max_single_notional: row.whale_max_single_notional,
        },
    }
}

fn collect_heatmap_levels(
    bids: &BTreeMap<i64, f64>,
    asks: &BTreeMap<i64, f64>,
) -> Vec<HeatmapLevelSample> {
    let mut merged = BTreeMap::<i64, BookLevelAgg>::new();
    for (tick, qty) in bids {
        merged.entry(*tick).or_default().bid_liquidity = *qty;
    }
    for (tick, qty) in asks {
        merged.entry(*tick).or_default().ask_liquidity = *qty;
    }
    merged
        .into_iter()
        .map(|(tick, level)| HeatmapLevelSample {
            tick,
            bid_qty: level.bid_liquidity,
            ask_qty: level.ask_liquidity,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::StateStore;
    use crate::ingest::decoder::{
        AggFundingMark1mEvent, AggFundingPoint, AggHeatmapLevel, AggLiq1mEvent, AggLiqLevel,
        AggMarkPoint, AggOrderbook1mEvent, AggProfileLevel, AggTrade1mEvent, AggVpinSnapshot,
        AggWhaleStats, BboEvent, EngineEvent, KlineEvent, LongShortRatio5mEvent,
        LongShortRatioType, MarketKind, MdData, OpenInterestHist5mEvent,
    };
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};
    use uuid::Uuid;

    fn agg_trade_event(
        ts_bucket: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        last_vpin: f64,
    ) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty,
                sell_qty,
                buy_notional: buy_qty * 2000.0,
                sell_notional: sell_qty * 2000.0,
                first_price: Some(2000.0),
                last_price: Some(2000.0),
                high_price: Some(2000.0),
                low_price: Some(2000.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin,
                }),
            }),
        }
    }

    fn agg_trade_event_spot(
        ts_bucket: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        last_vpin: f64,
    ) -> EngineEvent {
        let mut event = agg_trade_event(ts_bucket, buy_qty, sell_qty, last_vpin);
        event.market = MarketKind::Spot;
        event.routing_key = "md.agg.spot.trade.1m.testusdt".to_string();
        event
    }

    fn oi_hist_event(ts_bucket: chrono::DateTime<Utc>, oi_value: f64) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.open_interest.hist.5m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.futures.open_interest.hist.5m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::minutes(5),
            published_at: ts_bucket + ChronoDuration::minutes(5),
            data: MdData::OpenInterestHist5m(OpenInterestHist5mEvent {
                ts_bucket,
                open_interest_contracts: oi_value / 100.0,
                open_interest_value_usdt: oi_value,
                reference_price: Some(2000.0),
            }),
        }
    }

    fn ratio_event(
        ts_bucket: chrono::DateTime<Utc>,
        ratio_type: LongShortRatioType,
        value: f64,
    ) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.long_short_ratio.5m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.futures.long_short_ratio.5m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::minutes(5),
            published_at: ts_bucket + ChronoDuration::minutes(5),
            data: MdData::LongShortRatio5m(LongShortRatio5mEvent {
                ts_bucket,
                ratio_type,
                long_short_ratio: value,
                long_account_ratio: Some(0.6),
                short_account_ratio: Some(0.4),
            }),
        }
    }

    fn agg_trade_event_with_profile(
        ts_bucket: chrono::DateTime<Utc>,
        buy_qty: f64,
        sell_qty: f64,
        price: f64,
        last_vpin: f64,
    ) -> EngineEvent {
        let mut event = agg_trade_event(ts_bucket, buy_qty, sell_qty, last_vpin);
        if let MdData::AggTrade1m(ref mut trade) = event.data {
            trade.first_price = Some(price);
            trade.last_price = Some(price);
            trade.high_price = Some(price);
            trade.low_price = Some(price);
            trade.profile_levels = vec![AggProfileLevel {
                price,
                buy_qty,
                sell_qty,
            }];
        }
        event
    }

    fn agg_orderbook_event(
        ts_bucket: chrono::DateTime<Utc>,
        sample_count: i64,
        bbo_updates: i64,
        obi_k_dw_sum: f64,
    ) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.orderbook.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.orderbook.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggOrderbook1m(AggOrderbook1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                sample_count,
                bbo_updates,
                spread_sum: 0.5 * sample_count as f64,
                topk_depth_sum: 10.0 * sample_count as f64,
                obi_sum: 0.0,
                obi_l1_sum: 0.0,
                obi_k_sum: 0.0,
                obi_k_dw_sum,
                obi_k_dw_change_sum: 0.0,
                obi_k_dw_adj_sum: 0.0,
                microprice_sum: 0.0,
                microprice_classic_sum: 0.0,
                microprice_kappa_sum: 0.0,
                microprice_adj_sum: 0.0,
                ofi_sum: 0.0,
                obi_k_dw_close: Some(obi_k_dw_sum),
                heatmap_levels: vec![AggHeatmapLevel {
                    price: 2000.0,
                    bid_liquidity: 5.0,
                    ask_liquidity: 7.0,
                }],
                heatmap_loaded: true,
            }),
        }
    }

    fn agg_orderbook_event_spot(
        ts_bucket: chrono::DateTime<Utc>,
        sample_count: i64,
        bbo_updates: i64,
        obi_k_dw_sum: f64,
    ) -> EngineEvent {
        let mut event = agg_orderbook_event(ts_bucket, sample_count, bbo_updates, obi_k_dw_sum);
        event.market = MarketKind::Spot;
        event.routing_key = "md.agg.spot.orderbook.1m.testusdt".to_string();
        event
    }

    fn agg_liq_event(ts_bucket: chrono::DateTime<Utc>, notional: f64) -> EngineEvent {
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.liq.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.liq.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggLiq1m(AggLiq1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                levels: vec![AggLiqLevel {
                    price: 2000.0,
                    long_liq: notional,
                    short_liq: notional / 2.0,
                }],
            }),
        }
    }

    fn agg_funding_mark_event(
        ts_bucket: chrono::DateTime<Utc>,
        point_offset_secs: i64,
        mark_price: f64,
        funding_rate: f64,
    ) -> EngineEvent {
        let point_ts = ts_bucket + ChronoDuration::seconds(point_offset_secs);
        EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.funding_mark.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.funding_mark.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_bucket + ChronoDuration::seconds(59),
            published_at: ts_bucket + ChronoDuration::seconds(59),
            data: MdData::AggFundingMark1m(AggFundingMark1mEvent {
                ts_bucket,
                chunk_start_ts: ts_bucket,
                chunk_end_ts: ts_bucket + ChronoDuration::seconds(59),
                source_event_count: 1,
                mark_points: vec![AggMarkPoint {
                    ts: point_ts,
                    mark_price: Some(mark_price),
                    index_price: Some(mark_price + 1.0),
                    estimated_settle_price: None,
                    funding_rate: Some(funding_rate),
                    next_funding_time: None,
                }],
                funding_points: vec![AggFundingPoint {
                    ts: point_ts,
                    funding_time: Some(point_ts),
                    funding_rate,
                    mark_price: Some(mark_price),
                    next_funding_time: None,
                }],
            }),
        }
    }

    #[test]
    fn latest_continuous_canonical_segment_uses_last_complete_run() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);
        let ts_3 = ts_1 + ChronoDuration::minutes(2);
        let ts_4 = ts_1 + ChronoDuration::minutes(3);

        for ts in [ts_1, ts_3, ts_4] {
            store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
            store.ingest(agg_liq_event(ts, 10.0));
            store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
        }

        // Break continuity on ts_2 by omitting spot orderbook.
        store.ingest(agg_trade_event(ts_2, 1.0, 0.5, 0.2));
        store.ingest(agg_orderbook_event(ts_2, 4, 4, 0.1));
        store.ingest(agg_liq_event(ts_2, 10.0));
        store.ingest(agg_funding_mark_event(ts_2, 45, 2000.0, 0.01));
        store.ingest(agg_trade_event_spot(ts_2, 1.0, 0.5, 0.2));

        let segment = store.latest_continuous_canonical_segment().unwrap();
        assert_eq!(segment.0, ts_3);
        assert_eq!(segment.1, ts_4);
    }

    #[test]
    fn latest_contiguous_complete_canonical_minute_from_stops_on_missing_spot_inputs() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);
        let ts_3 = ts_1 + ChronoDuration::minutes(2);

        for ts in [ts_1, ts_2] {
            store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
            store.ingest(agg_liq_event(ts, 10.0));
            store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            if ts != ts_2 {
                store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
            }
        }
        for ts in [ts_2, ts_3] {
            if ts == ts_3 {
                store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
                store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
                store.ingest(agg_liq_event(ts, 10.0));
                store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
                store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
                store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
            }
        }

        let latest = store.latest_contiguous_complete_canonical_minute_from(ts_1, ts_3);
        assert_eq!(latest, Some(ts_1));
    }

    #[test]
    fn latest_contiguous_complete_canonical_minute_from_allows_missing_liq_inputs() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        for ts in [ts_1, ts_2] {
            store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
            store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
        }

        let latest = store.latest_contiguous_complete_canonical_minute_from(ts_1, ts_2);
        assert_eq!(latest, Some(ts_2));
    }

    #[test]
    fn latest_continuous_canonical_segment_allows_missing_liq_minutes() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        for ts in [ts_1, ts_2] {
            store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
            store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
        }

        let segment = store.latest_continuous_canonical_segment().unwrap();
        assert_eq!(segment.0, ts_1);
        assert_eq!(segment.1, ts_2);
    }

    #[test]
    fn canonical_minute_presence_reports_missing_required_sources() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();

        store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
        store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
        store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
        store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));

        let presence = store.canonical_minute_presence(ts);
        assert!(presence.minute_present);
        assert!(presence.trade_futures);
        assert!(presence.orderbook_futures);
        assert!(presence.funding_futures);
        assert!(presence.trade_spot);
        assert!(!presence.orderbook_spot);
        assert!(!presence.complete_under_current_policy());
        assert_eq!(presence.missing_required_sources(), vec!["orderbook_spot"]);
    }

    #[test]
    fn canonical_frontier_snapshot_tracks_each_source_frontier() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        for ts in [ts_1, ts_2] {
            store.ingest(agg_trade_event(ts, 1.0, 0.5, 0.2));
            store.ingest(agg_orderbook_event(ts, 4, 4, 0.1));
            store.ingest(agg_funding_mark_event(ts, 45, 2000.0, 0.01));
            store.ingest(agg_trade_event_spot(ts, 1.0, 0.5, 0.2));
            if ts == ts_1 {
                store.ingest(agg_orderbook_event_spot(ts, 4, 4, 0.1));
                store.ingest(agg_liq_event(ts, 10.0));
            }
        }

        let snapshot = store.canonical_frontier_snapshot();
        assert_eq!(snapshot.trade_futures_ts, Some(ts_2));
        assert_eq!(snapshot.orderbook_futures_ts, Some(ts_2));
        assert_eq!(snapshot.funding_futures_ts, Some(ts_2));
        assert_eq!(snapshot.trade_spot_ts, Some(ts_2));
        assert_eq!(snapshot.orderbook_spot_ts, Some(ts_1));
        assert_eq!(snapshot.liq_futures_ts, Some(ts_1));
        assert_eq!(snapshot.latest_contiguous_complete_minute_ts, Some(ts_1));
        assert_eq!(snapshot.last_canonical_minute_ts, Some(ts_2));
    }

    #[test]
    fn one_minute_kline_uses_open_time_bucket() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let open_time = Utc.with_ymd_and_hms(2026, 3, 6, 4, 49, 0).single().unwrap();
        let close_event_ts = Utc.with_ymd_and_hms(2026, 3, 6, 4, 50, 0).single().unwrap()
            + chrono::Duration::milliseconds(66);

        store.ingest(EngineEvent {
            schema_version: 1,
            msg_type: "md.kline".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.futures.kline.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: close_event_ts,
            published_at: close_event_ts,
            data: MdData::Kline(KlineEvent {
                interval_code: "1m".to_string(),
                open_time,
                close_time: open_time + chrono::Duration::minutes(1)
                    - chrono::Duration::milliseconds(1),
                open_price: 2067.21,
                high_price: 2068.52,
                low_price: 2067.20,
                close_price: 2068.31,
                volume_base: None,
                quote_volume: None,
                trade_count: None,
                is_closed: true,
            }),
        });

        assert!(store
            .buckets
            .contains_key(&(MarketKind::Futures, open_time.timestamp())));
        assert!(!store.buckets.contains_key(&(
            MarketKind::Futures,
            close_event_ts.timestamp() - close_event_ts.timestamp().rem_euclid(60)
        )));
    }

    #[test]
    fn finalize_minute_uses_minute_locked_vpin_not_later_global_state() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 49, 0).single().unwrap();
        let ts_2 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 50, 0).single().unwrap();

        let minute_1 = EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_1 + chrono::Duration::seconds(59),
            published_at: ts_1 + chrono::Duration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket: ts_1,
                chunk_start_ts: ts_1,
                chunk_end_ts: ts_1 + chrono::Duration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty: 1.0,
                sell_qty: 0.0,
                buy_notional: 2000.0,
                sell_notional: 0.0,
                first_price: Some(2000.0),
                last_price: Some(2000.0),
                high_price: Some(2000.0),
                low_price: Some(2000.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin: 0.25,
                }),
            }),
        };

        let minute_2 = EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_2 + chrono::Duration::seconds(59),
            published_at: ts_2 + chrono::Duration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket: ts_2,
                chunk_start_ts: ts_2,
                chunk_end_ts: ts_2 + chrono::Duration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty: 1.0,
                sell_qty: 0.0,
                buy_notional: 2100.0,
                sell_notional: 0.0,
                first_price: Some(2100.0),
                last_price: Some(2100.0),
                high_price: Some(2100.0),
                low_price: Some(2100.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin: 0.80,
                }),
            }),
        };

        store.ingest(minute_1);
        store.ingest(minute_2);
        let bundle = store.finalize_minute(ts_1);

        assert_eq!(bundle.history_futures.last().map(|h| h.vpin), Some(0.25));
    }

    #[test]
    fn finalize_minute_without_trades_carries_forward_previous_vpin() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 49, 0).single().unwrap();
        let ts_2 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 50, 0).single().unwrap();
        let ts_3 = Utc.with_ymd_and_hms(2026, 3, 6, 5, 51, 0).single().unwrap();

        store.ingest(EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_1 + chrono::Duration::seconds(59),
            published_at: ts_1 + chrono::Duration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket: ts_1,
                chunk_start_ts: ts_1,
                chunk_end_ts: ts_1 + chrono::Duration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty: 1.0,
                sell_qty: 0.0,
                buy_notional: 2000.0,
                sell_notional: 0.0,
                first_price: Some(2000.0),
                last_price: Some(2000.0),
                high_price: Some(2000.0),
                low_price: Some(2000.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin: 0.25,
                }),
            }),
        });
        let first_bundle = store.finalize_minute(ts_1);
        assert_eq!(
            first_bundle.history_futures.last().map(|h| h.vpin),
            Some(0.25)
        );

        store.ingest(EngineEvent {
            schema_version: 1,
            msg_type: "md.bbo".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.futures.bbo.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_2 + chrono::Duration::seconds(10),
            published_at: ts_2 + chrono::Duration::seconds(10),
            data: MdData::Bbo(BboEvent {
                bid_price: 2000.0,
                bid_qty: 1.0,
                ask_price: 2001.0,
                ask_qty: 1.0,
                sample_count: 1,
            }),
        });

        store.ingest(EngineEvent {
            schema_version: 1,
            msg_type: "md.agg.trade.1m".to_string(),
            message_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            routing_key: "md.agg.futures.trade.1m.testusdt".to_string(),
            market: MarketKind::Futures,
            symbol: "TESTUSDT".to_string(),
            source_kind: "test".to_string(),
            backfill_in_progress: false,
            event_ts: ts_3 + chrono::Duration::seconds(59),
            published_at: ts_3 + chrono::Duration::seconds(59),
            data: MdData::AggTrade1m(AggTrade1mEvent {
                ts_bucket: ts_3,
                chunk_start_ts: ts_3,
                chunk_end_ts: ts_3 + chrono::Duration::seconds(59),
                source_event_count: 1,
                trade_count: 1,
                buy_qty: 1.0,
                sell_qty: 0.0,
                buy_notional: 2100.0,
                sell_notional: 0.0,
                first_price: Some(2100.0),
                last_price: Some(2100.0),
                high_price: Some(2100.0),
                low_price: Some(2100.0),
                profile_levels: Vec::new(),
                whale: AggWhaleStats {
                    trade_count: 0,
                    buy_count: 0,
                    sell_count: 0,
                    notional_total: 0.0,
                    notional_buy: 0.0,
                    notional_sell: 0.0,
                    qty_eth_total: 0.0,
                    qty_eth_buy: 0.0,
                    qty_eth_sell: 0.0,
                    max_single_notional: 0.0,
                },
                vpin_snapshot: Some(AggVpinSnapshot {
                    current_buy: 0.0,
                    current_sell: 0.0,
                    current_fill: 0.0,
                    imbalances: Vec::new(),
                    imbalance_sum: 0.0,
                    last_vpin: 0.80,
                }),
            }),
        });

        let second_bundle = store.finalize_minute(ts_2);
        assert_eq!(
            second_bundle.history_futures.last().map(|h| h.vpin),
            Some(0.25)
        );
    }

    #[test]
    fn canonical_trade_correction_recomputes_finalized_suffix() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 6, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        store.ingest(agg_trade_event(ts_1, 2.0, 1.0, 0.20));
        store.finalize_minute(ts_1);
        store.ingest(agg_trade_event(ts_2, 1.0, 0.0, 0.30));
        store.finalize_minute(ts_2);

        store.ingest(agg_trade_event(ts_1, 5.0, 1.0, 0.55));
        let recomputed = store.recompute_dirty_finalized_minutes(10_000);

        assert_eq!(recomputed.len(), 2);
        assert_eq!(recomputed[0].ts_bucket, ts_1);
        assert_eq!(recomputed[0].futures.total_qty, 6.0);
        assert_eq!(
            recomputed[0].history_futures.last().map(|h| h.cvd),
            Some(4.0)
        );
        assert_eq!(
            recomputed[0].history_futures.last().map(|h| h.vpin),
            Some(0.55)
        );
        assert_eq!(recomputed[1].ts_bucket, ts_2);
        assert_eq!(
            recomputed[1].history_futures.last().map(|h| h.cvd),
            Some(5.0)
        );
        assert_eq!(
            recomputed[1].history_futures.last().map(|h| h.vpin),
            Some(0.30)
        );
    }

    #[test]
    fn dirty_recompute_remains_pending_until_suffix_fully_rebuilt() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 6, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        store.ingest(agg_trade_event(ts_1, 2.0, 1.0, 0.20));
        store.finalize_minute(ts_1);
        store.ingest(agg_trade_event(ts_2, 1.0, 0.0, 0.30));
        store.finalize_minute(ts_2);

        store.ingest(agg_trade_event(ts_1, 5.0, 1.0, 0.55));
        let first_batch = store.recompute_dirty_finalized_minutes(1);

        assert_eq!(first_batch.len(), 1);
        assert!(store.has_pending_dirty_recompute());

        let second_batch = store.recompute_dirty_finalized_minutes(1);
        assert_eq!(second_batch.len(), 1);
        assert!(!store.has_pending_dirty_recompute());
    }

    #[test]
    fn rewind_finalized_state_clears_dirty_recompute_markers() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 8, 0, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        store.ingest(agg_trade_event(ts_1, 2.0, 1.0, 0.20));
        store.finalize_minute(ts_1);
        store.ingest(agg_trade_event(ts_2, 1.0, 0.0, 0.30));
        store.finalize_minute(ts_2);

        store.ingest(agg_trade_event(ts_1, 5.0, 1.0, 0.55));
        assert!(store.has_pending_dirty_recompute());

        store.rewind_finalized_state_from(ts_1);
        store.clear_dirty_recompute_state();

        assert!(!store.has_pending_dirty_recompute());
        assert_eq!(store.last_finalized_minute(), None);
    }

    #[test]
    fn late_oi_hist_marks_patch_not_global_dirty() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 9, 0, 0).single().unwrap();
        store.finalize_minute(ts);

        store.ingest(oi_hist_event(ts, 1_000_000.0));

        assert!(store.has_pending_oi_ratio_patch());
        assert!(!store.has_pending_dirty_recompute());
        assert_eq!(store.pending_oi_ratio_patch_batch_range(10), Some((ts, ts)));
    }

    #[test]
    fn late_ratio_marks_patch_not_global_dirty() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 9, 5, 0).single().unwrap();
        store.finalize_minute(ts);

        store.ingest(ratio_event(ts, LongShortRatioType::GlobalAccount, 1.2));

        assert!(store.has_pending_oi_ratio_patch());
        assert!(!store.has_pending_dirty_recompute());
        assert_eq!(store.pending_oi_ratio_patch_batch_range(10), Some((ts, ts)));
    }

    #[test]
    fn frontier_snapshot_exposes_oi_ratio_patch_bounds() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 9, 10, 0).single().unwrap();
        store.finalize_minute(ts);
        store.ingest(oi_hist_event(ts, 1_100_000.0));

        let snapshot = store.canonical_frontier_snapshot();
        assert_eq!(snapshot.oi_ratio_patch_from_ts, Some(ts));
        assert_eq!(snapshot.oi_ratio_patch_end_ts, Some(ts));
    }

    #[test]
    fn canonical_trade_correction_beyond_15m_still_recomputes_suffix() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let start = Utc.with_ymd_and_hms(2026, 3, 6, 7, 0, 0).single().unwrap();

        for minute_offset in 0..20 {
            let ts = start + ChronoDuration::minutes(minute_offset);
            let buy_qty = 10.0 + minute_offset as f64;
            let sell_qty = 1.0;
            let vpin = 0.10 + minute_offset as f64 / 100.0;
            store.ingest(agg_trade_event(ts, buy_qty, sell_qty, vpin));
            store.finalize_minute(ts);
        }

        let corrected_ts = start;
        store.ingest(agg_trade_event(corrected_ts, 30.0, 2.0, 0.77));
        let recomputed = store.recompute_dirty_finalized_minutes(10_000);

        assert_eq!(recomputed.len(), 20);
        assert_eq!(recomputed[0].ts_bucket, corrected_ts);
        assert_eq!(recomputed[0].futures.total_qty, 32.0);
        assert_eq!(
            recomputed[0].history_futures.last().map(|h| h.cvd),
            Some(28.0)
        );
        assert_eq!(
            recomputed[0].history_futures.last().map(|h| h.vpin),
            Some(0.77)
        );

        let final_window = recomputed.last().expect("final recomputed minute");
        assert_eq!(final_window.ts_bucket, start + ChronoDuration::minutes(19));
        assert_eq!(final_window.futures.total_qty, 30.0);
        assert_eq!(
            final_window.history_futures.last().map(|h| h.cvd),
            Some(389.0)
        );
        let final_vpin = final_window
            .history_futures
            .last()
            .and_then(|h| Some(h.vpin))
            .expect("final vpin");
        assert!((final_vpin - 0.29).abs() < 1e-9);
    }

    #[test]
    fn build_window_bundle_backfills_missing_trade_minute_from_canonical_history() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_0 = Utc.with_ymd_and_hms(2026, 3, 7, 4, 16, 0).single().unwrap();
        let ts_1 = ts_0 + ChronoDuration::minutes(1);
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        store.ingest(agg_trade_event_with_profile(ts_0, 1.0, 0.0, 2000.0, 0.10));
        store.finalize_minute(ts_0);

        // Canonical row exists, but this minute never made it into finalized history.
        store.ingest(agg_trade_event_with_profile(ts_1, 4.0, 0.0, 2001.0, 0.20));

        store.ingest(agg_trade_event_with_profile(ts_2, 2.0, 0.0, 2002.0, 0.30));
        let bundle = store.finalize_minute(ts_2);

        assert_eq!(bundle.history_futures.len(), 2);
        assert_eq!(bundle.trade_history_futures.len(), 3);
        assert_eq!(
            bundle
                .trade_history_futures
                .iter()
                .map(|h| h.ts_bucket)
                .collect::<Vec<_>>(),
            vec![ts_0, ts_1, ts_2]
        );
        assert_eq!(
            bundle.trade_history_futures[1]
                .profile
                .values()
                .map(|level| level.total())
                .sum::<f64>(),
            4.0
        );
    }

    #[test]
    fn canonical_orderbook_correction_replaces_instead_of_accumulating() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 6, 10, 0).single().unwrap();

        store.ingest(agg_orderbook_event(ts_1, 2, 4, 1.0));
        let first = store.finalize_minute(ts_1);
        assert_eq!(first.futures.bbo_updates, 4);
        assert_eq!(first.futures.obi_k_dw_twa, Some(0.5));

        store.ingest(agg_orderbook_event(ts_1, 3, 9, 6.0));
        let recomputed = store.recompute_dirty_finalized_minutes(10_000);

        assert_eq!(recomputed.len(), 1);
        assert_eq!(recomputed[0].futures.bbo_updates, 9);
        assert_eq!(recomputed[0].futures.obi_k_dw_twa, Some(2.0));
    }

    #[test]
    fn scalar_only_orderbook_backfill_does_not_downgrade_loaded_heatmap() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 6, 11, 0).single().unwrap();

        store.ingest(agg_orderbook_event(ts, 2, 4, 1.0));
        assert!(!store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));

        let mut scalar_only = agg_orderbook_event(ts, 2, 4, 1.0);
        if let MdData::AggOrderbook1m(ref mut orderbook) = scalar_only.data {
            orderbook.heatmap_levels.clear();
            orderbook.heatmap_loaded = false;
        }
        store.ingest(scalar_only);

        assert!(!store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));
    }

    #[test]
    fn scalar_only_orderbook_backfill_can_be_upgraded_with_full_heatmap() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts = Utc.with_ymd_and_hms(2026, 3, 6, 6, 12, 0).single().unwrap();

        let mut scalar_only = agg_orderbook_event(ts, 2, 4, 1.0);
        if let MdData::AggOrderbook1m(ref mut orderbook) = scalar_only.data {
            orderbook.heatmap_levels.clear();
            orderbook.heatmap_loaded = false;
        }
        store.ingest(scalar_only);
        assert!(store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));

        store.ingest(agg_orderbook_event(ts, 2, 4, 1.0));
        assert!(!store.has_unhydrated_futures_orderbook_heatmap_in_range(ts, ts));
    }

    #[test]
    fn canonical_funding_correction_recomputes_recent_timeline() {
        let mut store = StateStore::new("TESTUSDT".to_string(), 1_000.0);
        let ts_1 = Utc.with_ymd_and_hms(2026, 3, 6, 6, 20, 0).single().unwrap();
        let ts_2 = ts_1 + ChronoDuration::minutes(1);

        store.ingest(agg_funding_mark_event(ts_1, 30, 2000.0, -0.0010));
        store.finalize_minute(ts_1);
        store.finalize_minute(ts_2);

        store.ingest(agg_funding_mark_event(ts_1, 30, 2010.0, -0.0020));
        let recomputed = store.recompute_dirty_finalized_minutes(10_000);

        assert_eq!(recomputed.len(), 2);
        assert_eq!(
            recomputed[0]
                .latest_mark
                .as_ref()
                .and_then(|v| v.mark_price),
            Some(2010.0)
        );
        assert_eq!(
            recomputed[0]
                .latest_funding
                .as_ref()
                .map(|v| v.funding_rate),
            Some(-0.0020)
        );
        assert_eq!(
            recomputed[1]
                .latest_mark
                .as_ref()
                .and_then(|v| v.mark_price),
            Some(2010.0)
        );
        assert_eq!(
            recomputed[1]
                .latest_funding
                .as_ref()
                .map(|v| v.funding_rate),
            Some(-0.0020)
        );
    }
}
