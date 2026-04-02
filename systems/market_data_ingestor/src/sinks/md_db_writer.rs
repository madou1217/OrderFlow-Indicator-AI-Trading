use crate::normalize::NormalizedMdEvent;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::HashSet;

const BBO_ROLLUP_WINDOW_MS: i64 = 250;
const ORDERBOOK_DB_HEATMAP_COMPACT_THRESHOLD: usize = 256;
const ORDERBOOK_DB_HEATMAP_TOP_TOTAL_LEVELS: usize = 64;
const ORDERBOOK_DB_HEATMAP_TOP_ABS_NET_LEVELS: usize = 32;
const ORDERBOOK_DB_HEATMAP_TOP_BID_LEVELS: usize = 16;
const ORDERBOOK_DB_HEATMAP_TOP_ASK_LEVELS: usize = 16;
const ORDERBOOK_DB_HEATMAP_NEAR_MID_LEVELS: usize = 64;

#[derive(Clone)]
pub struct MdDbWriter {
    pool: PgPool,
}

impl MdDbWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn write_md_event(&self, event: &NormalizedMdEvent) -> Result<()> {
        match event.msg_type.as_str() {
            // Aggregate-only ingest mode: raw websocket details are not written to PG.
            // Exception: klines are still written so kline_bar table stays current for IE bootstrap.
            "md.trade"
            | "md.depth"
            | "md.orderbook_snapshot_l2"
            | "md.bbo"
            | "md.mark_price"
            | "md.funding_rate"
            | "md.force_order" => Ok(()),
            "md.kline" => {
                if should_write_kline_hotpath(&event.data) {
                    self.insert_kline(event).await
                } else {
                    Ok(())
                }
            }
            "md.agg.trade.1m" => self.insert_agg_trade_1m(event).await,
            "md.agg.orderbook.1m" => self.insert_agg_orderbook_1m(event).await,
            "md.agg.liq.1m" => self.insert_agg_liq_1m(event).await,
            "md.agg.funding_mark.1m" => self.insert_agg_funding_mark_1m(event).await,
            "md.open_interest_current" => self.insert_open_interest_current_1m(event).await,
            "md.open_interest_hist_5m" => self.insert_open_interest_hist_5m(event).await,
            "md.long_short_ratio_5m" => self.insert_long_short_ratio_5m(event).await,
            "md.option_mark_greeks_5m" => self.insert_option_mark_greeks_5m(event).await,
            other => Err(anyhow!("unsupported msg_type for md writer: {}", other)),
        }
    }

    pub async fn write_md_events_batch(&self, events: &[NormalizedMdEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut open_interest_current_rows: Vec<OpenInterestCurrentBatchRow> = Vec::new();
        let mut open_interest_hist_rows: Vec<OpenInterestHist5mBatchRow> = Vec::new();
        let mut long_short_ratio_rows: Vec<LongShortRatio5mBatchRow> = Vec::new();
        let mut option_mark_rows: Vec<OptionMarkGreeks5mBatchRow> = Vec::new();
        let mut kline_rows: Vec<KlineBatchRow> = Vec::new();
        let mut agg_trade_rows: Vec<AggTrade1mBatchRow> = Vec::new();
        let mut agg_orderbook_rows: Vec<AggOrderbook1mBatchRow> = Vec::new();
        let mut agg_liq_rows: Vec<AggLiq1mBatchRow> = Vec::new();
        let mut agg_funding_rows: Vec<AggFundingMark1mBatchRow> = Vec::new();

        for event in events {
            match event.msg_type.as_str() {
                msg if is_raw_msg_type(msg) => {}
                "md.kline" => {
                    if should_write_kline_hotpath(&event.data) {
                        kline_rows.push(KlineBatchRow::from_event(event)?);
                    }
                }
                "md.open_interest_current" => {
                    open_interest_current_rows.push(OpenInterestCurrentBatchRow::from_event(event)?)
                }
                "md.open_interest_hist_5m" => {
                    open_interest_hist_rows.push(OpenInterestHist5mBatchRow::from_event(event)?)
                }
                "md.long_short_ratio_5m" => {
                    long_short_ratio_rows.push(LongShortRatio5mBatchRow::from_event(event)?)
                }
                "md.option_mark_greeks_5m" => {
                    option_mark_rows.push(OptionMarkGreeks5mBatchRow::from_event(event)?)
                }
                "md.agg.trade.1m" => agg_trade_rows.push(AggTrade1mBatchRow::from_event(event)?),
                "md.agg.orderbook.1m" => {
                    agg_orderbook_rows.push(AggOrderbook1mBatchRow::from_event(event)?)
                }
                "md.agg.liq.1m" => agg_liq_rows.push(AggLiq1mBatchRow::from_event(event)?),
                "md.agg.funding_mark.1m" => {
                    agg_funding_rows.push(AggFundingMark1mBatchRow::from_event(event)?)
                }
                _ => self.write_md_event(event).await?,
            }
        }

        dedupe_open_interest_hist_rows(&mut open_interest_hist_rows);
        dedupe_long_short_ratio_rows(&mut long_short_ratio_rows);
        dedupe_agg_trade_rows(&mut agg_trade_rows);
        dedupe_agg_orderbook_rows(&mut agg_orderbook_rows);
        dedupe_agg_liq_rows(&mut agg_liq_rows);
        dedupe_agg_funding_rows(&mut agg_funding_rows);

        let mut tx = self.pool.begin().await.context("begin md batch write tx")?;
        self.insert_open_interest_current_1m_batch(&mut tx, &open_interest_current_rows)
            .await?;
        self.insert_open_interest_hist_5m_batch(&mut tx, &open_interest_hist_rows)
            .await?;
        self.insert_long_short_ratio_5m_batch(&mut tx, &long_short_ratio_rows)
            .await?;
        self.insert_option_mark_greeks_5m_batch(&mut tx, &option_mark_rows)
            .await?;
        self.insert_kline_batch(&mut tx, &kline_rows).await?;
        self.insert_agg_trade_1m_batch(&mut tx, &agg_trade_rows)
            .await?;
        self.insert_agg_orderbook_1m_batch(&mut tx, &agg_orderbook_rows)
            .await?;
        self.insert_agg_liq_1m_batch(&mut tx, &agg_liq_rows).await?;
        self.insert_agg_funding_mark_1m_batch(&mut tx, &agg_funding_rows)
            .await?;
        tx.commit().await.context("commit md batch write tx")?;

        Ok(())
    }

    async fn insert_trade(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.trade_event (
                ts_event, market, symbol, source_kind, stream_name, event_type,
                exchange_trade_id, agg_trade_id, first_trade_id, last_trade_id,
                price, qty_raw, qty_eth, notional_usdt,
                is_buyer_maker, aggressor_side, is_best_match, payload_json
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type, $5, 'agg_trade',
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16, $17
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(event.event_ts)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(optional_i64(d, "exchange_trade_id"))
        .bind(optional_i64(d, "agg_trade_id"))
        .bind(optional_i64(d, "first_trade_id"))
        .bind(optional_i64(d, "last_trade_id"))
        .bind(required_f64(d, "price")?)
        .bind(required_f64(d, "qty_raw")?)
        .bind(required_f64_alias(d, &["qty_base", "qty_eth"])?)
        .bind(required_f64(d, "notional_usdt")?)
        .bind(optional_bool(d, "is_buyer_maker"))
        .bind(required_i64(d, "aggressor_side")? as i16)
        .bind(optional_bool(d, "is_best_match"))
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.trade_event")?;
        Ok(())
    }

    async fn insert_open_interest_current_1m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.open_interest_current_1m (
                ts_event, ts_recv, venue, market, symbol, source_kind, stream_name,
                open_interest_contracts, mark_price, open_interest_value_usdt, payload_json
            )
            VALUES (
                $1, $2, 'binance', $3::cfg.market_type, $4, $5::cfg.source_type, $6,
                $7, $8, $9, $10
            )
            ON CONFLICT (market, symbol, ts_event)
            DO UPDATE SET
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                open_interest_contracts = EXCLUDED.open_interest_contracts,
                mark_price = EXCLUDED.mark_price,
                open_interest_value_usdt = EXCLUDED.open_interest_value_usdt,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_recv")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_f64(d, "open_interest_contracts")?)
        .bind(optional_f64(d, "mark_price"))
        .bind(optional_f64(d, "open_interest_value_usdt"))
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.open_interest_current_1m")?;
        Ok(())
    }

    async fn insert_open_interest_hist_5m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.open_interest_hist_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, source_kind, stream_name,
                open_interest_contracts, open_interest_value_usdt, payload_json
            )
            VALUES (
                $1, $2, 'binance', $3, $4::cfg.market_type, $5, $6::cfg.source_type, $7,
                $8, $9, $10
            )
            ON CONFLICT (market, symbol, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                open_interest_contracts = EXCLUDED.open_interest_contracts,
                open_interest_value_usdt = EXCLUDED.open_interest_value_usdt,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_recv")?)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_f64(d, "open_interest_contracts")?)
        .bind(required_f64(d, "open_interest_value_usdt")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.open_interest_hist_5m")?;
        Ok(())
    }

    async fn insert_long_short_ratio_5m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.long_short_ratio_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, source_kind, stream_name,
                ratio_type, long_short_ratio, long_account_ratio, short_account_ratio, payload_json
            )
            VALUES (
                $1, $2, 'binance', $3, $4::cfg.market_type, $5, $6::cfg.source_type, $7,
                $8, $9, $10, $11, $12
            )
            ON CONFLICT (market, symbol, ratio_type, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                long_short_ratio = EXCLUDED.long_short_ratio,
                long_account_ratio = EXCLUDED.long_account_ratio,
                short_account_ratio = EXCLUDED.short_account_ratio,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_recv")?)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_str(d, "ratio_type")?)
        .bind(required_f64(d, "long_short_ratio")?)
        .bind(optional_f64(d, "long_account_ratio"))
        .bind(optional_f64(d, "short_account_ratio"))
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.long_short_ratio_5m")?;
        Ok(())
    }

    async fn insert_option_mark_greeks_5m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.option_mark_greeks_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, option_symbol,
                underlying_asset, source_kind, stream_name, expiry_ts, strike_price,
                contract_side, unit, index_price, mark_price, bid_iv, ask_iv, mark_iv,
                delta, gamma, vega, theta, risk_free_interest, payload_json
            )
            VALUES (
                $1, $2, 'binance', $3, $4::cfg.market_type, $5, $6,
                $7, $8::cfg.source_type, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24
            )
            ON CONFLICT (market, symbol, option_symbol, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                underlying_asset = EXCLUDED.underlying_asset,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                expiry_ts = EXCLUDED.expiry_ts,
                strike_price = EXCLUDED.strike_price,
                contract_side = EXCLUDED.contract_side,
                unit = EXCLUDED.unit,
                index_price = EXCLUDED.index_price,
                mark_price = EXCLUDED.mark_price,
                bid_iv = EXCLUDED.bid_iv,
                ask_iv = EXCLUDED.ask_iv,
                mark_iv = EXCLUDED.mark_iv,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                vega = EXCLUDED.vega,
                theta = EXCLUDED.theta,
                risk_free_interest = EXCLUDED.risk_free_interest,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_recv")?)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(required_str(d, "option_symbol")?)
        .bind(required_str(d, "underlying_asset")?)
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(parse_ts(d, "expiry_ts")?)
        .bind(required_f64(d, "strike_price")?)
        .bind(required_str(d, "contract_side")?)
        .bind(optional_f64(d, "unit"))
        .bind(optional_f64(d, "index_price"))
        .bind(optional_f64(d, "mark_price"))
        .bind(optional_f64(d, "bid_iv"))
        .bind(optional_f64(d, "ask_iv"))
        .bind(optional_f64(d, "mark_iv"))
        .bind(optional_f64(d, "delta"))
        .bind(optional_f64(d, "gamma"))
        .bind(optional_f64(d, "vega"))
        .bind(optional_f64(d, "theta"))
        .bind(optional_f64(d, "risk_free_interest"))
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.option_mark_greeks_5m")?;
        Ok(())
    }

    async fn insert_open_interest_current_1m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[OpenInterestCurrentBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.open_interest_current_1m (
                ts_event, ts_recv, venue, market, symbol, source_kind, stream_name,
                open_interest_contracts, mark_price, open_interest_value_usdt, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_recv)
                .push("'binance'")
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.open_interest_contracts)
                .push_bind(row.mark_price)
                .push_bind(row.open_interest_value_usdt)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, ts_event)
            DO UPDATE SET
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                open_interest_contracts = EXCLUDED.open_interest_contracts,
                mark_price = EXCLUDED.mark_price,
                open_interest_value_usdt = EXCLUDED.open_interest_value_usdt,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.open_interest_current_1m batch")?;

        Ok(())
    }

    async fn insert_open_interest_hist_5m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[OpenInterestHist5mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.open_interest_hist_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, source_kind, stream_name,
                open_interest_contracts, open_interest_value_usdt, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_recv)
                .push("'binance'")
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.open_interest_contracts)
                .push_bind(row.open_interest_value_usdt)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                open_interest_contracts = EXCLUDED.open_interest_contracts,
                open_interest_value_usdt = EXCLUDED.open_interest_value_usdt,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.open_interest_hist_5m batch")?;

        Ok(())
    }

    async fn insert_long_short_ratio_5m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[LongShortRatio5mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.long_short_ratio_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, source_kind, stream_name,
                ratio_type, long_short_ratio, long_account_ratio, short_account_ratio, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_recv)
                .push("'binance'")
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(&row.ratio_type)
                .push_bind(row.long_short_ratio)
                .push_bind(row.long_account_ratio)
                .push_bind(row.short_account_ratio)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, ratio_type, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                long_short_ratio = EXCLUDED.long_short_ratio,
                long_account_ratio = EXCLUDED.long_account_ratio,
                short_account_ratio = EXCLUDED.short_account_ratio,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.long_short_ratio_5m batch")?;

        Ok(())
    }

    async fn insert_option_mark_greeks_5m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[OptionMarkGreeks5mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.option_mark_greeks_5m (
                ts_event, ts_recv, venue, ts_bucket, market, symbol, option_symbol,
                underlying_asset, source_kind, stream_name, expiry_ts, strike_price,
                contract_side, unit, index_price, mark_price, bid_iv, ask_iv, mark_iv,
                delta, gamma, vega, theta, risk_free_interest, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_recv)
                .push("'binance'")
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.option_symbol)
                .push_bind(&row.underlying_asset)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.expiry_ts)
                .push_bind(row.strike_price)
                .push_bind(&row.contract_side)
                .push_bind(row.unit)
                .push_bind(row.index_price)
                .push_bind(row.mark_price)
                .push_bind(row.bid_iv)
                .push_bind(row.ask_iv)
                .push_bind(row.mark_iv)
                .push_bind(row.delta)
                .push_bind(row.gamma)
                .push_bind(row.vega)
                .push_bind(row.theta)
                .push_bind(row.risk_free_interest)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, option_symbol, ts_bucket)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                ts_recv = EXCLUDED.ts_recv,
                underlying_asset = EXCLUDED.underlying_asset,
                source_kind = EXCLUDED.source_kind,
                stream_name = EXCLUDED.stream_name,
                expiry_ts = EXCLUDED.expiry_ts,
                strike_price = EXCLUDED.strike_price,
                contract_side = EXCLUDED.contract_side,
                unit = EXCLUDED.unit,
                index_price = EXCLUDED.index_price,
                mark_price = EXCLUDED.mark_price,
                bid_iv = EXCLUDED.bid_iv,
                ask_iv = EXCLUDED.ask_iv,
                mark_iv = EXCLUDED.mark_iv,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                vega = EXCLUDED.vega,
                theta = EXCLUDED.theta,
                risk_free_interest = EXCLUDED.risk_free_interest,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.option_mark_greeks_5m batch")?;

        Ok(())
    }

    async fn insert_trade_batch(&self, rows: &[TradeBatchRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.trade_event (
                ts_event, market, symbol, source_kind, stream_name, event_type,
                exchange_trade_id, agg_trade_id, first_trade_id, last_trade_id,
                price, qty_raw, qty_eth, notional_usdt,
                is_buyer_maker, aggressor_side, is_best_match, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(&row.event_type)
                .push_bind(row.exchange_trade_id)
                .push_bind(row.agg_trade_id)
                .push_bind(row.first_trade_id)
                .push_bind(row.last_trade_id)
                .push_bind(row.price)
                .push_bind(row.qty_raw)
                .push_bind(row.qty_eth)
                .push_bind(row.notional_usdt)
                .push_bind(row.is_buyer_maker)
                .push_bind(row.aggressor_side)
                .push_bind(row.is_best_match)
                .push_bind(&row.payload_json);
        });

        builder.push(" ON CONFLICT DO NOTHING");
        builder
            .build()
            .execute(&self.pool)
            .await
            .context("insert md.trade_event batch")?;

        Ok(())
    }

    async fn insert_depth(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.depth_delta_l2 (
                ts_event, market, symbol, source_kind, stream_name,
                first_update_id, final_update_id, prev_final_update_id,
                bids_delta, asks_delta, event_count, payload_json
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type, $5,
                $6, $7, $8,
                $9, $10, $11, $12
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(event.event_ts)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(optional_i64(d, "first_update_id"))
        .bind(optional_i64(d, "final_update_id"))
        .bind(optional_i64(d, "prev_final_update_id"))
        .bind(required_value(d, "bids_delta")?)
        .bind(required_value(d, "asks_delta")?)
        .bind(optional_i64(d, "event_count").map(|v| v as i32))
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.depth_delta_l2")?;
        Ok(())
    }

    async fn insert_depth_batch(&self, rows: &[DepthBatchRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.depth_delta_l2 (
                ts_event, market, symbol, source_kind, stream_name,
                first_update_id, final_update_id, prev_final_update_id,
                bids_delta, asks_delta, event_count, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.first_update_id)
                .push_bind(row.final_update_id)
                .push_bind(row.prev_final_update_id)
                .push_bind(&row.bids_delta)
                .push_bind(&row.asks_delta)
                .push_bind(row.event_count)
                .push_bind(&row.payload_json);
        });

        builder.push(" ON CONFLICT DO NOTHING");
        builder
            .build()
            .execute(&self.pool)
            .await
            .context("insert md.depth_delta_l2 batch")?;

        Ok(())
    }

    async fn insert_orderbook_snapshot(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.orderbook_snapshot_l2 (
                ts_snapshot, market, symbol, source_kind,
                depth_levels, last_update_id, bids, asks, checksum, metadata
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type,
                $5, $6, $7, $8, $9, $10
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(parse_ts(d, "ts_snapshot")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_i64(d, "depth_levels")? as i32)
        .bind(optional_i64(d, "last_update_id"))
        .bind(required_value(d, "bids")?)
        .bind(required_value(d, "asks")?)
        .bind(optional_str(d, "checksum"))
        .bind(required_value(d, "metadata")?)
        .execute(&self.pool)
        .await
        .context("insert md.orderbook_snapshot_l2")?;
        Ok(())
    }

    async fn insert_bbo_rollup(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        let defaults = BboRollupDefaults::from_event(event)?;
        sqlx::query(
            r#"
            INSERT INTO md.bbo_event_live_rollup (
                ts_event, ts_window_start, ts_window_end,
                market, symbol, source_kind, stream_name,
                last_update_id, sample_count,
                bid_price, bid_qty, ask_price, ask_qty,
                spread_min, spread_max, spread_avg, mid_avg, payload_json
            )
            VALUES (
                $1, $2, $3,
                $4::cfg.market_type, $5, $6::cfg.source_type, $7,
                $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16, $17, $18
            )
            ON CONFLICT (market, symbol, stream_name, ts_window_start, ts_window_end)
            DO UPDATE SET
                ts_event = GREATEST(md.bbo_event_live_rollup.ts_event, EXCLUDED.ts_event),
                source_kind = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.source_kind
                    ELSE md.bbo_event_live_rollup.source_kind
                END,
                last_update_id = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.last_update_id
                    ELSE md.bbo_event_live_rollup.last_update_id
                END,
                sample_count = md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count,
                bid_price = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.bid_price
                    ELSE md.bbo_event_live_rollup.bid_price
                END,
                bid_qty = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.bid_qty
                    ELSE md.bbo_event_live_rollup.bid_qty
                END,
                ask_price = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.ask_price
                    ELSE md.bbo_event_live_rollup.ask_price
                END,
                ask_qty = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.ask_qty
                    ELSE md.bbo_event_live_rollup.ask_qty
                END,
                spread_min = LEAST(md.bbo_event_live_rollup.spread_min, EXCLUDED.spread_min),
                spread_max = GREATEST(md.bbo_event_live_rollup.spread_max, EXCLUDED.spread_max),
                spread_avg = (
                    (md.bbo_event_live_rollup.spread_avg * md.bbo_event_live_rollup.sample_count)
                    + (EXCLUDED.spread_avg * EXCLUDED.sample_count)
                ) / NULLIF(md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count, 0),
                mid_avg = (
                    (md.bbo_event_live_rollup.mid_avg * md.bbo_event_live_rollup.sample_count)
                    + (EXCLUDED.mid_avg * EXCLUDED.sample_count)
                ) / NULLIF(md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count, 0),
                payload_json = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.payload_json
                    ELSE md.bbo_event_live_rollup.payload_json
                END
            "#,
        )
        .bind(event.event_ts)
        .bind(defaults.ts_window_start)
        .bind(defaults.ts_window_end)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(optional_i64(d, "update_id"))
        .bind(defaults.sample_count)
        .bind(required_f64(d, "bid_price")?)
        .bind(required_f64(d, "bid_qty")?)
        .bind(required_f64(d, "ask_price")?)
        .bind(required_f64(d, "ask_qty")?)
        .bind(defaults.spread_min)
        .bind(defaults.spread_max)
        .bind(defaults.spread_avg)
        .bind(defaults.mid_avg)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.bbo_event_live_rollup")?;
        Ok(())
    }

    async fn insert_bbo_rollup_batch(&self, rows: &[BboBatchRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.bbo_event_live_rollup (
                ts_event, ts_window_start, ts_window_end,
                market, symbol, source_kind, stream_name,
                last_update_id, sample_count,
                bid_price, bid_qty, ask_price, ask_qty,
                spread_min, spread_max, spread_avg, mid_avg, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_window_start)
                .push_bind(row.ts_window_end)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.last_update_id)
                .push_bind(row.sample_count)
                .push_bind(row.bid_price)
                .push_bind(row.bid_qty)
                .push_bind(row.ask_price)
                .push_bind(row.ask_qty)
                .push_bind(row.spread_min)
                .push_bind(row.spread_max)
                .push_bind(row.spread_avg)
                .push_bind(row.mid_avg)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, stream_name, ts_window_start, ts_window_end)
            DO UPDATE SET
                ts_event = GREATEST(md.bbo_event_live_rollup.ts_event, EXCLUDED.ts_event),
                source_kind = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.source_kind
                    ELSE md.bbo_event_live_rollup.source_kind
                END,
                last_update_id = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.last_update_id
                    ELSE md.bbo_event_live_rollup.last_update_id
                END,
                sample_count = md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count,
                bid_price = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.bid_price
                    ELSE md.bbo_event_live_rollup.bid_price
                END,
                bid_qty = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.bid_qty
                    ELSE md.bbo_event_live_rollup.bid_qty
                END,
                ask_price = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.ask_price
                    ELSE md.bbo_event_live_rollup.ask_price
                END,
                ask_qty = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.ask_qty
                    ELSE md.bbo_event_live_rollup.ask_qty
                END,
                spread_min = LEAST(md.bbo_event_live_rollup.spread_min, EXCLUDED.spread_min),
                spread_max = GREATEST(md.bbo_event_live_rollup.spread_max, EXCLUDED.spread_max),
                spread_avg = (
                    (md.bbo_event_live_rollup.spread_avg * md.bbo_event_live_rollup.sample_count)
                    + (EXCLUDED.spread_avg * EXCLUDED.sample_count)
                ) / NULLIF(md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count, 0),
                mid_avg = (
                    (md.bbo_event_live_rollup.mid_avg * md.bbo_event_live_rollup.sample_count)
                    + (EXCLUDED.mid_avg * EXCLUDED.sample_count)
                ) / NULLIF(md.bbo_event_live_rollup.sample_count + EXCLUDED.sample_count, 0),
                payload_json = CASE
                    WHEN EXCLUDED.ts_event >= md.bbo_event_live_rollup.ts_event THEN EXCLUDED.payload_json
                    ELSE md.bbo_event_live_rollup.payload_json
                END
            "#,
        );
        builder
            .build()
            .execute(&self.pool)
            .await
            .context("insert md.bbo_event_live_rollup batch")?;

        Ok(())
    }

    async fn insert_kline(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.kline_bar (
                open_time, close_time, market, symbol, interval_code,
                source_kind, stream_name,
                open_price, high_price, low_price, close_price,
                volume_base, quote_volume, trade_count,
                taker_buy_base, taker_buy_quote, is_closed, payload_json
            )
            VALUES (
                $1, $2, $3::cfg.market_type, $4, $5,
                $6::cfg.source_type, $7,
                $8, $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17, $18
            )
            ON CONFLICT (market, symbol, interval_code, open_time)
            DO UPDATE SET
                close_time = EXCLUDED.close_time,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume_base = EXCLUDED.volume_base,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                taker_buy_base = EXCLUDED.taker_buy_base,
                taker_buy_quote = EXCLUDED.taker_buy_quote,
                is_closed = EXCLUDED.is_closed,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(parse_ts(d, "open_time")?)
        .bind(parse_ts(d, "close_time")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(required_str(d, "interval_code")?)
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_f64(d, "open_price")?)
        .bind(required_f64(d, "high_price")?)
        .bind(required_f64(d, "low_price")?)
        .bind(required_f64(d, "close_price")?)
        .bind(optional_f64(d, "volume_base"))
        .bind(optional_f64(d, "quote_volume"))
        .bind(optional_i64(d, "trade_count"))
        .bind(optional_f64(d, "taker_buy_base"))
        .bind(optional_f64(d, "taker_buy_quote"))
        .bind(required_bool(d, "is_closed")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.kline_bar")?;
        Ok(())
    }

    async fn insert_kline_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[KlineBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.kline_bar (
                open_time, close_time, market, symbol, interval_code,
                source_kind, stream_name,
                open_price, high_price, low_price, close_price,
                volume_base, quote_volume, trade_count,
                taker_buy_base, taker_buy_quote, is_closed, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.open_time)
                .push_bind(row.close_time)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.interval_code)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.open_price)
                .push_bind(row.high_price)
                .push_bind(row.low_price)
                .push_bind(row.close_price)
                .push_bind(row.volume_base)
                .push_bind(row.quote_volume)
                .push_bind(row.trade_count)
                .push_bind(row.taker_buy_base)
                .push_bind(row.taker_buy_quote)
                .push_bind(row.is_closed)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, interval_code, open_time)
            DO UPDATE SET
                close_time = EXCLUDED.close_time,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume_base = EXCLUDED.volume_base,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                taker_buy_base = EXCLUDED.taker_buy_base,
                taker_buy_quote = EXCLUDED.taker_buy_quote,
                is_closed = EXCLUDED.is_closed,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.kline_bar batch")?;

        Ok(())
    }

    async fn insert_mark_price(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.mark_price_funding_1s (
                ts_event, market, symbol, source_kind, stream_name,
                mark_price, index_price, estimated_settle_price,
                funding_rate, next_funding_time, payload_json
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type, $5,
                $6, $7, $8,
                $9, $10, $11
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(event.event_ts)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(optional_f64(d, "mark_price"))
        .bind(optional_f64(d, "index_price"))
        .bind(optional_f64(d, "estimated_settle_price"))
        .bind(optional_f64(d, "funding_rate"))
        .bind(optional_ts(d, "next_funding_time")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.mark_price_funding_1s")?;
        Ok(())
    }

    async fn insert_funding_rate(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        let funding_time = parse_ts(d, "funding_time")?;
        sqlx::query(
            r#"
            INSERT INTO md.funding_rate_event (
                funding_time, market, symbol, source_kind, stream_name,
                funding_rate, mark_price, next_funding_time, payload_json
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type, $5,
                $6, $7, $8, $9
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(funding_time)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_f64(d, "funding_rate")?)
        .bind(optional_f64(d, "mark_price"))
        .bind(optional_ts(d, "next_funding_time")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.funding_rate_event")?;
        Ok(())
    }

    async fn insert_force_order(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.force_order_event (
                ts_event, market, symbol, source_kind, stream_name,
                order_side, position_side, order_type, tif,
                original_qty, price, average_price, filled_qty, last_filled_qty,
                trade_time, notional_usdt, liq_side, payload_json
            )
            VALUES (
                $1, $2::cfg.market_type, $3, $4::cfg.source_type, $5,
                $6::cfg.side_type, $7, $8, $9,
                $10, $11, $12, $13, $14,
                $15, $16, $17, $18
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(event.event_ts)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(required_str(d, "stream_name")?)
        .bind(required_str(d, "order_side")?)
        .bind(optional_str(d, "position_side"))
        .bind(optional_str(d, "order_type"))
        .bind(optional_str(d, "tif"))
        .bind(optional_f64(d, "original_qty"))
        .bind(optional_f64(d, "price"))
        .bind(optional_f64(d, "average_price"))
        .bind(optional_f64(d, "filled_qty"))
        .bind(optional_f64(d, "last_filled_qty"))
        .bind(optional_ts(d, "trade_time")?)
        .bind(optional_f64(d, "notional_usdt"))
        .bind(required_str(d, "liq_side")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.force_order_event")?;
        Ok(())
    }

    async fn insert_agg_trade_1m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        let ts_bucket = parse_ts(d, "ts_bucket")?;
        let chunk_start_ts = parse_ts(d, "chunk_start_ts")?;
        let chunk_end_ts = parse_ts(d, "chunk_end_ts")?;
        sqlx::query(
            r#"
            INSERT INTO md.agg_trade_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                trade_count, buy_qty, sell_qty, buy_notional, sell_notional,
                first_price, last_price, high_price, low_price,
                profile_levels, whale_json, payload_json
            )
            VALUES (
                $1, $2, $3::cfg.market_type, $4, $5::cfg.source_type, $6,
                $7, $8, $9,
                $10, $11, $12, $13, $14,
                $15, $16, $17, $18,
                $19, $20, $21
            )
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                trade_count = EXCLUDED.trade_count,
                buy_qty = EXCLUDED.buy_qty,
                sell_qty = EXCLUDED.sell_qty,
                buy_notional = EXCLUDED.buy_notional,
                sell_notional = EXCLUDED.sell_notional,
                first_price = EXCLUDED.first_price,
                last_price = EXCLUDED.last_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                profile_levels = EXCLUDED.profile_levels,
                whale_json = EXCLUDED.whale_json,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(ts_bucket)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(event.stream_name.as_str())
        .bind(chunk_start_ts)
        .bind(chunk_end_ts)
        .bind(optional_i64(d, "source_event_count"))
        .bind(required_i64(d, "trade_count")?)
        .bind(required_f64(d, "buy_qty")?)
        .bind(required_f64(d, "sell_qty")?)
        .bind(required_f64(d, "buy_notional")?)
        .bind(required_f64(d, "sell_notional")?)
        .bind(optional_f64(d, "first_price"))
        .bind(optional_f64(d, "last_price"))
        .bind(optional_f64(d, "high_price"))
        .bind(optional_f64(d, "low_price"))
        .bind(required_value(d, "profile_levels")?)
        .bind(required_value(d, "whale")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.agg_trade_1m")?;
        Ok(())
    }

    async fn insert_agg_orderbook_1m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        let compacted = compact_orderbook_heatmap_for_db(d)?;
        sqlx::query(
            r#"
            INSERT INTO md.agg_orderbook_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                sample_count, bbo_updates,
                spread_sum, topk_depth_sum, obi_sum, obi_l1_sum, obi_k_sum,
                obi_k_dw_sum, obi_k_dw_change_sum, obi_k_dw_adj_sum,
                microprice_sum, microprice_classic_sum, microprice_kappa_sum, microprice_adj_sum,
                ofi_sum, obi_k_dw_close, heatmap_levels, payload_json
            )
            VALUES (
                $1, $2, $3::cfg.market_type, $4, $5::cfg.source_type, $6,
                $7, $8, $9,
                $10, $11,
                $12, $13, $14, $15, $16,
                $17, $18, $19,
                $20, $21, $22, $23,
                $24, $25, $26, $27
            )
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                sample_count = EXCLUDED.sample_count,
                bbo_updates = EXCLUDED.bbo_updates,
                spread_sum = EXCLUDED.spread_sum,
                topk_depth_sum = EXCLUDED.topk_depth_sum,
                obi_sum = EXCLUDED.obi_sum,
                obi_l1_sum = EXCLUDED.obi_l1_sum,
                obi_k_sum = EXCLUDED.obi_k_sum,
                obi_k_dw_sum = EXCLUDED.obi_k_dw_sum,
                obi_k_dw_change_sum = EXCLUDED.obi_k_dw_change_sum,
                obi_k_dw_adj_sum = EXCLUDED.obi_k_dw_adj_sum,
                microprice_sum = EXCLUDED.microprice_sum,
                microprice_classic_sum = EXCLUDED.microprice_classic_sum,
                microprice_kappa_sum = EXCLUDED.microprice_kappa_sum,
                microprice_adj_sum = EXCLUDED.microprice_adj_sum,
                ofi_sum = EXCLUDED.ofi_sum,
                obi_k_dw_close = EXCLUDED.obi_k_dw_close,
                heatmap_levels = EXCLUDED.heatmap_levels,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(event.stream_name.as_str())
        .bind(parse_ts(d, "chunk_start_ts")?)
        .bind(parse_ts(d, "chunk_end_ts")?)
        .bind(optional_i64(d, "source_event_count"))
        .bind(required_i64(d, "sample_count")?)
        .bind(required_i64(d, "bbo_updates")?)
        .bind(required_f64(d, "spread_sum")?)
        .bind(required_f64(d, "topk_depth_sum")?)
        .bind(required_f64(d, "obi_sum")?)
        .bind(required_f64(d, "obi_l1_sum")?)
        .bind(required_f64(d, "obi_k_sum")?)
        .bind(required_f64(d, "obi_k_dw_sum")?)
        .bind(required_f64(d, "obi_k_dw_change_sum")?)
        .bind(required_f64(d, "obi_k_dw_adj_sum")?)
        .bind(required_f64(d, "microprice_sum")?)
        .bind(required_f64(d, "microprice_classic_sum")?)
        .bind(required_f64(d, "microprice_kappa_sum")?)
        .bind(required_f64(d, "microprice_adj_sum")?)
        .bind(required_f64(d, "ofi_sum")?)
        .bind(optional_f64(d, "obi_k_dw_close"))
        .bind(compacted.heatmap_levels)
        .bind(compacted.payload_json)
        .execute(&self.pool)
        .await
        .context("insert md.agg_orderbook_1m")?;
        Ok(())
    }

    async fn insert_agg_liq_1m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.agg_liq_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                force_liq_levels, payload_json
            )
            VALUES (
                $1, $2, $3::cfg.market_type, $4, $5::cfg.source_type, $6,
                $7, $8, $9,
                $10, $11
            )
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                force_liq_levels = EXCLUDED.force_liq_levels,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(event.stream_name.as_str())
        .bind(parse_ts(d, "chunk_start_ts")?)
        .bind(parse_ts(d, "chunk_end_ts")?)
        .bind(optional_i64(d, "source_event_count"))
        .bind(required_value(d, "force_liq_levels")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.agg_liq_1m")?;
        Ok(())
    }

    async fn insert_agg_funding_mark_1m(&self, event: &NormalizedMdEvent) -> Result<()> {
        let d = &event.data;
        sqlx::query(
            r#"
            INSERT INTO md.agg_funding_mark_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                mark_points, funding_points, payload_json
            )
            VALUES (
                $1, $2, $3::cfg.market_type, $4, $5::cfg.source_type, $6,
                $7, $8, $9,
                $10, $11, $12
            )
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                mark_points = EXCLUDED.mark_points,
                funding_points = EXCLUDED.funding_points,
                payload_json = EXCLUDED.payload_json
            "#,
        )
        .bind(event.event_ts)
        .bind(parse_ts(d, "ts_bucket")?)
        .bind(event.market.as_str())
        .bind(event.symbol.as_str())
        .bind(event.source_kind.as_str())
        .bind(event.stream_name.as_str())
        .bind(parse_ts(d, "chunk_start_ts")?)
        .bind(parse_ts(d, "chunk_end_ts")?)
        .bind(optional_i64(d, "source_event_count"))
        .bind(required_value(d, "mark_points")?)
        .bind(required_value(d, "funding_points")?)
        .bind(payload_json(d))
        .execute(&self.pool)
        .await
        .context("insert md.agg_funding_mark_1m")?;
        Ok(())
    }

    async fn insert_agg_trade_1m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[AggTrade1mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.agg_trade_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                trade_count, buy_qty, sell_qty, buy_notional, sell_notional,
                first_price, last_price, high_price, low_price,
                profile_levels, whale_json, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.chunk_start_ts)
                .push_bind(row.chunk_end_ts)
                .push_bind(row.source_event_count)
                .push_bind(row.trade_count)
                .push_bind(row.buy_qty)
                .push_bind(row.sell_qty)
                .push_bind(row.buy_notional)
                .push_bind(row.sell_notional)
                .push_bind(row.first_price)
                .push_bind(row.last_price)
                .push_bind(row.high_price)
                .push_bind(row.low_price)
                .push_bind(&row.profile_levels)
                .push_bind(&row.whale_json)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                trade_count = EXCLUDED.trade_count,
                buy_qty = EXCLUDED.buy_qty,
                sell_qty = EXCLUDED.sell_qty,
                buy_notional = EXCLUDED.buy_notional,
                sell_notional = EXCLUDED.sell_notional,
                first_price = EXCLUDED.first_price,
                last_price = EXCLUDED.last_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                profile_levels = EXCLUDED.profile_levels,
                whale_json = EXCLUDED.whale_json,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.agg_trade_1m batch")?;
        Ok(())
    }

    async fn insert_agg_orderbook_1m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[AggOrderbook1mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.agg_orderbook_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                sample_count, bbo_updates,
                spread_sum, topk_depth_sum, obi_sum, obi_l1_sum, obi_k_sum,
                obi_k_dw_sum, obi_k_dw_change_sum, obi_k_dw_adj_sum,
                microprice_sum, microprice_classic_sum, microprice_kappa_sum, microprice_adj_sum,
                ofi_sum, obi_k_dw_close, heatmap_levels, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.chunk_start_ts)
                .push_bind(row.chunk_end_ts)
                .push_bind(row.source_event_count)
                .push_bind(row.sample_count)
                .push_bind(row.bbo_updates)
                .push_bind(row.spread_sum)
                .push_bind(row.topk_depth_sum)
                .push_bind(row.obi_sum)
                .push_bind(row.obi_l1_sum)
                .push_bind(row.obi_k_sum)
                .push_bind(row.obi_k_dw_sum)
                .push_bind(row.obi_k_dw_change_sum)
                .push_bind(row.obi_k_dw_adj_sum)
                .push_bind(row.microprice_sum)
                .push_bind(row.microprice_classic_sum)
                .push_bind(row.microprice_kappa_sum)
                .push_bind(row.microprice_adj_sum)
                .push_bind(row.ofi_sum)
                .push_bind(row.obi_k_dw_close)
                .push_bind(&row.heatmap_levels)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                sample_count = EXCLUDED.sample_count,
                bbo_updates = EXCLUDED.bbo_updates,
                spread_sum = EXCLUDED.spread_sum,
                topk_depth_sum = EXCLUDED.topk_depth_sum,
                obi_sum = EXCLUDED.obi_sum,
                obi_l1_sum = EXCLUDED.obi_l1_sum,
                obi_k_sum = EXCLUDED.obi_k_sum,
                obi_k_dw_sum = EXCLUDED.obi_k_dw_sum,
                obi_k_dw_change_sum = EXCLUDED.obi_k_dw_change_sum,
                obi_k_dw_adj_sum = EXCLUDED.obi_k_dw_adj_sum,
                microprice_sum = EXCLUDED.microprice_sum,
                microprice_classic_sum = EXCLUDED.microprice_classic_sum,
                microprice_kappa_sum = EXCLUDED.microprice_kappa_sum,
                microprice_adj_sum = EXCLUDED.microprice_adj_sum,
                ofi_sum = EXCLUDED.ofi_sum,
                obi_k_dw_close = EXCLUDED.obi_k_dw_close,
                heatmap_levels = EXCLUDED.heatmap_levels,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.agg_orderbook_1m batch")?;
        Ok(())
    }

    async fn insert_agg_liq_1m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[AggLiq1mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.agg_liq_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                force_liq_levels, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.chunk_start_ts)
                .push_bind(row.chunk_end_ts)
                .push_bind(row.source_event_count)
                .push_bind(&row.force_liq_levels)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                force_liq_levels = EXCLUDED.force_liq_levels,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.agg_liq_1m batch")?;
        Ok(())
    }

    async fn insert_agg_funding_mark_1m_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[AggFundingMark1mBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO md.agg_funding_mark_1m (
                ts_event, ts_bucket, market, symbol, source_kind, stream_name,
                chunk_start_ts, chunk_end_ts, source_event_count,
                mark_points, funding_points, payload_json
            )
            "#,
        );

        builder.push_values(rows, |mut b, row| {
            b.push_bind(row.ts_event)
                .push_bind(row.ts_bucket)
                .push_bind(&row.market)
                .push_unseparated("::cfg.market_type")
                .push_bind(&row.symbol)
                .push_bind(&row.source_kind)
                .push_unseparated("::cfg.source_type")
                .push_bind(&row.stream_name)
                .push_bind(row.chunk_start_ts)
                .push_bind(row.chunk_end_ts)
                .push_bind(row.source_event_count)
                .push_bind(&row.mark_points)
                .push_bind(&row.funding_points)
                .push_bind(&row.payload_json);
        });

        builder.push(
            r#"
            ON CONFLICT (market, symbol, stream_name, ts_bucket, chunk_start_ts, chunk_end_ts)
            DO UPDATE SET
                ts_event = EXCLUDED.ts_event,
                source_event_count = EXCLUDED.source_event_count,
                mark_points = EXCLUDED.mark_points,
                funding_points = EXCLUDED.funding_points,
                payload_json = EXCLUDED.payload_json
            "#,
        );
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("insert md.agg_funding_mark_1m batch")?;
        Ok(())
    }
}

fn is_raw_msg_type(msg_type: &str) -> bool {
    matches!(
        msg_type,
        "md.trade"
            | "md.depth"
            | "md.orderbook_snapshot_l2"
            | "md.bbo"
            | "md.mark_price"
            | "md.funding_rate"
            | "md.force_order"
    )
}

fn should_write_kline_hotpath(data: &Value) -> bool {
    let interval_code = data
        .get("interval_code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let is_closed = data
        .get("is_closed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    is_closed && matches!(interval_code, "1m" | "15m" | "1h" | "4h" | "1d" | "3d")
}

struct TradeBatchRow {
    ts_event: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    event_type: String,
    exchange_trade_id: Option<i64>,
    agg_trade_id: Option<i64>,
    first_trade_id: Option<i64>,
    last_trade_id: Option<i64>,
    price: f64,
    qty_raw: f64,
    qty_eth: f64,
    notional_usdt: f64,
    is_buyer_maker: Option<bool>,
    aggressor_side: i16,
    is_best_match: Option<bool>,
    payload_json: Value,
}

impl TradeBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            event_type: "agg_trade".to_string(),
            exchange_trade_id: optional_i64(d, "exchange_trade_id"),
            agg_trade_id: optional_i64(d, "agg_trade_id"),
            first_trade_id: optional_i64(d, "first_trade_id"),
            last_trade_id: optional_i64(d, "last_trade_id"),
            price: required_f64(d, "price")?,
            qty_raw: required_f64(d, "qty_raw")?,
            qty_eth: required_f64_alias(d, &["qty_base", "qty_eth"])?,
            notional_usdt: required_f64(d, "notional_usdt")?,
            is_buyer_maker: optional_bool(d, "is_buyer_maker"),
            aggressor_side: required_i64(d, "aggressor_side")? as i16,
            is_best_match: optional_bool(d, "is_best_match"),
            payload_json: payload_json(d),
        })
    }
}

struct DepthBatchRow {
    ts_event: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    first_update_id: Option<i64>,
    final_update_id: Option<i64>,
    prev_final_update_id: Option<i64>,
    bids_delta: Value,
    asks_delta: Value,
    event_count: Option<i32>,
    payload_json: Value,
}

impl DepthBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            first_update_id: optional_i64(d, "first_update_id"),
            final_update_id: optional_i64(d, "final_update_id"),
            prev_final_update_id: optional_i64(d, "prev_final_update_id"),
            bids_delta: required_value(d, "bids_delta")?,
            asks_delta: required_value(d, "asks_delta")?,
            event_count: optional_i64(d, "event_count").map(|v| v as i32),
            payload_json: payload_json(d),
        })
    }
}

struct BboBatchRow {
    ts_event: DateTime<Utc>,
    ts_window_start: DateTime<Utc>,
    ts_window_end: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    last_update_id: Option<i64>,
    sample_count: i32,
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
    spread_min: f64,
    spread_max: f64,
    spread_avg: f64,
    mid_avg: f64,
    payload_json: Value,
}

impl BboBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        let defaults = BboRollupDefaults::from_event(event)?;
        Ok(Self {
            ts_event: event.event_ts,
            ts_window_start: defaults.ts_window_start,
            ts_window_end: defaults.ts_window_end,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            last_update_id: optional_i64(d, "update_id"),
            sample_count: defaults.sample_count,
            bid_price: required_f64(d, "bid_price")?,
            bid_qty: required_f64(d, "bid_qty")?,
            ask_price: required_f64(d, "ask_price")?,
            ask_qty: required_f64(d, "ask_qty")?,
            spread_min: defaults.spread_min,
            spread_max: defaults.spread_max,
            spread_avg: defaults.spread_avg,
            mid_avg: defaults.mid_avg,
            payload_json: payload_json(d),
        })
    }
}

struct BboRollupDefaults {
    ts_window_start: DateTime<Utc>,
    ts_window_end: DateTime<Utc>,
    sample_count: i32,
    spread_min: f64,
    spread_max: f64,
    spread_avg: f64,
    mid_avg: f64,
}

impl BboRollupDefaults {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        let bid_price = required_f64(d, "bid_price")?;
        let ask_price = required_f64(d, "ask_price")?;
        let spread = (ask_price - bid_price).max(0.0);
        let mid = (ask_price + bid_price) / 2.0;
        let ts_window_start = optional_ts(d, "window_start_ts")?.unwrap_or_else(|| {
            let event_ms = event.event_ts.timestamp_millis();
            let floored = event_ms - event_ms.rem_euclid(BBO_ROLLUP_WINDOW_MS);
            DateTime::<Utc>::from_timestamp_millis(floored).unwrap_or(event.event_ts)
        });
        let ts_window_end = optional_ts(d, "window_end_ts")?.unwrap_or_else(|| {
            ts_window_start + chrono::Duration::milliseconds(BBO_ROLLUP_WINDOW_MS)
        });

        Ok(Self {
            ts_window_start,
            ts_window_end,
            sample_count: optional_i64(d, "sample_count")
                .or_else(|| optional_i64(d, "event_count"))
                .unwrap_or(1)
                .max(1) as i32,
            spread_min: optional_f64(d, "spread_min").unwrap_or(spread),
            spread_max: optional_f64(d, "spread_max").unwrap_or(spread),
            spread_avg: optional_f64(d, "spread_avg").unwrap_or(spread),
            mid_avg: optional_f64(d, "mid_avg").unwrap_or(mid),
        })
    }
}

struct OpenInterestCurrentBatchRow {
    ts_event: DateTime<Utc>,
    ts_recv: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    open_interest_contracts: f64,
    mark_price: Option<f64>,
    open_interest_value_usdt: Option<f64>,
    payload_json: Value,
}

impl OpenInterestCurrentBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_recv: parse_ts(d, "ts_recv")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            open_interest_contracts: required_f64(d, "open_interest_contracts")?,
            mark_price: optional_f64(d, "mark_price"),
            open_interest_value_usdt: optional_f64(d, "open_interest_value_usdt"),
            payload_json: payload_json(d),
        })
    }
}

struct OpenInterestHist5mBatchRow {
    ts_event: DateTime<Utc>,
    ts_recv: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    open_interest_contracts: f64,
    open_interest_value_usdt: f64,
    payload_json: Value,
}

impl OpenInterestHist5mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_recv: parse_ts(d, "ts_recv")?,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            open_interest_contracts: required_f64(d, "open_interest_contracts")?,
            open_interest_value_usdt: required_f64(d, "open_interest_value_usdt")?,
            payload_json: payload_json(d),
        })
    }
}

struct LongShortRatio5mBatchRow {
    ts_event: DateTime<Utc>,
    ts_recv: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    ratio_type: String,
    long_short_ratio: f64,
    long_account_ratio: Option<f64>,
    short_account_ratio: Option<f64>,
    payload_json: Value,
}

impl LongShortRatio5mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_recv: parse_ts(d, "ts_recv")?,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            ratio_type: required_str(d, "ratio_type")?,
            long_short_ratio: required_f64(d, "long_short_ratio")?,
            long_account_ratio: optional_f64(d, "long_account_ratio"),
            short_account_ratio: optional_f64(d, "short_account_ratio"),
            payload_json: payload_json(d),
        })
    }
}

struct OptionMarkGreeks5mBatchRow {
    ts_event: DateTime<Utc>,
    ts_recv: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    option_symbol: String,
    underlying_asset: String,
    source_kind: String,
    stream_name: String,
    expiry_ts: DateTime<Utc>,
    strike_price: f64,
    contract_side: String,
    unit: Option<f64>,
    index_price: Option<f64>,
    mark_price: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    mark_iv: Option<f64>,
    delta: Option<f64>,
    gamma: Option<f64>,
    vega: Option<f64>,
    theta: Option<f64>,
    risk_free_interest: Option<f64>,
    payload_json: Value,
}

impl OptionMarkGreeks5mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_recv: parse_ts(d, "ts_recv")?,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            option_symbol: required_str(d, "option_symbol")?,
            underlying_asset: required_str(d, "underlying_asset")?,
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            expiry_ts: parse_ts(d, "expiry_ts")?,
            strike_price: required_f64(d, "strike_price")?,
            contract_side: required_str(d, "contract_side")?,
            unit: optional_f64(d, "unit"),
            index_price: optional_f64(d, "index_price"),
            mark_price: optional_f64(d, "mark_price"),
            bid_iv: optional_f64(d, "bid_iv"),
            ask_iv: optional_f64(d, "ask_iv"),
            mark_iv: optional_f64(d, "mark_iv"),
            delta: optional_f64(d, "delta"),
            gamma: optional_f64(d, "gamma"),
            vega: optional_f64(d, "vega"),
            theta: optional_f64(d, "theta"),
            risk_free_interest: optional_f64(d, "risk_free_interest"),
            payload_json: payload_json(d),
        })
    }
}

struct KlineBatchRow {
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    market: String,
    symbol: String,
    interval_code: String,
    source_kind: String,
    stream_name: String,
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    volume_base: Option<f64>,
    quote_volume: Option<f64>,
    trade_count: Option<i64>,
    taker_buy_base: Option<f64>,
    taker_buy_quote: Option<f64>,
    is_closed: bool,
    payload_json: Value,
}

impl KlineBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            open_time: parse_ts(d, "open_time")?,
            close_time: parse_ts(d, "close_time")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            interval_code: required_str(d, "interval_code")?,
            source_kind: event.source_kind.clone(),
            stream_name: required_str(d, "stream_name")?,
            open_price: required_f64(d, "open_price")?,
            high_price: required_f64(d, "high_price")?,
            low_price: required_f64(d, "low_price")?,
            close_price: required_f64(d, "close_price")?,
            volume_base: optional_f64(d, "volume_base"),
            quote_volume: optional_f64(d, "quote_volume"),
            trade_count: optional_i64(d, "trade_count"),
            taker_buy_base: optional_f64(d, "taker_buy_base"),
            taker_buy_quote: optional_f64(d, "taker_buy_quote"),
            is_closed: required_bool(d, "is_closed")?,
            payload_json: payload_json(d),
        })
    }
}

struct AggTrade1mBatchRow {
    ts_event: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
    trade_count: i64,
    buy_qty: f64,
    sell_qty: f64,
    buy_notional: f64,
    sell_notional: f64,
    first_price: Option<f64>,
    last_price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    profile_levels: Value,
    whale_json: Value,
    payload_json: Value,
}

impl AggTrade1mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: event.stream_name.clone(),
            chunk_start_ts: parse_ts(d, "chunk_start_ts")?,
            chunk_end_ts: parse_ts(d, "chunk_end_ts")?,
            source_event_count: optional_i64(d, "source_event_count"),
            trade_count: required_i64(d, "trade_count")?,
            buy_qty: required_f64(d, "buy_qty")?,
            sell_qty: required_f64(d, "sell_qty")?,
            buy_notional: required_f64(d, "buy_notional")?,
            sell_notional: required_f64(d, "sell_notional")?,
            first_price: optional_f64(d, "first_price"),
            last_price: optional_f64(d, "last_price"),
            high_price: optional_f64(d, "high_price"),
            low_price: optional_f64(d, "low_price"),
            profile_levels: required_value(d, "profile_levels")?,
            whale_json: required_value(d, "whale")?,
            payload_json: payload_json(d),
        })
    }
}

struct AggOrderbook1mBatchRow {
    ts_event: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
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
    heatmap_levels: Value,
    payload_json: Value,
}

impl AggOrderbook1mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        let compacted = compact_orderbook_heatmap_for_db(d)?;
        Ok(Self {
            ts_event: event.event_ts,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: event.stream_name.clone(),
            chunk_start_ts: parse_ts(d, "chunk_start_ts")?,
            chunk_end_ts: parse_ts(d, "chunk_end_ts")?,
            source_event_count: optional_i64(d, "source_event_count"),
            sample_count: required_i64(d, "sample_count")?,
            bbo_updates: required_i64(d, "bbo_updates")?,
            spread_sum: required_f64(d, "spread_sum")?,
            topk_depth_sum: required_f64(d, "topk_depth_sum")?,
            obi_sum: required_f64(d, "obi_sum")?,
            obi_l1_sum: required_f64(d, "obi_l1_sum")?,
            obi_k_sum: required_f64(d, "obi_k_sum")?,
            obi_k_dw_sum: required_f64(d, "obi_k_dw_sum")?,
            obi_k_dw_change_sum: required_f64(d, "obi_k_dw_change_sum")?,
            obi_k_dw_adj_sum: required_f64(d, "obi_k_dw_adj_sum")?,
            microprice_sum: required_f64(d, "microprice_sum")?,
            microprice_classic_sum: required_f64(d, "microprice_classic_sum")?,
            microprice_kappa_sum: required_f64(d, "microprice_kappa_sum")?,
            microprice_adj_sum: required_f64(d, "microprice_adj_sum")?,
            ofi_sum: required_f64(d, "ofi_sum")?,
            obi_k_dw_close: optional_f64(d, "obi_k_dw_close"),
            heatmap_levels: compacted.heatmap_levels,
            payload_json: compacted.payload_json,
        })
    }
}

struct AggLiq1mBatchRow {
    ts_event: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
    force_liq_levels: Value,
    payload_json: Value,
}

impl AggLiq1mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: event.stream_name.clone(),
            chunk_start_ts: parse_ts(d, "chunk_start_ts")?,
            chunk_end_ts: parse_ts(d, "chunk_end_ts")?,
            source_event_count: optional_i64(d, "source_event_count"),
            force_liq_levels: required_value(d, "force_liq_levels")?,
            payload_json: payload_json(d),
        })
    }
}

struct AggFundingMark1mBatchRow {
    ts_event: DateTime<Utc>,
    ts_bucket: DateTime<Utc>,
    market: String,
    symbol: String,
    source_kind: String,
    stream_name: String,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
    source_event_count: Option<i64>,
    mark_points: Value,
    funding_points: Value,
    payload_json: Value,
}

impl AggFundingMark1mBatchRow {
    fn from_event(event: &NormalizedMdEvent) -> Result<Self> {
        let d = &event.data;
        Ok(Self {
            ts_event: event.event_ts,
            ts_bucket: parse_ts(d, "ts_bucket")?,
            market: event.market.clone(),
            symbol: event.symbol.clone(),
            source_kind: event.source_kind.clone(),
            stream_name: event.stream_name.clone(),
            chunk_start_ts: parse_ts(d, "chunk_start_ts")?,
            chunk_end_ts: parse_ts(d, "chunk_end_ts")?,
            source_event_count: optional_i64(d, "source_event_count"),
            mark_points: required_value(d, "mark_points")?,
            funding_points: required_value(d, "funding_points")?,
            payload_json: payload_json(d),
        })
    }
}

#[derive(Hash, Eq, PartialEq)]
struct ChunkConflictKey {
    market: String,
    symbol: String,
    stream_name: String,
    ts_bucket: DateTime<Utc>,
    chunk_start_ts: DateTime<Utc>,
    chunk_end_ts: DateTime<Utc>,
}

#[derive(Hash, Eq, PartialEq)]
struct OpenInterestHistConflictKey {
    market: String,
    symbol: String,
    ts_bucket: DateTime<Utc>,
}

#[derive(Hash, Eq, PartialEq)]
struct LongShortRatioConflictKey {
    market: String,
    symbol: String,
    ratio_type: String,
    ts_bucket: DateTime<Utc>,
}

// Keep the last row for each natural key so batched upserts match the final
// state produced by the single-row fallback path.
fn dedupe_keep_last<T, K, F>(rows: &mut Vec<T>, mut key_fn: F)
where
    K: Eq + std::hash::Hash,
    F: FnMut(&T) -> K,
{
    let mut seen = HashSet::with_capacity(rows.len());
    rows.reverse();
    rows.retain(|row| seen.insert(key_fn(row)));
    rows.reverse();
}

fn dedupe_open_interest_hist_rows(rows: &mut Vec<OpenInterestHist5mBatchRow>) {
    dedupe_keep_last(rows, |row| OpenInterestHistConflictKey {
        market: row.market.clone(),
        symbol: row.symbol.clone(),
        ts_bucket: row.ts_bucket,
    });
}

fn dedupe_long_short_ratio_rows(rows: &mut Vec<LongShortRatio5mBatchRow>) {
    dedupe_keep_last(rows, |row| LongShortRatioConflictKey {
        market: row.market.clone(),
        symbol: row.symbol.clone(),
        ratio_type: row.ratio_type.clone(),
        ts_bucket: row.ts_bucket,
    });
}

fn dedupe_agg_trade_rows(rows: &mut Vec<AggTrade1mBatchRow>) {
    let mut seen = HashSet::with_capacity(rows.len());
    rows.retain(|row| {
        let key = ChunkConflictKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket.clone(),
            chunk_start_ts: row.chunk_start_ts.clone(),
            chunk_end_ts: row.chunk_end_ts.clone(),
        };
        seen.insert(key)
    });
}

fn dedupe_agg_orderbook_rows(rows: &mut Vec<AggOrderbook1mBatchRow>) {
    let mut seen = HashSet::with_capacity(rows.len());
    rows.retain(|row| {
        let key = ChunkConflictKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket.clone(),
            chunk_start_ts: row.chunk_start_ts.clone(),
            chunk_end_ts: row.chunk_end_ts.clone(),
        };
        seen.insert(key)
    });
}

fn dedupe_agg_liq_rows(rows: &mut Vec<AggLiq1mBatchRow>) {
    let mut seen = HashSet::with_capacity(rows.len());
    rows.retain(|row| {
        let key = ChunkConflictKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket.clone(),
            chunk_start_ts: row.chunk_start_ts.clone(),
            chunk_end_ts: row.chunk_end_ts.clone(),
        };
        seen.insert(key)
    });
}

fn dedupe_agg_funding_rows(rows: &mut Vec<AggFundingMark1mBatchRow>) {
    let mut seen = HashSet::with_capacity(rows.len());
    rows.retain(|row| {
        let key = ChunkConflictKey {
            market: row.market.clone(),
            symbol: row.symbol.clone(),
            stream_name: row.stream_name.clone(),
            ts_bucket: row.ts_bucket.clone(),
            chunk_start_ts: row.chunk_start_ts.clone(),
            chunk_end_ts: row.chunk_end_ts.clone(),
        };
        seen.insert(key)
    });
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

fn optional_str(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
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

fn required_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing bool field: {}", key))
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

fn required_f64_alias(value: &Value, keys: &[&str]) -> Result<f64> {
    for key in keys {
        if let Some(raw) = value.get(*key) {
            return to_f64(raw).with_context(|| format!("parse decimal field {}", key));
        }
    }
    Err(anyhow!("missing decimal field: {}", keys.join(" | ")))
}

fn optional_f64(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|v| to_f64(v).ok())
}

fn parse_ts(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    let raw = required_str(value, key)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("parse timestamp field {}", key))
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

fn payload_json(value: &Value) -> Value {
    value
        .get("payload_json")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()))
}

#[derive(Debug, Clone)]
struct CompactOrderbookDbPayload {
    heatmap_levels: Value,
    payload_json: Value,
}

#[derive(Debug, Clone)]
struct HeatmapLevelCompact {
    price: f64,
    bid_liquidity: f64,
    ask_liquidity: f64,
}

impl HeatmapLevelCompact {
    fn total(&self) -> f64 {
        self.bid_liquidity + self.ask_liquidity
    }

    fn abs_net(&self) -> f64 {
        (self.bid_liquidity - self.ask_liquidity).abs()
    }
}

fn compact_orderbook_heatmap_for_db(data: &Value) -> Result<CompactOrderbookDbPayload> {
    let payload_json = payload_json(data);
    let levels_value = required_value(data, "heatmap_levels")?;
    let levels = parse_heatmap_levels_compact(&levels_value)?;
    let total_levels = levels.len();

    if total_levels <= ORDERBOOK_DB_HEATMAP_COMPACT_THRESHOLD {
        let payload_json =
            augment_orderbook_payload(payload_json, total_levels as i64, total_levels as i64);
        return Ok(CompactOrderbookDbPayload {
            heatmap_levels: levels_value,
            payload_json,
        });
    }

    let representative_mid = representative_orderbook_mid(data);
    let compact_levels = select_compact_heatmap_levels(&levels, representative_mid);
    let payload_json = augment_orderbook_payload(
        payload_json,
        total_levels as i64,
        compact_levels.len() as i64,
    );

    Ok(CompactOrderbookDbPayload {
        heatmap_levels: Value::Array(
            compact_levels
                .into_iter()
                .map(|level| json!([level.price, level.bid_liquidity, level.ask_liquidity]))
                .collect(),
        ),
        payload_json,
    })
}

fn augment_orderbook_payload(
    payload_json: Value,
    heatmap_total_levels: i64,
    heatmap_stored_levels: i64,
) -> Value {
    let mut payload_obj = match payload_json {
        Value::Object(map) => map,
        Value::Null => Map::new(),
        other => {
            let mut map = Map::new();
            map.insert("raw_payload_json".to_string(), other);
            map
        }
    };
    payload_obj.insert(
        "heatmap_total_levels".to_string(),
        json!(heatmap_total_levels.max(0)),
    );
    payload_obj.insert(
        "heatmap_stored_levels".to_string(),
        json!(heatmap_stored_levels.max(0)),
    );
    payload_obj.insert(
        "heatmap_db_compacted".to_string(),
        json!(heatmap_stored_levels < heatmap_total_levels),
    );
    Value::Object(payload_obj)
}

fn representative_orderbook_mid(data: &Value) -> Option<f64> {
    let sample_count = optional_i64(data, "sample_count").filter(|count| *count > 0)? as f64;
    optional_f64(data, "microprice_kappa_sum")
        .or_else(|| optional_f64(data, "microprice_sum"))
        .or_else(|| optional_f64(data, "microprice_classic_sum"))
        .or_else(|| optional_f64(data, "microprice_adj_sum"))
        .map(|sum| sum / sample_count)
        .filter(|mid| mid.is_finite() && *mid > 0.0)
}

fn parse_heatmap_levels_compact(value: &Value) -> Result<Vec<HeatmapLevelCompact>> {
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
        out.push(HeatmapLevelCompact {
            price: to_f64(&level[0])?,
            bid_liquidity: to_f64(&level[1])?,
            ask_liquidity: to_f64(&level[2])?,
        });
    }
    Ok(out)
}

fn select_compact_heatmap_levels(
    levels: &[HeatmapLevelCompact],
    representative_mid: Option<f64>,
) -> Vec<HeatmapLevelCompact> {
    let mut selected = HashSet::new();
    collect_top_heatmap_indices(
        levels,
        &mut selected,
        ORDERBOOK_DB_HEATMAP_TOP_TOTAL_LEVELS,
        |level| level.total(),
    );
    collect_top_heatmap_indices(
        levels,
        &mut selected,
        ORDERBOOK_DB_HEATMAP_TOP_ABS_NET_LEVELS,
        |level| level.abs_net(),
    );
    collect_top_heatmap_indices(
        levels,
        &mut selected,
        ORDERBOOK_DB_HEATMAP_TOP_BID_LEVELS,
        |level| level.bid_liquidity,
    );
    collect_top_heatmap_indices(
        levels,
        &mut selected,
        ORDERBOOK_DB_HEATMAP_TOP_ASK_LEVELS,
        |level| level.ask_liquidity,
    );

    if let Some(mid) = representative_mid {
        let mut indices = (0..levels.len()).collect::<Vec<_>>();
        indices.sort_by(|left, right| {
            let left_dist = (levels[*left].price - mid).abs();
            let right_dist = (levels[*right].price - mid).abs();
            left_dist
                .partial_cmp(&right_dist)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for idx in indices
            .into_iter()
            .take(ORDERBOOK_DB_HEATMAP_NEAR_MID_LEVELS)
        {
            selected.insert(idx);
        }
    }

    let mut compact = selected
        .into_iter()
        .map(|idx| levels[idx].clone())
        .collect::<Vec<_>>();
    compact.sort_by(|left, right| {
        left.price
            .partial_cmp(&right.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    compact
}

fn collect_top_heatmap_indices<F>(
    levels: &[HeatmapLevelCompact],
    selected: &mut HashSet<usize>,
    take: usize,
    score: F,
) where
    F: Fn(&HeatmapLevelCompact) -> f64,
{
    let mut indices = (0..levels.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        score(&levels[*right])
            .partial_cmp(&score(&levels[*left]))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for idx in indices.into_iter().take(take) {
        selected.insert(idx);
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
        Value::Null => Err(anyhow!("null cannot convert to f64")),
        _ => Err(anyhow!("unsupported decimal type")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compact_orderbook_heatmap_for_db, dedupe_long_short_ratio_rows,
        dedupe_open_interest_hist_rows, should_write_kline_hotpath, LongShortRatio5mBatchRow,
        OpenInterestHist5mBatchRow,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn closed_htf_klines_are_persisted() {
        for interval_code in ["1m", "15m", "1h", "4h", "1d", "3d"] {
            assert!(should_write_kline_hotpath(&json!({
                "interval_code": interval_code,
                "is_closed": true
            })));
        }
        assert!(!should_write_kline_hotpath(&json!({
            "interval_code": "1h",
            "is_closed": false
        })));
        assert!(!should_write_kline_hotpath(&json!({
            "interval_code": "5m",
            "is_closed": true
        })));
    }

    #[test]
    fn compacts_large_orderbook_heatmap_for_db() {
        let levels = (0..1024)
            .map(|idx| {
                let price = 100_000.0 + idx as f64 * 0.1;
                let bid = if idx % 2 == 0 {
                    (1024 - idx) as f64
                } else {
                    0.0
                };
                let ask = if idx % 2 == 1 { (idx + 1) as f64 } else { 0.0 };
                json!([price, bid, ask])
            })
            .collect::<Vec<_>>();
        let payload = json!({
            "sample_count": 60,
            "microprice_kappa_sum": 6_000_000.0,
            "heatmap_levels": levels,
            "payload_json": {
                "agg_mode": "canonical_1m"
            }
        });

        let compacted = compact_orderbook_heatmap_for_db(&payload).expect("compact orderbook");
        let stored = compacted
            .heatmap_levels
            .as_array()
            .expect("stored heatmap levels array");
        assert!(stored.len() < 256);
        assert_eq!(compacted.payload_json["heatmap_total_levels"], json!(1024));
        assert_eq!(compacted.payload_json["heatmap_db_compacted"], json!(true));
        assert_eq!(
            compacted.payload_json["heatmap_stored_levels"],
            json!(stored.len())
        );
    }

    #[test]
    fn keeps_small_orderbook_heatmap_uncompacted_for_db() {
        let payload = json!({
            "sample_count": 10,
            "microprice_sum": 1000.0,
            "heatmap_levels": [
                [100.0, 8.0, 2.0],
                [101.0, 1.0, 9.0]
            ],
            "payload_json": {}
        });

        let compacted = compact_orderbook_heatmap_for_db(&payload).expect("compact orderbook");
        let stored = compacted
            .heatmap_levels
            .as_array()
            .expect("stored heatmap levels array");
        assert_eq!(stored.len(), 2);
        assert_eq!(compacted.payload_json["heatmap_total_levels"], json!(2));
        assert_eq!(compacted.payload_json["heatmap_stored_levels"], json!(2));
        assert_eq!(compacted.payload_json["heatmap_db_compacted"], json!(false));
    }

    #[test]
    fn dedupe_open_interest_hist_rows_keeps_last_duplicate_bucket() {
        let bucket = ts("2026-04-02T11:25:00Z");
        let mut rows = vec![
            OpenInterestHist5mBatchRow {
                ts_event: ts("2026-04-02T11:25:01Z"),
                ts_recv: ts("2026-04-02T11:25:02Z"),
                ts_bucket: bucket,
                market: "futures".to_string(),
                symbol: "ETHUSDT".to_string(),
                source_kind: "rest".to_string(),
                stream_name: "first".to_string(),
                open_interest_contracts: 100.0,
                open_interest_value_usdt: 200.0,
                payload_json: json!({"version": 1}),
            },
            OpenInterestHist5mBatchRow {
                ts_event: ts("2026-04-02T11:30:01Z"),
                ts_recv: ts("2026-04-02T11:30:02Z"),
                ts_bucket: ts("2026-04-02T11:30:00Z"),
                market: "futures".to_string(),
                symbol: "ETHUSDT".to_string(),
                source_kind: "rest".to_string(),
                stream_name: "other".to_string(),
                open_interest_contracts: 300.0,
                open_interest_value_usdt: 400.0,
                payload_json: json!({"version": 1}),
            },
            OpenInterestHist5mBatchRow {
                ts_event: ts("2026-04-02T11:25:03Z"),
                ts_recv: ts("2026-04-02T11:25:04Z"),
                ts_bucket: bucket,
                market: "futures".to_string(),
                symbol: "ETHUSDT".to_string(),
                source_kind: "rest".to_string(),
                stream_name: "last".to_string(),
                open_interest_contracts: 101.0,
                open_interest_value_usdt: 201.0,
                payload_json: json!({"version": 2}),
            },
        ];

        dedupe_open_interest_hist_rows(&mut rows);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts_bucket, ts("2026-04-02T11:30:00Z"));
        assert_eq!(rows[1].ts_bucket, bucket);
        assert_eq!(rows[1].stream_name, "last");
        assert_eq!(rows[1].open_interest_value_usdt, 201.0);
    }

    #[test]
    fn dedupe_long_short_ratio_rows_keeps_last_duplicate_bucket() {
        let mut rows = vec![
            LongShortRatio5mBatchRow {
                ts_event: ts("2026-04-02T11:25:01Z"),
                ts_recv: ts("2026-04-02T11:25:02Z"),
                ts_bucket: ts("2026-04-02T11:25:00Z"),
                market: "futures".to_string(),
                symbol: "ETHUSDT".to_string(),
                source_kind: "rest".to_string(),
                stream_name: "first".to_string(),
                ratio_type: "global_account".to_string(),
                long_short_ratio: 1.1,
                long_account_ratio: Some(0.52),
                short_account_ratio: Some(0.48),
                payload_json: json!({"version": 1}),
            },
            LongShortRatio5mBatchRow {
                ts_event: ts("2026-04-02T11:25:03Z"),
                ts_recv: ts("2026-04-02T11:25:04Z"),
                ts_bucket: ts("2026-04-02T11:25:00Z"),
                market: "futures".to_string(),
                symbol: "ETHUSDT".to_string(),
                source_kind: "rest".to_string(),
                stream_name: "last".to_string(),
                ratio_type: "global_account".to_string(),
                long_short_ratio: 1.2,
                long_account_ratio: Some(0.55),
                short_account_ratio: Some(0.45),
                payload_json: json!({"version": 2}),
            },
        ];

        dedupe_long_short_ratio_rows(&mut rows);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stream_name, "last");
        assert_eq!(rows[0].long_short_ratio, 1.2);
    }
}
