CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agg_orderbook_1m_backfill_v2
ON md.agg_orderbook_1m (symbol, market, ts_bucket, ts_event);
