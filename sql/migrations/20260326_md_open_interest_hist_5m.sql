BEGIN;

CREATE TABLE IF NOT EXISTS md.open_interest_hist_5m (
    ts_event                  TIMESTAMPTZ NOT NULL,
    ts_recv                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                     TEXT NOT NULL DEFAULT 'binance',
    ts_bucket                 TIMESTAMPTZ NOT NULL,
    market                    cfg.market_type NOT NULL,
    symbol                    TEXT NOT NULL,
    source_kind               cfg.source_type NOT NULL,
    stream_name               TEXT NOT NULL,
    open_interest_contracts   DOUBLE PRECISION NOT NULL,
    open_interest_value_usdt  DOUBLE PRECISION NOT NULL,
    payload_json              JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_open_interest_hist_5m_natural
ON md.open_interest_hist_5m(market, symbol, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_open_interest_hist_5m_lookup
ON md.open_interest_hist_5m(symbol, ts_bucket DESC);

CREATE INDEX IF NOT EXISTS idx_open_interest_hist_5m_backfill
ON md.open_interest_hist_5m(symbol, ts_bucket, market);

COMMIT;
