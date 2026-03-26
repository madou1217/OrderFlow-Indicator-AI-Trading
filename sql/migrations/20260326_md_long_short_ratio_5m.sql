BEGIN;

CREATE TABLE IF NOT EXISTS md.long_short_ratio_5m (
    ts_event              TIMESTAMPTZ NOT NULL,
    ts_recv               TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                 TEXT NOT NULL DEFAULT 'binance',
    ts_bucket             TIMESTAMPTZ NOT NULL,
    market                cfg.market_type NOT NULL,
    symbol                TEXT NOT NULL,
    source_kind           cfg.source_type NOT NULL,
    stream_name           TEXT NOT NULL,
    ratio_type            TEXT NOT NULL,
    long_short_ratio      DOUBLE PRECISION NOT NULL,
    long_account_ratio    DOUBLE PRECISION,
    short_account_ratio   DOUBLE PRECISION,
    payload_json          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_long_short_ratio_5m_natural
ON md.long_short_ratio_5m(market, symbol, ratio_type, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_long_short_ratio_5m_lookup
ON md.long_short_ratio_5m(symbol, ratio_type, ts_bucket DESC);

CREATE INDEX IF NOT EXISTS idx_long_short_ratio_5m_backfill
ON md.long_short_ratio_5m(symbol, ts_bucket, market);

COMMIT;
