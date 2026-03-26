BEGIN;

CREATE TABLE IF NOT EXISTS md.open_interest_current_1m (
    ts_event                  TIMESTAMPTZ NOT NULL,
    ts_recv                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                     TEXT NOT NULL DEFAULT 'binance',
    market                    cfg.market_type NOT NULL,
    symbol                    TEXT NOT NULL,
    source_kind               cfg.source_type NOT NULL,
    stream_name               TEXT NOT NULL,
    open_interest_contracts   DOUBLE PRECISION NOT NULL,
    mark_price                DOUBLE PRECISION,
    open_interest_value_usdt  DOUBLE PRECISION,
    payload_json              JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_open_interest_current_1m_natural
ON md.open_interest_current_1m(market, symbol, ts_event);

CREATE INDEX IF NOT EXISTS idx_open_interest_current_1m_lookup
ON md.open_interest_current_1m(symbol, ts_event DESC);

COMMIT;
