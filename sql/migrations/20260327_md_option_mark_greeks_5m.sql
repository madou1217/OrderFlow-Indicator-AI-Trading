BEGIN;

CREATE TABLE IF NOT EXISTS md.option_mark_greeks_5m (
    ts_event             TIMESTAMPTZ NOT NULL,
    ts_recv              TIMESTAMPTZ NOT NULL DEFAULT now(),
    venue                TEXT NOT NULL DEFAULT 'binance',
    ts_bucket            TIMESTAMPTZ NOT NULL,
    market               cfg.market_type NOT NULL,
    symbol               TEXT NOT NULL,
    option_symbol        TEXT NOT NULL,
    underlying_asset     TEXT NOT NULL,
    source_kind          cfg.source_type NOT NULL,
    stream_name          TEXT NOT NULL,
    expiry_ts            TIMESTAMPTZ NOT NULL,
    strike_price         DOUBLE PRECISION NOT NULL,
    contract_side        TEXT NOT NULL,
    unit                 DOUBLE PRECISION,
    index_price          DOUBLE PRECISION,
    mark_price           DOUBLE PRECISION,
    bid_iv               DOUBLE PRECISION,
    ask_iv               DOUBLE PRECISION,
    mark_iv              DOUBLE PRECISION,
    delta                DOUBLE PRECISION,
    gamma                DOUBLE PRECISION,
    vega                 DOUBLE PRECISION,
    theta                DOUBLE PRECISION,
    risk_free_interest   DOUBLE PRECISION,
    payload_json         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_option_mark_greeks_5m_natural
ON md.option_mark_greeks_5m(market, symbol, option_symbol, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_option_mark_greeks_5m_lookup
ON md.option_mark_greeks_5m(symbol, ts_bucket DESC, expiry_ts, strike_price);

COMMIT;
