BEGIN;

CREATE TABLE IF NOT EXISTS feat.open_interest_feature (
    ts_bucket             TIMESTAMPTZ NOT NULL,
    bar_interval          INTERVAL NOT NULL,
    venue                 TEXT NOT NULL DEFAULT 'binance',
    symbol                TEXT NOT NULL,
    oi_latest_contracts   DOUBLE PRECISION,
    oi_latest_value_usdt  DOUBLE PRECISION,
    oi_start_value_usdt   DOUBLE PRECISION,
    oi_delta_abs          DOUBLE PRECISION,
    oi_delta_pct          DOUBLE PRECISION,
    oi_log_return         DOUBLE PRECISION,
    oi_zscore             DOUBLE PRECISION,
    oi_accel              DOUBLE PRECISION,
    price_start           DOUBLE PRECISION,
    price_end             DOUBLE PRECISION,
    price_delta_pct       DOUBLE PRECISION,
    price_oi_relation     TEXT,
    calc_version          TEXT NOT NULL,
    extra_json            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('feat.open_interest_feature', 'ts_bucket', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_open_interest_feature_natural
ON feat.open_interest_feature(venue, symbol, bar_interval, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_open_interest_feature_lookup
ON feat.open_interest_feature(symbol, bar_interval, ts_bucket DESC);

COMMIT;
