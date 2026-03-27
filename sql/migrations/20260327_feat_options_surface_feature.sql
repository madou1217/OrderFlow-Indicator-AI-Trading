BEGIN;

CREATE TABLE IF NOT EXISTS feat.options_surface_feature (
    ts_bucket                TIMESTAMPTZ NOT NULL,
    bar_interval             INTERVAL NOT NULL,
    venue                    TEXT NOT NULL DEFAULT 'binance',
    symbol                   TEXT NOT NULL,
    front_expiry_ts          TIMESTAMPTZ,
    second_expiry_ts         TIMESTAMPTZ,
    atm_strike_front         DOUBLE PRECISION,
    atm_iv_front             DOUBLE PRECISION,
    atm_iv_second            DOUBLE PRECISION,
    atm_iv_30d_proxy         DOUBLE PRECISION,
    atm_iv_regime            TEXT,
    rr_25d_front             DOUBLE PRECISION,
    rr_25d_second            DOUBLE PRECISION,
    atm_iv_front_change      DOUBLE PRECISION,
    rr_25d_front_change      DOUBLE PRECISION,
    skew_state               TEXT,
    term_structure_state     TEXT,
    calc_version             TEXT NOT NULL,
    extra_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('feat.options_surface_feature', 'ts_bucket', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_options_surface_feature_natural
ON feat.options_surface_feature(venue, symbol, bar_interval, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_options_surface_feature_lookup
ON feat.options_surface_feature(symbol, bar_interval, ts_bucket DESC);

COMMIT;
