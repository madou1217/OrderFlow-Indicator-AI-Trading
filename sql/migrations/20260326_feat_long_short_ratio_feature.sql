BEGIN;

CREATE TABLE IF NOT EXISTS feat.long_short_ratio_feature (
    ts_bucket                  TIMESTAMPTZ NOT NULL,
    bar_interval               INTERVAL NOT NULL,
    venue                      TEXT NOT NULL DEFAULT 'binance',
    symbol                     TEXT NOT NULL,
    global_ratio_latest        DOUBLE PRECISION,
    top_account_ratio_latest   DOUBLE PRECISION,
    top_position_ratio_latest  DOUBLE PRECISION,
    global_ratio_log           DOUBLE PRECISION,
    top_account_ratio_log      DOUBLE PRECISION,
    top_position_ratio_log     DOUBLE PRECISION,
    global_ratio_change        DOUBLE PRECISION,
    top_account_ratio_change   DOUBLE PRECISION,
    top_position_ratio_change  DOUBLE PRECISION,
    account_crowding_gap       DOUBLE PRECISION,
    position_crowding_gap      DOUBLE PRECISION,
    crowding_stretch           DOUBLE PRECISION,
    crowding_zscore            DOUBLE PRECISION,
    crowding_state             TEXT,
    calc_version               TEXT NOT NULL,
    extra_json                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('feat.long_short_ratio_feature', 'ts_bucket', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS uq_long_short_ratio_feature_natural
ON feat.long_short_ratio_feature(venue, symbol, bar_interval, ts_bucket);

CREATE INDEX IF NOT EXISTS idx_long_short_ratio_feature_lookup
ON feat.long_short_ratio_feature(symbol, bar_interval, ts_bucket DESC);

COMMIT;
