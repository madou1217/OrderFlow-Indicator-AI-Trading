BEGIN;

SELECT create_hypertable(
    'md.option_mark_greeks_5m',
    'ts_bucket',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

COMMIT;
