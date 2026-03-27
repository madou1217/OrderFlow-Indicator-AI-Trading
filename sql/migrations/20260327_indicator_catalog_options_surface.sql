BEGIN;

INSERT INTO cfg.indicator_catalog (
    indicator_code,
    display_name,
    category,
    output_kind,
    primary_market,
    needs_spot_confirm,
    notes
)
VALUES
    (
        'options_surface',
        'Options Surface',
        'flow',
        'timeseries+summary',
        'futures',
        FALSE,
        'Binance options ATM IV, RR/skew, and term-structure state by 5m/15m/4h/1d/3d windows'
    )
ON CONFLICT (indicator_code) DO NOTHING;

COMMIT;
