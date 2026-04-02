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
        'open_interest',
        'Open Interest',
        'flow',
        'timeseries+summary',
        'futures',
        FALSE,
        '5m normalized OI history with 5m/15m/4h/1d/3d crowding-state windows'
    ),
    (
        'long_short_ratios',
        'Long Short Ratios',
        'flow',
        'timeseries+summary',
        'futures',
        FALSE,
        'Binance global/top account/position long-short ratio windows'
    ),
    (
        'options_surface',
        'Options Surface',
        'flow',
        'timeseries+summary',
        'futures',
        FALSE,
        'Binance options ATM IV, RR/skew, and term-structure state by 5m/15m/4h/1d/3d windows'
    ),
    (
        'fvg',
        'Fair Value Gap',
        'structure',
        'timeseries+zones',
        'futures',
        FALSE,
        'HTF fair value gap structure zones for 1h/4h/1d filtering'
    )
ON CONFLICT (indicator_code) DO NOTHING;

COMMIT;
