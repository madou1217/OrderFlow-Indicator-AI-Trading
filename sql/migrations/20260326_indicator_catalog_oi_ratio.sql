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
    )
ON CONFLICT (indicator_code) DO NOTHING;

COMMIT;
