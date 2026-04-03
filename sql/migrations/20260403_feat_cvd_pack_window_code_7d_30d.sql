BEGIN;

ALTER TABLE feat.cvd_pack
    DROP CONSTRAINT IF EXISTS cvd_pack_window_code_check;

ALTER TABLE feat.cvd_pack
    ADD CONSTRAINT cvd_pack_window_code_check
    CHECK (window_code IN ('1m', '15m', '1h', '4h', '1d', '3d', '7d', '30d'))
    NOT VALID;

ALTER TABLE feat.cvd_pack
    VALIDATE CONSTRAINT cvd_pack_window_code_check;

COMMIT;
