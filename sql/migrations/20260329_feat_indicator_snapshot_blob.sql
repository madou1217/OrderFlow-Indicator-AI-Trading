CREATE TABLE IF NOT EXISTS feat.indicator_snapshot_blob (
    blob_hash TEXT PRIMARY KEY,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE feat.indicator_snapshot_blob IS
'Deduplicated large indicator snapshot subtrees referenced from feat.indicator_snapshot.payload_json.';

CREATE OR REPLACE FUNCTION feat.hydrate_indicator_snapshot_payload(payload jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    ref_meta jsonb;
    chunk_meta jsonb;
    blob_hash text;
    hydrated jsonb;
    child_key text;
    child_value jsonb;
    out_obj jsonb;
    out_arr jsonb;
BEGIN
    IF payload IS NULL THEN
        RETURN NULL;
    END IF;

    IF jsonb_typeof(payload) = 'object' THEN
        ref_meta := payload -> '__snapshot_blob_ref_v1';
        IF jsonb_typeof(ref_meta) = 'object' AND (ref_meta ? 'hash') THEN
            blob_hash := ref_meta ->> 'hash';
            SELECT b.payload_json
              INTO hydrated
              FROM feat.indicator_snapshot_blob b
             WHERE b.blob_hash = blob_hash;
            IF hydrated IS NULL THEN
                RAISE EXCEPTION 'missing indicator snapshot blob hash=%', blob_hash;
            END IF;
            RETURN feat.hydrate_indicator_snapshot_payload(hydrated);
        END IF;

        chunk_meta := payload -> '__snapshot_blob_chunks_v1';
        IF jsonb_typeof(chunk_meta) = 'object' AND jsonb_typeof(chunk_meta -> 'chunk_hashes') = 'array' THEN
            out_arr := '[]'::jsonb;
            FOR blob_hash IN
                SELECT value
                FROM jsonb_array_elements_text(chunk_meta -> 'chunk_hashes')
            LOOP
                SELECT b.payload_json
                  INTO hydrated
                  FROM feat.indicator_snapshot_blob b
                 WHERE b.blob_hash = blob_hash;
                IF hydrated IS NULL THEN
                    RAISE EXCEPTION 'missing indicator snapshot chunk hash=%', blob_hash;
                END IF;
                IF jsonb_typeof(hydrated) <> 'array' THEN
                    RAISE EXCEPTION 'indicator snapshot chunk blob is not an array hash=%', blob_hash;
                END IF;
                FOR child_value IN
                    SELECT value
                    FROM jsonb_array_elements(hydrated)
                LOOP
                    out_arr := out_arr || jsonb_build_array(
                        feat.hydrate_indicator_snapshot_payload(child_value)
                    );
                END LOOP;
            END LOOP;
            RETURN out_arr;
        END IF;

        out_obj := '{}'::jsonb;
        FOR child_key, child_value IN
            SELECT key, value
            FROM jsonb_each(payload)
        LOOP
            out_obj := out_obj || jsonb_build_object(
                child_key,
                feat.hydrate_indicator_snapshot_payload(child_value)
            );
        END LOOP;
        RETURN out_obj;
    ELSIF jsonb_typeof(payload) = 'array' THEN
        out_arr := '[]'::jsonb;
        FOR child_value IN
            SELECT value
            FROM jsonb_array_elements(payload)
        LOOP
            out_arr := out_arr || jsonb_build_array(
                feat.hydrate_indicator_snapshot_payload(child_value)
            );
        END LOOP;
        RETURN out_arr;
    END IF;

    RETURN payload;
END
$$;

CREATE OR REPLACE VIEW feat.v_indicator_snapshot_hydrated AS
SELECT
    ts_snapshot,
    bar_interval,
    venue,
    symbol,
    market_scope,
    indicator_code,
    window_code,
    primary_market,
    param_set_id,
    calc_version,
    feat.hydrate_indicator_snapshot_payload(payload_json) AS payload_json,
    tags,
    created_at
FROM feat.indicator_snapshot;
