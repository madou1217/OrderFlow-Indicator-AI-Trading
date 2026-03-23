-- Dedicated minute-bundle outbox for indicator_engine hot path.
-- Optional because runtime also auto-ensures these objects on startup.

CREATE TABLE IF NOT EXISTS ops.indicator_bundle_outbox (
    outbox_id BIGSERIAL PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count INTEGER NOT NULL DEFAULT 0,
    exchange_name TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    message_id UUID NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT indicator_bundle_outbox_retry_count_nonneg_chk
        CHECK (retry_count >= 0),
    CONSTRAINT indicator_bundle_outbox_schema_version_pos_chk
        CHECK (schema_version > 0),
    CONSTRAINT indicator_bundle_outbox_status_chk
        CHECK (status IN ('pending', 'sending', 'failed', 'dead'))
);

CREATE INDEX IF NOT EXISTS idx_indicator_bundle_outbox_ready
ON ops.indicator_bundle_outbox (exchange_name, available_at, outbox_id)
WHERE status IN ('pending', 'failed', 'sending');

CREATE OR REPLACE FUNCTION ops.notify_indicator_bundle_outbox_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('indicator_bundle_outbox_ready', NEW.exchange_name);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_indicator_bundle_outbox_notify
ON ops.indicator_bundle_outbox;
CREATE TRIGGER trg_indicator_bundle_outbox_notify
AFTER INSERT ON ops.indicator_bundle_outbox
FOR EACH ROW EXECUTE FUNCTION ops.notify_indicator_bundle_outbox_ready();

CREATE TABLE IF NOT EXISTS ops.indicator_snapshot_fanout_progress (
    symbol TEXT PRIMARY KEY,
    last_published_snapshot_ts TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_indicator_snapshot_symbol_ts
ON feat.indicator_snapshot (symbol, ts_snapshot);
