-- Speed up the outbox dispatcher's live non-trade claim path:
--   WHERE status IN ('pending','failed','sending')
--     AND available_at <= now()
--     AND exchange_name = $2
--     AND routing_key NOT LIKE 'md.%.trade.%'
--
-- Trade rows already have a dedicated partial index. Non-trade rows were falling
-- back to the broader ready index and paying an avoidable filter cost.

CREATE INDEX IF NOT EXISTS idx_outbox_ready_non_trade_priority
ON ops.outbox_event(exchange_name, available_at, outbox_id)
WHERE status IN ('pending', 'failed', 'sending')
  AND routing_key NOT LIKE 'md.%.trade.%';
