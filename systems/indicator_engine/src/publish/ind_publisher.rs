use crate::indicators::context::IndicatorSnapshotRow;
use anyhow::Result;
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use serde_json::{json, Value};
use std::io::Write;
use uuid::Uuid;

const INDICATOR_OUTBOX_SCHEMA_VERSION: i32 = 1;
const INDICATOR_MESSAGE_NAMESPACE: Uuid =
    Uuid::from_u128(0x8d9b_9f64_0eea_4f7b_9f35_4385_31a7_a651);

#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub exchange_name: String,
    pub routing_key: String,
    pub message_id: Uuid,
    pub schema_version: i32,
    pub headers_json: Value,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
pub struct BundleOutboxMessage {
    pub exchange_name: String,
    pub routing_key: String,
    pub message_id: Uuid,
    pub schema_version: i32,
    pub headers_json: Value,
    pub symbol: String,
    pub ts_bucket: DateTime<Utc>,
    pub indicator_count: i32,
    pub payload_encoding: String,
    pub payload_bytes: Vec<u8>,
    pub payload_json: Value,
}

#[derive(Clone)]
pub struct IndPublisher {
    exchange_name: String,
    producer_instance_id: String,
}

impl IndPublisher {
    pub fn new(exchange_name: String, producer_instance_id: String) -> Self {
        Self {
            exchange_name,
            producer_instance_id,
        }
    }

    pub fn build_snapshot_message(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        snapshot: &IndicatorSnapshotRow,
    ) -> Result<OutboxMessage> {
        self.build_snapshot_message_from_parts(
            ts_bucket,
            symbol,
            snapshot.indicator_code,
            snapshot.window_code,
            &snapshot.payload_json,
        )
    }

    pub fn build_snapshot_message_from_parts(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicator_code: &str,
        window_code: &str,
        payload_json: &Value,
    ) -> Result<OutboxMessage> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("evt.{}.{}", indicator_code, symbol_low);
        let identity = format!(
            "ind.snapshot|{}|{}|{}|{}|{}",
            symbol.to_uppercase(),
            ts_bucket.to_rfc3339(),
            indicator_code,
            window_code,
            payload_json
        );
        let message_id = stable_uuid("message", &identity);
        let trace_id = stable_uuid("trace", &identity);

        Ok(OutboxMessage {
            exchange_name: self.exchange_name.clone(),
            routing_key: routing_key.clone(),
            message_id,
            schema_version: INDICATOR_OUTBOX_SCHEMA_VERSION,
            headers_json: self.build_headers_json(),
            payload_json: json!({
                "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
                "msg_type": "ind.snapshot",
                "message_id": message_id,
                "trace_id": trace_id,
                "routing_key": routing_key,
                "indicator_code": indicator_code,
                "window_code": window_code,
                "symbol": symbol,
                "event_ts": ts_bucket.to_rfc3339(),
                "published_at": Utc::now().to_rfc3339(),
                "producer": {
                    "service": "indicator_engine",
                    "instance_id": self.producer_instance_id,
                },
                "data": payload_json,
            }),
        })
    }

    pub fn build_minute_bundle_message(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicators_json: &Value,
        indicator_count: usize,
    ) -> Result<OutboxMessage> {
        let (routing_key, message_id, trace_id, payload_json) =
            self.build_minute_bundle_payload(ts_bucket, symbol, indicators_json, indicator_count)?;

        Ok(OutboxMessage {
            exchange_name: self.exchange_name.clone(),
            routing_key,
            message_id,
            schema_version: INDICATOR_OUTBOX_SCHEMA_VERSION,
            headers_json: self.build_headers_json(),
            payload_json,
        })
    }

    pub fn build_minute_bundle_outbox_message(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicators_json: &Value,
        indicator_count: usize,
    ) -> Result<BundleOutboxMessage> {
        let (routing_key, message_id, _trace_id, payload_json) =
            self.build_minute_bundle_payload(ts_bucket, symbol, indicators_json, indicator_count)?;
        let payload_bytes = gzip_json_bytes(&payload_json)?;

        Ok(BundleOutboxMessage {
            exchange_name: self.exchange_name.clone(),
            routing_key,
            message_id,
            schema_version: INDICATOR_OUTBOX_SCHEMA_VERSION,
            headers_json: self.build_headers_json(),
            symbol: symbol.to_uppercase(),
            ts_bucket,
            indicator_count: indicator_count as i32,
            payload_encoding: "gzip".to_string(),
            payload_bytes,
            payload_json: json!({
                "symbol": symbol,
                "ts_bucket": ts_bucket.to_rfc3339(),
                "window_code": "1m",
                "indicator_count": indicator_count,
                "msg_type": "ind.minute_bundle",
            }),
        })
    }

    fn build_minute_bundle_payload(
        &self,
        ts_bucket: DateTime<Utc>,
        symbol: &str,
        indicators_json: &Value,
        indicator_count: usize,
    ) -> Result<(String, Uuid, Uuid, Value)> {
        let symbol_low = symbol.to_lowercase();
        let routing_key = format!("bundle.1m.{}", symbol_low);
        let identity = format!(
            "ind.minute_bundle|{}|{}|{}",
            symbol.to_uppercase(),
            ts_bucket.to_rfc3339(),
            indicators_json
        );
        let message_id = stable_uuid("message", &identity);
        let trace_id = stable_uuid("trace", &identity);

        Ok((
            routing_key.clone(),
            message_id,
            trace_id,
            json!({
                "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
                "msg_type": "ind.minute_bundle",
                "message_id": message_id,
                "trace_id": trace_id,
                "routing_key": routing_key,
                "symbol": symbol,
                "ts_bucket": ts_bucket.to_rfc3339(),
                "window_code": "1m",
                "indicator_count": indicator_count,
                "published_at": Utc::now().to_rfc3339(),
                "producer": {
                    "service": "indicator_engine",
                    "instance_id": self.producer_instance_id,
                },
                "indicators": indicators_json,
            }),
        ))
    }

    fn build_headers_json(&self) -> Value {
        json!({
            "schema_version": INDICATOR_OUTBOX_SCHEMA_VERSION,
            "producer_service": "indicator_engine",
            "producer_instance_id": self.producer_instance_id,
        })
    }
}

fn stable_uuid(scope: &str, identity: &str) -> Uuid {
    Uuid::new_v5(
        &INDICATOR_MESSAGE_NAMESPACE,
        format!("{}|{}", scope, identity).as_bytes(),
    )
}

fn gzip_json_bytes(payload_json: &Value) -> Result<Vec<u8>> {
    let raw = serde_json::to_vec(payload_json)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw)?;
    Ok(encoder.finish()?)
}

#[cfg(test)]
mod tests {
    use super::{gzip_json_bytes, IndPublisher};
    use crate::indicators::context::IndicatorSnapshotRow;
    use chrono::TimeZone;
    use flate2::read::GzDecoder;
    use serde_json::json;
    use std::io::Read;

    fn publisher() -> IndPublisher {
        IndPublisher::new("amq.ind".to_string(), "indicator-engine-test".to_string())
    }

    #[test]
    fn snapshot_message_id_is_stable_for_same_payload() {
        let publisher = publisher();
        let ts_bucket = chrono::Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).unwrap();
        let snapshot = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.42}),
        };

        let left = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &snapshot)
            .unwrap();
        let right = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &snapshot)
            .unwrap();

        assert_eq!(left.message_id, right.message_id);
        assert_eq!(left.routing_key, "evt.orderbook_depth.btcusdt");
        assert_eq!(left.payload_json["window_code"], "15m");
    }

    #[test]
    fn snapshot_message_id_changes_when_payload_changes() {
        let publisher = publisher();
        let ts_bucket = chrono::Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).unwrap();
        let before = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.42}),
        };
        let after = IndicatorSnapshotRow {
            indicator_code: "orderbook_depth",
            window_code: "15m",
            payload_json: json!({"imbalance": 0.73}),
        };

        let left = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &before)
            .unwrap();
        let right = publisher
            .build_snapshot_message(ts_bucket, "BTCUSDT", &after)
            .unwrap();

        assert_ne!(left.message_id, right.message_id);
    }

    #[test]
    fn minute_bundle_outbox_payload_round_trips() {
        let publisher = publisher();
        let ts_bucket = chrono::Utc.with_ymd_and_hms(2026, 3, 21, 3, 0, 0).unwrap();
        let indicators_json = json!({
            "footprint": {
                "window_code": "1m",
                "payload": {
                    "levels": [1, 2, 3],
                    "window_total_qty": 10.0
                }
            }
        });

        let msg = publisher
            .build_minute_bundle_outbox_message(ts_bucket, "BTCUSDT", &indicators_json, 1)
            .unwrap();
        assert_eq!(msg.payload_encoding, "gzip");

        let mut decoder = GzDecoder::new(msg.payload_bytes.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded["msg_type"], "ind.minute_bundle");
        assert_eq!(decoded["symbol"], "BTCUSDT");
        assert_eq!(decoded["indicators"], indicators_json);
    }

    #[test]
    fn gzip_json_bytes_preserves_payload() {
        let payload = json!({
            "msg_type": "ind.minute_bundle",
            "indicators": {
                "footprint": {"window_code": "1m", "payload": {"levels": [1,2,3]}}
            }
        });
        let compressed = gzip_json_bytes(&payload).unwrap();
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded, payload);
    }
}
