use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub seq_id: i64,
    pub server_time_ms: i64,
    pub event_type: String,
    pub payload: Value,
}

impl EventEnvelope {
    pub fn validate(&self) -> Result<(), String> {
        if self.seq_id <= 0 {
            return Err("seq_id must be > 0".to_string());
        }
        if self.event_type.trim().is_empty() {
            return Err("event_type must not be empty".to_string());
        }
        Ok(())
    }
}

pub struct EventType;

impl EventType {
    pub const ENGINE_TICK_V1: &'static str = "engine.tick.v1";
    pub const NET_DELIVER_V1: &'static str = "net.deliver.v1";
    pub const NODE_RECEIVED_V1: &'static str = "node.received.v1";
}
