use serde_json::Value;

use crate::domain::entities::EventEnvelope;
use crate::domain::repositories::DomainRepoError;

#[derive(Debug, Clone)]
pub struct EventLogInsert {
    pub seq_id: i64,
    pub event_type: String,
    pub server_time_ms: i64,
    pub payload_json: String,
}

impl EventLogInsert {
    pub fn from_event(event: &EventEnvelope) -> Result<Self, DomainRepoError> {
        let payload_json = serde_json::to_string(&event.payload)
            .map_err(|e| DomainRepoError(format!("serialize payload failed: {e}")))?;
        Ok(Self {
            seq_id: event.seq_id,
            event_type: event.event_type.clone(),
            server_time_ms: event.server_time_ms,
            payload_json,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EventLogRow {
    pub seq_id: i64,
    pub event_type: String,
    pub server_time_ms: i64,
    pub payload_json: Option<String>,
}

impl EventLogRow {
    pub fn into_event(self) -> Result<EventEnvelope, DomainRepoError> {
        let payload = match self.payload_json {
            Some(raw) => serde_json::from_str::<Value>(&raw)
                .map_err(|e| DomainRepoError(format!("decode payload_json failed: {e}")))?,
            None => Value::Null,
        };

        Ok(EventEnvelope {
            seq_id: self.seq_id,
            event_type: self.event_type,
            server_time_ms: self.server_time_ms,
            payload,
        })
    }
}
