/// Event log entry for replay/sync (used by EventRepository).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEntry {
    pub seq_id: i64,
    pub event_type: String,
    pub server_time_ms: i64,
    pub payload_json: Option<String>,
    pub room_id: String,
}
