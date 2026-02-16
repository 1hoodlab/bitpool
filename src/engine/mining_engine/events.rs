use crate::domain::entities::{EventEnvelope, EventType, ScheduledDelivery};
use crate::domain::repositories::{DomainRepoError, EventRepository};

use super::DeterministicEngineCore;

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) async fn emit_net_delivery_event(
        &mut self,
        delivery: ScheduledDelivery,
    ) -> Result<(), DomainRepoError> {
        self.emit_event(
            EventType::NET_DELIVER_V1,
            serde_json::json!({
                "deliver_at_ms": delivery.deliver_at_ms,
                "from": delivery.from,
                "to": delivery.to,
                "kind": delivery.kind.as_str(),
                "msg_id": delivery.msg_id,
                "payload": delivery.payload,
            }),
            true,
        )
        .await?;
        self.handle_delivery(&delivery).await
    }

    pub(super) async fn emit_event(
        &mut self,
        event_type: &str,
        payload: serde_json::Value,
        persist_to_db: bool,
    ) -> Result<(), DomainRepoError> {
        let next_seq = self.current_seq_id.saturating_add(1);
        let envelope = EventEnvelope {
            seq_id: next_seq,
            server_time_ms: self.sim_time_ms,
            event_type: event_type.to_string(),
            payload,
        };
        if persist_to_db {
            self.repo.append_event(&envelope).await?;
        }
        self.current_seq_id = next_seq;
        Ok(())
    }
}
