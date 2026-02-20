use serde_json::Value;

use crate::domain::entities::EventEnvelope;
use crate::domain::repositories::DomainRepoError;

use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "event_log")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub seq_id: i64,
    pub event_type: String,
    pub server_time_ms: i64,
    pub payload_json: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    /// Build ActiveModel from domain EventEnvelope for INSERT
    pub fn from_event(event: &EventEnvelope) -> Result<ActiveModel, DomainRepoError> {
        let payload_json = serde_json::to_string(&event.payload)
            .map_err(|e| DomainRepoError(format!("serialize payload failed: {e}")))?;
        Ok(ActiveModel {
            seq_id: Set(event.seq_id),
            event_type: Set(event.event_type.clone()),
            server_time_ms: Set(event.server_time_ms),
            payload_json: Set(Some(payload_json)),
            ..Default::default()
        })
    }

    /// Convert DB model to domain EventEnvelope
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
