use async_trait::async_trait;

use crate::domain::entities::EventEnvelope;

use super::DomainRepoError;

#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn append_event(&self, event: &EventEnvelope) -> Result<(), DomainRepoError>;

    async fn append_events(&self, events: &[EventEnvelope]) -> Result<(), DomainRepoError> {
        for event in events {
            self.append_event(event).await?;
        }
        Ok(())
    }

    async fn last_seq_id(&self) -> Result<Option<i64>, DomainRepoError>;
    async fn read_events_after(
        &self,
        after_seq_id: i64,
        limit: u64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError>;
    async fn read_events_range(
        &self,
        from_seq: i64,
        to_seq: i64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError>;
}
