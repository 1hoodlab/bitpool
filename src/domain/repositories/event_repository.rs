use async_trait::async_trait;

use super::DomainRepoError;
use super::EventEntry;

#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn append(&self, event: &EventEntry) -> Result<(), DomainRepoError>;
    async fn list_after(
        &self,
        room_id: &str,
        after_seq_id: i64,
        limit: u64,
    ) -> Result<Vec<EventEntry>, DomainRepoError>;
}
