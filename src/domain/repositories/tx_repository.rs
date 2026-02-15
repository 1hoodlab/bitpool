use async_trait::async_trait;

use crate::domain::entities::Tx;

use super::DomainRepoError;

#[async_trait]
pub trait TxRepository: Send + Sync {
    async fn find_by_id(&self, tx_id: &str) -> Result<Option<Tx>, DomainRepoError>;
    async fn list_pending(&self, limit: u64) -> Result<Vec<Tx>, DomainRepoError>;
}
