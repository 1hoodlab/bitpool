use async_trait::async_trait;

use crate::domain::entities::Block;

use super::DomainRepoError;

#[async_trait]
pub trait BlockRepository: Send + Sync {
    async fn find_by_id(&self, block_id: &str) -> Result<Option<Block>, DomainRepoError>;
    async fn find_best_tip(&self) -> Result<Option<Block>, DomainRepoError>;
}
