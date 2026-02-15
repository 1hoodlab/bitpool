use async_trait::async_trait;

use crate::domain::entities::Node;

use super::DomainRepoError;

#[async_trait]
pub trait NodeRepository: Send + Sync {
    async fn find_by_id(&self, node_id: &str) -> Result<Option<Node>, DomainRepoError>;
    async fn update_tip(&self, node_id: &str, tip_block_id: Option<&str>) -> Result<(), DomainRepoError>;
}
