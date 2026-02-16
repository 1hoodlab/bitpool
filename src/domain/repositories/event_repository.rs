use async_trait::async_trait;

use crate::domain::entities::{Block, BlockIndexEntry, ChainTip, EventEnvelope, Reward};

use super::DomainRepoError;

#[derive(Debug, Clone)]
pub struct RewardSettlementWrite {
    pub reward: Reward,
    pub confirmations: i32,
}

#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn append_event(&self, event: &EventEnvelope) -> Result<(), DomainRepoError>;

    async fn upsert_block(
        &self,
        _block: &Block,
        _cumulative_work: u128,
        _first_seen_seq_id: i64,
        _chain_status: &str,
    ) -> Result<(), DomainRepoError> {
        Ok(())
    }

    async fn load_chain_state(&self) -> Result<(Vec<BlockIndexEntry>, Option<ChainTip>), DomainRepoError> {
        Ok((Vec::new(), None))
    }

    async fn load_reward_settlements(&self) -> Result<Vec<Reward>, DomainRepoError> {
        Ok(Vec::new())
    }

    async fn upsert_reward_settlement(
        &self,
        _reward: &Reward,
        _confirmations: i32,
    ) -> Result<(), DomainRepoError> {
        Ok(())
    }

    async fn upsert_block_with_reward_settlements(
        &self,
        block: &Block,
        cumulative_work: u128,
        first_seen_seq_id: i64,
        chain_status: &str,
        reward_writes: &[RewardSettlementWrite],
    ) -> Result<(), DomainRepoError> {
        self.upsert_block(block, cumulative_work, first_seen_seq_id, chain_status)
            .await?;
        for write in reward_writes {
            self.upsert_reward_settlement(&write.reward, write.confirmations)
                .await?;
        }
        Ok(())
    }

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
