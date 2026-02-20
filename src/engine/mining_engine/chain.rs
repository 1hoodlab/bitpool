use serde_json::json;

use crate::domain::entities::{Block, BlockIndexEntry, BlockStatus, ChainTip, RewardStatus};
use crate::domain::repositories::{DomainRepoError, EventRepository, RewardSettlementWrite};
use crate::engine::consensus::ConsensusService;

use super::types::NodeTip;
use super::DeterministicEngineCore;

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) async fn connect_block(&mut self, block: &Block) -> Result<(), DomainRepoError> {
        // 1. Remove txs from mempool
        for tx_id in &block.tx_ids {
            self.mempool.remove(tx_id);
        }

        // 2. Create Reward (Immature)
        let reward_id = format!("reward-{}", block.header.block_id);
        let reward = crate::domain::entities::Reward {
            id: reward_id.clone(),
            block_hash: block.header.block_id.clone(),
            miner_id: block.header.miner_node_id.clone(),
            amount_sats: 50_000_000 + block.header.total_fees_sats, // 50 BTC + fees
            status: RewardStatus::Immature,
            created_at_ms: self.sim_time_ms,
            matured_at_height: None,
            settled_at_ms: None,
        };
        self.rewards.insert(reward_id, reward);

        // 3. Check Maturity (after configured confirmations)
        let mature_reward_ids = self
            .rewards
            .values()
            .filter(|reward| reward.status == RewardStatus::Immature)
            .filter(|reward| {
                self.reward_confirmations(&reward.block_hash)
                    >= super::REWARD_MATURITY_CONFIRMATIONS as i32
            })
            .map(|reward| reward.id.clone())
            .collect::<Vec<_>>();
        let mut matured = Vec::new();
        for reward_id in mature_reward_ids {
            if let Some(reward) = self.rewards.get_mut(&reward_id) {
                reward.status = RewardStatus::Confirmed;
                reward.matured_at_height = Some(block.header.height);
                reward.settled_at_ms = Some(self.sim_time_ms);
                matured.push(reward.clone());
            }
        }

        // Emit events after dropping mutable borrow on rewards
        for reward in matured {
            self.emit_event(
                "reward.matured.v1",
                json!({
                    "reward_id": reward.id,
                    "miner_id": reward.miner_id,
                    "amount_sats": reward.amount_sats,
                    "block_height": block.header.height,
                }),
                true,
            )
            .await?;
        }

        self.persist_block_and_all_reward_settlements(block, "active")
            .await?;

        Ok(())
    }

    pub(super) async fn disconnect_block(&mut self, block: &Block) -> Result<(), DomainRepoError> {
        // 1. Add txs back to mempool (Simplified: we don't have full tx content here unless we store it)
        // In this simulation, we might lose txs if we don't persist them fully.
        // For now, we accept that disconnected txs might be lost from mempool in this simplified model,
        // or we could fetch them if we had a TxRepository.

        // 2. Mark Reward as Orphaned
        let reward_id = format!("reward-{}", block.header.block_id);
        if let Some(reward) = self.rewards.get_mut(&reward_id) {
            reward.status = RewardStatus::Orphaned;
        }

        self.persist_block_and_all_reward_settlements(block, "pending")
            .await?;

        Ok(())
    }

    pub(super) fn canonical_tip_height(&self) -> i64 {
        self.block_index
            .values()
            .filter(|entry| matches!(entry.status, BlockStatus::Active))
            .map(|entry| entry.header.height)
            .max()
            .unwrap_or(0)
    }

    pub(super) fn reward_confirmations(&self, block_hash: &str) -> i32 {
        let Some(entry) = self.block_index.get(block_hash) else {
            return 0;
        };
        if !matches!(entry.status, BlockStatus::Active) {
            return 0;
        }
        let tip_height = self.canonical_tip_height();
        if tip_height < entry.header.height {
            return 0;
        }
        let conf = tip_height
            .saturating_sub(entry.header.height)
            .saturating_add(1);
        conf.min(i32::MAX as i64) as i32
    }

    pub(super) fn build_reward_settlement_writes(&self) -> Vec<RewardSettlementWrite> {
        let mut rewards = self.rewards.values().cloned().collect::<Vec<_>>();
        rewards.sort_by(|a, b| a.id.cmp(&b.id));
        rewards
            .into_iter()
            .map(|reward| RewardSettlementWrite {
                confirmations: self.reward_confirmations(&reward.block_hash),
                reward,
            })
            .collect()
    }

    pub(super) fn reconcile_rewards_after_bootstrap(&mut self) {
        let tip_height = self.canonical_tip_height();
        let reward_ids = self.rewards.keys().cloned().collect::<Vec<_>>();
        for reward_id in reward_ids {
            let Some(block_hash) = self.rewards.get(&reward_id).map(|r| r.block_hash.clone())
            else {
                continue;
            };
            let Some(entry) = self.block_index.get(&block_hash) else {
                continue;
            };
            let entry_height = entry.header.height;
            let is_active = matches!(entry.status, BlockStatus::Active);

            let Some(reward) = self.rewards.get_mut(&reward_id) else {
                continue;
            };

            if !is_active {
                if reward.status != RewardStatus::Orphaned {
                    reward.status = RewardStatus::Orphaned;
                }
                continue;
            }

            let confirmations = tip_height
                .saturating_sub(entry_height)
                .saturating_add(1)
                .max(0) as i32;
            if reward.status == RewardStatus::Immature
                && confirmations >= super::REWARD_MATURITY_CONFIRMATIONS as i32
            {
                reward.status = RewardStatus::Confirmed;
                reward.matured_at_height = Some(tip_height);
                if reward.settled_at_ms.is_none() {
                    reward.settled_at_ms = Some(self.sim_time_ms);
                }
            }
        }
    }

    pub(super) async fn persist_all_reward_settlements(&self) -> Result<(), DomainRepoError> {
        let reward_writes = self.build_reward_settlement_writes();
        for write in reward_writes {
            self.repo
                .upsert_reward_settlement(&write.reward, write.confirmations)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn persist_block_and_all_reward_settlements(
        &self,
        block: &Block,
        chain_status: &str,
    ) -> Result<(), DomainRepoError> {
        let reward_writes = self.build_reward_settlement_writes();
        self.repo
            .upsert_block_with_reward_settlements(
                block,
                self.block_index
                    .get(&block.header.block_id)
                    .map(|e| e.cumulative_work)
                    .unwrap_or(0),
                self.current_seq_id.saturating_add(1),
                chain_status,
                &reward_writes,
            )
            .await
    }

    pub(super) async fn update_tip(
        &mut self,
        node_id: &str,
        next_tip: NodeTip,
    ) -> Result<(), DomainRepoError> {
        let should_update = match self.node_tips.get(node_id) {
            Some(curr) => next_tip.height > curr.height,
            None => true,
        };
        if !should_update {
            return Ok(());
        }
        self.node_tips.insert(node_id.to_string(), next_tip.clone());
        self.emit_event(
            "node.tip_updated.v1",
            json!({
                "node": node_id,
                "block_id": next_tip.block_id,
                "height": next_tip.height,
            }),
            false,
        )
        .await
    }

    pub(super) async fn process_block(&mut self, block: Block) -> Result<(), DomainRepoError> {
        if self.block_index.contains_key(&block.header.block_id) {
            return Ok(()); // Already processed
        }

        // 1. Validate Parent
        let parent_entry = match self.block_index.get(&block.header.parent_hash) {
            Some(entry) => entry.clone(),
            None => {
                log::warn!("Orphan block received: {}", block.header.block_id);
                return Ok(()); // Orphan handling is out of scope for now
            }
        };

        // 2. Add to Index
        let cumulative_work = ConsensusService::calculate_cumulative_work(
            &block.header,
            parent_entry.cumulative_work,
        );

        let entry = BlockIndexEntry {
            header: block.header.clone(),
            tx_ids: block.tx_ids.clone(),
            cumulative_work,
            status: BlockStatus::Valid,
        };
        self.block_index
            .insert(block.header.block_id.clone(), entry.clone());
        self.repo
            .upsert_block(
                &block,
                cumulative_work,
                self.current_seq_id.saturating_add(1),
                "pending",
            )
            .await?;

        // 3. Chain Selection
        let current_tip_header = match &self.active_tip {
            Some(tip) => self.block_index.get(&tip.block_id).unwrap().header.clone(),
            None => parent_entry.header.clone(), // Should not happen if genesis exists
        };

        if ConsensusService::is_better_chain(&block.header, &current_tip_header, &self.block_index)
        {
            self.reorganize(&current_tip_header.block_id, &block.header.block_id)
                .await?;
        }

        Ok(())
    }

    pub(super) async fn reorganize(
        &mut self,
        old_tip_id: &str,
        new_tip_id: &str,
    ) -> Result<(), DomainRepoError> {
        let fork_point =
            ConsensusService::find_fork_point(old_tip_id, new_tip_id, &self.block_index)
                .ok_or(DomainRepoError("Fork point not found".to_string()))?;

        // 1. Collect blocks to disconnect
        let mut to_disconnect = Vec::new();
        let mut curr = old_tip_id.to_string();
        while curr != fork_point {
            if let Some(entry) = self.block_index.get(&curr) {
                to_disconnect.push(curr.clone());
                curr = entry.header.parent_hash.clone();
            } else {
                break;
            }
        }

        // Process disconnects
        for block_id in &to_disconnect {
            if let Some(entry) = self.block_index.get_mut(block_id) {
                entry.status = BlockStatus::Valid; // Downgrade
            }
            let (header, tx_ids) = {
                let entry = self.block_index.get(block_id).unwrap();
                (entry.header.clone(), entry.tx_ids.clone())
            };
            let block = Block { header, tx_ids };
            self.disconnect_block(&block).await?;
        }

        // 2. Collect blocks to connect
        let mut to_connect = Vec::new();
        let mut curr = new_tip_id.to_string();
        while curr != fork_point {
            to_connect.push(curr.clone());
            if let Some(entry) = self.block_index.get(&curr) {
                curr = entry.header.parent_hash.clone();
            } else {
                break;
            }
        }
        to_connect.reverse(); // Process forward

        let disconnect_count = to_disconnect.len();
        let connect_count = to_connect.len();
        let is_extension = fork_point == old_tip_id;
        if is_extension {
            log::info!(
                "Chain extended: {} -> {} (height +{})",
                old_tip_id,
                new_tip_id,
                connect_count
            );
        } else {
            log::info!(
                "Reorg triggered: {} -> {} (Fork: {}, disconnect={}, connect={})",
                old_tip_id,
                new_tip_id,
                fork_point,
                disconnect_count,
                connect_count
            );
        }

        // Process connects
        for block_id in to_connect {
            if let Some(entry) = self.block_index.get_mut(&block_id) {
                entry.status = BlockStatus::Active;
            }
            let (header, tx_ids) = {
                let entry = self.block_index.get(&block_id).unwrap();
                (entry.header.clone(), entry.tx_ids.clone())
            };
            let block = Block { header, tx_ids };
            self.connect_block(&block).await?;
        }

        // 3. Update Active Tip
        let new_tip_entry = self.block_index.get(new_tip_id).unwrap();
        self.active_tip = Some(ChainTip {
            block_id: new_tip_id.to_string(),
            height: new_tip_entry.header.height,
            cumulative_work: new_tip_entry.cumulative_work,
        });

        self.emit_event(
            "chain.reorg.v1",
            json!({
                "old_tip": old_tip_id,
                "new_tip": new_tip_id,
                "fork_point": fork_point,
                "height": new_tip_entry.header.height,
                "is_reorg": !is_extension,
                "disconnect_count": disconnect_count,
                "connect_count": connect_count,
            }),
            true,
        )
        .await?;

        Ok(())
    }
}
