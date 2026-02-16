use std::cmp::Reverse;

use serde_json::json;
use tokio::sync::mpsc;

use crate::domain::entities::{BlockHeader, BlockIndexEntry, BlockStatus, ChainTip, EventType};
use crate::domain::repositories::{DomainRepoError, EventRepository};
use crate::helper::deterministic_rng::{hash_str64, splitmix64, xorshift64};

use super::types::NodeTip;
use super::{DeterministicEngineCore, MinerNodeProfile, NodeMiningState};

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) fn initialize_network(&mut self) {
        if !self.network.enabled {
            return;
        }
        if self.total_online_hash_rate() == 0 {
            self.network.enabled = false;
            log::warn!("mining network disabled: no online miner nodes");
            return;
        }
        let mut node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        node_ids.sort();
        for node_id in node_ids {
            self.mining_state
                .entry(node_id)
                .or_insert(NodeMiningState { nonce_counter: 0 });
        }
    }

    pub(super) async fn run_single_tick(&mut self) -> Result<(), DomainRepoError> {
        self.sim_time_ms = self
            .sim_time_ms
            .saturating_add(self.tick_interval_ms as i64);

        self.emit_event(
            EventType::ENGINE_TICK_V1,
            json!({
                "tick_id": self.current_seq_id + 1,
                "sim_time_ms": self.sim_time_ms,
            }),
            false,
        )
        .await?;

        if self.network.enabled {
            self.run_network_tick().await?;
        }

        let mut drained = 0usize;
        while drained < self.max_inbox_drain_per_tick {
            match self.command_rx.try_recv() {
                Ok(cmd) => {
                    drained += 1;
                    match cmd {
                        super::config::EngineCommand::Send(send) => self.schedule_delivery(send),
                        super::config::EngineCommand::UpsertMinerNode(node) => {
                            self.apply_upsert_node(node).await?;
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        while let Some(Reverse(entry)) = self.delivery_heap.peek() {
            if entry.delivery.deliver_at_ms > self.sim_time_ms {
                break;
            }
            let Reverse(ready) = self
                .delivery_heap
                .pop()
                .expect("heap peek had value, pop must succeed");
            self.emit_net_delivery_event(ready.delivery).await?;
        }

        self.update_status().await;
        Ok(())
    }

    pub(super) fn initialize_genesis(&mut self) {
        if self.active_tip.is_some() {
            return;
        }

        // Create Genesis Block
        let genesis_header = BlockHeader {
            block_id: "genesis".to_string(),
            height: 0,
            parent_hash: "".to_string(),
            miner_node_id: "system".to_string(),
            timestamp_ms: self.sim_time_ms,
            difficulty: 1,
            nonce: 0,
            total_weight: 0,
            total_fees_sats: 0,
        };

        let genesis_entry = BlockIndexEntry {
            header: genesis_header.clone(),
            tx_ids: vec![],
            cumulative_work: 1, // Start with 1 work
            status: BlockStatus::Active,
        };

        self.block_index
            .insert("genesis".to_string(), genesis_entry);
        self.active_tip = Some(ChainTip {
            block_id: "genesis".to_string(),
            height: 0,
            cumulative_work: 1,
        });

        log::info!("Genesis block initialized");
    }

    pub(super) async fn bootstrap_from_repo(&mut self) -> Result<(), DomainRepoError> {
        // Always ensure synthetic genesis exists for parent traversal.
        self.initialize_genesis();

        let (entries, persisted_tip) = self.repo.load_chain_state().await?;
        for entry in entries {
            self.block_index
                .insert(entry.header.block_id.clone(), entry);
        }

        if let Some(tip) = persisted_tip {
            self.active_tip = Some(tip.clone());
            if let Some(tip_entry) = self.block_index.get(&tip.block_id) {
                self.sim_time_ms = self.sim_time_ms.max(tip_entry.header.timestamp_ms);
            }
            for node_id in self.nodes.keys() {
                self.node_tips.entry(node_id.clone()).or_insert(NodeTip {
                    block_id: tip.block_id.clone(),
                    height: tip.height,
                });
            }
            log::info!(
                "Engine chain bootstrap loaded: tip={} height={}",
                tip.block_id,
                tip.height
            );
        }

        let rewards = self.repo.load_reward_settlements().await?;
        for reward in rewards {
            self.rewards.insert(reward.id.clone(), reward);
        }
        self.reconcile_rewards_after_bootstrap();
        self.persist_all_reward_settlements().await?;

        Ok(())
    }

    pub(super) async fn update_status(&self) {
        let mut status = self.status.write().await;
        status.running = true;
        status.current_seq_id = self.current_seq_id;
        status.sim_time_ms = self.sim_time_ms;
        status.tick_interval_ms = self.tick_interval_ms;
        status.pending_inbox = self.command_rx.len();
        status.pending_deliveries = self.delivery_heap.len();
        status.mempool_size = self.mempool.len();
        status.online_nodes = self.nodes.values().filter(|n| n.is_online).count();

        if let Some(tip) = &self.active_tip {
            status.last_block_height = tip.height;
            status.last_block_found_id = Some(tip.block_id.clone());
        } else {
            status.last_block_height = 0;
            status.last_block_found_id = None;
        }
    }

    pub(super) async fn run_network_tick(&mut self) -> Result<(), DomainRepoError> {
        self.try_spawn_tx().await?;
        self.try_mine_block().await
    }

    pub(super) async fn apply_upsert_node(
        &mut self,
        node: MinerNodeProfile,
    ) -> Result<(), DomainRepoError> {
        let node_id = node.node_id.clone();
        self.nodes.insert(node_id.clone(), node.clone());
        self.mining_state
            .entry(node_id.clone())
            .or_insert(NodeMiningState { nonce_counter: 0 });
        self.node_tips
            .entry(node_id.clone())
            .or_insert_with(|| NodeTip {
                block_id: self
                    .active_tip
                    .as_ref()
                    .map(|t| t.block_id.clone())
                    .unwrap_or_else(|| "genesis".to_string()),
                height: self.active_tip.as_ref().map(|t| t.height).unwrap_or(0),
            });
        self.emit_event(
            "node.config_upserted.v1",
            json!({
                "node_id": node.node_id,
                "hash_rate": node.hash_rate,
                "is_online": node.is_online,
                "has_relay": node.has_relay,
            }),
            false,
        )
        .await?;

        // Auto-enable mining network when first online miner arrives at runtime.
        if !self.network.enabled && self.total_online_hash_rate() > 0 {
            self.network.enabled = true;
            self.emit_event(
                "engine.network_enabled.v1",
                json!({
                    "reason": "runtime_node_upsert",
                    "online_hash_rate": self.total_online_hash_rate(),
                }),
                false,
            )
            .await?;
        }

        Ok(())
    }

    pub(super) fn total_online_hash_rate(&self) -> u64 {
        self.nodes
            .values()
            .filter(|n| n.is_online)
            .map(|n| n.hash_rate)
            .fold(0u64, |acc, v| acc.saturating_add(v))
    }

    pub(super) fn sample_u64(&self, tag: &str, counter: u64) -> u64 {
        let mut seed = splitmix64(self.base_seed ^ counter ^ hash_str64(tag));
        seed = splitmix64(seed ^ (self.sim_time_ms as u64));
        xorshift64(seed)
    }
}
