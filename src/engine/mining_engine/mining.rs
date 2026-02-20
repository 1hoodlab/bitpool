use serde_json::json;

use crate::domain::entities::{Block, BlockHeader, MessageKind};
use crate::domain::repositories::{DomainRepoError, EventRepository};
use crate::engine::consensus::ConsensusService;
use crate::helper::deterministic_rng::{hash_str64, splitmix64};

use super::types::{BlockTemplate, MempoolTx, MiningHit, NodeMiningState, NodeTip};
use super::DeterministicEngineCore;

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) async fn try_spawn_tx(&mut self) -> Result<(), DomainRepoError> {
        if self.network.tx_spawn_chance_per_thousand == 0 {
            return Ok(());
        }
        let spawn_roll = self.sample_u64("tx_spawn", self.tx_counter.saturating_add(1)) % 1000;
        if spawn_roll >= self.network.tx_spawn_chance_per_thousand as u64 {
            return Ok(());
        }
        self.tx_counter = self.tx_counter.saturating_add(1);
        let tx = self.build_spawned_tx(self.tx_counter);
        let tx_payload = json!({
            "tx_id": tx.tx_id,
            "fee_sats": tx.fee_sats,
            "weight": tx.weight,
            "created_at_ms": tx.created_at_ms,
            "origin_node": tx.origin_node,
        });

        let inserted = self.insert_mempool_tx(tx.clone());
        if !inserted {
            return Ok(());
        }
        self.emit_event("mempool.tx_added.v1", tx_payload.clone(), true)
            .await?;
        self.broadcast_to_online_nodes(&tx.origin_node, MessageKind::Tx, tx_payload);
        Ok(())
    }

    pub(super) async fn try_mine_block(&mut self) -> Result<(), DomainRepoError> {
        let Some(template) = self.build_block_template() else {
            return Ok(());
        };
        self.emit_event(
            "pow.template_built.v1",
            json!({
                "sim_time_ms": self.sim_time_ms,
                "tx_ids": template.tx_ids,
                "total_weight": template.total_weight,
                "total_fees_sats": template.total_fees_sats,
            }),
            false,
        )
        .await?;

        // Get parent from active tip.
        let (parent_hash, parent_height) = match &self.active_tip {
            Some(tip) => (tip.block_id.clone(), tip.height),
            None => ("genesis".to_string(), 0),
        };
        let parent_header = self
            .block_index
            .get(&parent_hash)
            .map(|e| &e.header)
            .ok_or(DomainRepoError("parent header not found".to_string()))?;

        // Calculate next difficulty using the current active chain.
        let block_height = parent_height + 1;
        let difficulty = ConsensusService::calculate_next_difficulty(
            parent_header,
            &self.block_index,
            self.network.target_block_time_ms,
            10,
        );
        let target = self.current_mining_target(difficulty);

        let Some(hit) = self.find_mining_hit(&parent_hash, target) else {
            return Ok(());
        };

        let winner = hit.node_id;
        let block_id = format!(
            "blk-{}-{}",
            block_height,
            self.current_seq_id.saturating_add(1)
        );
        let header = BlockHeader {
            block_id: block_id.clone(),
            height: block_height,
            parent_hash: parent_hash.clone(),
            miner_node_id: winner.clone(),
            timestamp_ms: self.sim_time_ms,
            difficulty,
            nonce: hit.nonce,
            total_weight: template.total_weight,
            total_fees_sats: template.total_fees_sats,
        };

        let block = Block {
            header: header.clone(),
            tx_ids: template.tx_ids.clone(),
        };

        // Process locally first
        self.process_block(block.clone()).await?;

        for tx_id in &template.tx_ids {
            self.mempool.remove(tx_id);
        }

        self.emit_event(
            "pow.block_found.v1",
            json!({
                "block_id": block_id,
                "miner_node_id": winner,
                "height_candidate": block_height,
                "template_tx_ids": template.tx_ids,
                "sim_time_ms": self.sim_time_ms,
                "target": target,
                "winning_hash": hit.hash,
                "hit_trial_offset": hit.trial_offset,
            }),
            true,
        )
        .await?;

        self.update_tip(
            &winner,
            NodeTip {
                block_id: block_id.clone(),
                height: block_height,
            },
        )
        .await?;

        self.broadcast_to_online_nodes(
            &winner,
            MessageKind::Block,
            json!({
                "block_id": block_id,
                "height": block_height,
                "miner_node_id": winner,
                "parent_hash": parent_hash,
                "tx_ids": template.tx_ids,
                "nonce": hit.nonce,
                "difficulty": difficulty,
            }),
        );
        Ok(())
    }

    pub(super) fn build_spawned_tx(&self, tx_counter: u64) -> MempoolTx {
        let tx_id = format!("tx-{}-{}", self.sim_time_ms, tx_counter);
        let fee_sats = 100 + (self.sample_u64("tx_fee", tx_counter) % 10_000) as i64;
        let weight = 100 + (self.sample_u64("tx_weight", tx_counter) % 900) as i32;
        let origin_node = self
            .nodes
            .keys()
            .min()
            .cloned()
            .unwrap_or_else(|| "node-1".to_string());

        MempoolTx {
            tx_id,
            fee_sats,
            weight,
            created_at_ms: self.sim_time_ms,
            origin_node,
        }
    }

    pub(super) fn insert_mempool_tx(&mut self, tx: MempoolTx) -> bool {
        if self.mempool.contains_key(&tx.tx_id) {
            return false;
        }
        self.mempool.insert(tx.tx_id.clone(), tx);
        true
    }

    pub(super) fn build_block_template(&self) -> Option<BlockTemplate> {
        if self.mempool.is_empty() {
            return None;
        }
        let mut candidates = self.mempool.values().cloned().collect::<Vec<_>>();
        candidates.sort_by(|a, b| {
            let lhs = (a.fee_sats as i128) * (b.weight as i128);
            let rhs = (b.fee_sats as i128) * (a.weight as i128);
            rhs.cmp(&lhs)
                .then_with(|| a.created_at_ms.cmp(&b.created_at_ms))
                .then_with(|| a.tx_id.cmp(&b.tx_id))
        });

        let mut total_weight = 0i32;
        let mut total_fees_sats = 0i64;
        let mut tx_ids = Vec::new();
        for tx in candidates {
            if total_weight.saturating_add(tx.weight) > self.network.max_block_weight {
                continue;
            }
            total_weight = total_weight.saturating_add(tx.weight);
            total_fees_sats = total_fees_sats.saturating_add(tx.fee_sats);
            tx_ids.push(tx.tx_id);
        }
        if tx_ids.is_empty() {
            return None;
        }
        Some(BlockTemplate {
            tx_ids,
            total_weight,
            total_fees_sats,
        })
    }

    pub(super) fn find_mining_hit(&mut self, parent_hash: &str, target: u64) -> Option<MiningHit> {
        let mut node_ids = self
            .nodes
            .values()
            .filter(|n| n.is_online && n.hash_rate > 0)
            .map(|n| n.node_id.clone())
            .collect::<Vec<_>>();
        node_ids.sort();

        let mut best_hit: Option<MiningHit> = None;
        for node_id in node_ids {
            let hash_rate = self
                .nodes
                .get(&node_id)
                .map(|n| n.hash_rate)
                .unwrap_or_default();
            let trials = self.trials_for_tick(hash_rate);
            if trials == 0 {
                continue;
            }

            let mut nonce_counter = self
                .mining_state
                .get(&node_id)
                .map(|s| s.nonce_counter)
                .unwrap_or(0);

            for local_offset in 0..trials {
                let nonce = nonce_counter;
                nonce_counter = nonce_counter.saturating_add(1);

                let hash = self.mini_pow_hash(&node_id, parent_hash, nonce, local_offset);
                if hash >= target {
                    continue;
                }

                let candidate = MiningHit {
                    node_id: node_id.clone(),
                    nonce,
                    hash,
                    trial_offset: local_offset,
                };
                let replace = match &best_hit {
                    None => true,
                    Some(current) => {
                        (candidate.trial_offset, &candidate.node_id, candidate.nonce)
                            < (current.trial_offset, &current.node_id, current.nonce)
                    }
                };
                if replace {
                    best_hit = Some(candidate);
                }
            }

            self.mining_state
                .insert(node_id.clone(), NodeMiningState { nonce_counter });
        }
        best_hit
    }

    pub(super) fn trials_for_tick(&self, hash_rate: u64) -> u64 {
        // hash_rate-to-trials conversion keeps the loop cheap and deterministic.
        const TRIAL_RATE_SCALE: u64 = 10;
        const MAX_TRIALS_PER_TICK_PER_NODE: u64 = 100_000;
        if hash_rate == 0 {
            return 0;
        }
        let trials = hash_rate
            .saturating_mul(self.tick_interval_ms.max(1))
            .saturating_div(TRIAL_RATE_SCALE);
        trials.max(1).min(MAX_TRIALS_PER_TICK_PER_NODE)
    }

    pub(super) fn current_mining_target(&self, difficulty: u64) -> u64 {
        // Compute target from effective (capped) trials, not raw hash rate.
        // This keeps expected block time stable even when per-node trial caps are active.
        let total_trials_per_tick = self.total_online_trials_per_tick().max(1);
        let expected_trials = total_trials_per_tick
            .saturating_mul(self.network.target_block_time_ms.max(1))
            .saturating_div(self.tick_interval_ms.max(1))
            .max(1);
        let effective_trials = expected_trials.saturating_mul(difficulty.max(1));
        (u64::MAX / effective_trials.max(1)).max(1)
    }

    pub(super) fn total_online_trials_per_tick(&self) -> u64 {
        self.nodes
            .values()
            .filter(|n| n.is_online && n.hash_rate > 0)
            .map(|n| self.trials_for_tick(n.hash_rate))
            .fold(0u64, |acc, v| acc.saturating_add(v))
    }

    pub(super) fn mini_pow_hash(
        &self,
        node_id: &str,
        parent_hash: &str,
        nonce: u64,
        trial_offset: u64,
    ) -> u64 {
        // Mix each component in sequence to avoid xor-cancellation patterns
        // that can bias low-hit probability in large deterministic loops.
        let mut seed = splitmix64(self.base_seed ^ hash_str64(node_id));
        seed = splitmix64(seed ^ hash_str64(parent_hash));
        seed = splitmix64(seed ^ nonce);
        seed = splitmix64(seed ^ trial_offset);
        seed = splitmix64(seed ^ (self.sim_time_ms as u64));
        seed = splitmix64(seed ^ self.schedule_counter);
        splitmix64(seed)
    }
}
