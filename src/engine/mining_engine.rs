use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

use crate::domain::entities::{
    Block, BlockHeader, BlockIndexEntry, BlockStatus, ChainTip, EventEnvelope, EventType,
    NodeSendCommand, Reward, RewardStatus, ScheduledDelivery,
};
use crate::domain::repositories::{DomainRepoError, EventRepository, RewardSettlementWrite};
use crate::domain::services::consensus::ConsensusService;
use crate::helper::deterministic_rng::{hash_str64, splitmix64, xorshift64};

const REWARD_MATURITY_CONFIRMATIONS: i64 = 10;

#[derive(Debug, Clone, Serialize)]
pub struct EngineStatus {
    pub running: bool,
    pub current_seq_id: i64,
    pub sim_time_ms: i64,
    pub tick_interval_ms: u64,
    pub pending_inbox: usize,
    pub pending_deliveries: usize,
    pub mempool_size: usize,
    pub online_nodes: usize,
    pub last_block_found_id: Option<String>,
    pub last_block_height: i64,
}

impl Default for EngineStatus {
    fn default() -> Self {
        Self {
            running: false,
            current_seq_id: 0,
            sim_time_ms: 0,
            tick_interval_ms: 100,
            pending_inbox: 0,
            pending_deliveries: 0,
            mempool_size: 0,
            online_nodes: 0,
            last_block_found_id: None,
            last_block_height: 0,
        }
    }
}

pub struct EngineHandle {
    command_tx: mpsc::UnboundedSender<EngineCommand>,
    status: Arc<RwLock<EngineStatus>>,
}

impl EngineHandle {
    pub fn enqueue_send(&self, cmd: NodeSendCommand) -> Result<(), DomainRepoError> {
        self.command_tx
            .send(EngineCommand::Send(cmd))
            .map_err(|e| DomainRepoError(format!("engine inbox closed: {e}")))
    }

    pub fn upsert_miner_node(&self, node: MinerNodeProfile) -> Result<(), DomainRepoError> {
        self.command_tx
            .send(EngineCommand::UpsertMinerNode(node))
            .map_err(|e| DomainRepoError(format!("engine inbox closed: {e}")))
    }

    pub async fn status(&self) -> EngineStatus {
        self.status.read().await.clone()
    }
}

#[derive(Debug, Clone)]
enum EngineCommand {
    Send(NodeSendCommand),
    UpsertMinerNode(MinerNodeProfile),
}

#[derive(Debug, Clone)]
pub struct MiningNetworkConfig {
    pub enabled: bool,
    pub tx_spawn_chance_per_thousand: u16,
    pub max_block_weight: i32,
    pub target_block_time_ms: u64,
    pub propagation_base_delay_ms: u64,
    pub propagation_jitter_ms: u64,
    pub miner_nodes: Vec<MinerNodeProfile>,
}

impl Default for MiningNetworkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tx_spawn_chance_per_thousand: 0,
            max_block_weight: 4000,
            target_block_time_ms: 15_000,
            propagation_base_delay_ms: 10,
            propagation_jitter_ms: 40,
            miner_nodes: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct MinerNodeProfile {
    pub node_id: String,
    pub hash_rate: u64,
    pub is_online: bool,
    pub has_relay: bool,
}

impl MinerNodeProfile {
    pub fn new(node_id: impl Into<String>, hash_rate: u64) -> Self {
        Self {
            node_id: node_id.into(),
            hash_rate,
            is_online: true,
            has_relay: false,
        }
    }
}

pub async fn start_deterministic_engine<R: EventRepository + 'static>(
    repo: Arc<R>,
    tick_interval_ms: u64,
    base_seed: u64,
    max_inbox_drain_per_tick: usize,
) -> Result<EngineHandle, DomainRepoError> {
    start_deterministic_engine_with_config(
        repo,
        tick_interval_ms,
        base_seed,
        max_inbox_drain_per_tick,
        MiningNetworkConfig::default(),
    )
    .await
}

pub async fn start_deterministic_engine_with_config<R: EventRepository + 'static>(
    repo: Arc<R>,
    tick_interval_ms: u64,
    base_seed: u64,
    max_inbox_drain_per_tick: usize,
    network_config: MiningNetworkConfig,
) -> Result<EngineHandle, DomainRepoError> {
    let initial_seq_id = repo.last_seq_id().await?.unwrap_or(0);
    let (command_tx, command_rx) = mpsc::unbounded_channel::<EngineCommand>();
    let status = Arc::new(RwLock::new(EngineStatus {
        running: true,
        current_seq_id: initial_seq_id,
        sim_time_ms: initial_seq_id.saturating_mul(tick_interval_ms as i64),
        tick_interval_ms,
        pending_inbox: 0,
        pending_deliveries: 0,
        mempool_size: 0,
        online_nodes: network_config
            .miner_nodes
            .iter()
            .filter(|n| n.is_online)
            .count(),
        last_block_found_id: None,
        last_block_height: 0,
    }));

    let nodes = network_config
        .miner_nodes
        .iter()
        .map(|node| (node.node_id.clone(), node.clone()))
        .collect::<HashMap<_, _>>();

    let mut core = DeterministicEngineCore {
        repo,
        current_seq_id: initial_seq_id,
        sim_time_ms: initial_seq_id.saturating_mul(tick_interval_ms as i64),
        tick_interval_ms,
        base_seed,
        schedule_counter: 0,
        command_rx,
        delivery_heap: BinaryHeap::new(),
        status: Arc::clone(&status),
        max_inbox_drain_per_tick,
        network: network_config,
        tx_counter: 0,
        mempool: HashMap::new(),
        nodes,
        mining_state: HashMap::new(),
        node_tips: HashMap::new(),
        block_index: HashMap::new(),
        active_tip: None,
        rewards: HashMap::new(),
    };
    core.initialize_network();
    core.bootstrap_from_repo().await?;

    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(tick_interval_ms));
        loop {
            ticker.tick().await;
            if let Err(err) = core.run_single_tick().await {
                log::error!("engine tick failed: {err}");
            }
        }
    });

    Ok(EngineHandle { command_tx, status })
}

struct DeterministicEngineCore<R: EventRepository> {
    repo: Arc<R>,
    current_seq_id: i64,
    sim_time_ms: i64,
    tick_interval_ms: u64,
    base_seed: u64,
    schedule_counter: u64,
    command_rx: mpsc::UnboundedReceiver<EngineCommand>,
    delivery_heap: BinaryHeap<Reverse<HeapEntry>>,
    status: Arc<RwLock<EngineStatus>>,
    max_inbox_drain_per_tick: usize,
    network: MiningNetworkConfig,
    tx_counter: u64,
    mempool: HashMap<String, MempoolTx>,
    nodes: HashMap<String, MinerNodeProfile>,
    mining_state: HashMap<String, NodeMiningState>,
    node_tips: HashMap<String, NodeTip>,
    
    // Phase 3: Consensus & Chain State
    block_index: HashMap<String, BlockIndexEntry>,
    active_tip: Option<ChainTip>,
    rewards: HashMap<String, Reward>, // reward_id -> Reward
}

#[derive(Debug, Clone)]
struct HeapEntry {
    delivery: ScheduledDelivery,
}

#[derive(Debug, Clone)]
struct MempoolTx {
    tx_id: String,
    fee_sats: i64,
    weight: i32,
    created_at_ms: i64,
    origin_node: String,
}

#[derive(Debug, Clone)]
struct NodeTip {
    block_id: String,
    height: i64,
}

#[derive(Debug, Clone)]
struct BlockTemplate {
    tx_ids: Vec<String>,
    total_weight: i32,
    total_fees_sats: i64,
}

#[derive(Debug, Clone)]
struct NodeMiningState {
    nonce_counter: u64,
}

#[derive(Debug, Clone)]
struct MiningHit {
    node_id: String,
    nonce: u64,
    hash: u64,
    trial_offset: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.delivery.key() == other.delivery.key()
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delivery.key().cmp(&other.delivery.key())
    }
}

impl<R: EventRepository> DeterministicEngineCore<R> {
    fn initialize_network(&mut self) {
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

    async fn run_single_tick(&mut self) -> Result<(), DomainRepoError> {
        self.sim_time_ms = self.sim_time_ms.saturating_add(self.tick_interval_ms as i64);

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
                        EngineCommand::Send(send) => self.schedule_delivery(send),
                        EngineCommand::UpsertMinerNode(node) => {
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

    fn schedule_delivery(&mut self, cmd: NodeSendCommand) {
        self.schedule_counter = self.schedule_counter.saturating_add(1);
        let latency_ms = cmd
            .latency_override_ms
            .unwrap_or_else(|| self.sample_latency_ms(&cmd, self.schedule_counter));
        let deliver_at_ms = self.sim_time_ms.saturating_add(latency_ms as i64);
        let msg_id = format!(
            "{}:{}:{}:{}",
            cmd.from,
            cmd.to,
            cmd.kind.as_str(),
            self.schedule_counter
        );

        let delivery = ScheduledDelivery {
            deliver_at_ms,
            from: cmd.from,
            to: cmd.to,
            kind: cmd.kind,
            payload: cmd.payload,
            msg_id,
            schedule_counter: self.schedule_counter,
        };
        self.delivery_heap.push(Reverse(HeapEntry { delivery }));
    }

    fn schedule_delivery_internal(
        &mut self,
        from: &str,
        to: &str,
        kind: crate::domain::entities::MessageKind,
        payload: serde_json::Value,
    ) {
        let latency_override_ms = Some(self.sample_propagation_delay_ms(from, to, &kind));
        self.schedule_delivery(NodeSendCommand {
            from: from.to_string(),
            to: to.to_string(),
            kind,
            payload,
            latency_override_ms,
        });
    }

    fn sample_latency_ms(&self, cmd: &NodeSendCommand, schedule_counter: u64) -> u64 {
        let seed = stable_seed_for_command(self.base_seed, cmd, schedule_counter);
        let jitter = xorshift64(seed) % 41;
        10 + jitter
    }

    async fn emit_net_delivery_event(
        &mut self,
        delivery: ScheduledDelivery,
    ) -> Result<(), DomainRepoError> {
        self.emit_event(
            EventType::NET_DELIVER_V1,
            json!({
                "deliver_at_ms": delivery.deliver_at_ms,
                "from": delivery.from,
                "to": delivery.to,
                "kind": delivery.kind.as_str(),
                "msg_id": delivery.msg_id,
                "payload": delivery.payload,
            }),
            false,
        )
        .await?;
        self.handle_delivery(&delivery).await
    }

    async fn emit_event(
        &mut self,
        event_type: &str,
        payload: serde_json::Value,
        persist_to_db: bool,
    ) -> Result<(), DomainRepoError> {
        let next_seq = self.current_seq_id.saturating_add(1);
        let envelope = EventEnvelope {
            seq_id: next_seq,
            server_time_ms: self.sim_time_ms,
            event_type: event_type.to_string(),
            payload,
        };
        if persist_to_db {
            self.repo.append_event(&envelope).await?;
        }
        self.current_seq_id = next_seq;
        Ok(())
    }

    fn initialize_genesis(&mut self) {
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
        
        self.block_index.insert("genesis".to_string(), genesis_entry);
        self.active_tip = Some(ChainTip {
            block_id: "genesis".to_string(),
            height: 0,
            cumulative_work: 1,
        });
        
        log::info!("Genesis block initialized");
    }

    async fn bootstrap_from_repo(&mut self) -> Result<(), DomainRepoError> {
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

    async fn update_status(&self) {
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

    async fn run_network_tick(&mut self) -> Result<(), DomainRepoError> {
        self.try_spawn_tx().await?;
        self.try_mine_block().await
    }

    async fn apply_upsert_node(&mut self, node: MinerNodeProfile) -> Result<(), DomainRepoError> {
        let node_id = node.node_id.clone();
        self.nodes.insert(node_id.clone(), node.clone());
        self.mining_state
            .entry(node_id.clone())
            .or_insert(NodeMiningState { nonce_counter: 0 });
        self.node_tips.entry(node_id.clone()).or_insert_with(|| NodeTip {
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

    async fn try_spawn_tx(&mut self) -> Result<(), DomainRepoError> {
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
        self.broadcast_to_online_nodes(
            &tx.origin_node,
            crate::domain::entities::MessageKind::Tx,
            tx_payload,
        );
        Ok(())
    }

    async fn try_mine_block(&mut self) -> Result<(), DomainRepoError> {
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
            crate::domain::entities::MessageKind::Block,
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

    async fn connect_block(&mut self, block: &Block) -> Result<(), DomainRepoError> {
        // 1. Remove txs from mempool
        for tx_id in &block.tx_ids {
            self.mempool.remove(tx_id);
        }

        // 2. Create Reward (Immature)
        let reward_id = format!("reward-{}", block.header.block_id);
        let reward = Reward {
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
                self.reward_confirmations(&reward.block_hash) >= REWARD_MATURITY_CONFIRMATIONS as i32
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

    async fn disconnect_block(&mut self, block: &Block) -> Result<(), DomainRepoError> {
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

    fn canonical_tip_height(&self) -> i64 {
        self.block_index
            .values()
            .filter(|entry| matches!(entry.status, BlockStatus::Active))
            .map(|entry| entry.header.height)
            .max()
            .unwrap_or(0)
    }

    fn reward_confirmations(&self, block_hash: &str) -> i32 {
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

    fn build_reward_settlement_writes(&self) -> Vec<RewardSettlementWrite> {
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

    fn reconcile_rewards_after_bootstrap(&mut self) {
        let tip_height = self.canonical_tip_height();
        let reward_ids = self.rewards.keys().cloned().collect::<Vec<_>>();
        for reward_id in reward_ids {
            let Some(block_hash) = self.rewards.get(&reward_id).map(|r| r.block_hash.clone()) else {
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
                && confirmations >= REWARD_MATURITY_CONFIRMATIONS as i32
            {
                reward.status = RewardStatus::Confirmed;
                reward.matured_at_height = Some(tip_height);
                if reward.settled_at_ms.is_none() {
                    reward.settled_at_ms = Some(self.sim_time_ms);
                }
            }
        }
    }

    async fn persist_all_reward_settlements(&self) -> Result<(), DomainRepoError> {
        let reward_writes = self.build_reward_settlement_writes();
        for write in reward_writes {
            self.repo
                .upsert_reward_settlement(&write.reward, write.confirmations)
                .await?;
        }
        Ok(())
    }

    async fn persist_block_and_all_reward_settlements(
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

    fn build_spawned_tx(&self, tx_counter: u64) -> MempoolTx {
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

    fn insert_mempool_tx(&mut self, tx: MempoolTx) -> bool {
        if self.mempool.contains_key(&tx.tx_id) {
            return false;
        }
        self.mempool.insert(tx.tx_id.clone(), tx);
        true
    }

    fn build_block_template(&self) -> Option<BlockTemplate> {
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

    fn find_mining_hit(&mut self, parent_hash: &str, target: u64) -> Option<MiningHit> {
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

    fn trials_for_tick(&self, hash_rate: u64) -> u64 {
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

    fn current_mining_target(&self, difficulty: u64) -> u64 {
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

    fn total_online_trials_per_tick(&self) -> u64 {
        self.nodes
            .values()
            .filter(|n| n.is_online && n.hash_rate > 0)
            .map(|n| self.trials_for_tick(n.hash_rate))
            .fold(0u64, |acc, v| acc.saturating_add(v))
    }

    fn mini_pow_hash(&self, node_id: &str, parent_hash: &str, nonce: u64, trial_offset: u64) -> u64 {
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

    fn broadcast_to_online_nodes(
        &mut self,
        from: &str,
        kind: crate::domain::entities::MessageKind,
        payload: serde_json::Value,
    ) {
        let mut recipients = self
            .nodes
            .values()
            .filter(|n| n.is_online && n.node_id != from)
            .map(|n| n.node_id.clone())
            .collect::<Vec<_>>();
        recipients.sort();

        for to in recipients {
            self.schedule_delivery_internal(from, &to, kind.clone(), payload.clone());
        }
    }

    async fn handle_delivery(
        &mut self,
        delivery: &ScheduledDelivery,
    ) -> Result<(), DomainRepoError> {
        match delivery.kind {
            crate::domain::entities::MessageKind::Tx => {
                let tx_id = delivery
                    .payload
                    .get("tx_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if !tx_id.is_empty() {
                    let tx = MempoolTx {
                        tx_id: tx_id.clone(),
                        fee_sats: delivery
                            .payload
                            .get("fee_sats")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        weight: delivery
                            .payload
                            .get("weight")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32,
                        created_at_ms: delivery
                            .payload
                            .get("created_at_ms")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(self.sim_time_ms),
                        origin_node: delivery.from.clone(),
                    };
                    let inserted = self.insert_mempool_tx(tx);
                    if inserted {
                        self.emit_event(
                            "node.tx_received.v1",
                            json!({
                                "node": delivery.to,
                                "from": delivery.from,
                                "tx_id": tx_id,
                            }),
                            false,
                        )
                        .await?;
                    }
                }
            }
            crate::domain::entities::MessageKind::Block => {
                let block_id = delivery
                    .payload
                    .get("block_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let height = delivery
                    .payload
                    .get("height")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let parent_hash = delivery
                    .payload
                    .get("parent_hash")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let miner_node_id = delivery
                    .payload
                    .get("miner_node_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let difficulty = delivery
                    .payload
                    .get("difficulty")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1);
                let nonce = delivery
                    .payload
                    .get("nonce")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let tx_ids = delivery
                    .payload
                    .get("tx_ids")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(ToString::to_string))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                
                // Reconstruct block from payload (simplified for simulation)
                let header = BlockHeader {
                    block_id: block_id.clone(),
                    height,
                    parent_hash,
                    miner_node_id,
                    timestamp_ms: self.sim_time_ms, // Approximation
                    difficulty,
                    nonce,
                    total_weight: 0,
                    total_fees_sats: 0,
                };
                let block = Block { header, tx_ids };

                self.process_block(block).await?;

                self.emit_event(
                    "node.block_received.v1",
                    json!({
                        "node": delivery.to,
                        "from": delivery.from,
                        "block_id": block_id,
                        "height": height,
                    }),
                    false,
                )
                .await?;
                self.update_tip(
                    &delivery.to,
                    NodeTip {
                        block_id,
                        height,
                    },
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn update_tip(&mut self, node_id: &str, next_tip: NodeTip) -> Result<(), DomainRepoError> {
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

    async fn process_block(&mut self, block: Block) -> Result<(), DomainRepoError> {
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
        self.block_index.insert(block.header.block_id.clone(), entry.clone());
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

        if ConsensusService::is_better_chain(&block.header, &current_tip_header, &self.block_index) {
            self.reorganize(&current_tip_header.block_id, &block.header.block_id).await?;
        }

        Ok(())
    }

    async fn reorganize(&mut self, old_tip_id: &str, new_tip_id: &str) -> Result<(), DomainRepoError> {
        let fork_point = ConsensusService::find_fork_point(old_tip_id, new_tip_id, &self.block_index)
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
            // Reconstruct block for disconnect logic
            // We need to clone data to avoid borrow conflict
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
             // Reconstruct block for connect logic
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
        ).await?;

        Ok(())
    }


    fn sample_propagation_delay_ms(
        &self,
        from: &str,
        to: &str,
        kind: &crate::domain::entities::MessageKind,
    ) -> u64 {
        let counter = self.schedule_counter.saturating_add(1);
        let seed = stable_seed_for_command(
            self.base_seed,
            &NodeSendCommand {
                from: from.to_string(),
                to: to.to_string(),
                kind: kind.clone(),
                payload: json!({}),
                latency_override_ms: None,
            },
            counter,
        );
        let jitter = if self.network.propagation_jitter_ms == 0 {
            0
        } else {
            xorshift64(seed) % (self.network.propagation_jitter_ms + 1)
        };
        self.network
            .propagation_base_delay_ms
            .saturating_add(jitter)
            .max(1)
    }

    fn total_online_hash_rate(&self) -> u64 {
        self.nodes
            .values()
            .filter(|n| n.is_online)
            .map(|n| n.hash_rate)
            .fold(0u64, |acc, v| acc.saturating_add(v))
    }

    fn sample_u64(&self, tag: &str, counter: u64) -> u64 {
        let mut seed = splitmix64(self.base_seed ^ counter ^ hash_str64(tag));
        seed = splitmix64(seed ^ (self.sim_time_ms as u64));
        xorshift64(seed)
    }
}

fn stable_seed_for_command(base_seed: u64, cmd: &NodeSendCommand, schedule_counter: u64) -> u64 {
    let mut seed = splitmix64(base_seed ^ schedule_counter);
    seed ^= hash_str64(&cmd.from);
    seed = splitmix64(seed ^ hash_str64(&cmd.to));
    seed = splitmix64(seed ^ (cmd.kind.rank() as u64));
    seed
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::time::{sleep, Duration, Instant};

    use crate::domain::entities::{EventEnvelope, MessageKind, NodeSendCommand};
    use crate::domain::repositories::{DomainRepoError, EventRepository};

    use super::{
        start_deterministic_engine, start_deterministic_engine_with_config, MinerNodeProfile,
        MiningNetworkConfig,
    };

    #[derive(Default)]
    struct MockEventRepository {
        events: Mutex<Vec<EventEnvelope>>,
    }

    #[async_trait]
    impl EventRepository for MockEventRepository {
        async fn append_event(&self, event: &EventEnvelope) -> Result<(), DomainRepoError> {
            let mut guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            if guard.iter().any(|e| e.seq_id == event.seq_id) {
                return Err(DomainRepoError("duplicate seq".to_string()));
            }
            guard.push(event.clone());
            Ok(())
        }

        async fn last_seq_id(&self) -> Result<Option<i64>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard.last().map(|e| e.seq_id))
        }

        async fn read_events_after(
            &self,
            after_seq_id: i64,
            limit: u64,
        ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard
                .iter()
                .filter(|e| e.seq_id > after_seq_id)
                .take(limit as usize)
                .cloned()
                .collect())
        }

        async fn read_events_range(
            &self,
            from_seq: i64,
            to_seq: i64,
        ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard
                .iter()
                .filter(|e| e.seq_id >= from_seq && e.seq_id <= to_seq)
                .cloned()
                .collect())
        }
    }

    fn snapshot_events(repo: &Arc<MockEventRepository>) -> Vec<EventEnvelope> {
        repo.events
            .lock()
            .expect("mock event repo lock must be available")
            .clone()
    }

    async fn wait_for_delivery_count(
        repo: &Arc<MockEventRepository>,
        expected_count: usize,
        timeout: Duration,
    ) {
        let deadline = Instant::now() + timeout;
        loop {
            let count = snapshot_events(repo)
                .into_iter()
                .filter(|e| e.event_type == "net.deliver.v1")
                .count();
            if count >= expected_count || Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn latency_ordering_prefers_earlier_delivery() {
        let repo = Arc::new(MockEventRepository::default());
        let engine = start_deterministic_engine(Arc::clone(&repo), 5, 42, 1024)
            .await
            .expect("engine should start");

        engine
            .enqueue_send(NodeSendCommand {
                from: "node-a".to_string(),
                to: "node-b".to_string(),
                kind: MessageKind::Tx,
                payload: json!({"name":"first"}),
                latency_override_ms: Some(10),
            })
            .expect("enqueue should succeed");
        engine
            .enqueue_send(NodeSendCommand {
                from: "node-a".to_string(),
                to: "node-c".to_string(),
                kind: MessageKind::Tx,
                payload: json!({"name":"second"}),
                latency_override_ms: Some(20),
            })
            .expect("enqueue should succeed");

        wait_for_delivery_count(&repo, 2, Duration::from_millis(500)).await;

        let events = snapshot_events(&repo);
        let delivers: Vec<_> = events
            .into_iter()
            .filter(|e| e.event_type == "net.deliver.v1")
            .collect();
        assert!(delivers.len() >= 2);

        let first_to = delivers[0]
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let second_to = delivers[1]
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(first_to, "node-b");
        assert_eq!(second_to, "node-c");
    }

    #[tokio::test]
    async fn tie_break_is_stable_across_runs() {
        let mut baseline: Option<Vec<String>> = None;
        for _ in 0..10 {
            let repo = Arc::new(MockEventRepository::default());
            let engine = start_deterministic_engine(Arc::clone(&repo), 5, 7, 1024)
                .await
                .expect("engine should start");

            let sends = vec![
                ("node-a", "node-z", MessageKind::Ping),
                ("node-a", "node-b", MessageKind::Tx),
                ("node-a", "node-c", MessageKind::Inv),
                ("node-a", "node-d", MessageKind::Block),
            ];
            for (from, to, kind) in sends {
                engine
                    .enqueue_send(NodeSendCommand {
                        from: from.to_string(),
                        to: to.to_string(),
                        kind,
                        payload: json!({"batch":"same-time"}),
                        latency_override_ms: Some(15),
                    })
                    .expect("enqueue should succeed");
            }

            wait_for_delivery_count(&repo, 4, Duration::from_millis(500)).await;

            let events = snapshot_events(&repo);
            let order: Vec<String> = events
                .into_iter()
                .filter(|e| e.event_type == "net.deliver.v1")
                .map(|e| {
                    format!(
                        "{}:{}",
                        e.payload
                            .get("kind")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default(),
                        e.payload
                            .get("to")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                    )
                })
                .collect();

            if let Some(expected) = &baseline {
                assert_eq!(expected, &order);
            } else {
                baseline = Some(order);
            }
        }
    }

    #[tokio::test]
    async fn same_seed_and_commands_produce_same_event_stream() {
        async fn run_once() -> Vec<(i64, String, String)> {
            let repo = Arc::new(MockEventRepository::default());
            let engine = start_deterministic_engine(Arc::clone(&repo), 5, 9999, 1024)
                .await
                .expect("engine should start");

            for idx in 0..4 {
                engine
                    .enqueue_send(NodeSendCommand {
                        from: "node-main".to_string(),
                        to: format!("node-{idx}"),
                        kind: MessageKind::Tx,
                        payload: json!({"idx": idx}),
                        latency_override_ms: None,
                    })
                    .expect("enqueue should succeed");
            }

            wait_for_delivery_count(&repo, 4, Duration::from_millis(500)).await;

            snapshot_events(&repo)
                .into_iter()
                .map(|e| (e.seq_id, e.event_type, e.payload.to_string()))
                .collect()
        }

        let left = run_once().await;
        let right = run_once().await;
        assert_eq!(left, right);
    }

    #[tokio::test]
    async fn mining_network_generates_block_flow_when_enabled() {
        let repo = Arc::new(MockEventRepository::default());
        let cfg = MiningNetworkConfig {
            enabled: true,
            tx_spawn_chance_per_thousand: 1000,
            max_block_weight: 1000,
            target_block_time_ms: 20,
            propagation_base_delay_ms: 1,
            propagation_jitter_ms: 2,
            miner_nodes: vec![
                MinerNodeProfile::new("node-1", 100),
                MinerNodeProfile::new("node-2", 200),
            ],
        };
        let _engine = start_deterministic_engine_with_config(Arc::clone(&repo), 5, 99, 1024, cfg)
            .await
            .expect("engine should start");

        sleep(Duration::from_millis(250)).await;
        let events = snapshot_events(&repo);
        assert!(
            events.iter().any(|e| e.event_type == "mempool.tx_added.v1"),
            "must generate mempool tx events"
        );
        assert!(
            events.iter().any(|e| e.event_type == "pow.block_found.v1"),
            "must generate block found events"
        );
        assert!(
            events.iter().any(|e| e.event_type == "node.tip_updated.v1"),
            "must update node tips"
        );
    }
}
