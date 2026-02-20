use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

use crate::domain::entities::{BlockIndexEntry, ChainTip, NodeSendCommand, Reward};
use crate::domain::repositories::{DomainRepoError, EventRepository};
use config::EngineCommand;
use types::{HeapEntry, MempoolTx, NodeMiningState, NodeTip};

mod chain;
mod config;
mod delivery;
mod events;
mod mining;
mod network;
mod runtime;
mod seed;
mod types;
pub use config::{MinerNodeProfile, MiningNetworkConfig, PeerEdgeProfile};

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

#[derive(Clone)]
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

    pub fn upsert_peer_edge(&self, edge: PeerEdgeProfile) -> Result<(), DomainRepoError> {
        self.command_tx
            .send(EngineCommand::UpsertPeerEdge(edge))
            .map_err(|e| DomainRepoError(format!("engine inbox closed: {e}")))
    }

    pub async fn status(&self) -> EngineStatus {
        self.status.read().await.clone()
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
    let peer_latencies_ms = network_config.peer_latencies_ms.clone();
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
        peer_latencies_ms,
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
    peer_latencies_ms: HashMap<(String, String), u64>,

    // Phase 3: Consensus & Chain State
    block_index: HashMap<String, BlockIndexEntry>,
    active_tip: Option<ChainTip>,
    rewards: HashMap<String, Reward>, // reward_id -> Reward
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
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
            peer_latencies_ms: HashMap::new(),
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
