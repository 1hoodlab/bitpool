use std::collections::HashMap;

use crate::domain::entities::NodeSendCommand;

#[derive(Debug, Clone)]
pub(super) enum EngineCommand {
    Send(NodeSendCommand),
    UpsertMinerNode(MinerNodeProfile),
    UpsertPeerEdge(PeerEdgeProfile),
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
    pub peer_latencies_ms: HashMap<(String, String), u64>,
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
            peer_latencies_ms: HashMap::new(),
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

#[derive(Debug, Clone)]
pub struct PeerEdgeProfile {
    pub from_node_id: String,
    pub to_node_id: String,
    pub latency_ms: u64,
}
