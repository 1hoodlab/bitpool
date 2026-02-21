use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreateNodeRequest {
    /// Format: nation-{nation_id}-{owner_id} or nation-{nation_id}
    pub node_id: String,
    pub hash_rate: u64,
    pub is_online: Option<bool>,
    pub has_relay: Option<bool>,
    pub version: u64,
    pub command_id: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNodeHashrateRequest {
    pub hash_rate: u64,
    pub version: u64,
    pub command_id: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNationPolicyRequest {
    pub policy_type: String,
    pub params: serde_json::Value,
    pub version: u64,
    pub command_id: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNationStatusRequest {
    pub status: String,
    pub reason: Option<String>,
    pub version: u64,
    pub command_id: String,
}

#[derive(Debug, Serialize)]
pub struct CommandAcceptedResponse {
    pub accepted: bool,
    pub command_id: String,
    pub applied: bool,
}

#[derive(Debug, Serialize)]
pub struct ChainTipResponse {
    pub block_id: Option<String>,
    pub height: i64,
}

#[derive(Debug, Serialize)]
pub struct NetworkStatsResponse {
    pub online_nodes: usize,
    pub mempool_size: usize,
    pub pending_inbox: usize,
    pub pending_deliveries: usize,
    pub tick_interval_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct NationStatsResponse {
    pub nation_id: String,
    pub total_mined_sats: i64,
    pub confirmed_sats: i64,
    pub immature_sats: i64,
    pub orphaned_sats: i64,
    pub reward_count: i64,
}

#[derive(Debug, Serialize)]
pub struct GlobalStatsResponse {
    pub total_mined_sats: i64,
    pub confirmed_sats: i64,
    pub immature_sats: i64,
    pub orphaned_sats: i64,
    pub reward_count: i64,
    pub active_block_count: i64,
}
