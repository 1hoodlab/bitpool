use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    pub block_id: String,
    pub height: i64,
    pub parent_hash: String,
    pub miner_node_id: String,
    pub timestamp_ms: i64,
    pub difficulty: u64,
    pub nonce: u64,
    pub total_weight: i32,
    pub total_fees_sats: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub tx_ids: Vec<String>,
}
