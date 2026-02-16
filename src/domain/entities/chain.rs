use super::block::BlockHeader;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BlockStatus {
    Valid,
    Invalid,
    Active, // Part of the current best chain
    Orphan, // Valid but not part of best chain
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockIndexEntry {
    pub header: BlockHeader,
    pub tx_ids: Vec<String>, // Added to support reorgs
    pub cumulative_work: u128,
    pub status: BlockStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainTip {
    pub block_id: String,
    pub height: i64,
    pub cumulative_work: u128,
}
