use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RewardStatus {
    Immature,
    Confirmed,
    Orphaned,
    Spent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reward {
    pub id: String,
    pub block_hash: String,
    pub miner_id: String,
    pub amount_sats: i64,
    pub status: RewardStatus,
    pub created_at_ms: i64,
    pub matured_at_height: Option<i64>,
    pub settled_at_ms: Option<i64>,
}
