#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub block_id: String,
    pub parent_id: Option<String>,
    pub height: i64,
    pub total_weight: i32,
    pub cumulative_work: i64,
    pub first_seen_seq_id: i64,
    pub chain_status: String,
    pub miner_node_id: String,
}

impl Block {
    pub const MAX_BLOCK_WEIGHT: i32 = 4000;

    pub fn has_valid_weight(&self) -> bool {
        self.total_weight >= 0 && self.total_weight <= Self::MAX_BLOCK_WEIGHT
    }
}
