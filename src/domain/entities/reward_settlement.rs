#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewardState {
    Pending,
    Confirmed,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewardSettlement {
    pub block_id: String,
    pub miner_node_id: String,
    pub state: RewardState,
    pub confirmations: i32,
    pub amount_sats: i64,
}
