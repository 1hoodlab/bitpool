#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub node_id: String,
    pub is_ai: bool,
    pub hash_rate: i64,
    pub is_online: bool,
    pub balance_sats: i64,
    pub opex_per_sec_sats: i64,
    pub tip_block_id: Option<String>,
}

impl Node {
    pub fn has_valid_hash_rate(&self) -> bool {
        self.hash_rate >= 0
    }
}
