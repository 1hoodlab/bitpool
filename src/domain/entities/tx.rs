#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tx {
    pub tx_id: String,
    pub fee_sats: i64,
    pub weight: i32,
    pub created_at_ms: i64,
    pub status: String,
}

impl Tx {
    pub fn has_valid_fee(&self) -> bool {
        self.fee_sats >= 0
    }
}
