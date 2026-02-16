use crate::domain::entities::ScheduledDelivery;

#[derive(Debug, Clone)]
pub(super) struct HeapEntry {
    pub(super) delivery: ScheduledDelivery,
}

#[derive(Debug, Clone)]
pub(super) struct MempoolTx {
    pub(super) tx_id: String,
    pub(super) fee_sats: i64,
    pub(super) weight: i32,
    pub(super) created_at_ms: i64,
    pub(super) origin_node: String,
}

#[derive(Debug, Clone)]
pub(super) struct NodeTip {
    pub(super) block_id: String,
    pub(super) height: i64,
}

#[derive(Debug, Clone)]
pub(super) struct BlockTemplate {
    pub(super) tx_ids: Vec<String>,
    pub(super) total_weight: i32,
    pub(super) total_fees_sats: i64,
}

#[derive(Debug, Clone)]
pub(super) struct NodeMiningState {
    pub(super) nonce_counter: u64,
}

#[derive(Debug, Clone)]
pub(super) struct MiningHit {
    pub(super) node_id: String,
    pub(super) nonce: u64,
    pub(super) hash: u64,
    pub(super) trial_offset: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.delivery.key() == other.delivery.key()
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delivery.key().cmp(&other.delivery.key())
    }
}
