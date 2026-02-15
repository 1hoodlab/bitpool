// Domain entities: one concept per file. They represent business objects,
// not DB tables directly (tables live in infrastructure/persistence).

mod event_envelope;
mod network_sim;
pub mod block;
pub mod chain;
pub mod reward_settlement;
pub mod difficulty_state;

pub use event_envelope::{EventEnvelope, EventType};
pub use network_sim::{MessageKind, NodeSendCommand, ScheduledDelivery};
pub use block::{Block, BlockHeader};
pub use chain::{BlockIndexEntry, BlockStatus, ChainTip};
pub use reward_settlement::{Reward, RewardStatus};
