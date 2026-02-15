// Domain entities: one concept per file. They represent business objects,
// not DB tables directly (tables live in infrastructure/persistence).

mod block;
mod difficulty_state;
mod node;
mod reward_settlement;
mod tx;

pub use block::Block;
pub use difficulty_state::DifficultyState;
pub use node::Node;
pub use reward_settlement::{RewardSettlement, RewardState};
pub use tx::Tx;
