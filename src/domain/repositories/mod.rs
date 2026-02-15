// Repository traits: define what data is needed, not how it is fetched (CONVENTION).

mod block_repository;
mod error;
mod event_entry;
mod event_repository;
mod node_repository;
mod tx_repository;

pub use block_repository::BlockRepository;
pub use error::DomainRepoError;
pub use event_entry::EventEntry;
pub use event_repository::EventRepository;
pub use node_repository::NodeRepository;
pub use tx_repository::TxRepository;
