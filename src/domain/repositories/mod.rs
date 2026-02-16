// Repository traits: define what data is needed, not how it is fetched (CONVENTION).

mod error;
mod event_repository;

pub use error::DomainRepoError;
pub use event_repository::{EventRepository, RewardSettlementWrite};
