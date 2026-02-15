// Domain entities: one concept per file. They represent business objects,
// not DB tables directly (tables live in infrastructure/persistence).

mod event_envelope;
mod network_sim;
pub use event_envelope::{EventEnvelope, EventType};
pub use network_sim::{MessageKind, NodeSendCommand, ScheduledDelivery};
