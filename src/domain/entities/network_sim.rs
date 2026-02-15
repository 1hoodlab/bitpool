use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MessageKind {
    Block,
    Tx,
    Inv,
    Ping,
}

impl MessageKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Tx => "tx",
            Self::Inv => "inv",
            Self::Ping => "ping",
        }
    }

    pub fn rank(&self) -> u8 {
        match self {
            Self::Block => 0,
            Self::Tx => 1,
            Self::Inv => 2,
            Self::Ping => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeSendCommand {
    pub from: String,
    pub to: String,
    pub kind: MessageKind,
    pub payload: Value,
    pub latency_override_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryKey {
    pub deliver_at_ms: i64,
    pub kind_rank: u8,
    pub from_id: String,
    pub to_id: String,
    pub schedule_counter: u64,
}

impl Ord for DeliveryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.deliver_at_ms,
            self.kind_rank,
            &self.from_id,
            &self.to_id,
            self.schedule_counter,
        )
            .cmp(&(
                other.deliver_at_ms,
                other.kind_rank,
                &other.from_id,
                &other.to_id,
                other.schedule_counter,
            ))
    }
}

impl PartialOrd for DeliveryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledDelivery {
    pub deliver_at_ms: i64,
    pub from: String,
    pub to: String,
    pub kind: MessageKind,
    pub payload: Value,
    pub msg_id: String,
    pub schedule_counter: u64,
}

impl ScheduledDelivery {
    pub fn key(&self) -> DeliveryKey {
        DeliveryKey {
            deliver_at_ms: self.deliver_at_ms,
            kind_rank: self.kind.rank(),
            from_id: self.from.clone(),
            to_id: self.to.clone(),
            schedule_counter: self.schedule_counter,
        }
    }
}
