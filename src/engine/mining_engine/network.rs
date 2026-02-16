use crate::domain::entities::{MessageKind, NodeSendCommand};
use crate::domain::repositories::EventRepository;
use crate::helper::deterministic_rng::xorshift64;

use super::seed::stable_seed_for_command;
use super::DeterministicEngineCore;

impl<R: EventRepository> DeterministicEngineCore<R> {
    pub(super) fn schedule_delivery(&mut self, cmd: NodeSendCommand) {
        self.schedule_counter = self.schedule_counter.saturating_add(1);
        let latency_ms = cmd
            .latency_override_ms
            .unwrap_or_else(|| self.sample_latency_ms(&cmd, self.schedule_counter));
        let deliver_at_ms = self.sim_time_ms.saturating_add(latency_ms as i64);
        let msg_id = format!(
            "{}:{}:{}:{}",
            cmd.from,
            cmd.to,
            cmd.kind.as_str(),
            self.schedule_counter
        );

        let delivery = crate::domain::entities::ScheduledDelivery {
            deliver_at_ms,
            from: cmd.from,
            to: cmd.to,
            kind: cmd.kind,
            payload: cmd.payload,
            msg_id,
            schedule_counter: self.schedule_counter,
        };
        self.delivery_heap
            .push(std::cmp::Reverse(super::types::HeapEntry { delivery }));
    }

    pub(super) fn schedule_delivery_internal(
        &mut self,
        from: &str,
        to: &str,
        kind: MessageKind,
        payload: serde_json::Value,
    ) {
        let latency_override_ms = Some(self.sample_propagation_delay_ms(from, to, &kind));
        self.schedule_delivery(NodeSendCommand {
            from: from.to_string(),
            to: to.to_string(),
            kind,
            payload,
            latency_override_ms,
        });
    }

    pub(super) fn sample_latency_ms(&self, cmd: &NodeSendCommand, schedule_counter: u64) -> u64 {
        let seed = stable_seed_for_command(self.base_seed, cmd, schedule_counter);
        let jitter = xorshift64(seed) % 41;
        10 + jitter
    }

    pub(super) fn broadcast_to_online_nodes(
        &mut self,
        from: &str,
        kind: MessageKind,
        payload: serde_json::Value,
    ) {
        let mut recipients = self
            .nodes
            .values()
            .filter(|n| n.is_online && n.node_id != from)
            .map(|n| n.node_id.clone())
            .collect::<Vec<_>>();
        recipients.sort();

        for to in recipients {
            self.schedule_delivery_internal(from, &to, kind.clone(), payload.clone());
        }
    }

    pub(super) fn sample_propagation_delay_ms(
        &self,
        from: &str,
        to: &str,
        kind: &MessageKind,
    ) -> u64 {
        let counter = self.schedule_counter.saturating_add(1);
        let seed = stable_seed_for_command(
            self.base_seed,
            &NodeSendCommand {
                from: from.to_string(),
                to: to.to_string(),
                kind: kind.clone(),
                payload: serde_json::json!({}),
                latency_override_ms: None,
            },
            counter,
        );
        let jitter = if self.network.propagation_jitter_ms == 0 {
            0
        } else {
            xorshift64(seed) % (self.network.propagation_jitter_ms + 1)
        };
        self.network
            .propagation_base_delay_ms
            .saturating_add(jitter)
            .max(1)
    }
}
