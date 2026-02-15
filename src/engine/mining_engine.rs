use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

use crate::domain::entities::{EventEnvelope, EventType, NodeSendCommand, ScheduledDelivery};
use crate::domain::repositories::{DomainRepoError, EventRepository};
use crate::helper::deterministic_rng::{hash_str64, splitmix64, xorshift64};

#[derive(Debug, Clone, Serialize)]
pub struct EngineStatus {
    pub running: bool,
    pub current_seq_id: i64,
    pub sim_time_ms: i64,
    pub tick_interval_ms: u64,
    pub pending_inbox: usize,
    pub pending_deliveries: usize,
}

impl Default for EngineStatus {
    fn default() -> Self {
        Self {
            running: false,
            current_seq_id: 0,
            sim_time_ms: 0,
            tick_interval_ms: 100,
            pending_inbox: 0,
            pending_deliveries: 0,
        }
    }
}

pub struct EngineHandle {
    command_tx: mpsc::UnboundedSender<NodeSendCommand>,
    status: Arc<RwLock<EngineStatus>>,
}

impl EngineHandle {
    pub fn enqueue_send(&self, cmd: NodeSendCommand) -> Result<(), DomainRepoError> {
        self.command_tx
            .send(cmd)
            .map_err(|e| DomainRepoError(format!("engine inbox closed: {e}")))
    }

    pub async fn status(&self) -> EngineStatus {
        self.status.read().await.clone()
    }
}

pub async fn start_deterministic_engine<R: EventRepository + 'static>(
    repo: Arc<R>,
    tick_interval_ms: u64,
    base_seed: u64,
    max_inbox_drain_per_tick: usize,
) -> Result<EngineHandle, DomainRepoError> {
    let initial_seq_id = repo.last_seq_id().await?.unwrap_or(0);
    let (command_tx, command_rx) = mpsc::unbounded_channel::<NodeSendCommand>();
    let status = Arc::new(RwLock::new(EngineStatus {
        running: true,
        current_seq_id: initial_seq_id,
        sim_time_ms: initial_seq_id.saturating_mul(tick_interval_ms as i64),
        tick_interval_ms,
        pending_inbox: 0,
        pending_deliveries: 0,
    }));

    let mut core = DeterministicEngineCore {
        repo,
        current_seq_id: initial_seq_id,
        sim_time_ms: initial_seq_id.saturating_mul(tick_interval_ms as i64),
        tick_interval_ms,
        base_seed,
        schedule_counter: 0,
        command_rx,
        delivery_heap: BinaryHeap::new(),
        status: Arc::clone(&status),
        max_inbox_drain_per_tick,
    };

    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_millis(tick_interval_ms));
        loop {
            ticker.tick().await;
            if let Err(err) = core.run_single_tick().await {
                log::error!("engine tick failed: {err}");
            }
        }
    });

    Ok(EngineHandle { command_tx, status })
}

struct DeterministicEngineCore<R: EventRepository> {
    repo: Arc<R>,
    current_seq_id: i64,
    sim_time_ms: i64,
    tick_interval_ms: u64,
    base_seed: u64,
    schedule_counter: u64,
    command_rx: mpsc::UnboundedReceiver<NodeSendCommand>,
    delivery_heap: BinaryHeap<Reverse<HeapEntry>>,
    status: Arc<RwLock<EngineStatus>>,
    max_inbox_drain_per_tick: usize,
}

#[derive(Debug, Clone)]
struct HeapEntry {
    delivery: ScheduledDelivery,
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

impl<R: EventRepository> DeterministicEngineCore<R> {
    async fn run_single_tick(&mut self) -> Result<(), DomainRepoError> {
        self.sim_time_ms = self.sim_time_ms.saturating_add(self.tick_interval_ms as i64);

        self.emit_event(
            EventType::ENGINE_TICK_V1,
            json!({
                "tick_id": self.current_seq_id + 1,
                "sim_time_ms": self.sim_time_ms,
            }),
            false,
        )
        .await?;

        let mut drained = 0usize;
        while drained < self.max_inbox_drain_per_tick {
            match self.command_rx.try_recv() {
                Ok(cmd) => {
                    drained += 1;
                    self.schedule_delivery(cmd);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        while let Some(Reverse(entry)) = self.delivery_heap.peek() {
            if entry.delivery.deliver_at_ms > self.sim_time_ms {
                break;
            }
            let Reverse(ready) = self
                .delivery_heap
                .pop()
                .expect("heap peek had value, pop must succeed");
            self.emit_net_delivery_event(ready.delivery).await?;
        }

        self.update_status().await;
        Ok(())
    }

    fn schedule_delivery(&mut self, cmd: NodeSendCommand) {
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

        let delivery = ScheduledDelivery {
            deliver_at_ms,
            from: cmd.from,
            to: cmd.to,
            kind: cmd.kind,
            payload: cmd.payload,
            msg_id,
            schedule_counter: self.schedule_counter,
        };
        self.delivery_heap.push(Reverse(HeapEntry { delivery }));
    }

    fn sample_latency_ms(&self, cmd: &NodeSendCommand, schedule_counter: u64) -> u64 {
        let seed = stable_seed_for_command(self.base_seed, cmd, schedule_counter);
        let jitter = xorshift64(seed) % 41;
        10 + jitter
    }

    async fn emit_net_delivery_event(
        &mut self,
        delivery: ScheduledDelivery,
    ) -> Result<(), DomainRepoError> {
        self.emit_event(
            EventType::NET_DELIVER_V1,
            json!({
                "deliver_at_ms": delivery.deliver_at_ms,
                "from": delivery.from,
                "to": delivery.to,
                "kind": delivery.kind.as_str(),
                "msg_id": delivery.msg_id,
                "payload": delivery.payload,
            }),
            true,
        )
        .await
    }

    async fn emit_event(
        &mut self,
        event_type: &str,
        payload: serde_json::Value,
        persist_to_db: bool,
    ) -> Result<(), DomainRepoError> {
        let next_seq = self.current_seq_id.saturating_add(1);
        let envelope = EventEnvelope {
            seq_id: next_seq,
            server_time_ms: self.sim_time_ms,
            event_type: event_type.to_string(),
            payload,
        };
        if persist_to_db {
            self.repo.append_event(&envelope).await?;
        }
        self.current_seq_id = next_seq;
        Ok(())
    }

    async fn update_status(&self) {
        let mut status = self.status.write().await;
        status.running = true;
        status.current_seq_id = self.current_seq_id;
        status.sim_time_ms = self.sim_time_ms;
        status.tick_interval_ms = self.tick_interval_ms;
        status.pending_inbox = self.command_rx.len();
        status.pending_deliveries = self.delivery_heap.len();
    }
}

fn stable_seed_for_command(base_seed: u64, cmd: &NodeSendCommand, schedule_counter: u64) -> u64 {
    let mut seed = splitmix64(base_seed ^ schedule_counter);
    seed ^= hash_str64(&cmd.from);
    seed = splitmix64(seed ^ hash_str64(&cmd.to));
    seed = splitmix64(seed ^ (cmd.kind.rank() as u64));
    seed
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use tokio::time::{sleep, Duration, Instant};

    use crate::domain::entities::{EventEnvelope, MessageKind, NodeSendCommand};
    use crate::domain::repositories::{DomainRepoError, EventRepository};

    use super::start_deterministic_engine;

    #[derive(Default)]
    struct MockEventRepository {
        events: Mutex<Vec<EventEnvelope>>,
    }

    #[async_trait]
    impl EventRepository for MockEventRepository {
        async fn append_event(&self, event: &EventEnvelope) -> Result<(), DomainRepoError> {
            let mut guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            if guard.iter().any(|e| e.seq_id == event.seq_id) {
                return Err(DomainRepoError("duplicate seq".to_string()));
            }
            guard.push(event.clone());
            Ok(())
        }

        async fn last_seq_id(&self) -> Result<Option<i64>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard.last().map(|e| e.seq_id))
        }

        async fn read_events_after(
            &self,
            after_seq_id: i64,
            limit: u64,
        ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard
                .iter()
                .filter(|e| e.seq_id > after_seq_id)
                .take(limit as usize)
                .cloned()
                .collect())
        }

        async fn read_events_range(
            &self,
            from_seq: i64,
            to_seq: i64,
        ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
            let guard = self
                .events
                .lock()
                .map_err(|e| DomainRepoError(format!("mutex poisoned: {e}")))?;
            Ok(guard
                .iter()
                .filter(|e| e.seq_id >= from_seq && e.seq_id <= to_seq)
                .cloned()
                .collect())
        }
    }

    fn snapshot_events(repo: &Arc<MockEventRepository>) -> Vec<EventEnvelope> {
        repo.events
            .lock()
            .expect("mock event repo lock must be available")
            .clone()
    }

    async fn wait_for_delivery_count(
        repo: &Arc<MockEventRepository>,
        expected_count: usize,
        timeout: Duration,
    ) {
        let deadline = Instant::now() + timeout;
        loop {
            let count = snapshot_events(repo)
                .into_iter()
                .filter(|e| e.event_type == "net.deliver.v1")
                .count();
            if count >= expected_count || Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn latency_ordering_prefers_earlier_delivery() {
        let repo = Arc::new(MockEventRepository::default());
        let engine = start_deterministic_engine(Arc::clone(&repo), 5, 42, 1024)
            .await
            .expect("engine should start");

        engine
            .enqueue_send(NodeSendCommand {
                from: "node-a".to_string(),
                to: "node-b".to_string(),
                kind: MessageKind::Tx,
                payload: json!({"name":"first"}),
                latency_override_ms: Some(10),
            })
            .expect("enqueue should succeed");
        engine
            .enqueue_send(NodeSendCommand {
                from: "node-a".to_string(),
                to: "node-c".to_string(),
                kind: MessageKind::Tx,
                payload: json!({"name":"second"}),
                latency_override_ms: Some(20),
            })
            .expect("enqueue should succeed");

        wait_for_delivery_count(&repo, 2, Duration::from_millis(500)).await;

        let events = snapshot_events(&repo);
        let delivers: Vec<_> = events
            .into_iter()
            .filter(|e| e.event_type == "net.deliver.v1")
            .collect();
        assert!(delivers.len() >= 2);

        let first_to = delivers[0]
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let second_to = delivers[1]
            .payload
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(first_to, "node-b");
        assert_eq!(second_to, "node-c");
    }

    #[tokio::test]
    async fn tie_break_is_stable_across_runs() {
        let mut baseline: Option<Vec<String>> = None;
        for _ in 0..10 {
            let repo = Arc::new(MockEventRepository::default());
            let engine = start_deterministic_engine(Arc::clone(&repo), 5, 7, 1024)
                .await
                .expect("engine should start");

            let sends = vec![
                ("node-a", "node-z", MessageKind::Ping),
                ("node-a", "node-b", MessageKind::Tx),
                ("node-a", "node-c", MessageKind::Inv),
                ("node-a", "node-d", MessageKind::Block),
            ];
            for (from, to, kind) in sends {
                engine
                    .enqueue_send(NodeSendCommand {
                        from: from.to_string(),
                        to: to.to_string(),
                        kind,
                        payload: json!({"batch":"same-time"}),
                        latency_override_ms: Some(15),
                    })
                    .expect("enqueue should succeed");
            }

            wait_for_delivery_count(&repo, 4, Duration::from_millis(500)).await;

            let events = snapshot_events(&repo);
            let order: Vec<String> = events
                .into_iter()
                .filter(|e| e.event_type == "net.deliver.v1")
                .map(|e| {
                    format!(
                        "{}:{}",
                        e.payload
                            .get("kind")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default(),
                        e.payload
                            .get("to")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                    )
                })
                .collect();

            if let Some(expected) = &baseline {
                assert_eq!(expected, &order);
            } else {
                baseline = Some(order);
            }
        }
    }

    #[tokio::test]
    async fn same_seed_and_commands_produce_same_event_stream() {
        async fn run_once() -> Vec<(i64, String, String)> {
            let repo = Arc::new(MockEventRepository::default());
            let engine = start_deterministic_engine(Arc::clone(&repo), 5, 9999, 1024)
                .await
                .expect("engine should start");

            for idx in 0..4 {
                engine
                    .enqueue_send(NodeSendCommand {
                        from: "node-main".to_string(),
                        to: format!("node-{idx}"),
                        kind: MessageKind::Tx,
                        payload: json!({"idx": idx}),
                        latency_override_ms: None,
                    })
                    .expect("enqueue should succeed");
            }

            wait_for_delivery_count(&repo, 4, Duration::from_millis(500)).await;

            snapshot_events(&repo)
                .into_iter()
                .map(|e| (e.seq_id, e.event_type, e.payload.to_string()))
                .collect()
        }

        let left = run_once().await;
        let right = run_once().await;
        assert_eq!(left, right);
    }
}
