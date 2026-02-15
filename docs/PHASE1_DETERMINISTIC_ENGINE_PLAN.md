# Phase 1 Detailed Plan - Deterministic Engine Core

## 1) Phase Goal

Build a deterministic heartbeat engine for server-authoritative simulation with:

- Monotonic `seq_id`
- Canonical simulation time (`sim_time_ms`) mapped to `server_time_ms`
- Tick scheduler
- Standard event envelope
- Durable event stream persistence in `event_log`
- Network-realistic discrete-event scheduling baseline (inbox + delivery queue)

This phase is complete when the engine can run continuously without duplicate or missing sequence IDs, and event order is stable under concurrent producers.

## 2) Scope

### In Scope

- Engine tick loop and deterministic event sequencing.
- Discrete-event network scheduling contract (`NodeSend` -> `NetworkDeliver`).
- Event envelope schema and serialization contract.
- Event append/read repository APIs.
- Persistence implementation for event append/read.
- Basic engine status endpoint for observability and validation.
- Unit/integration tests for monotonicity, ordering, and idempotency.

### Out of Scope (Phase 2+)

- Full mempool/PoW logic.
- Multi-node propagation and fork simulation.
- Consensus/reorg settlement.
- Snapshot/replay.

## 3) Expected Files to Touch

- `src/application/use_cases.rs` (or split into `src/application/use_cases/*`)
- `src/domain/entities/mod.rs`
- `src/domain/repositories/mod.rs`
- `src/domain/repositories/event_repository.rs` (new)
- `src/infrastructure/persistence.rs`
- `src/main.rs`
- `migration/src/m20260215_000001_baseline_schema.rs` (only if schema missing required event fields)
- `docs/RUN.md` (add local run/test instructions)

## 4) Prerequisites and Decisions

1. **Canonical time source**  
   Use `sim_time_ms` as canonical simulation time. Persist `server_time_ms = sim_time_ms` for deterministic replay.

2. **Wall clock observability**  
   If needed, expose `wall_time_ms` in status endpoint or tick payload for ops/debug; it is not part of deterministic ordering.

3. **Single writer for seq_id**  
   Only engine core assigns `seq_id` to guarantee ordering.

4. **Tick interval**  
   Start with fixed tick (for example `100ms`) configurable from environment.

5. **Persistence strategy**  
   Use append-only `event_log` writes in transaction-safe method; avoid in-place mutation.

6. **Idempotency strategy**  
   Add unique DB constraint on `seq_id` (or `(stream_id, seq_id)` if stream-partitioned) and handle duplicate insert as no-op/failure with explicit error mapping.

7. **Tie-break rule (mandatory)**  
   For same `deliver_at_ms`, use stable key ordering: `(deliver_at_ms, kind_rank, from_id, to_id, schedule_counter)`.

8. **Deterministic latency RNG**  
   Use per-message local seed:
   `rng_seed = hash(base_seed, from_id, to_id, kind, schedule_counter)`.
   Do not use a global mutable RNG.

## 4.1) Step C Locked Minimal Spec (Phase 1 Contract)

### Engine model

- **Inbox (concurrent producers)** accepts commands:
  - `NodeSend { from, to, kind, payload }`
- **Delivery queue (min-heap)** stores scheduled network deliveries:
  - `NetworkDeliver { deliver_at_ms, from, to, kind, payload, tie_break }`

### Per-tick execution order

1. Increment `sim_time_ms` by fixed interval.
2. Emit `engine.tick.v1`.
3. Drain inbox (full drain or bounded drain by config).
4. Schedule deliveries into min-heap (with deterministic latency).
5. Pop all deliveries with `deliver_at_ms <= sim_time_ms` and emit in heap order.

### Canonical event types (minimum)

1. `engine.tick.v1`
2. `net.deliver.v1`
3. `node.received.v1` (optional in Phase 1 persistence; useful for debugging handlers)

## 5) Step-by-Step Implementation Plan

## Step 0 - Baseline Check and Branch Safety

### Tasks

- Run compile check on current baseline.
- Verify migration status is healthy.
- Confirm there are no local blockers.

### Done Criteria

- `cargo check` passes.
- Migration tools are runnable locally.

---

## Step 1 - Define Domain Event Envelope

### Tasks

- Introduce core event envelope type in domain layer:
  - `seq_id: i64` (or `u64` mapped safely)
  - `server_time_ms: i64` (canonical sim time)
  - `event_type: String` or enum
  - `payload: serde_json::Value` (or typed payload + enum)
- Standardize minimum event payload contracts:
  - `engine.tick.v1` => `{ "tick_id": number, "sim_time_ms": number }`
  - `net.deliver.v1` => `{ "deliver_at_ms": number, "from": "...", "to": "...", "kind": "tx|block|inv|ping", "msg_id": "...", "payload": { ... } }`
  - `node.received.v1` => `{ "node": "...", "from": "...", "kind": "...", "msg_id": "...", "payload": { ... } }`
- Add constructor/validator to enforce:
  - `seq_id > 0`
  - non-empty `event_type`
  - deterministic serialization path

### Notes

- Keep event envelope independent from transport (HTTP/WS).
- Define and document `MessageKind` rank mapping for deterministic tie-break (for example `block=0, tx=1, inv=2, ping=3`).

### Done Criteria

- Domain event compiles and can serialize/deserialize consistently.

---

## Step 2 - Define Repository Contract

### Tasks

- Create `event_repository.rs` trait with explicit methods:
  - `append_event(event) -> Result<()>`
  - `append_events(batch) -> Result<()>` (optional now, useful soon)
  - `last_seq_id() -> Result<Option<i64>>`
  - `read_events_after(seq_id, limit) -> Result<Vec<EventEnvelope>>`
- Optional helper for deterministic replay windows:
  - `read_events_range(from_seq, to_seq) -> Result<Vec<EventEnvelope>>`
- Export trait via `src/domain/repositories/mod.rs`.
- Define domain-level repository errors (duplicate seq, db unavailable, serialization failure).

### Done Criteria

- Application layer depends only on trait, no DB types leaking into domain/application.

---

## Step 3 - Event Log Schema Validation

### Tasks

- Verify existing migration already contains needed `event_log` columns:
  - `seq_id` (indexed + unique)
  - `server_time_ms`
  - `event_type`
  - `payload` (json/jsonb/text)
  - `created_at` (optional but recommended)
- If missing fields/indexes:
  - Create a **new migration** (do not rewrite applied migration) to add constraints/indexes.

### Done Criteria

- Schema supports deterministic append/read at scale.

---

## Step 4 - Persistence Implementation

### Tasks

- Implement `EventRepository` in `src/infrastructure/persistence.rs`.
- Ensure append path is atomic and maps DB uniqueness conflict clearly.
- Add query for `last_seq_id` with correct empty-state behavior.
- Add ordered pagination query:
  - `WHERE seq_id > ? ORDER BY seq_id ASC LIMIT ?`

### Done Criteria

- Repository implementation passes compile and basic integration checks.

---

## Step 5 - Engine State and Tick Scheduler

### Tasks

- Introduce `EngineState` in application layer:
  - current `seq_id`
  - current `sim_time_ms`
  - running flag
  - tick interval
  - `base_seed`
  - `schedule_counter` (`u64`)
  - inbox queue handle
  - delivery min-heap
- On startup, initialize current sequence from repository `last_seq_id`.
- Implement scheduler loop per tick:
  1. increment `sim_time_ms`
  2. emit `engine.tick.v1`
  3. drain inbox commands
  4. schedule `NetworkDeliver` entries (deterministic latency)
  5. pop all heap items `<= sim_time_ms` and emit `net.deliver.v1` in key order

### Determinism Rules

- Always increment `seq_id` by exactly `+1` per emitted event.
- Assign `server_time_ms = sim_time_ms`.
- Keep event generation path single-threaded or synchronized by one sequencing critical section.
- Keep heap ordering stable with key:
  `(deliver_at_ms, kind_rank, from_id, to_id, schedule_counter)`.

### Done Criteria

- Engine runs and appends heartbeat events in strict sequence.

---

## Step 6 - Event Producer Interface (Internal)

### Tasks

- Expose internal API:
  - `enqueue_send(cmd: NodeSend)` (async-safe for concurrent producers)
- Producers (node/AI/use case) only enqueue `NodeSend` command and never set:
  - `seq_id`
  - `deliver_at_ms`
- Engine loop alone performs final sequencing and scheduling to `NetworkDeliver`.
- Define backpressure behavior for producer bursts:
  - bounded queue with drop/reject policy, or
  - unbounded queue with warning metric (temporary only).
- Define deterministic latency model:
  - `latency_ms = base + jitter`
  - optional relay fast-lane profile (lower mean/clamp)
  - `rng_seed = hash(base_seed, from_id, to_id, kind, schedule_counter)`

### Done Criteria

- Multiple producers can submit events without breaking sequence guarantees.

---

## Step 7 - Engine Status API

### Tasks

- Add lightweight endpoint in `src/main.rs`, for example:
  - `GET /health`
  - `GET /engine/status`
- Return:
  - `running`
  - `current_seq_id`
  - `tick_interval_ms`
  - optional `last_event_time_ms`

### Done Criteria

- API can be used to verify liveness and sequence progress quickly.

---

## Step 8 - Unit Tests (Must Have)

### Test Set

1. **Monotonic sequence generation**
   - multiple ticks => strict `1..N` increments.
2. **No duplicate sequence under concurrent producer input**
   - concurrent submissions still persisted with unique sequence.
3. **Ordered reads**
   - `read_events_after` always returns ascending contiguous sequence in stored range.
4. **Idempotent duplicate handling**
   - duplicate insert path is predictable and does not corrupt stream.
5. **Latency ordering**
   - schedule A->B with `10ms` and A->C with `20ms`; B delivery must be emitted first.
6. **Tie-break stability**
   - multiple same-`deliver_at_ms` messages always produce identical order across repeated runs.
7. **Seed determinism**
   - same `base_seed` + same command stream => same emitted tuple stream `(seq_id, event_type, payload)`.

### Done Criteria

- Unit tests pass reliably in repeated runs.

---

## Step 9 - Integration Tests (Must Have)

### Test Set

- Start engine with test DB and run for fixed duration (for example 60s).
- Validate:
  - no sequence gaps in persisted rows
  - no duplicate `seq_id`
  - `server_time_ms` is non-decreasing and equals expected simulated progression
- Add stress variant with burst producer activity.
- Add replay-equivalence test:
  - run same input with same `base_seed` twice and compare event stream equivalence.

### Done Criteria

- Integration suite passes in local CI-like conditions.

---

## Step 10 - Runtime Validation (5-10 Minute Soak)

### Tasks

- Run engine continuously for 5-10 minutes.
- Collect sequence stats:
  - first seq
  - last seq
  - expected count vs actual count
  - duplicate count
  - gap count
- Validate status endpoint responsiveness throughout run.

### Done Criteria

- Zero duplicates, zero gaps, stable status endpoint.

---

## Step 11 - Documentation Update

### Tasks

- Update `docs/RUN.md` with:
  - how to run engine locally
  - how to query engine status
  - how to inspect latest events in DB
  - troubleshooting for DB unique constraint or clock drift issues
- Add test commands and expected outcomes.

### Done Criteria

- A new developer can bootstrap and validate Phase 1 without verbal guidance.

---

## Step 12 - Phase Exit Checklist

All items must be true:

- [ ] Engine emits deterministic events continuously by tick.
- [ ] `seq_id` strictly monotonic, no duplicates, no gaps.
- [ ] Event envelope contract is stable and documented.
- [ ] Event repository trait + implementation complete.
- [ ] Step C discrete-event contract (inbox + heap + tie-break + seed) implemented.
- [ ] Status endpoints available and validated.
- [ ] Unit and integration tests green.
- [ ] 5-10 minute soak test passes.
- [ ] Docs updated in `docs/RUN.md`.

## 6) Suggested Work Breakdown (Execution Order)

1. Domain envelope + repository trait.
2. Schema verification/migration.
3. Persistence implementation.
4. Engine scheduler and sequence assignment.
5. Status endpoint.
6. Unit tests.
7. Integration + soak.
8. Docs finalization.

## 7) Risk Register and Mitigation

- **Race condition on sequence assignment**  
  Mitigation: single sequencing authority in engine core; DB unique guard on `seq_id`.

- **Non-determinism from delivery tie or RNG**  
  Mitigation: enforce heap key order + per-message seeded RNG with `schedule_counter`.

- **Write throughput bottleneck**  
  Mitigation: prepare batch append API and index tuning; profile before Phase 2.

- **Event payload instability**  
  Mitigation: define versioned event schema (`event_type` + optional `event_version`).

## 8) Command Checklist (Local)

- `cargo check`
- `cargo test`
- `cargo run --bin migrate`
- `cargo run` (or project-specific engine start command)
- DB query for validating sequence continuity in `event_log`

## 9) Hand-off Note for Phase 2

Once this plan is complete, Phase 2 can safely build on top of:

- Deterministic sequencing foundation.
- Stable event persistence.
- Reliable internal producer-to-engine flow.
- Basic operational visibility (`/health`, `/engine/status`).
