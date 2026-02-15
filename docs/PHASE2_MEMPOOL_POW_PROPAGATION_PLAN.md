# Phase 2 Detailed Plan - Mempool + PoW + Propagation

## 1) Phase Goal

Build a live network simulation on top of the deterministic engine:

- Transaction spawn and mempool growth
- Block template selection (fee/weight constrained)
- PoW winner timing from node hash power
- Block/tx propagation across nodes with latency
- Continuous block flow with natural fork possibility in multi-node mode

This phase is complete when block production is stable, mempool behaves predictably, and propagation creates realistic ordering and occasional forks.

## 2) Scope

### In Scope

- Node profile for mining/network behavior.
- In-memory mempool model + selector policy.
- PoW scheduling integrated into existing engine loop.
- Network propagation events for tx/block/inv.
- Persist business events (`net.deliver`, `pow.block_found`, `mempool.tx_*`) as needed.
- Unit + integration tests for correctness and determinism.

### Out of Scope

- Best-chain selection and reorg settlement rules (Phase 3).
- Difficulty retarget final economics (Phase 3).
- Snapshot/replay/pruning (Phase 4).

## 3) Architecture Fit (Current Codebase)

- Engine implementation: `src/engine/mining_engine.rs`
- DB entities only: `src/database/entities/*`
- DB adapter implementation: `src/adapter/postgres_event_repository.rs`
- RNG utilities: `src/helper/deterministic_rng.rs`
- App wiring and API: `src/main.rs`

Phase 2 must preserve deterministic ordering guarantees from Phase 1:

- Single sequencing authority (`seq_id`) in engine loop
- Stable tie-break ordering in delivery queue
- Seeded randomness only (no global mutable RNG)

## 4) Domain Contracts to Add

## Step 1 - Node Mining/Network Profile

### Tasks

- Extend or add node simulation profile entity:
  - `node_id`
  - `hash_rate`
  - `is_online`
  - optional `has_relay`
  - optional latency profile metadata
- Add validation:
  - `hash_rate >= 0`
  - at least one online miner for block production scenarios

### Files

- `src/domain/entities/network_sim.rs` (or split file if preferred)
- `src/engine/mining_engine.rs`

### Done Criteria

- Engine can load and use per-node hash/network parameters.

---

## Step 2 - Mempool Data Model

### Tasks

- Add mempool entry model:
  - `tx_id`
  - `fee_sats`
  - `weight`
  - `created_at_ms`
  - optional `origin_node`
- Add mempool state container:
  - insert/update/remove
  - dedup by `tx_id`
  - pending count/total weight stats

### Policy

- Keep max block weight rule explicit (e.g. 4000 from baseline assumptions).
- Keep deterministic selection order for equal priority candidates.

### Files

- `src/domain/entities/network_sim.rs` or new file in `src/domain/entities/`
- `src/engine/mining_engine.rs`

### Done Criteria

- Mempool accepts tx spawn/receive and enforces dedup.

---

## Step 3 - Tx Spawn Generator

### Tasks

- Add deterministic tx generator in engine loop:
  - spawn rate config (`TX_SPAWN_RATE_PER_SEC` or per tick)
  - fee/weight distribution from seeded RNG
- Emit event when tx is created and inserted:
  - `mempool.tx_added.v1`

### Determinism Rules

- Seed generation from `(base_seed, sim_time_ms, tx_counter)` style inputs.
- No wall-time dependency for generation logic.

### Files

- `src/engine/mining_engine.rs`
- `src/helper/deterministic_rng.rs` (if new helper function needed)

### Done Criteria

- Tx volume and content are reproducible for same seed/config.

---

## Step 4 - Block Template Selector

### Tasks

- Implement template build from mempool:
  - priority by fee density (or chosen policy)
  - stop at max block weight
  - deterministic tie-break (`fee`, `created_at_ms`, `tx_id`)
- Emit template build event:
  - `pow.template_built.v1`

### Files

- `src/engine/mining_engine.rs`
- `src/domain/entities/event_envelope.rs` (event type constants if needed)

### Done Criteria

- Template always valid and deterministic for same mempool state.

---

## Step 5 - PoW Winner Timing

### Tasks

- Compute effective block find delay from total online hash rate.
- Pick winner node deterministically from weighted hash rate.
- Emit block found event:
  - `pow.block_found.v1`
  - include `miner_node_id`, `height_candidate`, `template_tx_ids`

### Determinism Rules

- Use seed based on `(base_seed, schedule_counter, current_tip, total_hash_rate)`.
- Stable node ordering before weighted selection.

### Files

- `src/engine/mining_engine.rs`
- `src/helper/deterministic_rng.rs`

### Done Criteria

- Same seed + same node state => same winner sequence.

---

## Step 6 - Propagation Flow for Tx/Block

### Tasks

- Convert produced tx/block into per-node `NodeSend` commands.
- Reuse existing delivery queue and latency model.
- Add `kind` support:
  - `tx`
  - `block`
  - optional `inv`
- Emit received/accepted events:
  - `node.tx_received.v1`
  - `node.block_received.v1`

### Files

- `src/engine/mining_engine.rs`
- `src/domain/entities/network_sim.rs` (message kind/payload contract)

### Done Criteria

- Broadcast propagation occurs through deterministic queue ordering.

---

## Step 7 - Minimal Chain Tip Tracking (Pre-Consensus)

### Tasks

- Maintain lightweight in-memory chain tip per node for Phase 2 flow only:
  - latest seen block id
  - latest seen height
- Do not implement full reorg economics yet.
- Emit diagnostic event:
  - `node.tip_updated.v1`

### Files

- `src/engine/mining_engine.rs`

### Done Criteria

- Each node advances tip based on received blocks; multi-node divergence can appear.

---

## Step 8 - Event Persistence Policy for Phase 2

### Tasks

- Keep heartbeat (`engine.tick.v1`) in-memory only (already applied in Phase 1 refinement).
- Persist business-relevant events:
  - `net.deliver.v1`
  - `mempool.tx_added.v1`
  - `pow.template_built.v1`
  - `pow.block_found.v1`
  - optional node receive/tip update events
- Ensure adapter mapping remains append-only and ordered by `seq_id`.

### Files

- `src/engine/mining_engine.rs`
- `src/adapter/postgres_event_repository.rs`
- `src/database/entities/event_log_entity.rs`

### Done Criteria

- DB growth remains useful for replay/debug without heartbeat spam.

---

## Step 9 - API/Debug Surface for Phase 2

### Tasks

- Extend `GET /engine/status` response:
  - mempool size
  - pending deliveries
  - online nodes
  - last block found summary
- Optional debug endpoints:
  - `GET /engine/mempool`
  - `GET /engine/nodes`

### Files

- `src/main.rs`
- `src/engine/mining_engine.rs`

### Done Criteria

- Operator can validate Phase 2 behavior without DB deep-dive.

---

## Step 10 - Test Plan (Required)

## Unit Tests

1. **Template weight limit**
   - selected tx set never exceeds max block weight.
2. **Template deterministic order**
   - same mempool input yields exact same tx order.
3. **PoW winner determinism**
   - same seed/hashrate snapshot yields same winner.
4. **Propagation ordering**
   - lower latency block arrives before higher latency counterpart.

## Integration Tests

1. **Single-node block flow**
   - tx spawn -> template -> block found -> tip update.
2. **Multi-node propagation**
   - one producer broadcasts block; all online peers receive.
3. **Natural fork scenario**
   - with tuned latency/hashrate, competing tips appear occasionally.

## Non-flaky Test Rule

- Avoid fixed `sleep` assertions for sequence length.
- Wait for specific event count/type predicates with timeout.

### Done Criteria

- `cargo test --bin bitpool` green for all Phase 2 scenarios.

---

## Step 11 - Config Matrix

Add and document config knobs:

- `ENGINE_TICK_INTERVAL_MS`
- `ENGINE_BASE_SEED`
- `ENGINE_MAX_INBOX_DRAIN_PER_TICK`
- `TX_SPAWN_RATE_PER_SEC` (new)
- `MAX_BLOCK_WEIGHT` (new)
- `POW_TARGET_BLOCK_TIME_MS` (new)
- `PROPAGATION_BASE_DELAY_MS` / `PROPAGATION_JITTER_MS` (new)

### Done Criteria

- Behavior can be tuned without code edits.

---

## Step 12 - Phase Exit Checklist

All items must be true:

- [ ] Tx generator and mempool operate continuously.
- [ ] Template selector enforces max block weight.
- [ ] PoW winner flow emits deterministic block found events.
- [ ] Propagation works through delivery queue for tx/block.
- [ ] Multi-node mode can show natural fork candidates.
- [ ] Business events persisted, heartbeat not persisted.
- [ ] Unit and integration tests pass.
- [ ] Docs updated for run and tuning.

## 5) Risks and Mitigation

- **Mempool policy bias reduces realism**  
  Mitigation: keep policy explicit and configurable; log template stats.

- **Propagation too fast/slow hides forks**  
  Mitigation: tune delay distribution via env matrix and validate with integration tests.

- **PoW timing unstable across runs**  
  Mitigation: all random decisions must use seeded deterministic helper functions.

- **Event volume too high**  
  Mitigation: persist only business events; keep heartbeat in memory.

## 6) Suggested Execution Order

1. Node profile + mempool model
2. Tx generator
3. Template selector
4. PoW winner timing
5. Propagation integration
6. Status/debug endpoints
7. Test suite + tuning matrix
8. Docs finalization

## 7) Docs to Update After Implementation

- `docs/RUN.md` (how to run Phase 2 simulation)
- `docs/DB_DESIGN.md` (event types persisted in Phase 2)
- `docs/OBSERVABILITY.md` (status metrics and debug endpoints)
