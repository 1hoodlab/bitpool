# Engine Modularization Plan - `mining_engine.rs`

## 1) Goal

Refactor `src/engine/mining_engine.rs` into focused files/modules to improve readability, testability, and long-term maintainability **without changing runtime behavior**.

## 2) Current Problem

- `mining_engine.rs` currently mixes many responsibilities:
  - app command ingress
  - tick loop orchestration
  - network simulation and delivery queue
  - mempool and template building
  - mining and PoW logic
  - chain selection and reorg
  - reward lifecycle
  - event emission and persistence
- A single large file slows onboarding and increases change risk.

## 3) Refactor Principles

- Behavior-preserving first (no feature changes in first pass).
- Small, verifiable steps (compile + tests each step).
- Keep public API stable:
  - `EngineHandle`
  - `EngineStatus`
  - `start_deterministic_engine_with_config`
- Prefer `pub(crate)` visibility for internals.
- Move code first, optimize later.

## 4) Target Module Structure

Proposed layout under `src/engine/`:

- `mod.rs` - module wiring and public exports
- `core.rs` - `DeterministicEngineCore`, tick orchestration
- `commands.rs` - `EngineCommand`, inbox draining, command handlers
- `network.rs` - scheduling, propagation delay, delivery handling
- `events.rs` - `emit_event`, `emit_net_delivery_event`
- `mempool.rs` - tx pool model and block template selection
- `mining.rs` - PoW hit search, trials/target logic
- `chain.rs` - process block, tip update, reorg flow
- `rewards.rs` - maturity/orphan/settlement reconciliation and writes
- `bootstrap.rs` - genesis setup + load from repository
- `types.rs` - internal helper structs (`NodeTip`, `MiningHit`, `HeapEntry`, etc.)

## 5) Detailed Execution Plan

### Step 0 - Baseline Safety Net

Tasks:
- Ensure current branch compiles and tests pass.
- Record baseline behavior:
  - `cargo check`
  - `cargo test`
- Save a short baseline note in PR description for comparison.

Acceptance criteria:
- Green baseline before any file move.

---

### Step 1 - Create Engine Module Entry

Tasks:
- Convert `src/engine/mod.rs` to be the entry point.
- Keep `mining_engine.rs` temporarily, but re-export the current public API from `mod.rs`.
- Introduce empty new files from the target structure.

Acceptance criteria:
- No functional change.
- Existing imports from `engine::mining_engine::...` remain valid (or are migrated in one controlled step).

---

### Step 2 - Extract Core Shell and Types

Tasks:
- Move foundational types to `types.rs`:
  - `MempoolTx`, `NodeTip`, `BlockTemplate`, `NodeMiningState`, `MiningHit`, `HeapEntry`
- Move `DeterministicEngineCore` struct definition to `core.rs`.
- Keep method bodies in original file if needed, then progressively relocate impl blocks.

Acceptance criteria:
- Compile passes after move.
- No logic edits, only import/visibility adjustments.

---

### Step 3 - Extract Command Layer

Tasks:
- Move:
  - `EngineCommand`
  - command handling path in `run_single_tick` (inbox drain)
  - `apply_upsert_node`
- Create clear function boundary:
  - `drain_commands(&mut self) -> Result<(), DomainRepoError>`

Acceptance criteria:
- `POST /engine/send` and `POST /nodes` continue to work unchanged.

---

### Step 4 - Extract Network Simulation

Tasks:
- Move to `network.rs`:
  - `schedule_delivery`
  - `schedule_delivery_internal`
  - `sample_latency_ms`
  - `sample_propagation_delay_ms`
  - `broadcast_to_online_nodes`
  - delivery pop/dispatch loop
  - `handle_delivery`
- Keep deterministic behavior identical.

Acceptance criteria:
- Delivery ordering and timing remain deterministic.
- Existing network event flow still emitted (`net.deliver.v1`, node receive events).

---

### Step 5 - Extract Event Emission and Persistence Boundary

Tasks:
- Move to `events.rs`:
  - `emit_event`
  - `emit_net_delivery_event`
- Keep `seq_id` behavior and persistence flags exactly as before.

Acceptance criteria:
- Realtime consumers still receive expected event order.

---

### Step 6 - Extract Mempool + Mining

Tasks:
- Move to `mempool.rs`:
  - `build_spawned_tx`
  - `insert_mempool_tx`
  - `build_block_template`
  - mempool tx sorting/selection logic
- Move to `mining.rs`:
  - `try_mine_block`
  - `find_mining_hit`
  - `trials_for_tick`
  - `current_mining_target`
  - `total_online_trials_per_tick`
  - `mini_pow_hash`
  - helper seed functions if tightly coupled

Acceptance criteria:
- Block production rate and deterministic replay remain equivalent.

---

### Step 7 - Extract Chain and Reorg

Tasks:
- Move to `chain.rs`:
  - `process_block`
  - `update_tip`
  - `reorganize`
  - `connect_block`
  - `disconnect_block`
  - `canonical_tip_height`
- Keep all `ConsensusService` calls intact.

Acceptance criteria:
- Chain extension and reorg scenarios still pass.

---

### Step 8 - Extract Rewards and Bootstrap

Tasks:
- Move reward lifecycle/reconciliation to `rewards.rs`.
- Move genesis/bootstrap loading to `bootstrap.rs`:
  - `initialize_genesis`
  - `bootstrap_from_repo`
- Keep DB write ordering unchanged.

Acceptance criteria:
- Reward states (`Immature/Confirmed/Orphaned`) remain consistent before/after refactor.

---

### Step 9 - Stabilize Public API and Imports

Tasks:
- Expose only required public items from `engine/mod.rs`.
- Replace cross-file implicit coupling with explicit imports.
- Remove dead constants/helpers if truly unused (separate commit).

Acceptance criteria:
- No external module depends on internals unexpectedly.

---

### Step 10 - Final Cleanup and Documentation

Tasks:
- Run `cargo fmt`, `cargo clippy` (if enabled), `cargo test`.
- Add/update inline docs on module responsibilities.
- Add a short architecture note under `docs/` referencing new module map.

Acceptance criteria:
- Tooling green.
- New developer can locate features in under 1 minute.

## 6) Suggested Commit Strategy

- Commit 1: module skeleton + no-op wiring
- Commit 2: types + core extraction
- Commit 3: commands + network extraction
- Commit 4: mempool + mining extraction
- Commit 5: chain + rewards + bootstrap extraction
- Commit 6: cleanup + docs

Each commit must compile independently.

## 7) Verification Matrix

After each major step, verify:

- Engine tick progresses (`/engine/status`)
- Node command path works (`/engine/send`, `/nodes`)
- Realtime bootstrap and ws endpoints still work
- Event sequence monotonicity preserved (`seq_id`)
- Mined block flow still emits:
  - `mempool.tx_added.v1`
  - `pow.block_found.v1`
  - `net.deliver.v1`
  - `chain.reorg.v1` (when applicable)
  - `reward.matured.v1` (when applicable)

## 8) Risks and Mitigation

- Risk: accidental behavior change during move
  - Mitigation: move-only commits, no logic changes mixed in.
- Risk: visibility/import breakage
  - Mitigation: enforce `pub(crate)` boundaries and compile after every extraction.
- Risk: hidden coupling between methods
  - Mitigation: extract by cohesive groups, not by random function order.

## 9) Out of Scope (This Plan)

- Introducing new consensus logic.
- Changing persistence schema.
- Adding new event types or transport protocols.
- Performance optimization beyond parity.

## 10) Done Definition

Refactor is complete when:
- `mining_engine.rs` is replaced by modular engine files.
- Public API remains stable for callers.
- All existing behavior and event flow remain equivalent.
- Build/tests pass and docs reflect new structure.
