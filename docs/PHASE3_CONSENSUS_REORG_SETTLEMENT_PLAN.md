# Phase 3 Detailed Plan - Consensus, Reorg, and Settlement

## 1) Phase Goal

Implement the core consensus mechanism (Nakamoto Consensus) to establish a single source of truth (the "best chain") among nodes. This phase introduces the complexity of handling competing chains (forks), reorganization (reorgs), and the economic lifecycle of mining rewards (maturity and settlement).

## 2) Scope

### In Scope
- **Chain Selection**: Logic to select the chain with the most cumulative work (or height).
- **Block Index**: Data structure to track all known blocks and their relationships (parent/child).
- **Reorganization (Reorg)**: Atomic mechanism to switch the active chain state from one tip to another.
- **State Transition**: Applying and undoing blocks (connect/disconnect logic).
- **Reward Lifecycle**: Tracking mining rewards from `Immature` -> `Mature` (Settled) or `Orphaned` (Lost).
- **Difficulty Adjustment**: Retargeting algorithm to maintain block time.

### Out of Scope
- **P2P Networking**: We continue to use the simulated event bus from Phase 2.
- **Complex Smart Contracts**: Only basic transfer/reward logic.
- **Wallet/Account Management**: Focus is on the backend ledger state.

## 3) Architecture Fit

- **Engine**: `src/engine/mining_engine.rs` will be expanded to manage `BlockIndex` and `ChainState`.
- **Domain**: New entities for `BlockIndexEntry`, `ChainTip`, and `Reward`.
- **Persistence**: New repositories for storing the chain structure and rewards.

## 4) Detailed Implementation Steps

### Step 1: Block Index & Chain Tree
**Goal**: Maintain a tree of all valid headers received, not just the active chain.

**Tasks**:
- Create `BlockIndex` struct:
  - Map `block_hash -> BlockIndexEntry`
  - `BlockIndexEntry` contains: `header`, `height`, `cumulative_work`, `parent_hash`, `status` (Valid, Invalid, Active).
- Update `process_block` to add to Index first, then trigger Chain Selection.

**Files**:
- `src/domain/entities/chain.rs` (new)
- `src/engine/mining_engine.rs`

### Step 2: Chain Selection (Longest Chain Rule)
**Goal**: Determine which branch of the tree is the "active" chain.

**Tasks**:
- Implement `find_best_tip()`:
  - Compare `cumulative_work` of the new block vs current tip.
  - If `new_work > current_work`, trigger Reorg.
- Handle "Tie-breaking": If work is equal, keep current tip (or use deterministic rule like lower hash).

**Files**:
- `src/domain/services/consensus.rs` (new service logic)

### Step 3: Reorg Logic (The Core)
**Goal**: Switch state from `Old Tip` to `New Tip`.

**Tasks**:
- Implement `reorganize(old_tip, new_tip)`:
  1. Find **Fork Point** (common ancestor).
  2. **Disconnect** blocks from `old_tip` back to `fork_point`.
  3. **Connect** blocks from `fork_point` forward to `new_tip`.
- Ensure atomicity: If any step fails, rollback to `old_tip`.

**Files**:
- `src/engine/mining_engine.rs`

### Step 4: State Updates (Connect/Disconnect)
**Goal**: Update the "World State" (Balances, UTXOs, Rewards) when blocks are added/removed.

**Tasks**:
- **Connect Block**:
  - Remove txs from Mempool.
  - Create `Reward` record (Status: `Immature`).
  - Update user balances (if implementing transfers).
- **Disconnect Block**:
  - Add txs back to Mempool.
  - Mark `Reward` record as `Orphaned` (or delete).
  - Revert user balance changes.

**Files**:
- `src/domain/entities/reward_settlement.rs`
- `src/application/use_cases.rs`

### Step 5: Reward Maturity & Settlement
**Goal**: Enforce that rewards cannot be spent until `N` confirmations.

**Tasks**:
- Define `MATURITY_WINDOW` (e.g., 100 blocks).
- On every `Connect Block`:
  - Check rewards at height `current_height - MATURITY_WINDOW`.
  - Update their status from `Immature` -> `Confirmed`.
  - Credit the miner's "Spendable Balance".
- On `Disconnect Block`:
  - If reverting a block that matured rewards, revert the "Spendable Balance" credit.

**Files**:
- `src/domain/entities/reward_settlement.rs`

### Step 6: Difficulty Adjustment
**Goal**: Adjust PoW target to keep average block time stable.

**Tasks**:
- Define `RETARGET_WINDOW` (e.g., 2016 blocks).
- On block `height % RETARGET_WINDOW == 0`:
  - Calculate `actual_time` = `timestamp(tip) - timestamp(tip - window)`.
  - Calculate `target_time` = `window * target_spacing`.
  - `new_difficulty` = `old_difficulty * (actual / target)`.
  - Apply clamps (e.g., max 4x increase/decrease).

**Files**:
- `src/domain/entities/difficulty_state.rs`

## 5) Database Schema Changes

**New Tables/Fields**:
- `chain_tips`: To track the active tip persistently.
- `block_index`: To store the tree structure (if not fully reconstructing from blocks).
- `rewards`:
  - `id`, `block_hash`, `miner_id`, `amount`, `status` (Immature, Confirmed, Orphaned), `created_at`.

## 6) Testing Strategy

- **Unit Tests**:
  - `test_reorg_find_fork_point`: Verify common ancestor logic.
  - `test_difficulty_retarget`: Verify math and clamping.
- **Integration Tests**:
  - **Fork Scenario**: Manually feed Block A1 and Block B1 (same parent) to engine. Verify tip doesn't change if work is equal. Feed B2. Verify reorg to B chain.
  - **Settlement**: Mine 100 blocks. Verify block 1 reward becomes spendable. Reorg deep (101 blocks - unlikely but testable). Verify reward 1 reverts (if chain overwrites it).

## 7) Risks & Mitigation

- **Risk**: Deep reorgs causing massive DB thrashing.
  - *Mitigation*: Limit reorg depth (checkpointing).
- **Risk**: "Nothing at Stake" in simulation (if we move to PoS later, but for PoW this is fine).
- **Risk**: Complexity of state rollback.
  - *Mitigation*: Strict ordering of operations (Disconnect then Connect).

## 8) Execution Order

1. **Data Structures**: Define `BlockIndex`, `Reward` entities.
2. **Chain Logic**: Implement `find_best_tip` and `fork_point`.
3. **State Transitions**: Implement `connect_block` / `disconnect_block`.
4. **Reorg Manager**: Stitch it together in `reorganize`.
5. **Settlement**: Add maturity logic.
6. **Difficulty**: Add retargeting.
7. **Tests**: Verify scenarios.
