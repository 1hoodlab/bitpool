# Business Invariants

## Chain and block validity

- `total_weight <= 4000` must hold for all blocks.
- `parent_id` must reference an existing block except genesis.
- Best tip selection uses `(cumulative_work DESC, first_seen_seq_id ASC)`.

## Reward settlement

- Rewards start in `Pending`.
- Rewards move to `Confirmed` only when block reaches K confirmations on canonical chain.
- Reorg can move rewards from `Pending/Confirmed` to `Lost` when block leaves canonical chain.
- Reward updates must be transactional with chain status updates.

## Node tip consistency

- `nodes.tip_block_id` must reference the node-local best chain tip.
- During reorg, all affected node tips must update atomically with reward and block status changes.

## Event and snapshot consistency

- `event_log.seq_id` is globally increasing per room for deterministic replay.
- Snapshots are cut at a known `snapshot_seq_id`.
- Replay from `event_log(room_id, seq_id)` after snapshot must reconstruct equivalent state.
