# Business Invariants

## Chain and block validity

- `total_weight <= 4000` must hold for all blocks.
- `parent_id` must reference an existing block except genesis.
- Best tip selection uses `(cumulative_work DESC, first_seen_seq_id ASC)`.

## Reward settlement

- Rewards start in `Immature`.
- Rewards move to `Confirmed` only when block reaches K confirmations on canonical chain.
- Reorg can move rewards from `Immature/Confirmed` to `Orphaned` when block leaves canonical chain.
- Reward updates must be transactional with chain status updates.

## Node tip consistency

- `nodes.tip_block_id` must reference the node-local best chain tip.
- During reorg, all affected node tips must update atomically with reward and block status changes.

## Event consistency

- `event_log.seq_id` is globally increasing for deterministic replay.
- Replay from `event_log(seq_id)` must reconstruct equivalent state.
