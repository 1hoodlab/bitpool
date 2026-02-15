# Database Observability

## Logging dimensions

- `request_id`
- `room_id`
- `seq_id`
- `operation` (append_block, reorg, retarget)

## Metrics

- `db_query_latency_ms_p95`
- `event_append_rate_per_sec`
- `block_acceptance_rate`
- `reorg_count`
- `reward_settlement_latency_ms`
- `tip_lag_blocks` (`current_tip_height - confirmed_tip_height`)

## Alert conditions

- Query p95 latency above threshold for 5m.
- Reorg count spikes above expected baseline.
- Tip lag exceeds configured threshold.
- Failed transaction rate exceeds threshold.

## Post-deploy verification checklist

- Baseline migration applied successfully.
- Expected indexes exist and are used in explain plans.
- Reward settlement transaction path succeeds in staging.
- Reorg simulation updates chain status, node tips, and rewards atomically.
- Snapshot + replay reproduces deterministic state.
