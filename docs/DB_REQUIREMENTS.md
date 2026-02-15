# Database Requirements

This document consolidates requirements from the two NotebookLM sources and `docs/CONVENTION.md`.

## Domain scope

- Server-authoritative mining simulation with a PoW-like block flow.
- Persistent PostgreSQL storage for business state and reconciliation.
- Realtime synchronization through event log replay and periodic snapshots.

## Core entities

- `nodes`: player/AI operator state.
- `transactions`: mempool transactions.
- `blocks`: DAG/chain blocks.
- `block_transactions`: block-to-transaction mapping.
- `difficulty_states`: retarget history.
- `reward_settlements`: pending/confirmed/lost rewards.
- `event_log`: ordered event stream.
- `room_snapshots`: snapshot metadata.

## Required constraints

- Unique identifiers for `node_id`, `tx_id`, `block_id`, `seq_id`.
- Referential integrity for chain and reward relationships.
- Data checks:
  - `hash_rate >= 0`
  - `fee_sats >= 0`
  - `confirmations >= 0`
  - `total_weight <= 4000`

## Consistency requirements

- Reward confirmation happens only after K confirmations.
- Reorg updates are atomic for:
  - block chain state
  - reward settlements
  - node tips
- Server is the source of truth for canonical tip and confirmation status.

## Architecture constraints

- Domain and Application layers are pure Rust and independent of DB/Actix.
- Repository traits are in Domain, implementations are in Infrastructure.
- Handlers call use cases only; no direct DB access in Presentation.
