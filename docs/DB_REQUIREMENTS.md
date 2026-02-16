# Database Requirements

This document consolidates requirements from the two NotebookLM sources and `docs/CONVENTION.md`.

## Domain scope

- Server-authoritative mining simulation with a PoW-like block flow.
- Persistent PostgreSQL storage for business state and reconciliation.
- Realtime synchronization through event log replay.

## Core entities

- `nodes`: player/AI operator state.
- `blocks`: DAG/chain blocks.
- `reward_settlements`: immature/confirmed/orphaned reward lifecycle.
- `event_log`: ordered global event stream.

## Required constraints

- Unique identifiers for `node_id`, `block_id`, `seq_id`.
- Referential integrity for chain and reward relationships.
- Data checks:
  - `hash_rate >= 0`
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
