# Realtime UI Data Contract (v1)

This document defines the minimum backend contract needed for a realtime UI that shows:

- node status and metrics
- inter-node communication flow
- block graph and chain reorg behavior
- reward maturity lifecycle

## Goals

- Fast initial render (single bootstrap request)
- Low-latency updates (event stream)
- Robust reconnect without losing events
- Decoupled UI store model

## Transport

- Bootstrap: HTTP `GET`
- Realtime stream: WebSocket (preferred) or SSE
- Event ordering: strictly increasing `seq_id`

## 1) Bootstrap Endpoint

`GET /realtime/bootstrap`

Query params:

- `blocks_limit` (optional, default `200`)
- `include_rewards` (optional, default `true`)

Response:

```json
{
  "server_time_ms": 1739761000000,
  "current_seq_id": 184523,
  "network": {
    "tick_interval_ms": 100,
    "target_block_time_ms": 15000,
    "difficulty": 42,
    "total_online_hash_rate": 125000
  },
  "nodes": [
    {
      "node_id": "node-11",
      "is_online": true,
      "hash_rate": 1000,
      "has_relay": false,
      "tip_block_id": "blk-184-121782",
      "tip_height": 184,
      "mempool_size": 23,
      "mined_blocks": 58,
      "orphaned_blocks": 4
    }
  ],
  "blocks": [
    {
      "block_id": "blk-184-121782",
      "parent_id": "blk-183-121544",
      "height": 184,
      "miner_node_id": "node-13",
      "found_time_ms": 1739760999123,
      "difficulty": 42,
      "cumulative_work": "7561",
      "chain_status": "active",
      "tx_count": 12,
      "fees_sats": 37273
    }
  ],
  "rewards": [
    {
      "block_id": "blk-175-118229",
      "miner_node_id": "node-13",
      "state": "confirmed",
      "confirmations": 10,
      "settled_at_ms": 1739760980000,
      "amount_sats": 5004979
    }
  ]
}
```

## 2) Stream Endpoint

Recommended:

- `GET /realtime/ws?since_seq_id=<N>` (WebSocket)

Alternative:

- `GET /realtime/sse?since_seq_id=<N>` (SSE)

Server rules:

- If `since_seq_id` is provided, replay events where `seq_id > since_seq_id`.
- Then continue live streaming.
- Keep event order by `seq_id`.

## 3) Event Envelope

All stream messages use one envelope:

```json
{
  "seq_id": 184524,
  "server_time_ms": 1739761000100,
  "event_type": "pow.block_found.v1",
  "payload": {}
}
```

## 4) Event Types for UI

Persisted business events (already in use):

- `mempool.tx_added.v1`
- `pow.block_found.v1`
- `reward.matured.v1`
- `chain.reorg.v1`

Recommended realtime-only UI events (can be memory-only):

- `node.status.updated.v1`
- `node.tip.updated.v1`
- `net.message.sent.v1`
- `net.message.delivered.v1`
- `block.connected.v1`
- `block.disconnected.v1`
- `reward.state.updated.v1`

## 5) Required Payload Shapes

### `mempool.tx_added.v1`

```json
{
  "tx_id": "tx-123",
  "fee_sats": 1530,
  "weight": 420,
  "created_at_ms": 1739761000100,
  "origin_node": "node-11"
}
```

### `pow.block_found.v1`

```json
{
  "block_id": "blk-185-121900",
  "miner_node_id": "node-11",
  "height_candidate": 185,
  "template_tx_ids": ["tx-1", "tx-2"],
  "sim_time_ms": 1739761000200,
  "target": 1099511627775,
  "winning_hash": 10024,
  "hit_trial_offset": 31
}
```

### `reward.matured.v1`

```json
{
  "reward_id": "reward-blk-175-118229",
  "miner_id": "node-13",
  "amount_sats": 5004979,
  "block_height": 185
}
```

### `chain.reorg.v1`

```json
{
  "old_tip": "blk-190-a",
  "new_tip": "blk-190-b",
  "fork_point": "blk-183-x",
  "height": 190,
  "is_reorg": true,
  "disconnect_count": 7,
  "connect_count": 7
}
```

### `net.message.delivered.v1` (recommended realtime-only)

```json
{
  "msg_id": "node-1:node-2:block:9981",
  "from": "node-1",
  "to": "node-2",
  "kind": "block",
  "deliver_at_ms": 1739761000300,
  "latency_ms": 14
}
```

## 6) Frontend Store Model

Use normalized maps for fast updates:

- `nodesById: Map<string, NodeState>`
- `blocksById: Map<string, BlockState>`
- `childrenByParentId: Map<string, string[]>`
- `messagesById: Map<string, NetMessage>`
- `rewardsByBlockId: Map<string, RewardState>`
- `lastSeqId: number`

Derived selectors:

- canonical path from active tip
- node communication heat (messages per 10s)
- orphan/reorg counters
- reward maturity progress (`confirmations / threshold`)

## 7) Reconnect Rules

- Client keeps `lastSeqId`.
- On reconnect, request `since_seq_id=lastSeqId`.
- If server cannot replay that far (retention gap), return:

```json
{
  "error": "replay_gap",
  "next_action": "bootstrap_required"
}
```

Client then:

1. fetches `/realtime/bootstrap`
2. reconnects stream with returned `current_seq_id`

## 8) Suggested Retention Policy

- Persisted events: keep full history in `event_log` (or archive by partition)
- Realtime-only high-volume events:
  - keep in memory ring buffer (30s-120s)
  - optionally persist aggregate metrics only

## 9) Versioning

- Keep suffix `.v1` in `event_type`
- Introduce new shape via `.v2` (do not break old payload contract)
