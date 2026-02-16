# Timeline-to-Consensus Architecture

This document describes the implementation order and runtime flow for the mining engine, with a clear separation between:

- Timeline simulation (lower layer)
- Consensus and chain selection (upper layer)

---

## 1) Implementation Roadmap (Design -> Build)

```mermaid
flowchart TD
  A["Phase 0: Define Scope<br/>- Tick model<br/>- Event types<br/>- Determinism goals"] --> B
  B["Phase 1: Timeline Core<br/>- sim_time_ms<br/>- run_single_tick()<br/>- delivery heap"] --> C
  C["Phase 2: Network Simulation<br/>- schedule_delivery()<br/>- latency/jitter model<br/>- handle_delivery()"] --> D
  D["Phase 3: Mempool + Tx Spawn<br/>- try_spawn_tx()<br/>- insert_mempool_tx()<br/>- tx broadcast"] --> E
  E["Phase 4: Mining Loop<br/>- block template build<br/>- hit finding<br/>- block creation"] --> F
  F["Phase 5: Consensus Engine<br/>- cumulative work<br/>- better-chain rule<br/>- fork-point discovery"] --> G
  G["Phase 6: Reorg + State Transition<br/>- disconnect/connect blocks<br/>- active tip update"] --> H
  H["Phase 7: Persistence + Observability<br/>- append_event()<br/>- upsert_block()<br/>- status updates"] --> I
  I["Phase 8: Testing<br/>- deterministic replay<br/>- reorg scenarios<br/>- latency race cases"]
```

---

## 2) Layered Architecture (Timeline Below, Consensus Above)

```mermaid
flowchart TB
  subgraph L1["Layer 1 - Timeline Foundation"]
    T1["Clock: sim_time_ms + tick_interval_ms"]
    T2["Scheduler: delivery_heap (deliver_at_ms)"]
    T3["Tick Loop: run_single_tick()"]
  end

  subgraph L2["Layer 2 - Event / Network Runtime"]
    N1["Command Inbox"]
    N2["Delivery Runtime: schedule + handle"]
    N3["Latency / Jitter Model"]
  end

  subgraph L3["Layer 3 - State Producers"]
    S1["Tx Spawn + Mempool"]
    S2["Mining Attempts + Block Candidate"]
  end

  subgraph L4["Layer 4 - Consensus"]
    C1["Difficulty Adjustment"]
    C2["Chain Selection (better chain)"]
    C3["Fork Point + Reorg"]
  end

  subgraph L5["Layer 5 - Persistence / Projection"]
    P1["Block Index + Active Tip + Rewards"]
    P2["EventRepository + upsert_block()"]
    P3["EngineStatus Projection"]
  end

  T3 --> N1
  T3 --> N2
  T2 --> N2
  N2 --> S1
  N2 --> S2
  S2 --> C1
  S2 --> C2
  C2 --> C3
  C3 --> P1
  P1 --> P2
  T3 --> P3
```

---

## 3) Per-Tick Runtime Sequence

```mermaid
sequenceDiagram
  participant Tick as TickLoop
  participant Time as TimelineClock
  participant Net as DeliveryScheduler
  participant Pool as Mempool/TxSpawn
  participant Mine as Mining
  participant Cons as Consensus
  participant Repo as Repository

  Tick->>Time: advance sim_time_ms
  Tick->>Pool: try_spawn_tx()
  Pool-->>Net: broadcast tx (scheduled deliveries)
  Tick->>Mine: build template + find mining hit

  alt block found
    Mine->>Cons: process_block(candidate)
    Cons->>Cons: compare with current tip
    alt better chain
      Cons->>Cons: reorganize(disconnect/connect)
    end
    Cons->>Repo: upsert_block / update chain state
  end

  Tick->>Net: pop deliveries where deliver_at_ms <= sim_time_ms
  Net->>Cons: handle block/tx delivery
  Tick->>Repo: append events + update status
```

---

## Notes

- Timeline controls *when* events happen.
- Consensus controls *which chain wins*.
- Deterministic RNG makes runs reproducible with the same seed and config.
