```
flowchart TB
  %% Layer 1: API / Entry
  subgraph L1["Layer 1 - Public API / Entry"]
    A1["start_deterministic_engine(_with_config)"]
    A2["EngineHandle<br/>- enqueue_send()<br/>- upsert_miner_node()<br/>- status()"]
  end

  %% Layer 2: Runtime Core
  subgraph L2["Layer 2 - Runtime Core"]
    B1["DeterministicEngineCore<State>"]
    B2["run_single_tick()"]
    B3["command_rx (EngineCommand)"]
    B4["delivery_heap (ScheduledDelivery)"]
    B5["update_status() -> EngineStatus"]
  end

  %% Layer 3: Network Simulation
  subgraph L3["Layer 3 - P2P Simulation"]
    C1["schedule_delivery() / schedule_delivery_internal()"]
    C2["emit_net_delivery_event()"]
    C3["handle_delivery(Tx | Block)"]
    C4["broadcast_to_online_nodes()"]
    C5["sample_latency_ms()<br/>sample_propagation_delay_ms()"]
  end

  %% Layer 4: Mining & Mempool
  subgraph L4["Layer 4 - Mining + Mempool"]
    D1["try_spawn_tx()"]
    D2["build_spawned_tx()"]
    D3["mempool (MempoolTx)"]
    D4["build_block_template()"]
    D5["try_mine_block()"]
    D6["find_mining_hit()<br/>trials_for_tick()<br/>mini_pow_hash()"]
  end

  %% Layer 5: Consensus & Chain State
  subgraph L5["Layer 5 - Consensus / Chain"]
    E1["process_block()"]
    E2["reorganize()"]
    E3["connect_block() / disconnect_block()"]
    E4["block_index + active_tip + node_tips"]
    E5["ConsensusService<br/>- calculate_next_difficulty<br/>- calculate_cumulative_work<br/>- is_better_chain<br/>- find_fork_point"]
    E6["rewards (Immature/Confirmed/Orphaned)"]
  end

  %% Layer 6: Persistence / Events
  subgraph L6["Layer 6 - Persistence"]
    F1["emit_event(EventEnvelope)"]
    F2["EventRepository<br/>append_event(), load_chain_state(), upsert_block(), ..."]
  end

  %% Cross-cutting
  G1["Deterministic RNG<br/>splitmix64 / xorshift64 / hash_str64"]

  A1 --> B1
  A2 --> B3
  B1 --> B2
  B2 --> C1
  B2 --> C2
  B2 --> D1
  B2 --> D5
  B2 --> B5

  C1 --> B4
  B4 --> C2
  C2 --> C3
  C3 --> D3
  D1 --> D2
  D2 --> D3
  D1 --> C4
  D5 --> D4
  D5 --> D6
  D5 --> E1
  C3 --> E1

  E1 --> E2
  E2 --> E3
  E1 --> E4
  E1 --> E5
  E3 --> E6

  B2 --> F1
  C2 --> F1
  D1 --> F1
  D5 --> F1
  E1 --> F2
  E3 --> F2
  F1 --> F2

  G1 --> C5
  G1 --> D1
  G1 --> D6
```