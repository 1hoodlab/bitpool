mod adapter;
mod api;
mod database;
mod domain;
mod engine;
mod helper;

use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use env_logger::Env;

use adapter::postgres_event_repository::{connect_postgres, SeaOrmEventRepository};
use adapter::postgres_node_loader::load_miner_nodes_from_db;
use adapter::postgres_peer_edge_loader::load_peer_latencies_from_db;
use api::routes::{
    create_node, get_chain_tip, get_global_stats, get_nation_stats, get_network_stats, get_status,
    healthz, update_nation_policy, update_nation_status, update_node_hashrate, ApiState,
};
use engine::mining_engine::{
    start_deterministic_engine_with_config, MinerNodeProfile, MiningNetworkConfig,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(Env::default().default_filter_or("info,sqlx::query=off")).init();

    let database_url = std::env::var("DATABASE_URL").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "DATABASE_URL is not set. Set it in .env or environment.",
        )
    })?;
    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8080);
    let tick_interval_ms = std::env::var("ENGINE_TICK_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(100);
    let base_seed = std::env::var("ENGINE_BASE_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(123_456);
    let max_inbox_drain_per_tick = std::env::var("ENGINE_MAX_INBOX_DRAIN_PER_TICK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_024);
    let mining_network_enabled = std::env::var("MINING_NETWORK_ENABLED")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .or_else(|| {
            std::env::var("PHASE2_ENABLED")
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
        })
        .unwrap_or(true);
    let tx_spawn_chance_per_thousand = std::env::var("TX_SPAWN_CHANCE_PER_THOUSAND")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(120);
    let max_block_weight = std::env::var("MAX_BLOCK_WEIGHT")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(4_000);
    let pow_target_block_time_ms = std::env::var("POW_TARGET_BLOCK_TIME_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(15_000);
    let propagation_base_delay_ms = std::env::var("PROPAGATION_BASE_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10);
    let propagation_jitter_ms = std::env::var("PROPAGATION_JITTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(40);

    let db = connect_postgres(&database_url).await.map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("failed to connect postgres: {e}"),
        )
    })?;
    let event_repo = Arc::new(SeaOrmEventRepository { db });
    let miner_nodes = load_miner_nodes_from_db(&event_repo.db)
        .await
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to load nodes from database: {e}"),
            )
        })?
        .into_iter()
        .map(|node| MinerNodeProfile {
            node_id: node.node_id,
            hash_rate: node.hash_rate,
            is_online: node.is_online,
            has_relay: node.has_relay,
        })
        .collect::<Vec<_>>();
    let peer_latencies_ms = load_peer_latencies_from_db(&event_repo.db)
        .await
        .map_err(|e| std::io::Error::other(format!("failed to load peer edges from database: {e}")))?;
    let network_config = MiningNetworkConfig {
        enabled: mining_network_enabled,
        tx_spawn_chance_per_thousand,
        max_block_weight,
        target_block_time_ms: pow_target_block_time_ms,
        propagation_base_delay_ms,
        propagation_jitter_ms,
        miner_nodes,
        peer_latencies_ms,
    };

    let engine = start_deterministic_engine_with_config(
        Arc::clone(&event_repo),
        tick_interval_ms,
        base_seed,
        max_inbox_drain_per_tick,
        network_config,
    )
    .await
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    let api_state = web::Data::new(ApiState {
        engine,
        db: event_repo.db.clone(),
    });
    log::info!("Blockchain engine started. API listening on {}:{}", host, port);

    HttpServer::new(move || {
        App::new()
            .app_data(api_state.clone())
            .service(healthz)
            .service(create_node)
            .service(update_node_hashrate)
            .service(update_nation_policy)
            .service(update_nation_status)
            .service(get_status)
            .service(get_chain_tip)
            .service(get_network_stats)
            .service(get_nation_stats)
            .service(get_global_stats)
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
