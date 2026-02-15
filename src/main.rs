mod adapter;
mod database;
mod domain;
mod engine;
mod helper;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use env_logger::Env;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use serde::{Deserialize, Serialize};
use serde_json::json;

use adapter::postgres_event_repository::{connect_postgres, SeaOrmEventRepository};
use adapter::postgres_node_loader::load_miner_nodes_from_db;
use domain::entities::{MessageKind, NodeSendCommand};
use engine::mining_engine::{
    start_deterministic_engine_with_config, EngineHandle, MinerNodeProfile, MiningNetworkConfig,
};

struct AppState {
    engine: EngineHandle,
    repo: Arc<SeaOrmEventRepository>,
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

#[get("/engine/status")]
async fn engine_status(state: web::Data<AppState>) -> impl Responder {
    let status = state.engine.status().await;
    HttpResponse::Ok().json(status)
}

#[derive(Debug, Deserialize)]
struct SendCommandRequest {
    from: String,
    to: String,
    kind: String,
    payload: serde_json::Value,
    latency_override_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UpsertNodeRequest {
    node_id: String,
    hash_rate: u64,
    is_online: Option<bool>,
    has_relay: Option<bool>,
}

#[derive(Debug, Serialize)]
struct UpsertNodeResponse {
    node_id: String,
    hash_rate: u64,
    is_online: bool,
    has_relay: bool,
    hot_applied: bool,
}

async fn send_command(
    state: web::Data<AppState>,
    body: web::Json<SendCommandRequest>,
) -> impl Responder {
    let kind = match body.kind.as_str() {
        "block" => MessageKind::Block,
        "tx" => MessageKind::Tx,
        "inv" => MessageKind::Inv,
        "ping" => MessageKind::Ping,
        _ => {
            return HttpResponse::BadRequest().json(json!({
                "error": "invalid kind. expected one of: block, tx, inv, ping"
            }))
        }
    };

    let cmd = NodeSendCommand {
        from: body.from.clone(),
        to: body.to.clone(),
        kind,
        payload: body.payload.clone(),
        latency_override_ms: body.latency_override_ms,
    };
    match state.engine.enqueue_send(cmd) {
        Ok(_) => HttpResponse::Accepted().json(json!({"queued": true})),
        Err(e) => HttpResponse::InternalServerError().json(json!({ "error": e.to_string() })),
    }
}

async fn upsert_node(
    state: web::Data<AppState>,
    body: web::Json<UpsertNodeRequest>,
) -> impl Responder {
    if body.node_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({
            "error": "node_id is required"
        }));
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let is_online = body.is_online.unwrap_or(true);
    let has_relay = body.has_relay.unwrap_or(false);
    let hash_rate_i64 = body.hash_rate.min(i64::MAX as u64) as i64;

    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO nodes (node_id, hash_rate, is_online, has_relay, updated_at_ms, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (node_id)
        DO UPDATE SET
            hash_rate = EXCLUDED.hash_rate,
            is_online = EXCLUDED.is_online,
            has_relay = EXCLUDED.has_relay,
            updated_at_ms = EXCLUDED.updated_at_ms
        "#
        .to_string(),
        vec![
            body.node_id.clone().into(),
            hash_rate_i64.into(),
            is_online.into(),
            has_relay.into(),
            now_ms.into(),
            now_ms.into(),
        ],
    );

    if let Err(e) = state.repo.db.execute(stmt).await {
        return HttpResponse::InternalServerError().json(json!({
            "error": format!("failed to upsert node in database: {e}")
        }));
    }

    let profile = MinerNodeProfile {
        node_id: body.node_id.clone(),
        hash_rate: body.hash_rate,
        is_online,
        has_relay,
    };
    if let Err(e) = state.engine.upsert_miner_node(profile) {
        return HttpResponse::InternalServerError().json(json!({
            "error": format!("failed to apply node into engine runtime: {e}")
        }));
    }

    HttpResponse::Created().json(UpsertNodeResponse {
        node_id: body.node_id.clone(),
        hash_rate: body.hash_rate,
        is_online,
        has_relay,
        hot_applied: true,
    })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(Env::default().default_filter_or("info,sqlx::query=off")).init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .unwrap_or(8080);
    let database_url = std::env::var("DATABASE_URL").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "DATABASE_URL is not set. Set it in .env or environment.",
        )
    })?;
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
    let network_config = MiningNetworkConfig {
        enabled: mining_network_enabled,
        tx_spawn_chance_per_thousand,
        max_block_weight,
        target_block_time_ms: pow_target_block_time_ms,
        propagation_base_delay_ms,
        propagation_jitter_ms,
        miner_nodes,
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
    let app_state = web::Data::new(AppState {
        engine,
        repo: Arc::clone(&event_repo),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(health)
            .service(engine_status)
            .route("/engine/send", web::post().to(send_command))
            .route("/nodes", web::post().to(upsert_node))
    })
        .bind((host.as_str(), port))?
        .run()
        .await
}