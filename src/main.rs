mod adapter;
mod database;
mod domain;
mod engine;
mod helper;

use std::sync::Arc;

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use env_logger::Env;
use serde::Deserialize;
use serde_json::json;

use adapter::postgres_event_repository::{connect_postgres, SeaOrmEventRepository};
use domain::entities::{MessageKind, NodeSendCommand};
use engine::mining_engine::{start_deterministic_engine, EngineHandle};

struct AppState {
    engine: EngineHandle,
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

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

    let db = connect_postgres(&database_url).await.map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("failed to connect postgres: {e}"),
        )
    })?;
    let event_repo = Arc::new(SeaOrmEventRepository { db });
    let engine = start_deterministic_engine(
        event_repo,
        tick_interval_ms,
        base_seed,
        max_inbox_drain_per_tick,
    )
    .await
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let app_state = web::Data::new(AppState { engine });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(health)
            .service(engine_status)
            .route("/engine/send", web::post().to(send_command))
    })
        .bind((host.as_str(), port))?
        .run()
        .await
}