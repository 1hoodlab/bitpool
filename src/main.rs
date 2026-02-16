mod adapter;
mod database;
mod domain;
mod engine;
mod helper;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, ContextFutureSpawner, WrapFuture};
use actix_web::{
    get, web, App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_web_actors::ws;
use env_logger::Env;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use serde::{Deserialize, Serialize};
use serde_json::json;

use adapter::postgres_event_repository::{connect_postgres, SeaOrmEventRepository};
use adapter::postgres_node_loader::load_miner_nodes_from_db;
use domain::entities::{MessageKind, NodeSendCommand};
use domain::repositories::EventRepository;
use engine::mining_engine::{
    start_deterministic_engine_with_config, EngineHandle, MinerNodeProfile, MiningNetworkConfig,
};

const MAX_BOOTSTRAP_BLOCKS_LIMIT: u64 = 1_000;
const WS_POLL_INTERVAL_MS: u64 = 250;
const WS_BATCH_LIMIT: u64 = 500;

#[derive(Clone)]
struct RealtimeConfig {
    target_block_time_ms: u64,
}

struct AppState {
    engine: EngineHandle,
    repo: Arc<SeaOrmEventRepository>,
    realtime: RealtimeConfig,
}

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

#[get("/realtime/test")]
async fn realtime_test_page() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../docs/realtime_test.html"))
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

#[derive(Debug, Deserialize)]
struct RealtimeBootstrapQuery {
    blocks_limit: Option<u64>,
    include_rewards: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RealtimeWsQuery {
    since_seq_id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RealtimeBootstrapResponse {
    server_time_ms: i64,
    current_seq_id: i64,
    network: RealtimeNetworkSnapshot,
    nodes: Vec<RealtimeNodeSnapshot>,
    blocks: Vec<RealtimeBlockSnapshot>,
    rewards: Vec<RealtimeRewardSnapshot>,
}

#[derive(Debug, Serialize)]
struct RealtimeNetworkSnapshot {
    tick_interval_ms: u64,
    target_block_time_ms: u64,
    difficulty: u64,
    total_online_hash_rate: u64,
}

#[derive(Debug, Serialize)]
struct RealtimeNodeSnapshot {
    node_id: String,
    is_online: bool,
    hash_rate: u64,
    has_relay: bool,
    tip_block_id: Option<String>,
    tip_height: i64,
    mempool_size: i64,
    mined_blocks: i64,
    orphaned_blocks: i64,
}

#[derive(Debug, Serialize)]
struct RealtimeBlockSnapshot {
    block_id: String,
    parent_id: Option<String>,
    height: i64,
    miner_node_id: String,
    found_time_ms: i64,
    difficulty: u64,
    cumulative_work: String,
    chain_status: String,
    tx_count: i64,
    fees_sats: i64,
}

#[derive(Debug, Serialize)]
struct RealtimeRewardSnapshot {
    block_id: String,
    miner_node_id: String,
    state: String,
    confirmations: i32,
    settled_at_ms: Option<i64>,
    amount_sats: i64,
}

struct RealtimeWsSession {
    repo: Arc<SeaOrmEventRepository>,
    last_seq_id: i64,
}

impl Actor for RealtimeWsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(
            std::time::Duration::from_millis(WS_POLL_INTERVAL_MS),
            |actor, ctx| {
                let repo = Arc::clone(&actor.repo);
                let after_seq_id = actor.last_seq_id;
                async move { repo.read_events_after(after_seq_id, WS_BATCH_LIMIT).await }
                    .into_actor(actor)
                    .map(|result, actor, ctx| match result {
                        Ok(events) => {
                            for event in events {
                                actor.last_seq_id = event.seq_id;
                                if let Ok(message) = serde_json::to_string(&event) {
                                    ctx.text(message);
                                }
                            }
                        }
                        Err(error) => {
                            ctx.text(
                                json!({
                                    "error": "stream_failed",
                                    "detail": error.to_string(),
                                })
                                .to_string(),
                            );
                            ctx.stop();
                        }
                    })
                    .wait(ctx);
            },
        );
    }
}

impl actix::StreamHandler<Result<ws::Message, ws::ProtocolError>> for RealtimeWsSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Pong(_))
            | Ok(ws::Message::Text(_))
            | Ok(ws::Message::Binary(_))
            | Ok(ws::Message::Continuation(_))
            | Ok(ws::Message::Nop) => {}
            Err(_) => {
                ctx.stop();
            }
        }
    }
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

async fn realtime_bootstrap(
    state: web::Data<AppState>,
    query: web::Query<RealtimeBootstrapQuery>,
) -> impl Responder {
    let blocks_limit = query
        .blocks_limit
        .unwrap_or(200)
        .clamp(1, MAX_BOOTSTRAP_BLOCKS_LIMIT);
    let include_rewards = query.include_rewards.unwrap_or(true);

    let current_seq_id = match state.repo.last_seq_id().await {
        Ok(Some(seq_id)) => seq_id,
        Ok(None) => 0,
        Err(error) => {
            return HttpResponse::InternalServerError().json(json!({
                "error": format!("failed to load current_seq_id: {error}")
            }));
        }
    };

    let nodes_stmt = Statement::from_string(
        DbBackend::Postgres,
        r#"
        SELECT
            n.node_id,
            n.is_online,
            n.hash_rate,
            n.has_relay,
            n.tip_block_id,
            COALESCE(tb.height, 0) AS tip_height,
            COALESCE(n.mined_blocks, 0) AS mined_blocks,
            COALESCE(n.orphaned_blocks, 0) AS orphaned_blocks
        FROM nodes n
        LEFT JOIN blocks tb ON tb.block_id = n.tip_block_id
        ORDER BY n.node_id ASC
        "#
        .to_string(),
    );
    let node_rows = match state.repo.db.query_all(nodes_stmt).await {
        Ok(rows) => rows,
        Err(error) => {
            return HttpResponse::InternalServerError().json(json!({
                "error": format!("failed to load nodes snapshot: {error}")
            }));
        }
    };

    let mut total_online_hash_rate = 0u64;
    let mut nodes = Vec::with_capacity(node_rows.len());
    for row in node_rows {
        let node_id: String = row.try_get("", "node_id").unwrap_or_default();
        let is_online: bool = row.try_get("", "is_online").unwrap_or(false);
        let hash_rate_i64: i64 = row.try_get("", "hash_rate").unwrap_or(0);
        let hash_rate = hash_rate_i64.max(0) as u64;
        if is_online {
            total_online_hash_rate = total_online_hash_rate.saturating_add(hash_rate);
        }
        nodes.push(RealtimeNodeSnapshot {
            node_id,
            is_online,
            hash_rate,
            has_relay: row.try_get("", "has_relay").unwrap_or(false),
            tip_block_id: row.try_get("", "tip_block_id").unwrap_or(None),
            tip_height: row.try_get("", "tip_height").unwrap_or(0),
            mempool_size: 0,
            mined_blocks: row.try_get("", "mined_blocks").unwrap_or(0),
            orphaned_blocks: row.try_get("", "orphaned_blocks").unwrap_or(0),
        });
    }

    let blocks_stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        SELECT
            block_id,
            parent_id,
            height,
            miner_node_id,
            found_time_ms,
            target,
            cumulative_work,
            chain_status,
            fees_sats
        FROM blocks
        ORDER BY height DESC, found_time_ms DESC, block_id DESC
        LIMIT $1
        "#
        .to_string(),
        vec![(blocks_limit as i64).into()],
    );
    let block_rows = match state.repo.db.query_all(blocks_stmt).await {
        Ok(rows) => rows,
        Err(error) => {
            return HttpResponse::InternalServerError().json(json!({
                "error": format!("failed to load blocks snapshot: {error}")
            }));
        }
    };

    let mut blocks = Vec::with_capacity(block_rows.len());
    for row in block_rows {
        let difficulty = row
            .try_get::<String>("", "target")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(1);
        let cumulative_work_i64: i64 = row.try_get("", "cumulative_work").unwrap_or(0);
        blocks.push(RealtimeBlockSnapshot {
            block_id: row.try_get("", "block_id").unwrap_or_default(),
            parent_id: row.try_get("", "parent_id").unwrap_or(None),
            height: row.try_get("", "height").unwrap_or(0),
            miner_node_id: row.try_get("", "miner_node_id").unwrap_or_default(),
            found_time_ms: row.try_get("", "found_time_ms").unwrap_or(0),
            difficulty,
            cumulative_work: cumulative_work_i64.max(0).to_string(),
            chain_status: row
                .try_get::<String>("", "chain_status")
                .unwrap_or_else(|_| "pending".to_string()),
            tx_count: 0,
            fees_sats: row.try_get("", "fees_sats").unwrap_or(0),
        });
    }
    blocks.reverse();

    let network_difficulty = blocks
        .iter()
        .rev()
        .find(|block| block.chain_status == "active")
        .map(|block| block.difficulty)
        .or_else(|| blocks.last().map(|block| block.difficulty))
        .unwrap_or(1);

    let rewards = if include_rewards {
        let rewards_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            SELECT
                block_id,
                miner_node_id,
                state,
                confirmations,
                settled_at_ms,
                amount_sats
            FROM reward_settlements
            ORDER BY id DESC
            LIMIT $1
            "#
            .to_string(),
            vec![(blocks_limit as i64).into()],
        );
        match state.repo.db.query_all(rewards_stmt).await {
            Ok(rows) => rows
                .into_iter()
                .map(|row| RealtimeRewardSnapshot {
                    block_id: row.try_get("", "block_id").unwrap_or_default(),
                    miner_node_id: row.try_get("", "miner_node_id").unwrap_or_default(),
                    state: row
                        .try_get::<String>("", "state")
                        .unwrap_or_else(|_| "immature".to_string()),
                    confirmations: row.try_get("", "confirmations").unwrap_or(0),
                    settled_at_ms: row.try_get("", "settled_at_ms").unwrap_or(None),
                    amount_sats: row.try_get("", "amount_sats").unwrap_or(0),
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                return HttpResponse::InternalServerError().json(json!({
                    "error": format!("failed to load rewards snapshot: {error}")
                }));
            }
        }
    } else {
        Vec::new()
    };

    let runtime_status = state.engine.status().await;
    HttpResponse::Ok().json(RealtimeBootstrapResponse {
        server_time_ms: unix_now_ms(),
        current_seq_id,
        network: RealtimeNetworkSnapshot {
            tick_interval_ms: runtime_status.tick_interval_ms,
            target_block_time_ms: state.realtime.target_block_time_ms,
            difficulty: network_difficulty,
            total_online_hash_rate,
        },
        nodes,
        blocks,
        rewards,
    })
}

async fn realtime_ws(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
    query: web::Query<RealtimeWsQuery>,
) -> Result<HttpResponse, ActixError> {
    if let Some(since_seq_id) = query.since_seq_id {
        if since_seq_id < 0 {
            return Ok(HttpResponse::BadRequest().json(json!({
                "error": "since_seq_id must be >= 0"
            })));
        }
    }

    let last_known_seq_id = match state.repo.last_seq_id().await {
        Ok(Some(seq_id)) => seq_id,
        Ok(None) => 0,
        Err(error) => {
            return Ok(HttpResponse::InternalServerError().json(json!({
                "error": format!("failed to read last_seq_id: {error}")
            })));
        }
    };
    let initial_seq_id = query.since_seq_id.unwrap_or(last_known_seq_id);
    let ws_session = RealtimeWsSession {
        repo: Arc::clone(&state.repo),
        last_seq_id: initial_seq_id,
    };

    ws::start(ws_session, &req, stream)
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
        realtime: RealtimeConfig {
            target_block_time_ms: pow_target_block_time_ms,
        },
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(health)
            .service(realtime_test_page)
            .service(engine_status)
            .route("/engine/send", web::post().to(send_command))
            .route("/nodes", web::post().to(upsert_node))
            .route("/realtime/bootstrap", web::get().to(realtime_bootstrap))
            .route("/realtime/ws", web::get().to(realtime_ws))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
