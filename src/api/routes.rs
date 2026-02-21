use actix_web::{get, patch, post, web, HttpResponse, Responder};
use rand::seq::SliceRandom;
use rand::Rng;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::dto::{
    ChainTipResponse, CommandAcceptedResponse, CreateNodeRequest, GlobalStatsResponse,
    NationStatsResponse, NetworkStatsResponse, UpdateNationPolicyRequest,
    UpdateNationStatusRequest, UpdateNodeHashrateRequest,
};
use crate::engine::mining_engine::{MinerNodeProfile, PeerEdgeProfile};

#[derive(Clone)]
pub struct ApiState {
    pub engine: crate::engine::mining_engine::EngineHandle,
    pub db: DatabaseConnection,
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse nation_id from node_id format: nation-{nation_id}-{owner_id} or nation-{nation_id}
fn parse_nation_id_from_node_id(node_id: &str) -> Option<String> {
    let parts: Vec<&str> = node_id.splitn(3, '-').collect();
    if parts.len() >= 2 && parts[0] == "nation" && !parts[1].is_empty() {
        Some(parts[1].to_string())
    } else {
        None
    }
}

async fn register_command_once(
    db: &DatabaseConnection,
    command_id: &str,
    command_type: &str,
    scope_id: &str,
) -> Result<bool, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO processed_commands (command_id, command_type, nation_id, received_at_ms)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (command_id) DO NOTHING
        "#
        .to_string(),
        vec![
            command_id.to_string().into(),
            command_type.to_string().into(),
            scope_id.to_string().into(),
            unix_now_ms().into(),
        ],
    );
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected() > 0)
}

async fn create_node_row(
    db: &DatabaseConnection,
    node_id: &str,
    nation_id: Option<&str>,
    hash_rate: u64,
    is_online: bool,
    has_relay: bool,
    now_ms: i64,
) -> Result<u64, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO nodes (
            node_id,
            is_ai,
            hash_rate,
            is_online,
            has_relay,
            latency_ms,
            mined_blocks,
            orphaned_blocks,
            version,
            created_at_ms,
            updated_at_ms,
            nation_id
        )
        VALUES ($1, FALSE, $2, $3, $4, NULL, 0, 0, 0, $5, $5, $6)
        "#
        .to_string(),
        vec![
            node_id.to_string().into(),
            (hash_rate as i64).into(),
            is_online.into(),
            has_relay.into(),
            now_ms.into(),
            nation_id.map(|s| s.to_string()).into(),
        ],
    );
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

async fn update_node_hash_rate_row(
    db: &DatabaseConnection,
    node_id: &str,
    hash_rate: u64,
    now_ms: i64,
) -> Result<u64, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        UPDATE nodes
        SET hash_rate = $2, updated_at_ms = $3
        WHERE node_id = $1
        "#
        .to_string(),
        vec![node_id.to_string().into(), (hash_rate as i64).into(), now_ms.into()],
    );
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

async fn load_node_runtime_profile(
    db: &DatabaseConnection,
    node_id: &str,
) -> Result<Option<(bool, bool)>, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        SELECT is_online, has_relay
        FROM nodes
        WHERE node_id = $1
        LIMIT 1
        "#
        .to_string(),
        vec![node_id.to_string().into()],
    );
    let maybe_row = db.query_one(stmt).await?;
    let Some(row) = maybe_row else {
        return Ok(None);
    };
    let is_online: bool = row.try_get("", "is_online")?;
    let has_relay: bool = row.try_get("", "has_relay")?;
    Ok(Some((is_online, has_relay)))
}

async fn pick_partial_mesh_peers(
    db: &DatabaseConnection,
    node_id: &str,
) -> Result<Vec<String>, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT node_id FROM nodes WHERE node_id <> $1 ORDER BY node_id ASC".to_string(),
        vec![node_id.to_string().into()],
    );
    let rows = db.query_all(stmt).await?;
    let mut peers = rows
        .into_iter()
        .filter_map(|row| row.try_get::<String>("", "node_id").ok())
        .collect::<Vec<_>>();
    if peers.is_empty() {
        return Ok(Vec::new());
    }

    let mut rng = rand::thread_rng();
    peers.shuffle(&mut rng);
    let max_pick = peers.len().min(8);
    let min_pick = peers.len().min(3);
    let pick = if min_pick == max_pick {
        min_pick
    } else {
        rng.gen_range(min_pick..=max_pick)
    };
    peers.truncate(pick);
    Ok(peers)
}

async fn upsert_peer_edge_row(
    db: &DatabaseConnection,
    from: &str,
    to: &str,
    latency_ms: u64,
) -> Result<(), sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO peer_edges (from_node_id, to_node_id, latency_ms)
        VALUES ($1, $2, $3)
        ON CONFLICT (from_node_id, to_node_id)
        DO UPDATE SET latency_ms = EXCLUDED.latency_ms
        "#
        .to_string(),
        vec![
            from.to_string().into(),
            to.to_string().into(),
            (latency_ms as i32).into(),
        ],
    );
    db.execute(stmt).await?;
    Ok(())
}

#[get("/healthz")]
pub async fn healthz() -> impl Responder {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}

#[post("/v1/commands/nodes")]
pub async fn create_node(
    state: web::Data<ApiState>,
    body: web::Json<CreateNodeRequest>,
) -> impl Responder {
    if body.node_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "node_id is required" }));
    }
    if body.command_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "command_id is required" }));
    }
    let nation_scope = parse_nation_id_from_node_id(&body.node_id).unwrap_or_else(|| body.node_id.clone());
    let is_new = match register_command_once(
        &state.db,
        &body.command_id,
        "CreateNode",
        &nation_scope,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if !is_new {
        return HttpResponse::Accepted().json(CommandAcceptedResponse {
            accepted: true,
            command_id: body.command_id.clone(),
            applied: false,
        });
    }

    let node_id = body.node_id.trim().to_string();
    let now_ms = unix_now_ms();
    let is_online = body.is_online.unwrap_or(true);
    let has_relay = body.has_relay.unwrap_or(false);
    let nation_id = parse_nation_id_from_node_id(&node_id);
    let created = match create_node_row(
        &state.db,
        &node_id,
        nation_id.as_deref(),
        body.hash_rate,
        is_online,
        has_relay,
        now_ms,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if created == 0 {
        return HttpResponse::Conflict().json(json!({ "error": "node already exists" }));
    }

    let peers = match pick_partial_mesh_peers(&state.db, &node_id).await {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    let mut rng = rand::thread_rng();
    for peer in peers {
        let latency_ab = rng.gen_range(10_u64..=120_u64);
        let latency_ba = rng.gen_range(10_u64..=120_u64);

        if let Err(err) = upsert_peer_edge_row(&state.db, &node_id, &peer, latency_ab).await {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
        if let Err(err) = upsert_peer_edge_row(&state.db, &peer, &node_id, latency_ba).await {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }

        if let Err(err) = state.engine.upsert_peer_edge(PeerEdgeProfile {
            from_node_id: node_id.clone(),
            to_node_id: peer.clone(),
            latency_ms: latency_ab,
        }) {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
        if let Err(err) = state.engine.upsert_peer_edge(PeerEdgeProfile {
            from_node_id: peer.clone(),
            to_node_id: node_id.clone(),
            latency_ms: latency_ba,
        }) {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    }

    let profile = MinerNodeProfile {
        node_id: node_id.clone(),
        hash_rate: body.hash_rate,
        is_online,
        has_relay,
    };
    if let Err(err) = state.engine.upsert_miner_node(profile) {
        return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
    }

    log::info!(
        "accepted CreateNode node_id={}, version={}, command_id={}",
        node_id,
        body.version,
        body.command_id,
    );

    HttpResponse::Accepted().json(CommandAcceptedResponse {
        accepted: true,
        command_id: body.command_id.clone(),
        applied: false,
    })
}

#[patch("/v1/commands/nodes/{node_id}/hashrate")]
pub async fn update_node_hashrate(
    state: web::Data<ApiState>,
    path: web::Path<String>,
    body: web::Json<UpdateNodeHashrateRequest>,
) -> impl Responder {
    let node_id = path.into_inner();
    if node_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "node_id is required" }));
    }
    if body.command_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "command_id is required" }));
    }
    let is_new = match register_command_once(
        &state.db,
        &body.command_id,
        "UpdateNodeHashrate",
        &node_id,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if !is_new {
        return HttpResponse::Accepted().json(CommandAcceptedResponse {
            accepted: true,
            command_id: body.command_id.clone(),
            applied: false,
        });
    }

    let now_ms = unix_now_ms();
    let updated = match update_node_hash_rate_row(&state.db, &node_id, body.hash_rate, now_ms).await {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if updated == 0 {
        return HttpResponse::NotFound().json(json!({ "error": "node not found" }));
    }

    let (is_online, has_relay) = match load_node_runtime_profile(&state.db, &node_id).await {
        Ok(Some(v)) => v,
        Ok(None) => {
            return HttpResponse::NotFound().json(json!({ "error": "node not found" }));
        }
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };

    let profile = MinerNodeProfile {
        node_id: node_id.clone(),
        hash_rate: body.hash_rate,
        is_online,
        has_relay,
    };
    if let Err(err) = state.engine.upsert_miner_node(profile) {
        return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
    }

    log::info!(
        "accepted UpdateNodeHashrate node_id={}, version={}, command_id={}",
        node_id,
        body.version,
        body.command_id
    );

    HttpResponse::Accepted().json(CommandAcceptedResponse {
        accepted: true,
        command_id: body.command_id.clone(),
        applied: false,
    })
}

#[post("/v1/commands/nations/{nation_id}/policy")]
pub async fn update_nation_policy(
    state: web::Data<ApiState>,
    path: web::Path<String>,
    body: web::Json<UpdateNationPolicyRequest>,
) -> impl Responder {
    let nation_id = path.into_inner();
    if nation_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "nation_id is required" }));
    }
    if body.command_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "command_id is required" }));
    }
    let is_new = match register_command_once(
        &state.db,
        &body.command_id,
        "UpdateNationPolicy",
        &nation_id,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if !is_new {
        return HttpResponse::Accepted().json(CommandAcceptedResponse {
            accepted: true,
            command_id: body.command_id.clone(),
            applied: false,
        });
    }

    // Policy command is accepted at API layer; domain behavior can be added later.
    log::info!(
        "accepted UpdateNationPolicy nation_id={}, policy_type={}, version={}, command_id={}, params={}",
        nation_id,
        body.policy_type,
        body.version,
        body.command_id,
        body.params
    );

    HttpResponse::Accepted().json(CommandAcceptedResponse {
        accepted: true,
        command_id: body.command_id.clone(),
        applied: false,
    })
}

#[post("/v1/commands/nations/{nation_id}/status")]
pub async fn update_nation_status(
    state: web::Data<ApiState>,
    path: web::Path<String>,
    body: web::Json<UpdateNationStatusRequest>,
) -> impl Responder {
    let nation_id = path.into_inner();
    if nation_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "nation_id is required" }));
    }
    if body.command_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "command_id is required" }));
    }
    let is_new = match register_command_once(
        &state.db,
        &body.command_id,
        "UpdateNationStatus",
        &nation_id,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    if !is_new {
        return HttpResponse::Accepted().json(CommandAcceptedResponse {
            accepted: true,
            command_id: body.command_id.clone(),
            applied: false,
        });
    }

    // Status command is accepted at API layer; domain behavior can be added later.
    log::info!(
        "accepted UpdateNationStatus nation_id={}, status={}, version={}, command_id={}, reason={:?}",
        nation_id,
        body.status,
        body.version,
        body.command_id,
        body.reason
    );

    HttpResponse::Accepted().json(CommandAcceptedResponse {
        accepted: true,
        command_id: body.command_id.clone(),
        applied: false,
    })
}

#[get("/v1/status")]
pub async fn get_status(state: web::Data<ApiState>) -> impl Responder {
    let status = state.engine.status().await;
    HttpResponse::Ok().json(status)
}

#[get("/v1/chain/tip")]
pub async fn get_chain_tip(state: web::Data<ApiState>) -> impl Responder {
    let status = state.engine.status().await;
    HttpResponse::Ok().json(ChainTipResponse {
        block_id: status.last_block_found_id,
        height: status.last_block_height,
    })
}

#[get("/v1/network/stats")]
pub async fn get_network_stats(state: web::Data<ApiState>) -> impl Responder {
    let status = state.engine.status().await;
    HttpResponse::Ok().json(NetworkStatsResponse {
        online_nodes: status.online_nodes,
        mempool_size: status.mempool_size,
        pending_inbox: status.pending_inbox,
        pending_deliveries: status.pending_deliveries,
        tick_interval_ms: status.tick_interval_ms,
    })
}

#[get("/v1/stats/nations/{nation_id}")]
pub async fn get_nation_stats(
    state: web::Data<ApiState>,
    path: web::Path<String>,
) -> impl Responder {
    let nation_id = path.into_inner();
    if nation_id.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({ "error": "nation_id is required" }));
    }
    // Match by nation_id column (preferred) or node_id format: nation-{nation_id} or nation-{nation_id}-{owner_id}
    let exact_prefix = format!("nation-{}", nation_id);
    let prefix_with_dash = format!("nation-{}-", nation_id);
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        SELECT
            COALESCE(SUM(rs.amount_sats), 0)::BIGINT AS total_mined_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'confirmed' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS confirmed_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'immature' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS immature_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'orphaned' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS orphaned_sats,
            COALESCE(COUNT(rs.block_id), 0) AS reward_count
        FROM reward_settlements rs
        INNER JOIN nodes n ON n.node_id = rs.miner_node_id
        WHERE n.nation_id = $1 OR n.node_id = $2 OR n.node_id LIKE $3
        "#
        .to_string(),
        vec![
            nation_id.clone().into(),
            exact_prefix.clone().into(),
            format!("{}%", prefix_with_dash).into(),
        ],
    );
    let row = match state.db.query_one(stmt).await {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    let Some(row) = row else {
        return HttpResponse::Ok().json(NationStatsResponse {
            nation_id,
            total_mined_sats: 0,
            confirmed_sats: 0,
            immature_sats: 0,
            orphaned_sats: 0,
            reward_count: 0,
        });
    };

    let total_mined_sats: i64 = row.try_get("", "total_mined_sats").unwrap_or(0);
    let confirmed_sats: i64 = row.try_get("", "confirmed_sats").unwrap_or(0);
    let immature_sats: i64 = row.try_get("", "immature_sats").unwrap_or(0);
    let orphaned_sats: i64 = row.try_get("", "orphaned_sats").unwrap_or(0);
    let reward_count: i64 = row.try_get("", "reward_count").unwrap_or(0);

    HttpResponse::Ok().json(NationStatsResponse {
        nation_id,
        total_mined_sats,
        confirmed_sats,
        immature_sats,
        orphaned_sats,
        reward_count,
    })
}

#[get("/v1/stats/global")]
pub async fn get_global_stats(state: web::Data<ApiState>) -> impl Responder {
    let stmt = Statement::from_string(
        DbBackend::Postgres,
        r#"
        SELECT
            COALESCE(SUM(rs.amount_sats), 0)::BIGINT AS total_mined_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'confirmed' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS confirmed_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'immature' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS immature_sats,
            COALESCE(SUM(CASE WHEN rs.state = 'orphaned' THEN rs.amount_sats ELSE 0 END), 0)::BIGINT AS orphaned_sats,
            COALESCE(COUNT(rs.block_id), 0) AS reward_count,
            COALESCE(SUM(CASE WHEN b.chain_status = 'active' THEN 1 ELSE 0 END), 0) AS active_block_count
        FROM reward_settlements rs
        LEFT JOIN blocks b ON b.block_id = rs.block_id
        "#
        .to_string(),
    );
    let row = match state.db.query_one(stmt).await {
        Ok(v) => v,
        Err(err) => {
            return HttpResponse::InternalServerError().json(json!({ "error": err.to_string() }));
        }
    };
    let Some(row) = row else {
        return HttpResponse::Ok().json(GlobalStatsResponse {
            total_mined_sats: 0,
            confirmed_sats: 0,
            immature_sats: 0,
            orphaned_sats: 0,
            reward_count: 0,
            active_block_count: 0,
        });
    };

    HttpResponse::Ok().json(GlobalStatsResponse {
        total_mined_sats: row.try_get("", "total_mined_sats").unwrap_or(0),
        confirmed_sats: row.try_get("", "confirmed_sats").unwrap_or(0),
        immature_sats: row.try_get("", "immature_sats").unwrap_or(0),
        orphaned_sats: row.try_get("", "orphaned_sats").unwrap_or(0),
        reward_count: row.try_get("", "reward_count").unwrap_or(0),
        active_block_count: row.try_get("", "active_block_count").unwrap_or(0),
    })
}
