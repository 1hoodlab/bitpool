use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

#[derive(Debug, Clone)]
pub struct DbMinerNode {
    pub node_id: String,
    pub hash_rate: u64,
    pub is_online: bool,
    pub has_relay: bool,
}

pub async fn load_miner_nodes_from_db(
    db: &DatabaseConnection,
) -> Result<Vec<DbMinerNode>, sea_orm::DbErr> {
    let stmt = Statement::from_string(
        DbBackend::Postgres,
        r#"
        SELECT node_id, hash_rate, is_online, has_relay
        FROM nodes
        ORDER BY node_id ASC
        "#
        .to_string(),
    );
    let rows = db.query_all(stmt).await?;

    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        let hash_rate_i64: i64 = row.try_get("", "hash_rate")?;
        nodes.push(DbMinerNode {
            node_id: row.try_get("", "node_id")?,
            hash_rate: hash_rate_i64.max(0) as u64,
            is_online: row.try_get("", "is_online")?,
            has_relay: row.try_get("", "has_relay")?,
        });
    }
    Ok(nodes)
}
