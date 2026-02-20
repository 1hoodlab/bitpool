use std::collections::HashMap;

use sea_orm::{DatabaseConnection, EntityTrait};

use crate::database::entities::peer_edge_entity;

pub async fn load_peer_latencies_from_db(
    db: &DatabaseConnection,
) -> Result<HashMap<(String, String), u64>, sea_orm::DbErr> {
    let rows = peer_edge_entity::Entity::find().all(db).await?;
    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        map.insert(
            (row.from_node_id, row.to_node_id),
            row.latency_ms.max(1) as u64,
        );
    }
    Ok(map)
}
