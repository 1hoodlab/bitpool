use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};

use crate::database::entities::node_entity;

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
    let models = node_entity::Entity::find()
        .order_by_asc(node_entity::Column::NodeId)
        .all(db)
        .await?;

    let nodes = models
        .into_iter()
        .map(|m| DbMinerNode {
            node_id: m.node_id,
            hash_rate: m.hash_rate.max(0) as u64,
            is_online: m.is_online,
            has_relay: m.has_relay,
        })
        .collect();
    Ok(nodes)
}
