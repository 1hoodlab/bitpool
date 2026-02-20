use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "blocks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub block_id: String,
    pub parent_id: Option<String>,
    pub height: i64,
    pub found_time_ms: i64,
    pub target: String,
    pub total_weight: i32,
    pub fees_sats: i64,
    pub work: i64,
    pub cumulative_work: i64,
    pub miner_node_id: String,
    pub first_seen_seq_id: i64,
    pub chain_status: String,
    pub nonce_visual: Option<String>,
    pub subsidy_sats: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::node_entity::Entity",
        from = "Column::MinerNodeId",
        to = "super::node_entity::Column::NodeId"
    )]
    Miner,
}

impl Related<super::node_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Miner.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
