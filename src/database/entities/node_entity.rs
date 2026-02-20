use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "nodes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub node_id: String,
    pub is_ai: bool,
    pub hash_rate: i64,
    pub is_online: bool,
    pub tip_block_id: Option<String>,
    pub has_relay: bool,
    pub latency_ms: Option<i32>,
    pub mined_blocks: i64,
    pub orphaned_blocks: i64,
    pub version: i32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub nation_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::block_entity::Entity")]
    MinedBlocks,
}

impl Related<super::block_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MinedBlocks.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
