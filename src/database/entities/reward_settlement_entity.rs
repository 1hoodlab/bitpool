use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "reward_settlements")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i32,
    pub block_id: String,
    pub miner_node_id: String,
    pub state: String,
    pub confirmations: i32,
    pub settled_at_ms: Option<i64>,
    pub amount_sats: i64,
    pub nation_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::block_entity::Entity",
        from = "Column::BlockId",
        to = "super::block_entity::Column::BlockId"
    )]
    Block,
    #[sea_orm(
        belongs_to = "super::node_entity::Entity",
        from = "Column::MinerNodeId",
        to = "super::node_entity::Column::NodeId"
    )]
    Miner,
}

impl Related<super::block_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Block.def()
    }
}

impl Related<super::node_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Miner.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
