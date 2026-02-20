use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "chain_metadata")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub current_tip_block_id: Option<String>,
    pub current_height: i64,
    pub current_difficulty: String,
    pub total_hashrate: i64,
    pub last_adjustment_time_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::block_entity::Entity",
        from = "Column::CurrentTipBlockId",
        to = "super::block_entity::Column::BlockId"
    )]
    TipBlock,
}

impl Related<super::block_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TipBlock.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
