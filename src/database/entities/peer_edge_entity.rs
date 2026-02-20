use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "peer_edges")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub from_node_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub to_node_id: String,
    pub latency_ms: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::node_entity::Entity",
        from = "Column::FromNodeId",
        to = "super::node_entity::Column::NodeId"
    )]
    FromNode,
    #[sea_orm(
        belongs_to = "super::node_entity::Entity",
        from = "Column::ToNodeId",
        to = "super::node_entity::Column::NodeId"
    )]
    ToNode,
}

impl Related<super::node_entity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::FromNode.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
