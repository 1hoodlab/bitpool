pub use sea_orm_migration::prelude::*;

mod m20260215_000001_baseline_schema;
mod m20260216_000002_drop_unused_tables;
mod m20260216_000003_reward_settlements_unique_block_id;
mod m20260220_000004_microservice_schema;
mod m20260220_000005_processed_commands;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260215_000001_baseline_schema::Migration),
            Box::new(m20260216_000002_drop_unused_tables::Migration),
            Box::new(m20260216_000003_reward_settlements_unique_block_id::Migration),
            Box::new(m20260220_000004_microservice_schema::Migration),
            Box::new(m20260220_000005_processed_commands::Migration),
        ]
    }
}
