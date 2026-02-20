use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS processed_commands (
                command_id VARCHAR(128) PRIMARY KEY,
                command_type VARCHAR(64) NOT NULL,
                nation_id VARCHAR(64) NOT NULL,
                received_at_ms BIGINT NOT NULL
            );
            "#,
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_processed_commands_nation_id ON processed_commands(nation_id);",
        )
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TABLE IF EXISTS processed_commands;")
            .await?;
        Ok(())
    }
}
