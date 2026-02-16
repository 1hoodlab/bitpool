use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TABLE IF EXISTS block_transactions;")
            .await?;
        db.execute_unprepared("DROP TABLE IF EXISTS snapshots;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS difficulty_states;")
            .await?;
        db.execute_unprepared("DROP TABLE IF EXISTS transactions;").await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS transactions (
                tx_id VARCHAR(64) PRIMARY KEY,
                fee_sats BIGINT NOT NULL DEFAULT 0 CHECK (fee_sats >= 0),
                weight INTEGER NOT NULL DEFAULT 0,
                created_at_ms BIGINT NOT NULL,
                risk VARCHAR(64),
                status VARCHAR(32) NOT NULL DEFAULT 'pending'
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS block_transactions (
                id BIGSERIAL PRIMARY KEY,
                block_id VARCHAR(64) NOT NULL REFERENCES blocks(block_id) ON DELETE CASCADE,
                tx_id VARCHAR(64) NOT NULL REFERENCES transactions(tx_id),
                position INTEGER NOT NULL,
                UNIQUE(block_id, position)
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS difficulty_states (
                id SERIAL PRIMARY KEY,
                height_anchor BIGINT NOT NULL,
                target VARCHAR(64) NOT NULL,
                window_n INTEGER NOT NULL,
                anchor_time_ms BIGINT NOT NULL,
                clamp_meta VARCHAR(128),
                expected_block_time_ms BIGINT,
                min_target VARCHAR(64),
                max_target VARCHAR(64)
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS snapshots (
                id SERIAL PRIMARY KEY,
                snapshot_seq_id BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                storage_uri TEXT,
                dag_pruned_to_height BIGINT
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_block_transactions_block_position ON block_transactions(block_id, position);",
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_block_transactions_tx_id ON block_transactions(tx_id);",
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_transactions_status_created ON transactions(status, created_at_ms);",
        )
        .await?;

        Ok(())
    }
}
