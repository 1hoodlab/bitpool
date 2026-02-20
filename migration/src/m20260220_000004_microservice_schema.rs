use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        // 1. Alter nodes: drop economic columns, add nation_id
        db.execute_unprepared(
            r#"
            ALTER TABLE nodes
                DROP COLUMN IF EXISTS balance_sats,
                DROP COLUMN IF EXISTS opex_per_sec_sats,
                ADD COLUMN IF NOT EXISTS nation_id VARCHAR(64);
            "#,
        )
        .await?;

        // 2. Create peer_edges for P2P topology
        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS peer_edges (
                from_node_id VARCHAR(64) NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
                to_node_id VARCHAR(64) NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
                latency_ms INTEGER NOT NULL DEFAULT 10,
                PRIMARY KEY (from_node_id, to_node_id),
                CHECK (from_node_id != to_node_id)
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_peer_edges_from ON peer_edges(from_node_id);",
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_peer_edges_to ON peer_edges(to_node_id);",
        )
        .await?;

        // 3. Create chain_metadata (singleton for quick state exposure)
        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS chain_metadata (
                id INTEGER PRIMARY KEY DEFAULT 1,
                current_tip_block_id VARCHAR(64) REFERENCES blocks(block_id),
                current_height BIGINT NOT NULL DEFAULT 0,
                current_difficulty VARCHAR(64) NOT NULL DEFAULT '1',
                total_hashrate BIGINT NOT NULL DEFAULT 0,
                last_adjustment_time_ms BIGINT,
                updated_at_ms BIGINT NOT NULL DEFAULT 0
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            r#"
            INSERT INTO chain_metadata (id, current_height, current_difficulty, total_hashrate, updated_at_ms)
            VALUES (1, 0, '1', 0, 0)
            ON CONFLICT (id) DO NOTHING;
            "#,
        )
        .await?;

        // 4. Add nation_id to reward_settlements (optional, for event payload)
        db.execute_unprepared(
            "ALTER TABLE reward_settlements ADD COLUMN IF NOT EXISTS nation_id VARCHAR(64);",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        db.execute_unprepared("ALTER TABLE reward_settlements DROP COLUMN IF EXISTS nation_id;")
            .await?;

        db.execute_unprepared("DROP TABLE IF EXISTS chain_metadata;")
            .await?;

        db.execute_unprepared("DROP TABLE IF EXISTS peer_edges;")
            .await?;

        db.execute_unprepared(
            r#"
            ALTER TABLE nodes
                DROP COLUMN IF EXISTS nation_id,
                ADD COLUMN IF NOT EXISTS balance_sats BIGINT NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS opex_per_sec_sats BIGINT NOT NULL DEFAULT 0;
            "#,
        )
        .await?;

        Ok(())
    }
}
