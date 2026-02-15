use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS nodes (
                node_id VARCHAR(64) PRIMARY KEY,
                is_ai BOOLEAN NOT NULL DEFAULT FALSE,
                hash_rate BIGINT NOT NULL DEFAULT 0 CHECK (hash_rate >= 0),
                is_online BOOLEAN NOT NULL DEFAULT TRUE,
                balance_sats BIGINT NOT NULL DEFAULT 0,
                opex_per_sec_sats BIGINT NOT NULL DEFAULT 0,
                tip_block_id VARCHAR(64),
                has_relay BOOLEAN NOT NULL DEFAULT FALSE,
                latency_ms INTEGER,
                mined_blocks BIGINT NOT NULL DEFAULT 0,
                orphaned_blocks BIGINT NOT NULL DEFAULT 0,
                version INTEGER NOT NULL DEFAULT 0,
                created_at_ms BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL DEFAULT 0
            );
            "#,
        )
        .await?;

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
            CREATE TABLE IF NOT EXISTS blocks (
                block_id VARCHAR(64) PRIMARY KEY,
                parent_id VARCHAR(64) REFERENCES blocks(block_id),
                height BIGINT NOT NULL,
                found_time_ms BIGINT NOT NULL,
                target VARCHAR(64) NOT NULL,
                total_weight INTEGER NOT NULL DEFAULT 0 CHECK (total_weight <= 4000),
                fees_sats BIGINT NOT NULL DEFAULT 0,
                work BIGINT NOT NULL DEFAULT 0,
                cumulative_work BIGINT NOT NULL DEFAULT 0,
                miner_node_id VARCHAR(64) NOT NULL REFERENCES nodes(node_id),
                first_seen_seq_id BIGINT NOT NULL,
                chain_status VARCHAR(32) NOT NULL DEFAULT 'pending',
                nonce_visual VARCHAR(64),
                subsidy_sats BIGINT NOT NULL DEFAULT 0
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
            CREATE TABLE IF NOT EXISTS reward_settlements (
                id SERIAL PRIMARY KEY,
                block_id VARCHAR(64) NOT NULL REFERENCES blocks(block_id),
                miner_node_id VARCHAR(64) NOT NULL REFERENCES nodes(node_id),
                state VARCHAR(32) NOT NULL,
                confirmations INTEGER NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
                settled_at_ms BIGINT,
                amount_sats BIGINT NOT NULL DEFAULT 0
            );
            "#,
        )
        .await?;

        db.execute_unprepared(
            r#"
            CREATE TABLE IF NOT EXISTS event_log (
                seq_id BIGINT PRIMARY KEY,
                event_type VARCHAR(64) NOT NULL,
                server_time_ms BIGINT NOT NULL,
                payload_json TEXT
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

        db.execute_unprepared("CREATE INDEX IF NOT EXISTS idx_blocks_parent_id ON blocks(parent_id);")
            .await?;
        db.execute_unprepared("CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);")
            .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_blocks_cumulative_work_first_seen ON blocks(cumulative_work DESC, first_seen_seq_id ASC);",
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
        db.execute_unprepared("CREATE INDEX IF NOT EXISTS idx_event_log_seq ON event_log(seq_id);")
            .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx_reward_settlements_miner_state ON reward_settlements(miner_node_id, state);",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP TABLE IF EXISTS snapshots;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS event_log;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS reward_settlements;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS difficulty_states;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS block_transactions;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS blocks;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS transactions;").await?;
        db.execute_unprepared("DROP TABLE IF EXISTS nodes;").await?;
        Ok(())
    }
}
