use async_trait::async_trait;
use sea_orm::{
    ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement, TransactionTrait,
};

use crate::database::entities::{EventLogInsert, EventLogRow};
use crate::domain::entities::{
    Block, BlockHeader, BlockIndexEntry, BlockStatus, ChainTip, EventEnvelope, Reward, RewardStatus,
};
use crate::domain::repositories::{DomainRepoError, EventRepository, RewardSettlementWrite};

pub async fn connect_postgres(database_url: &str) -> Result<DatabaseConnection, sea_orm::DbErr> {
    Database::connect(database_url).await
}

#[derive(Clone)]
pub struct SeaOrmEventRepository {
    pub db: DatabaseConnection,
}

#[async_trait]
impl EventRepository for SeaOrmEventRepository {
    async fn append_event(&self, event: &EventEnvelope) -> Result<(), DomainRepoError> {
        event
            .validate()
            .map_err(|e| DomainRepoError(format!("invalid event envelope: {e}")))?;

        let insert = EventLogInsert::from_event(event)?;
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            INSERT INTO event_log (seq_id, event_type, server_time_ms, payload_json)
            VALUES ($1, $2, $3, $4)
            "#
            .to_string(),
            vec![
                insert.seq_id.into(),
                insert.event_type.into(),
                insert.server_time_ms.into(),
                insert.payload_json.into(),
            ],
        );

        self.db.execute(stmt).await.map_err(|e| {
            if e.to_string().contains("duplicate key value") {
                DomainRepoError(format!("duplicate seq_id {}: {e}", event.seq_id))
            } else {
                DomainRepoError(format!("append event failed: {e}"))
            }
        })?;
        Ok(())
    }

    async fn last_seq_id(&self) -> Result<Option<i64>, DomainRepoError> {
        let stmt = Statement::from_string(
            DbBackend::Postgres,
            "SELECT seq_id FROM event_log ORDER BY seq_id DESC LIMIT 1".to_string(),
        );
        let rows = self
            .db
            .query_all(stmt)
            .await
            .map_err(|e| DomainRepoError(format!("query last_seq_id failed: {e}")))?;

        if let Some(row) = rows.first() {
            let seq_id: i64 = row
                .try_get("", "seq_id")
                .map_err(|e| DomainRepoError(format!("parse last_seq_id failed: {e}")))?;
            Ok(Some(seq_id))
        } else {
            Ok(None)
        }
    }

    async fn read_events_after(
        &self,
        after_seq_id: i64,
        limit: u64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            SELECT seq_id, event_type, server_time_ms, payload_json
            FROM event_log
            WHERE seq_id > $1
            ORDER BY seq_id ASC
            LIMIT $2
            "#
            .to_string(),
            vec![after_seq_id.into(), (limit as i64).into()],
        );

        self.read_events_with_statement(stmt).await
    }

    async fn read_events_range(
        &self,
        from_seq: i64,
        to_seq: i64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"
            SELECT seq_id, event_type, server_time_ms, payload_json
            FROM event_log
            WHERE seq_id >= $1 AND seq_id <= $2
            ORDER BY seq_id ASC
            "#
            .to_string(),
            vec![from_seq.into(), to_seq.into()],
        );

        self.read_events_with_statement(stmt).await
    }

    async fn upsert_block(
        &self,
        block: &Block,
        cumulative_work: u128,
        first_seen_seq_id: i64,
        chain_status: &str,
    ) -> Result<(), DomainRepoError> {
        let stmt =
            build_upsert_block_statement(block, cumulative_work, first_seen_seq_id, chain_status);

        self.db
            .execute(stmt)
            .await
            .map_err(|e| DomainRepoError(format!("upsert block failed: {e}")))?;
        Ok(())
    }

    async fn upsert_block_with_reward_settlements(
        &self,
        block: &Block,
        cumulative_work: u128,
        first_seen_seq_id: i64,
        chain_status: &str,
        reward_writes: &[RewardSettlementWrite],
    ) -> Result<(), DomainRepoError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| DomainRepoError(format!("begin transaction failed: {e}")))?;

        let block_stmt =
            build_upsert_block_statement(block, cumulative_work, first_seen_seq_id, chain_status);
        txn.execute(block_stmt)
            .await
            .map_err(|e| DomainRepoError(format!("upsert block in transaction failed: {e}")))?;

        for write in reward_writes {
            let update_stmt =
                build_update_reward_settlement_statement(&write.reward, write.confirmations);
            let update_result = txn.execute(update_stmt).await.map_err(|e| {
                DomainRepoError(format!(
                    "update reward settlement in transaction failed: {e}"
                ))
            })?;
            if update_result.rows_affected() == 0 {
                let insert_stmt =
                    build_insert_reward_settlement_statement(&write.reward, write.confirmations);
                txn.execute(insert_stmt).await.map_err(|e| {
                    DomainRepoError(format!(
                        "insert reward settlement in transaction failed: {e}"
                    ))
                })?;
            }
        }

        txn.commit()
            .await
            .map_err(|e| DomainRepoError(format!("commit transaction failed: {e}")))?;
        Ok(())
    }

    async fn load_chain_state(
        &self,
    ) -> Result<(Vec<BlockIndexEntry>, Option<ChainTip>), DomainRepoError> {
        let stmt = Statement::from_string(
            DbBackend::Postgres,
            r#"
            SELECT
                block_id,
                parent_id,
                height,
                found_time_ms,
                target,
                total_weight,
                fees_sats,
                cumulative_work,
                miner_node_id,
                chain_status,
                nonce_visual
            FROM blocks
            ORDER BY height ASC, found_time_ms ASC, block_id ASC
            "#
            .to_string(),
        );
        let rows = self
            .db
            .query_all(stmt)
            .await
            .map_err(|e| DomainRepoError(format!("load chain state failed: {e}")))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let block_id: String = row
                .try_get("", "block_id")
                .map_err(|e| DomainRepoError(format!("parse block_id failed: {e}")))?;
            let parent_id: Option<String> = row
                .try_get("", "parent_id")
                .map_err(|e| DomainRepoError(format!("parse parent_id failed: {e}")))?;
            let height: i64 = row
                .try_get("", "height")
                .map_err(|e| DomainRepoError(format!("parse height failed: {e}")))?;
            let found_time_ms: i64 = row
                .try_get("", "found_time_ms")
                .map_err(|e| DomainRepoError(format!("parse found_time_ms failed: {e}")))?;
            let target: String = row
                .try_get("", "target")
                .map_err(|e| DomainRepoError(format!("parse target failed: {e}")))?;
            let total_weight_i32: i32 = row
                .try_get("", "total_weight")
                .map_err(|e| DomainRepoError(format!("parse total_weight failed: {e}")))?;
            let fees_sats: i64 = row
                .try_get("", "fees_sats")
                .map_err(|e| DomainRepoError(format!("parse fees_sats failed: {e}")))?;
            let cumulative_work_i64: i64 = row
                .try_get("", "cumulative_work")
                .map_err(|e| DomainRepoError(format!("parse cumulative_work failed: {e}")))?;
            let miner_node_id: String = row
                .try_get("", "miner_node_id")
                .map_err(|e| DomainRepoError(format!("parse miner_node_id failed: {e}")))?;
            let chain_status_raw: String = row
                .try_get("", "chain_status")
                .map_err(|e| DomainRepoError(format!("parse chain_status failed: {e}")))?;
            let nonce_visual: Option<String> = row
                .try_get("", "nonce_visual")
                .map_err(|e| DomainRepoError(format!("parse nonce_visual failed: {e}")))?;

            let difficulty = target.parse::<u64>().unwrap_or(1);
            let nonce = nonce_visual
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let status = match chain_status_raw.as_str() {
                "active" => BlockStatus::Active,
                "orphan" => BlockStatus::Orphan,
                "invalid" => BlockStatus::Invalid,
                _ => BlockStatus::Valid,
            };

            entries.push(BlockIndexEntry {
                header: BlockHeader {
                    block_id,
                    height,
                    parent_hash: parent_id.unwrap_or_else(|| "genesis".to_string()),
                    miner_node_id,
                    timestamp_ms: found_time_ms,
                    difficulty,
                    nonce,
                    total_weight: total_weight_i32,
                    total_fees_sats: fees_sats,
                },
                tx_ids: Vec::new(),
                cumulative_work: cumulative_work_i64.max(0) as u128,
                status,
            });
        }

        let tip_entry = entries
            .iter()
            .filter(|e| matches!(e.status, BlockStatus::Active))
            .max_by(|a, b| {
                a.cumulative_work
                    .cmp(&b.cumulative_work)
                    .then_with(|| a.header.height.cmp(&b.header.height))
                    .then_with(|| a.header.block_id.cmp(&b.header.block_id))
            })
            .or_else(|| {
                entries.iter().max_by(|a, b| {
                    a.cumulative_work
                        .cmp(&b.cumulative_work)
                        .then_with(|| a.header.height.cmp(&b.header.height))
                        .then_with(|| a.header.block_id.cmp(&b.header.block_id))
                })
            });

        let tip = tip_entry.map(|entry| ChainTip {
            block_id: entry.header.block_id.clone(),
            height: entry.header.height,
            cumulative_work: entry.cumulative_work,
        });

        Ok((entries, tip))
    }

    async fn load_reward_settlements(&self) -> Result<Vec<Reward>, DomainRepoError> {
        let stmt = Statement::from_string(
            DbBackend::Postgres,
            r#"
            SELECT
                rs.block_id,
                rs.miner_node_id,
                rs.state,
                rs.amount_sats,
                rs.settled_at_ms,
                b.found_time_ms
            FROM reward_settlements rs
            LEFT JOIN blocks b ON b.block_id = rs.block_id
            ORDER BY rs.id ASC
            "#
            .to_string(),
        );
        let rows = self
            .db
            .query_all(stmt)
            .await
            .map_err(|e| DomainRepoError(format!("load reward settlements failed: {e}")))?;

        let mut rewards = Vec::with_capacity(rows.len());
        for row in rows {
            let block_hash: String = row
                .try_get("", "block_id")
                .map_err(|e| DomainRepoError(format!("parse reward block_id failed: {e}")))?;
            let miner_id: String = row
                .try_get("", "miner_node_id")
                .map_err(|e| DomainRepoError(format!("parse reward miner_node_id failed: {e}")))?;
            let state_raw: String = row
                .try_get("", "state")
                .map_err(|e| DomainRepoError(format!("parse reward state failed: {e}")))?;
            let amount_sats: i64 = row
                .try_get("", "amount_sats")
                .map_err(|e| DomainRepoError(format!("parse reward amount_sats failed: {e}")))?;
            let settled_at_ms: Option<i64> = row
                .try_get("", "settled_at_ms")
                .map_err(|e| DomainRepoError(format!("parse reward settled_at_ms failed: {e}")))?;
            let created_at_ms: Option<i64> = row
                .try_get("", "found_time_ms")
                .map_err(|e| DomainRepoError(format!("parse reward found_time_ms failed: {e}")))?;

            let status = match state_raw.as_str() {
                "confirmed" => RewardStatus::Confirmed,
                "orphaned" => RewardStatus::Orphaned,
                "spent" => RewardStatus::Spent,
                _ => RewardStatus::Immature,
            };

            rewards.push(Reward {
                id: format!("reward-{block_hash}"),
                block_hash,
                miner_id,
                amount_sats,
                status,
                created_at_ms: created_at_ms.unwrap_or_default(),
                matured_at_height: None,
                settled_at_ms,
            });
        }
        Ok(rewards)
    }

    async fn upsert_reward_settlement(
        &self,
        reward: &Reward,
        confirmations: i32,
    ) -> Result<(), DomainRepoError> {
        let update_stmt = build_update_reward_settlement_statement(reward, confirmations);
        let update_result = self
            .db
            .execute(update_stmt)
            .await
            .map_err(|e| DomainRepoError(format!("update reward settlement failed: {e}")))?;
        if update_result.rows_affected() > 0 {
            return Ok(());
        }

        let insert_stmt = build_insert_reward_settlement_statement(reward, confirmations);
        self.db
            .execute(insert_stmt)
            .await
            .map_err(|e| DomainRepoError(format!("insert reward settlement failed: {e}")))?;
        Ok(())
    }
}

impl SeaOrmEventRepository {
    async fn read_events_with_statement(
        &self,
        stmt: Statement,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
        let rows = self
            .db
            .query_all(stmt)
            .await
            .map_err(|e| DomainRepoError(format!("query events failed: {e}")))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let db_row = EventLogRow {
                seq_id: row
                    .try_get("", "seq_id")
                    .map_err(|e| DomainRepoError(format!("parse seq_id failed: {e}")))?,
                event_type: row
                    .try_get("", "event_type")
                    .map_err(|e| DomainRepoError(format!("parse event_type failed: {e}")))?,
                server_time_ms: row
                    .try_get("", "server_time_ms")
                    .map_err(|e| DomainRepoError(format!("parse server_time_ms failed: {e}")))?,
                payload_json: row
                    .try_get("", "payload_json")
                    .map_err(|e| DomainRepoError(format!("parse payload_json failed: {e}")))?,
            };
            events.push(db_row.into_event()?);
        }
        Ok(events)
    }
}

fn reward_state_to_str(status: RewardStatus) -> &'static str {
    match status {
        RewardStatus::Immature => "immature",
        RewardStatus::Confirmed => "confirmed",
        RewardStatus::Orphaned => "orphaned",
        RewardStatus::Spent => "spent",
    }
}

fn build_upsert_block_statement(
    block: &Block,
    cumulative_work: u128,
    first_seen_seq_id: i64,
    chain_status: &str,
) -> Statement {
    let cumulative_work_i64 = cumulative_work.min(i64::MAX as u128) as i64;
    let work_i64 = (block.header.difficulty as u128).min(i64::MAX as u128) as i64;
    let nonce_visual = block.header.nonce.to_string();
    let target = block.header.difficulty.to_string();
    let parent_id = if block.header.parent_hash.is_empty() || block.header.parent_hash == "genesis"
    {
        None
    } else {
        Some(block.header.parent_hash.clone())
    };

    Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO blocks (
            block_id,
            parent_id,
            height,
            found_time_ms,
            target,
            total_weight,
            fees_sats,
            work,
            cumulative_work,
            miner_node_id,
            first_seen_seq_id,
            chain_status,
            nonce_visual,
            subsidy_sats
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (block_id)
        DO UPDATE SET
            parent_id = EXCLUDED.parent_id,
            height = EXCLUDED.height,
            found_time_ms = EXCLUDED.found_time_ms,
            target = EXCLUDED.target,
            total_weight = EXCLUDED.total_weight,
            fees_sats = EXCLUDED.fees_sats,
            work = EXCLUDED.work,
            cumulative_work = EXCLUDED.cumulative_work,
            miner_node_id = EXCLUDED.miner_node_id,
            first_seen_seq_id = LEAST(blocks.first_seen_seq_id, EXCLUDED.first_seen_seq_id),
            chain_status = EXCLUDED.chain_status,
            nonce_visual = EXCLUDED.nonce_visual
        "#
        .to_string(),
        vec![
            block.header.block_id.clone().into(),
            parent_id.into(),
            block.header.height.into(),
            block.header.timestamp_ms.into(),
            target.into(),
            (block.header.total_weight as i64).into(),
            block.header.total_fees_sats.into(),
            work_i64.into(),
            cumulative_work_i64.into(),
            block.header.miner_node_id.clone().into(),
            first_seen_seq_id.into(),
            chain_status.to_string().into(),
            nonce_visual.into(),
            0i64.into(),
        ],
    )
}

fn build_update_reward_settlement_statement(reward: &Reward, confirmations: i32) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        UPDATE reward_settlements
        SET
            miner_node_id = $2,
            state = $3,
            confirmations = $4,
            settled_at_ms = $5,
            amount_sats = $6
        WHERE block_id = $1
        "#
        .to_string(),
        vec![
            reward.block_hash.clone().into(),
            reward.miner_id.clone().into(),
            reward_state_to_str(reward.status.clone()).into(),
            confirmations.into(),
            reward.settled_at_ms.into(),
            reward.amount_sats.into(),
        ],
    )
}

fn build_insert_reward_settlement_statement(reward: &Reward, confirmations: i32) -> Statement {
    Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
        INSERT INTO reward_settlements (
            block_id,
            miner_node_id,
            state,
            confirmations,
            settled_at_ms,
            amount_sats
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#
        .to_string(),
        vec![
            reward.block_hash.clone().into(),
            reward.miner_id.clone().into(),
            reward_state_to_str(reward.status.clone()).into(),
            confirmations.into(),
            reward.settled_at_ms.into(),
            reward.amount_sats.into(),
        ],
    )
}
