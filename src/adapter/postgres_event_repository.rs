use async_trait::async_trait;
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Statement, TransactionTrait,
};

use crate::database::entities::{
    block_entity, event_log_entity, reward_settlement_entity,
};
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

        let am = event_log_entity::Model::from_event(event)?;
        event_log_entity::Entity::insert(am)
            .exec(&self.db)
            .await
            .map_err(|e| {
                if e.to_string().contains("duplicate key value") {
                    DomainRepoError(format!("duplicate seq_id {}: {e}", event.seq_id))
                } else {
                    DomainRepoError(format!("append event failed: {e}"))
                }
            })?;
        Ok(())
    }

    async fn last_seq_id(&self) -> Result<Option<i64>, DomainRepoError> {
        let model = event_log_entity::Entity::find()
            .order_by_desc(event_log_entity::Column::SeqId)
            .one(&self.db)
            .await
            .map_err(|e| DomainRepoError(format!("query last_seq_id failed: {e}")))?;
        Ok(model.map(|m| m.seq_id))
    }

    async fn read_events_after(
        &self,
        after_seq_id: i64,
        limit: u64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
        let models = event_log_entity::Entity::find()
            .filter(event_log_entity::Column::SeqId.gt(after_seq_id))
            .order_by_asc(event_log_entity::Column::SeqId)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(|e| DomainRepoError(format!("query events failed: {e}")))?;
        models
            .into_iter()
            .map(|m| m.into_event())
            .collect::<Result<Vec<_>, _>>()
    }

    async fn read_events_range(
        &self,
        from_seq: i64,
        to_seq: i64,
    ) -> Result<Vec<EventEnvelope>, DomainRepoError> {
        let models = event_log_entity::Entity::find()
            .filter(event_log_entity::Column::SeqId.gte(from_seq))
            .filter(event_log_entity::Column::SeqId.lte(to_seq))
            .order_by_asc(event_log_entity::Column::SeqId)
            .all(&self.db)
            .await
            .map_err(|e| DomainRepoError(format!("query events failed: {e}")))?;
        models
            .into_iter()
            .map(|m| m.into_event())
            .collect::<Result<Vec<_>, _>>()
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
        let models = block_entity::Entity::find()
            .order_by_asc(block_entity::Column::Height)
            .order_by_asc(block_entity::Column::FoundTimeMs)
            .order_by_asc(block_entity::Column::BlockId)
            .all(&self.db)
            .await
            .map_err(|e| DomainRepoError(format!("load chain state failed: {e}")))?;

        let entries: Vec<BlockIndexEntry> = models
            .into_iter()
            .map(|m| {
                let difficulty = m.target.parse::<u64>().unwrap_or(1);
                let nonce = m
                    .nonce_visual
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let status = match m.chain_status.as_str() {
                    "active" => BlockStatus::Active,
                    "orphan" => BlockStatus::Orphan,
                    "invalid" => BlockStatus::Invalid,
                    _ => BlockStatus::Valid,
                };
                BlockIndexEntry {
                    header: BlockHeader {
                        block_id: m.block_id,
                        height: m.height,
                        parent_hash: m
                            .parent_id
                            .unwrap_or_else(|| "genesis".to_string()),
                        miner_node_id: m.miner_node_id,
                        timestamp_ms: m.found_time_ms,
                        difficulty,
                        nonce,
                        total_weight: m.total_weight,
                        total_fees_sats: m.fees_sats,
                    },
                    tx_ids: Vec::new(),
                    cumulative_work: m.cumulative_work.max(0) as u128,
                    status,
                }
            })
            .collect();

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
        let pairs = reward_settlement_entity::Entity::find()
            .find_also_related(block_entity::Entity)
            .order_by_asc(reward_settlement_entity::Column::Id)
            .all(&self.db)
            .await
            .map_err(|e| DomainRepoError(format!("load reward settlements failed: {e}")))?;

        let rewards: Vec<Reward> = pairs
            .into_iter()
            .map(|(rs, block)| {
                let status = match rs.state.as_str() {
                    "confirmed" => RewardStatus::Confirmed,
                    "orphaned" => RewardStatus::Orphaned,
                    "spent" => RewardStatus::Spent,
                    _ => RewardStatus::Immature,
                };
                let created_at_ms = block
                    .map(|b| b.found_time_ms)
                    .unwrap_or_default();
                Reward {
                    id: format!("reward-{}", rs.block_id),
                    block_hash: rs.block_id,
                    miner_id: rs.miner_node_id,
                    amount_sats: rs.amount_sats,
                    status,
                    created_at_ms,
                    matured_at_height: None,
                    settled_at_ms: rs.settled_at_ms,
                }
            })
            .collect();
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
