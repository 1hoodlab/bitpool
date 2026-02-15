use async_trait::async_trait;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

use crate::database::entities::{EventLogInsert, EventLogRow};
use crate::domain::entities::EventEnvelope;
use crate::domain::repositories::{DomainRepoError, EventRepository};

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
