use async_trait::async_trait;
use sea_orm::{Database, DatabaseConnection, IsolationLevel, TransactionTrait};

use crate::domain::repositories::DomainRepoError;

pub async fn connect_postgres(database_url: &str) -> Result<DatabaseConnection, sea_orm::DbErr> {
    Database::connect(database_url).await
}

#[async_trait]
pub trait TransactionRunner: Send + Sync {
    async fn run_in_transaction(&self, operation: &str) -> Result<(), DomainRepoError>;
}

#[derive(Clone)]
pub struct SeaOrmTransactionRunner {
    pub db: DatabaseConnection,
}

#[async_trait]
impl TransactionRunner for SeaOrmTransactionRunner {
    async fn run_in_transaction(&self, _operation: &str) -> Result<(), DomainRepoError> {
        self.db
            .transaction_with_config::<_, (), sea_orm::DbErr>(
                |txn| {
                    Box::pin(async move {
                        let _ = txn;
                        Ok(())
                    })
                },
                Some(IsolationLevel::ReadCommitted),
                None,
            )
            .await
            .map_err(|e| DomainRepoError(format!("transaction failed: {e}")))?;
        Ok(())
    }
}
