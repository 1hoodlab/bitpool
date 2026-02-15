use crate::domain::repositories::DomainRepoError;
use crate::infrastructure::persistence::TransactionRunner;

pub struct AppendBlockAndSettlePendingRewardsUseCase<R: TransactionRunner> {
    pub tx_runner: R,
}

impl<R: TransactionRunner> AppendBlockAndSettlePendingRewardsUseCase<R> {
    pub async fn execute(&self) -> Result<(), DomainRepoError> {
        self.tx_runner
            .run_in_transaction("append_block_and_settle_pending_rewards")
            .await
    }
}

pub struct HandleReorgAndRecomputeRewardsUseCase<R: TransactionRunner> {
    pub tx_runner: R,
}

impl<R: TransactionRunner> HandleReorgAndRecomputeRewardsUseCase<R> {
    pub async fn execute(&self) -> Result<(), DomainRepoError> {
        self.tx_runner
            .run_in_transaction("handle_reorg_and_recompute_rewards")
            .await
    }
}

pub struct ApplyDifficultyRetargetUseCase<R: TransactionRunner> {
    pub tx_runner: R,
}

impl<R: TransactionRunner> ApplyDifficultyRetargetUseCase<R> {
    pub async fn execute(&self) -> Result<(), DomainRepoError> {
        self.tx_runner
            .run_in_transaction("apply_difficulty_retarget")
            .await
    }
}
