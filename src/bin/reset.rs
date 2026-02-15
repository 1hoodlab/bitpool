// Reset database schema from migrations. Use: cargo run --bin reset
//
// Requires DATABASE_URL in .env or environment.

use migration::MigratorTrait;
use sea_orm::{Database, DatabaseConnection};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is not set. Set it in .env or environment.")?;

    if let Some(redacted) = redact_url(&url) {
        eprintln!("Using: {}", redacted);
    }

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_reset(url.trim()))?;
    Ok(())
}

fn redact_url(url: &str) -> Option<String> {
    let url = url.trim();
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let after_scheme = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .unwrap_or(url);
        let (user_pass, host_db) = after_scheme.split_once('@')?;
        let user = user_pass.split_once(':').map(|(u, _)| u).unwrap_or(user_pass);
        Some(format!("postgres://{}:***@{}", user, host_db))
    } else {
        None
    }
}

async fn run_reset(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db: DatabaseConnection = Database::connect(url).await?;
    migration::Migrator::fresh(&db)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    println!("Database reset completed (fresh migration applied).");
    Ok(())
}
