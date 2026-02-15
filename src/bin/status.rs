// Show applied migrations. Use: cargo run --bin status
//
// Requires DATABASE_URL in .env or environment.

use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is not set. Set it in .env or environment.")?;

    if let Some(redacted) = redact_url(&url) {
        eprintln!("Using: {}", redacted);
    }

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_status(url.trim()))?;
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
        let user = user_pass
            .split_once(':')
            .map(|(u, _)| u)
            .unwrap_or(user_pass);
        Some(format!("postgres://{}:***@{}", user, host_db))
    } else {
        None
    }
}

async fn run_status(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let db: DatabaseConnection = Database::connect(url).await?;
    let stmt = Statement::from_string(
        DbBackend::Postgres,
        "SELECT version FROM seaql_migrations ORDER BY version".to_string(),
    );
    let rows = db
        .query_all(stmt)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    if rows.is_empty() {
        println!("No applied migrations.");
        return Ok(());
    }

    println!("Applied migrations:");
    for row in rows {
        let version: String = row.try_get("", "version")?;
        println!("- {}", version);
    }
    Ok(())
}
