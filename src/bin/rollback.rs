// Roll back migrations. Use:
// - cargo run --bin rollback        (down 1)
// - cargo run --bin rollback -- 3   (down 3)
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

    let steps = std::env::args()
        .nth(1)
        .map(|s| s.parse::<u32>())
        .transpose()
        .map_err(|_| {
            "Invalid steps. Use a positive integer, e.g. `cargo run --bin rollback -- 2`"
        })?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_rollback(url.trim(), steps))?;
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

async fn run_rollback(url: &str, steps: Option<u32>) -> Result<(), Box<dyn std::error::Error>> {
    let db: DatabaseConnection = Database::connect(url).await?;
    migration::Migrator::down(&db, steps)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    match steps {
        Some(n) => println!("Rollback completed: down {} migration(s).", n),
        None => println!("Rollback completed: down 1 migration."),
    }
    Ok(())
}
