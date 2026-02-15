# Config env and run

## 1. Environment variables

Copy the example file and edit:

```bash
cp .env.example .env
```

Edit `.env` with your values:

| Variable       | Description                    | Example |
|----------------|--------------------------------|--------|
| `HOST`         | HTTP bind host                 | `127.0.0.1` |
| `PORT`         | HTTP bind port                 | `8080` |
| `DATABASE_URL` | PostgreSQL connection string   | `postgres://user:pass@localhost:5432/bitpool` |
| `RUST_LOG`     | Log level                      | `info` |

`.env` is loaded at startup (optional; app runs with defaults if missing).

## 2. Run the app

```bash
cargo run
```

Server listens on `http://HOST:PORT` (default `http://127.0.0.1:8080`). Test:

```bash
curl http://127.0.0.1:8080/
```

## 3. Run database migrations

With PostgreSQL running and `DATABASE_URL` set in `.env`:

```bash
# Install CLI once (optional)
cargo install sea-orm-cli

# From project root, run migrations
sea-orm-cli migrate run -u $env:DATABASE_URL
```

On Windows PowerShell, use:

```powershell
$env:DATABASE_URL = "postgres://user:password@localhost:5432/bitpool"
sea-orm-cli migrate run
```

Or run migrations from code at startup (see migration crate `Migrator::up()`).
