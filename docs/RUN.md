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
cargo run --bin migrate
```

This runs the built-in migrate binary (reads `DATABASE_URL` from `.env`). Alternative: install and use SeaORM CLI:

```bash
cargo install sea-orm-cli
sea-orm-cli migrate run
```

(Set `DATABASE_URL` in the environment if not using `.env`.)

## 4. PostgreSQL with Docker Compose

From project root:

```bash
docker compose up -d postgres
```

Uses `postgres:latest`, port 5432. Default user/db/password: `bitpool` / `bitpool` / `bitpool`. Override with `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT` in `.env` or environment. Data is stored in volume `postgres_data`. Set `DATABASE_URL=postgres://bitpool:bitpool@localhost:5432/bitpool` in `.env`, then run `cargo run --bin migrate`.
