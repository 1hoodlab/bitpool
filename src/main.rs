mod application;
mod domain;
mod infrastructure;

use actix_web::{get, App, HttpServer, Responder};
use env_logger::Env;

#[get("/")]
async fn hello() -> impl Responder {
    "Hello from Actix Web!"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load .env from project root (optional; app still runs if file missing)
    dotenvy::dotenv().ok();

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .unwrap_or(8080);

    HttpServer::new(|| App::new().service(hello))
        .bind((host.as_str(), port))?
        .run()
        .await
}