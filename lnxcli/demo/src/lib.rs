#[macro_use]
extern crate log;

use std::net::SocketAddr;
use axum::Router;
use axum::handler::{get, post};
use anyhow::Error;

mod routes;

pub struct Context {
    pub bind: SocketAddr,
    pub target_server: String,
    pub no_prep: bool,
    pub index: String,
}

pub fn run(ctx: Context) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(start(ctx))
}

async fn start(ctx: Context) -> anyhow::Result<()> {
    if !ctx.target_server.starts_with("http") {
        return Err(Error::msg("target server must include the http protocol."))
    }

    let _ = routes::TARGET_URL.set(format!("{}/indexes/{}/search", &ctx.target_server, &ctx.index));

    let app = Router::new()
        .route("/", get(routes::index))
        .route("/search", post(routes::search));

    info!("serving demo app @ http://{}", &ctx.bind);
    axum::Server::bind(&ctx.bind)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}