#[macro_use]
extern crate log;

use std::net::SocketAddr;
use axum::Router;
use axum::handler::{get, post};
use anyhow::Error;
use std::time::Instant;
use hyper::http::StatusCode;
use tokio::time::Duration;

mod routes;

pub struct Context {
    pub bind: SocketAddr,
    pub target_server: String,
    pub no_prep: bool,
    pub index: String,
    pub use_fast_fuzzy: bool,
    pub strip_stop_words: bool,
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

    if !ctx.no_prep {
        prep(&ctx).await?;
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

async fn prep(ctx: &Context) -> anyhow::Result<()> {
    let data: serde_json::Value = serde_json::from_str(include_str!("../static/movies.json"))?;

    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "name": &ctx.index,

        "writer_buffer": 60_000_000,
        "writer_threads": 4,

        "reader_threads": 2,

        "max_concurrency": 4,
        "search_fields": [
            "title",
            "overview"
        ],

        "storage_type": "tempdir",

        "fields": {
            "id": {
                "type": "string",
                "stored": true
            },
            "poster": {
                "type": "string",
                "stored": true
            },
            "release_date": {
                "type": "i64",
                "stored": true,
                "indexed": false,
                "fast": "single"
            },
            "title": {
                "type": "text",
                "stored": true
            },
            "overview": {
               "type": "text",
               "stored": true
            },
            "genres": {
               "type": "text",
               "stored": true
            }
        },

        "boost_fields": {
            "title": 2.0,
            "overview": 0.8
        },

        "use_fast_fuzzy": ctx.use_fast_fuzzy,
        "strip_stop_words": ctx.strip_stop_words,
    });

    let r = client.post(format!("{}/indexes?override_if_exists=true", &ctx.target_server))
        .json(&payload)
        .send()
        .await?;

    if r.status() != StatusCode::OK {
        return Err(Error::msg(
            "server returned a non 200 OK code when creating index. Check your server logs."))
    }

    // let changed propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Clear the existing docs
    let r = client
        .delete(format!("{}/indexes/{}/documents/clear", &ctx.target_server, &ctx.index))
        .send()
        .await?;

    if r.status() != StatusCode::OK {
        return Err(Error::msg(
            "server returned a non 200 OK code when clearing docs. Check your server logs."))
    }

    let start = Instant::now();
    let r = client
        .post(format!("{}/indexes/{}/documents", &ctx.target_server, &ctx.index))
        .json(&data)
        .send()
        .await?;

    if r.status() != StatusCode::OK {
        return Err(Error::msg(
            "server returned a non 200 OK code when adding docs. Check your server logs."))
    }

    let r = client
        .post(format!("{}/indexes/{}/commit", &ctx.target_server, &ctx.index))
        .send()
        .await?;

    if r.status() != StatusCode::OK {
        return Err(Error::msg(
            "server returned a non 200 OK code when committing changes. Check your server logs."))
    }

    let delta = start.elapsed();
    info!("lnx took {:?} to process submitted documents", delta);

    Ok(())
}