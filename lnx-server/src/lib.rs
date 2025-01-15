mod apis;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use poem::listener::TcpListener;
use poem::Route;
use poem_openapi::OpenApiService;
use tracing::info;

use crate::apis::{LnxHealthApi, LnxInfoApi, LnxQueryApi};

/// Runs the lnx server.
pub async fn run(listen_address: SocketAddr, data_path: PathBuf) -> Result<()> {
    run_rest_api(listen_address)
        .await
        .context("Run lnx REST API")?;

    Ok(())
}

async fn run_rest_api(listen_address: SocketAddr) -> Result<()> {
    let api = (LnxHealthApi, LnxInfoApi, LnxQueryApi);

    let api_service =
        OpenApiService::new(api, "lnx REST API", env!("CARGO_PKG_VERSION"));
    let ui = api_service.redoc();
    let app = Route::new()
        .nest("/api/v0", api_service)
        .nest("/api/docs", ui);

    info!(
        "You can access local OpenAPI documentation is available @ http://localhost:{}/api/docs", 
        listen_address.port(),
    );
    info!(listen_addr = %listen_address, "Service API is running");
    poem::Server::new(TcpListener::bind(listen_address))
        .run_with_graceful_shutdown(
            app,
            lnx_common::signal_handler::wait_shutdown_signal()?,
            Some(Duration::from_secs(5)),
        )
        .await?;

    Ok(())
}
