mod index;
mod models;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use anyhow::Context;
use poem::listener::TcpListener;
use poem::{Route, Server};
use poem_openapi::{OpenApiService, Tags};

/// Settings for the REST HTTP server.
pub struct RestSettings {
    /// The listen address of the server.
    pub listen_address: SocketAddr,
}

impl Default for RestSettings {
    fn default() -> Self {
        Self {
            listen_address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        }
    }
}

/// Runs the lnx REST api.
///
/// This exposes the provided engine via a REST api with documentation
/// generated for the OpenAPI spec.
pub async fn run_rest_server(settings: RestSettings) -> anyhow::Result<()> {
    let merged_services = index::IndexApiRoutes {};

    let api_service =
        OpenApiService::new(merged_services, "lnx REST API", env!("CARGO_PKG_VERSION"));

    let ui = api_service.redoc();
    let app = Route::new().nest("/", api_service).nest("/docs", ui);

    Server::new(TcpListener::bind(settings.listen_address))
        .run(app)
        .await
        .context("Run REST server")?;

    Ok(())
}

#[derive(Tags)]
pub enum Tag {
    /// API routes for managing and interacting with indexes.
    Indexes,
}
