mod auth;
mod error;
mod helpers;
mod responders;
mod routes;
mod state;

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use bincode::Options;
use clap::Parser;
use engine::structures::IndexDeclaration;
use engine::Engine;
use hyper::Server;
use mimalloc::MiMalloc;
use routerify::RouterService;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use crate::auth::AuthManager;
use crate::state::State;

static STORAGE_PATH: &str = "./index/engine-storage";
static INDEX_KEYSPACE: &str = "persistent_indexes";

#[derive(Debug, Parser)]
#[clap(name = "lnx", about, version)]
struct Settings {
    /// The log level filter, any logs that are above this level won't
    /// be displayed.
    ///
    /// For more detailed control you can use the `RUST_LOG` env var.
    #[clap(long, default_value = "info", env)]
    log_level: Level,

    /// An optional bool to disable ASNI colours and pretty formatting for logs.
    /// You probably want to disable this if using file-based logging.
    #[clap(long, env)]
    disable_pretty_logs: bool,

    /// The host to bind to (normally: '127.0.0.1' or '0.0.0.0'.)
    #[clap(long, short, default_value = "127.0.0.1", env)]
    host: String,

    /// The port to bind the server to.
    #[clap(long, short, default_value = "8000", env)]
    port: u16,

    /// The super user key.
    ///
    /// If specified this will enable auth mode and require a token
    /// bearer on every endpoint.
    ///
    /// The super user key is used to make tokens with given permissions.
    #[clap(long, env, hide_env_values = true)]
    super_user_key: Option<String>,

    /// The number of threads to use for the tokio runtime.
    ///
    /// If this is not set, the number of logical cores on the machine is used.
    #[clap(long, short = 't', env)]
    runtime_threads: Option<usize>,

    /// A optional directory to send persistent logs.
    ///
    /// Logs are split into hourly chunks.
    #[clap(long, env)]
    log_dir: Option<String>,

    /// If enabled each search request wont be logged.
    #[clap(long, env)]
    silent_search: bool,
}

fn main() {
    let settings = match setup() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error during server config parsing: {:?}", e);
            return;
        },
    };

    let _guard = setup_logger(
        settings.log_level,
        &settings.log_dir,
        !settings.disable_pretty_logs,
    );

    let threads = settings.runtime_threads.unwrap_or_else(num_cpus::get);
    info!("starting runtime with {} threads", threads);
    let maybe_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build();

    let result = match maybe_runtime {
        Ok(runtime) => runtime.block_on(start(settings)),
        Err(e) => {
            error!("error during runtime creation: {:?}", e);
            return;
        },
    };

    if let Err(e) = result {
        error!("error during server runtime: {:?}", e);
    }
}

fn setup_logger(
    level: Level,
    log_dir: &Option<String>,
    pretty: bool,
) -> Option<WorkerGuard> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", format!("{},compress=off,tantivy=info", level));
    }

    if let Some(dir) = log_dir {
        let file_appender = tracing_appender::rolling::hourly(dir, "lnx_.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let fmt = tracing_subscriber::fmt()
            .with_target(true)
            .with_writer(std::io::stdout.and(non_blocking))
            .with_thread_names(true)
            .with_thread_ids(true);

        if pretty {
            fmt.pretty().with_ansi(true).init();
        } else {
            fmt.json().with_ansi(false).init();
        }

        Some(guard)
    } else {
        let fmt = tracing_subscriber::fmt()
            .with_target(false)
            .with_thread_ids(true)
            .with_env_filter(EnvFilter::from_default_env());

        if pretty {
            fmt.pretty().with_ansi(true).init();
        } else {
            fmt.compact().with_ansi(false).init();
        }

        None
    }
}

/// Parses the config and sets up logging
fn setup() -> Result<Settings> {
    let config: Settings = Settings::parse();
    Ok(config)
}

async fn start(settings: Settings) -> Result<()> {
    let state = create_state(&settings).await?;
    let router = routes::get_router(state.clone());
    let service = RouterService::new(router).unwrap();

    let address: SocketAddr = format!("{}:{}", &settings.host, settings.port).parse()?;
    let server = Server::bind(&address).serve(service);

    info!("Lnx has started!");
    info!(
        "serving requests @ http://{}:{}",
        &settings.host, settings.port
    );
    info!("GitHub: https://github.com/lnx-search/lnx");
    info!("To ask questions visit: https://github.com/lnx-search/lnx/discussions");
    info!(
        "To get started you can check out the documentation @ http://{}:{}/docs",
        &settings.host, settings.port
    );
    if let Err(e) = server.await {
        error!("server error: {:?}", e)
    };

    info!("shutting down engine...");
    state.engine.shutdown().await?;

    Ok(())
}

async fn create_state(settings: &Settings) -> Result<State> {
    let db = sled::Config::new()
        .path(STORAGE_PATH)
        .mode(sled::Mode::HighThroughput)
        .use_compression(true)
        .open()?;

    let engine = load_existing_indexes(&db).await?;
    let auth = setup_authentication(&db, settings)?;

    Ok(State::new(engine, db, auth, !settings.silent_search))
}

#[instrument(name = "setup-existing-indexes", level = "info", skip(db))]
async fn load_existing_indexes(db: &sled::Db) -> Result<Engine> {
    info!("loading existing indexes...");

    let existing_indexes: Vec<IndexDeclaration> =
        if let Some(buff) = db.get(INDEX_KEYSPACE)? {
            let buff: Vec<u8> = bincode::options()
                .with_big_endian()
                .deserialize(&buff)
                .context("failed to deserialize index payload from persisted values.")?;

            serde_json::from_slice(&buff)?
        } else {
            vec![]
        };

    info!(
        " {} existing indexes discovered, recreating state...",
        existing_indexes.len()
    );

    let engine = Engine::default();
    for index in existing_indexes {
        engine.add_index(index, true).await?;
    }

    Ok(engine)
}

#[instrument(name = "setup-authentication", level = "info", skip(db, settings))]
fn setup_authentication(db: &sled::Db, settings: &Settings) -> Result<AuthManager> {
    let (enabled, key) = if let Some(ref key) = settings.super_user_key {
        (true, key.to_string())
    } else {
        (false, String::new())
    };

    let auth = AuthManager::new(enabled, key, &db)?;

    Ok(auth)
}
