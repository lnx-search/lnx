mod auth;
mod error;
mod helpers;
mod responders;
mod routes;
mod snapshot;
mod state;

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bincode::Options;
use clap::Parser;
use engine::structures::{IndexDeclaration, ROOT_PATH};
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
use crate::snapshot::{create_snapshot, load_snapshot};
use crate::state::State;

pub static STORAGE_SUB_ROOT_PATH: &str = "engine-storage";
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
    disable_asni_logs: bool,

    /// An optional bool to enable verbose logging, this includes additional metadata
    /// like span targets, thread-names, ids, etc... as well as the existing info.
    ///
    /// Generally you probably dont need this unless you're debugging.
    #[clap(long, env)]
    verbose_logs: bool,

    /// An optional bool to enable pretty formatting logging.
    ///
    /// This for most people, this is probably too much pretty formatting however, it
    /// can make reading the logs easier especially when trying to debug and / or test.
    #[clap(long, env)]
    pretty_logs: bool,

    /// An optional bool to enable json formatting logging.
    ///
    /// This formats the resulting log data into line-by-line JSON objects.
    /// This can be useful for log files or automatic log ingestion systems however,
    /// this can come at the cost for performance at very high loads.
    #[clap(long, env)]
    json_logs: bool,

    /// A optional directory to send persistent logs.
    ///
    /// Logs are split into hourly chunks.
    #[clap(long, env)]
    log_directory: Option<String>,

    /// If enabled each search request wont be logged.
    #[clap(long, env)]
    silent_search: bool,

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

    /// Load a past snapshot and use it's data.
    ///
    /// This expects `./index` not to have any existing data or exist.
    ///
    /// This is technically a separate sub command.
    #[clap(long)]
    load_snapshot: Option<String>,

    /// The interval time in hours to take an automatic snapshot.
    ///
    /// The limits are between 1 and 255 hours.
    ///
    /// This is quite a heavy process at larger index sizes so anything less
    /// than 24 hours is generally recommended against.
    ///
    /// The extracted snapshot will be saved in the directory provided by `--snapshot-dir`.
    #[clap(long, env)]
    auto_snapshot_period: Option<u8>,

    /// Generates a snapshot of the current server setup.
    ///
    /// The extracted snapshot will be saved in the directory provided by `--snapshot-dir`.
    #[clap(long)]
    snapshot: bool,

    /// The output directory where snapshots should be extracted to.
    #[clap(long, default_value = "./snapshots", env)]
    snapshot_dir: String,
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
        &settings.log_directory,
        !settings.disable_asni_logs,
        settings.pretty_logs,
        settings.json_logs,
        settings.verbose_logs,
    );

    if let Some(snapshot) = settings.load_snapshot {
        if let Err(e) = load_snapshot(Path::new(&snapshot)) {
            error!("error during snapshot extraction: {}", e);
        } else {
            info!(
                "snapshot successfully extracted, restart the server without \
                the `--load-snapshot` flag to start normally."
            );
        }
        return;
    }

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
        error!("error during lnx runtime: {:?}", e);
    }
}

fn setup_logger(
    level: Level,
    log_dir: &Option<String>,
    asni_colours: bool,
    pretty: bool,
    json: bool,
    verbose: bool,
) -> Option<WorkerGuard> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", format!("{},compress=off,tantivy=info", level));
    }

    let fmt = tracing_subscriber::fmt()
        .with_target(verbose)
        .with_thread_names(verbose)
        .with_thread_ids(verbose)
        .with_ansi(asni_colours)
        .with_env_filter(EnvFilter::from_default_env());

    if let Some(dir) = log_dir {
        let file_appender = tracing_appender::rolling::hourly(dir, "lnx.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let fmt = fmt.with_writer(std::io::stdout.and(non_blocking));

        if pretty {
            fmt.pretty().init();
        } else if json {
            fmt.json().init();
        } else {
            fmt.compact().init();
        }

        Some(guard)
    } else {
        if pretty {
            fmt.pretty().init();
        } else if json {
            fmt.json().init();
        } else {
            fmt.compact().init();
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
    tokio::fs::create_dir_all(Path::new(ROOT_PATH).join(STORAGE_SUB_ROOT_PATH)).await?;

    if settings.snapshot {
        info!("beginning snapshot process, this may take a while...");
        create_snapshot(Path::new(&settings.snapshot_dir)).await?;
        info!("snapshot process completed!");
        return Ok(());
    }

    if let Some(hours) = settings.auto_snapshot_period {
        if hours >= 1 {
            let output_dir = PathBuf::from(settings.snapshot_dir.clone());
            let interval_period = Duration::from_secs(hours as u64 * 60 * 60);
            tokio::spawn(
                async move { snapshot_loop(interval_period, output_dir).await },
            );
        }
    }

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

    tokio::select! {
        _r = tokio::signal::ctrl_c() => {
            info!("got shutdown signal, preparing shutdown");
        },
        r = server => {
            if let Err(e) = r {
                error!("server error: {:?}", e)
            };
        }
    }

    info!("shutting down engine...");
    state.engine.shutdown().await?;

    Ok(())
}

async fn create_state(settings: &Settings) -> Result<State> {
    let db = sled::Config::new()
        .path(Path::new(ROOT_PATH).join(STORAGE_SUB_ROOT_PATH))
        .mode(sled::Mode::HighThroughput)
        .use_compression(true)
        .open()
        .map_err(|e| anyhow!("failed to open database due to error {}", e))?;

    let engine = load_existing_indexes(&db)
        .await
        .map_err(|e| anyhow!("failed to load existing indexes due to error {}", e))?;
    let auth = setup_authentication(&db, settings)
        .map_err(|e| anyhow!("failed to load authentication data due to error {}", e))?;

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

#[instrument(name = "snapshot-loop")]
async fn snapshot_loop(interval_time: Duration, output: PathBuf) {
    let mut interval = tokio::time::interval(interval_time);

    loop {
        interval.tick().await;

        if let Err(e) = create_snapshot(&output).await {
            error!("failed to create snapshot due to error: {}", e);
        }
    }
}
