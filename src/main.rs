mod auth;
mod responders;
mod routes;
mod state;
mod helpers;

#[macro_use]
extern crate log;

use anyhow::Result;
use engine::structures::IndexDeclaration;
use engine::{Engine, StorageBackend};
use fern::colors::{Color, ColoredLevelConfig};
use log::LevelFilter;
use structopt::StructOpt;
use thruster::{async_middleware, App, Request, Server, ThrusterServer};

use crate::auth::AuthManager;
use crate::routes::auth::{create_token, revoke_token, revoke_all_tokens};
use crate::routes::default_handlers::handle_404;
use crate::state::{generate_context, Ctx, State};

static STORAGE_PATH: &str = "./index/engine-storage";

#[allow(unused)]
#[derive(Debug, StructOpt)]
#[structopt(name = "lnx", about = "A ultra-fast, adaptable search engine.")]
struct Settings {
    /// The log level filter, any logs that are above this level won't
    /// be displayed.
    #[structopt(long, default_value = "info", env)]
    log_level: LevelFilter,

    /// An optional bool to use ASNI colours for log levels.
    /// You probably want to disable this if using file-based logging.
    #[structopt(long, env)]
    pretty_logs: Option<bool>,

    /// The host to bind to (normally: '127.0.0.1' or '0.0.0.0'.)
    #[structopt(long, short, default_value = "127.0.0.1", env)]
    host: String,

    /// The port to bind the server to.
    #[structopt(long, short, default_value = "8000", env)]
    port: u16,

    /// If specified this will be used in the TLS config for HTTPS.
    #[structopt(long, env)]
    tls_key_file: Option<String>,

    /// If specified this will be used in the TLS config for HTTPS.
    #[structopt(long, env)]
    tls_cert_file: Option<String>,

    /// The super user key.
    ///
    /// If specified this will enable auth mode and require a token
    /// bearer on every endpoint.
    ///
    /// The super user key is used to make tokens with given permissions.
    #[structopt(long, short = "auth", env, hide_env_values = true)]
    super_user_key: Option<String>,

    /// The number of threads to use for the tokio runtime.
    ///
    /// If this is not set, the number of logical cores on the machine is used.
    #[structopt(long, short = "threads", env)]
    runtime_threads: Option<usize>,

    /// A optional file to send persistent logs.
    #[structopt(long, env)]
    log_file: Option<String>,
}

fn main() {
    let settings = match setup() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error during server setup: {:?}", e);
            return;
        },
    };

    let threads = settings.runtime_threads.unwrap_or_else(|| num_cpus::get());
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

fn setup_logger(level: LevelFilter, log_file: &Option<String>, pretty: bool) -> Result<()> {
    let mut colours = ColoredLevelConfig::new();

    if pretty {
        colours = colours
            .info(Color::Green)
            .warn(Color::Yellow)
            .error(Color::BrightRed)
            .debug(Color::Magenta)
            .trace(Color::Cyan);
    }

    let mut builder = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{} | {} | {:<5} - {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                colours.color(record.level()),
                message,
            ))
        })
        .level(level)
        .chain(std::io::stdout());

    if let Some(file) = log_file {
        builder = builder.chain(fern::log_file(file)?);
    }

    builder.apply()?;

    Ok(())
}

/// Parses the config and sets up logging
fn setup() -> Result<Settings> {
    let config: Settings = Settings::from_args();
    setup_logger(
        config.log_level,
        &config.log_file,
        config.pretty_logs.unwrap_or(true),
    )?;
    Ok(config)
}

async fn start(settings: Settings) -> Result<()> {
    let state = create_state().await?;
    let mut app = App::<Request, Ctx, State>::create(generate_context, state);

    app.set404(async_middleware!(Ctx, [handle_404]));

    app.post("/auth", async_middleware!(Ctx, [create_token]));
    app.delete("/auth", async_middleware!(Ctx, [revoke_all_tokens]));
    app.post("/auth/:token/revoke", async_middleware!(Ctx, [revoke_token]));
    //app.put("/auth/:token", async_middleware!(Ctx, [create_token]));

    //app.put("/indexes/:index/search", async_middleware!(Ctx, [create_token]));
    //app.post("/indexes/:index/stopwords", async_middleware!(Ctx, [create_token]));
    //app.delete("/indexes/:index/stopwords", async_middleware!(Ctx, [create_token]));
    //app.post("/indexes/:token/documents", async_middleware!(Ctx, [create_token]));
    //app.delete("/indexes/:token/documents", async_middleware!(Ctx, [create_token]));
    //app.get("/indexes/:token/documents/:document_id", async_middleware!(Ctx, [create_token]));

    let server = Server::new(app);
    server.build(&settings.host, settings.port).await;
    Ok(())
}

async fn create_state() -> Result<State> {
    let storage = StorageBackend::connect(Some(STORAGE_PATH.to_string()))?;
    let engine = {
        info!("loading existing indexes...");
        let buffer = storage.load_structure("persistent_indexes")?;
        let existing_indexes: Vec<IndexDeclaration> = bincode::deserialize(&buffer)?;
        info!(
            " {} existing indexes discovered, recreating state...",
            existing_indexes.len()
        );

        let engine = Engine::new();
        for index in existing_indexes {
            engine.add_index(&index).await?;
        }

        engine
    };

    let auth = AuthManager::new();

    Ok(State::new(engine, storage, auth))
}
