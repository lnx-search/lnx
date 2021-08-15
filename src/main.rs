#[macro_use]
extern crate log;

use anyhow::{Result, Error};
use structopt::StructOpt;
use log::LevelFilter;
use fern::colors::{Color, ColoredLevelConfig};

#[derive(Debug, StructOpt)]
#[structopt(name = "lnx", about = "A ultra-fast, adaptable search engine.")]
struct Settings {
    #[structopt(long, default_value = "info", env)]
    log_level: LevelFilter,

    #[structopt(long, env)]
    pretty_logs: Option<bool>,

    #[structopt(long, short, default_value = "127.0.0.1", env)]
    host: String,

    #[structopt(long, short, default_value = "8000", env)]
    port: u16,

    #[structopt(long, env)]
    tls_key_file: Option<String>,

    #[structopt(long, env)]
    tls_cert_file: Option<String>,

    #[structopt(long, short = "auth", env, hide_env_values = true)]
    authentication_key: Option<String>,

    #[structopt(long, short = "threads", env)]
    runtime_threads: Option<usize>,

    #[structopt(long, env)]
    log_file: Option<String>,
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


fn main()  {
    let s = match setup() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error during server setup: {:?}", e);
            return;
        }
    };

    let threads = s.runtime_threads.unwrap_or_else(|| num_cpus::get());
    info!("starting runtime with {} threads", threads);
    let maybe_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build();

    let result = match maybe_runtime {
        Ok(runtime) => runtime.block_on(start()),
        Err(e) => {
            error!("error during runtime creation: {:?}", e);
            return;
        },
    };

    if let Err(e) = result {
        error!("error during server runtime: {:?}", e);
    }
}

/// Parses the config and sets up logging
fn setup() -> Result<Settings> {
    let config: Settings = Settings::from_args_safe()?;
    setup_logger(config.log_level, &config.log_file, config.pretty_logs.unwrap_or(true))?;
    Ok(config)
}


/// Starts the server in an async context.
async fn start() -> Result<()> {
    Err(Error::msg("hello"))
}
