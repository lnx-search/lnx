mod commands;
mod logging;

use anyhow::Context;
use clap::Parser;
use tracing::info;

use crate::logging::init_logging;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(version, about)]
/// lnx is a fast and efficient search engine for performing full-text search, user-facing search
/// and analytics.
pub struct Args {
    #[arg(long, env = "LNX_LOG_LEVEL", default_value = "info")]
    /// Set the log level of the server.
    ///
    /// This can filter on various levels, for example `info,lnx::indexer=warn`
    /// will display all logs at `info` level severity and above, except for log
    /// events that appear within the `lnx::indexer` block of operations.
    log_level: String,
    #[arg(long, env = "LNX_LOG_JSON")]
    /// Emit logs in JSON format rather than as plain text.
    log_json: bool,
    #[arg(long, env = "LNX_LOG_NO_ANSI")]
    /// Disable ANSI colour codes being present in the logs.
    ///
    /// ANSI codes can cause rendering issues on various observability platforms
    /// or text file logging due to its escape sequences.
    log_no_ansi: bool,
    #[command(subcommand)]
    command: commands::Commands,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_logging(&args).context("Init logging")?;

    info!("🦀 lnx v{}", env!("CARGO_PKG_VERSION"));
    info!("Check us out @ {}", env!("CARGO_PKG_HOMEPAGE"));
    info!("Support @ {}/discord", env!("CARGO_PKG_HOMEPAGE"));
    info!("Docs @ {}/docs", env!("CARGO_PKG_HOMEPAGE"));

    args.command.display_startup_message();
    args.command.execute().await?;

    Ok(())
}
