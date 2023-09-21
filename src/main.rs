use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// The lnx CLI arguments.
struct Args {
    #[arg(long, env = "LNX_LOG_LEVEL", default_value = "info")]
    /// The display logging level.
    ///
    /// Supported values are:
    ///
    /// - `error`
    /// - `warn`
    /// - `info`
    /// - `debug`
    /// - `trace`
    ///
    /// Specific modules can be adjusted using a key-value pair:
    ///
    /// E.g. `info,lnx_metastore=debug` will enable a logging level of `info` for everything
    /// other than the `lnx_metastore` which will have a logging level of `debug`.
    ///
    /// Sometimes you might run into an error and need to lower the level to `debug`.
    /// It is very rare that you ever need the `trace` level
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
/// The commands the lnx CLI supports.
enum Commands {
    /// Runs the lnx search service.
    Run {
        #[arg(long, env = "LNX_BIND_ADDRESS", default_value = "127.0.0.1:8080")]
        /// The socket address for the server to bind to.
        ///
        /// Defaults to `127.0.0.1:8080`.
        bind: SocketAddr,

        #[arg(long, env = "LNX_BROADCAST_ADDRESS", default_value = "127.0.0.1:8080")]
        /// The address that is broadcast to other nodes in order
        /// to replicate data across.
        ///
        /// If you are running a standalone instance you do not need to worry
        /// about this parameter.
        ///
        /// If you are running
        broadcast_address: String,

        #[arg(
            long,
            env = "LNX_METASTORE_URI",
            default_value = "sqlite://./lnx-metastore.db"
        )]
        /// The metastore service URI.
        ///
        /// This tells lnx what it can find the global metadata across a cluster.
        /// By default, this uses SQLite for convenience running as a standalone node.
        ///
        /// Possible options are:
        ///
        /// - `sqlite://./path/to/sqlite.db`    (SQLite3 Support, No HA Support)
        /// - `postgres://localhost:5432/my-metastore-db`   (PostgreSQL Support)
        /// - `scylla://localhost:5432/my-cassandra-keyspace`   (ScyllaDB/Cassandra Support)
        /// - `tikv://localhost:5432/my-tikv-keyspace`  (TiKV Support)
        metastore_uri: String,

        #[arg(long, env = "LNX_MAX_THREADS")]
        /// The maximum number of threads lnx can use for indexing *and* searching.
        ///
        /// By default resources are split so 1/3 of the number of threads are used
        /// for indexing, and the rest are used for searching.
        ///
        /// If no value is specified, this will default to the number of CPU cores
        /// available on the instance.
        max_threads: Option<NonZeroUsize>,

        #[arg(long, env = "LNX_BASE_PATH", default_value = "lnx-data")]
        /// The base path used by lnx when storing data.
        ///
        /// All persisted data will be written within this folder.
        ///
        /// By default this uses a relative path, writing to the `lnx-data` folder
        /// in the current working directory of lnx.
        base_path: PathBuf,
    },
    /// Ingest local files quickly and easily.
    Ingest {
        #[arg(short, long)]
        /// The input file path to ingest.
        ///
        /// This supports globs to select multiple files to ingest.
        path: String,

        #[arg(short, long, env = "LNX_SERVICE_ADDRESS")]
        /// The lnx indexer address to feed the data into.
        address: String,
    },
}

fn main() -> Result<()> {
    let args: Args = Args::parse();

    setup_logging(&args.log_level)?;

    Ok(())
}

fn setup_logging(level: &str) -> Result<()> {
    std::env::set_var("RUST_LOG", level);
    tracing_subscriber::fmt::init();
    Ok(())
}
