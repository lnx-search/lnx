#[macro_use]
extern crate log;

use structopt::StructOpt;
use std::net::SocketAddr;

use benchmark::{self, BenchMode, BenchTarget};


#[derive(Debug, StructOpt)]
#[structopt(name = "lnxcli", about = "A utility cli for benchmarking and testing")]
pub enum Commands {
    /// Benchmark lnx or MeiliSearch to get stats and info on the current
    /// configuration.
    ///
    /// This is very useful to do when adjusting your worker thread counts
    /// to test latency and throughput.
    Bench {
        /// The address of the server to benchmark.
        #[structopt(long, short = "a")]
        address: String,

        /// The target platform to bench mark, either 'lnx' or 'meilisearch'.
        #[structopt(long)]
        target: BenchTarget,

        /// The target platform to bench mark, either 'lnx' or 'meilisearch'.
        #[structopt(long, short = "m")]
        mode: BenchMode,

        /// The path to the target JSON file to load data for the benchmark.
        #[structopt(long, short = "data")]
        data_file: String,

        /// The amount of concurrent searches to run at any one time.
        #[structopt(long, short = "c")]
        concurrency: usize,

        /// The number of threads to run the test with.
        ///
        /// If not set the number of logical CPU cores is used.
        #[structopt(long)]
        threads: Option<usize>,

        /// The directory to output the image results.
        #[structopt(long, short = "out")]
        output_dir: String,

        /// The path to get the query string data.
        #[structopt(long, short = "terms")]
        search_terms: String,

        /// Whether or not use the existing data in the system or flush it.
        #[structopt(long)]
        no_prep: bool,
    },

    /// Runs a demo app to play around with the search as you type setup.
    Demo {
        /// The address to bind the webserver to.
        #[structopt(long, short = "b", default_value = "127.0.0.1:5000")]
        bind: SocketAddr,

        /// The target server address, e.g. http://127.0.0.1:8000
        ///
        /// This expects that the url isn't changed from the defaults.
        #[structopt(long = "target")]
        target_server: String,

        /// If this is set the system will not upload a new set of docs when
        /// started.
        #[structopt(long)]
        no_prep: bool,

        /// The index name to target.
        #[structopt(long, short, default_value = "demo")]
        index: String,

        /// Uses the fast fuzzy system when creating the index.
        #[structopt(long)]
        use_fast_fuzzy: bool,

        /// Enables stop word stripping when creating the index.
        #[structopt(long)]
        strip_stop_words: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let _ = std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    let cmd: Commands = Commands::from_args();

    match cmd {
        Commands::Bench {
            address,
            target,
            mode,
            data_file,
            concurrency,
            threads,
            output_dir,
            search_terms,
            no_prep,
        } => {
            let ctx = benchmark::Context {
                address,
                data_file,
                concurrency,
                target,
                mode,
                search_terms,
                no_prep,
                threads: threads.unwrap_or_else(|| num_cpus::get()),
                output: output_dir,
            };

            info!("starting benchmark system");
            benchmark::run(ctx)
        },

        Commands::Demo {
            bind,
            target_server,
            no_prep,
            index,
            use_fast_fuzzy,
            strip_stop_words,
        } => {
            let ctx = demo::Context {
                bind,
                target_server,
                no_prep,
                index,
                use_fast_fuzzy,
                strip_stop_words,
            };

            info!("starting demo app");
            demo::run(ctx)
        }
    }?;

    info!("commands complete!");
    Ok(())
}
