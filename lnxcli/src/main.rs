#[macro_use] extern crate log;

use structopt::StructOpt;
use benchmark::{self, BenchTarget, BenchMode};


#[derive(Debug, StructOpt)]
#[structopt(name = "lnxcli", about = "A utility cli for benchmarking and testing")]
pub enum Commands {
    Bench {
        /// The address of the server to benchmark.
        #[structopt(long, short = "addr")]
        address: String,

        /// The target platform to bench mark, either 'lnx' or 'meilisearch'.
        #[structopt(long)]
        target: BenchTarget,

        /// The target platform to bench mark, either 'lnx' or 'meilisearch'.
        #[structopt(long)]
        mode: BenchMode,

        /// The path to the target JSON file to load data for the benchmark.
        #[structopt(long)]
        data_file: String,

        /// The amount of concurrent searches to run at any one time.
        #[structopt(long, short)]
        concurrency: usize,

        /// The number of threads to run the test with.
        ///
        /// If not set the number of logical CPU cores is used.
        #[structopt(long, short)]
        threads: Option<usize>,
    },
}

fn main() -> anyhow::Result<()> {
    let _ = std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    let cmd: Commands = Commands::from_args();

    match cmd {
        Commands::Bench { address, target, mode, data_file, concurrency, threads } => {
            let ctx = benchmark::Context {
                address,
                data_file,
                concurrency,
                target,
                mode,
                threads: threads.unwrap_or_else(|| num_cpus::get())
            };

            info!("starting benchmark system");
            benchmark::run(ctx)
        }
    }?;

    info!("commands complete!");
    Ok(())
}
