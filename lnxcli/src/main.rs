#[macro_use]
extern crate log;

use benchmark::{self, BenchMode, BenchTarget};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "lnxcli", about = "A utility cli for benchmarking and testing")]
pub enum Commands {
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
        #[structopt(long, short = "t")]
        threads: Option<usize>,

        /// The directory to output the image results.
        #[structopt(long, short = "out")]
        output_dir: String,

        /// The path to get the query string data.
        #[structopt(long, short = "terms")]
        search_terms: String,
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
        } => {
            let ctx = benchmark::Context {
                address,
                data_file,
                concurrency,
                target,
                mode,
                search_terms,
                threads: threads.unwrap_or_else(|| num_cpus::get()),
                output: output_dir,
            };

            info!("starting benchmark system");
            benchmark::run(ctx)
        }
    }?;

    info!("commands complete!");
    Ok(())
}
