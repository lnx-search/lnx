mod basic_tantivy_indexing;
mod config;
mod models;
mod vfs_tantivy_indexing;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long)]
    datasets_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var(
            "RUST_LOG",
            "info,lnx_fs=warn,lnx_tantivy::indexer=warn,tantivy=warn",
        );
    }

    tracing_subscriber::fmt::init();

    let args = Args::parse();
    models::set_dataset_base(args.datasets_path);

    info!("Starting benchmarks");

    basic_tantivy_indexing::main().await?;
    vfs_tantivy_indexing::main().await?;

    Ok(())
}
