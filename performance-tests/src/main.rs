use std::env::temp_dir;
use std::time::Duration;

use anyhow::Context;
use tracing::warn;

mod storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "performance_tests=info");
    }

    lnx_executor::build_default_pools(8)?;
    tracing_subscriber::fmt::init();

    storage::run_small_blocks_bench()
        .await
        .context("Failed to run small_blocks_bench")?;
    storage::run_big_blocks_bench()
        .await
        .context("Failed to run big_blocks_bench")?;
    storage::run_fragments_bench()
        .await
        .context("Failed to run run_fragments_bench")?;

    tokio::time::sleep(Duration::from_secs(30)).await;

    if let Err(e) = std::fs::remove_dir_all(temp_dir().join("lnx-tests")) {
        warn!(error = ?e, "Failed to remove tmp directory");
    }

    Ok(())
}
