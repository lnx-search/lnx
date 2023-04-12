use anyhow::Context;

mod storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "lnx_storage=info,performance_tests=info");
    }

    lnx_executor::build_default_pools(8)?;
    tracing_subscriber::fmt::init();

    //storage::run_small_blocks_bench()
    //    .await
    //    .context("Failed to run small_blocks_bench")?;
    //storage::run_big_blocks_bench()
    //    .await
    //    .context("Failed to run big_blocks_bench")?;
    storage::run_fragments_bench()
        .await
        .context("Failed to run run_fragments_bench")?;
    Ok(())
}
