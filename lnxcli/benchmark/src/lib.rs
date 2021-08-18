// #![allow(unused)]

#[macro_use] extern crate log;

mod sampler;
mod meilisearch;
mod lnx;

use std::str::FromStr;
use std::sync::Arc;

use tokio::fs;
use serde_json::Value;

/// The two benchmarking targets.
///
/// This was designed to compare MeiliSearch and lnx.
#[derive(Debug, Copy, Clone)]
pub enum BenchTarget {
    MeiliSearch,
    Lnx,
}

impl FromStr for BenchTarget {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "meilisearch" => Ok(Self::MeiliSearch),
            "lnx" => Ok(Self::Lnx),
            other => Err(format!(
                "unknown target type got {:?}, expected either 'meilisearch' or 'lnx'",
                other,
            ))
        }
    }
}

/// The benchmark type to run, this allows us to have several
/// modes and areas to test.
#[derive(Debug, Copy, Clone)]
pub enum BenchMode {
    /// Simulates typing our a sentence / word a character at a time.
    Typing,

    /// Simulates a standard searching a set of words.
    Standard,
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "typing" => Ok(Self::Typing),
            "standard" => Ok(Self::Standard),
            other => Err(format!(
                "unknown benchmark type got {:?}, expected either 'typing' or 'standard'",
                other,
            ))
        }
    }
}

pub struct Context {
    pub address: String,
    pub data_file: String,
    pub concurrency: usize,
    pub target: BenchTarget,
    pub mode: BenchMode,
    pub threads: usize,
    pub output: String,
}

pub fn run(ctx: Context) -> anyhow::Result<()> {
    info!("starting runtime with {} threads", ctx.threads);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(ctx.threads)
        .build()?;

    runtime.block_on(start(ctx))
}


async fn start(ctx: Context) -> anyhow::Result<()> {
    let mut sample_system = sampler::Sampler::new(ctx.output.clone());
    let ctx = Arc::new(ctx);

    let target = ctx.target;
    let mode = ctx.mode;

    let json_data = fs::read_to_string(&ctx.data_file).await?;
    let json_data: Value = serde_json::from_str(&json_data)?;
    match target {
        BenchTarget::MeiliSearch => meilisearch::prep(&ctx.address, json_data).await,
        BenchTarget::Lnx => lnx::prep(&ctx.address, json_data).await,
    }?;

    for _ in 0..ctx.concurrency {
        let temp_ctx = ctx.clone();
        let sample_handler = sample_system.get_handle();

        tokio::spawn(async move {
            let res = match (target, mode) {
                (BenchTarget::MeiliSearch, BenchMode::Standard) =>
                    meilisearch::bench_standard(temp_ctx, sample_handler).await,
                (BenchTarget::MeiliSearch, BenchMode::Typing) =>
                    meilisearch::bench_typing(temp_ctx, sample_handler).await,
                (BenchTarget::Lnx, BenchMode::Standard) =>
                    lnx::bench_standard(temp_ctx, sample_handler).await,
                (BenchTarget::Lnx, BenchMode::Typing) =>
                    lnx::bench_standard(temp_ctx, sample_handler).await,
            };

            if let Err(e) = res {
                error!("failed to start benching on worker {:?}", e);
            }
        });
    }

    sample_system.wait_and_sample().await?;

    Ok(())
}

