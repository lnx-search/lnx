#[macro_use]
extern crate log;

mod lnx;
mod meilisearch;
mod sampler;
mod shared;

use anyhow::Result;

use rand::seq::SliceRandom;
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value;
use tokio::fs;
use tokio::task::JoinHandle;

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
            )),
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
            )),
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
    pub search_terms: String,
    pub no_prep: bool,
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

    if !ctx.no_prep {
        prep_systems(target, &ctx.address, &ctx.data_file).await?;
    }
    let terms = get_terms(&ctx.search_terms).await?;
    let address = Arc::new(ctx.address.clone());

    info!("Service ready! Beginning benchmark.");
    info!("     Concurrency @ {} clients", ctx.concurrency);
    info!("     Searching @ {} sentences", terms.len());
    info!("     Mode @ {:?}", mode);

    let mut handles = vec![];
    for _ in 0..ctx.concurrency {
        let addr = address.clone();
        let mut temp_terms = terms.clone();
        let mut rng = rand::thread_rng();
        temp_terms.shuffle(&mut rng);

        let sample_handler = sample_system.get_handle();

        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            match (target, mode) {
                (BenchTarget::MeiliSearch, BenchMode::Standard) => {
                    meilisearch::bench_standard(addr, sample_handler, temp_terms).await
                }
                (BenchTarget::MeiliSearch, BenchMode::Typing) => {
                    meilisearch::bench_typing(addr, sample_handler, temp_terms).await
                }
                (BenchTarget::Lnx, BenchMode::Standard) => {
                    lnx::bench_standard(addr, sample_handler, temp_terms).await
                }
                (BenchTarget::Lnx, BenchMode::Typing) => {
                    lnx::bench_typing(addr, sample_handler, temp_terms).await
                }
            }
        });

        handles.push(handle)
    }

    for handle in handles {
        let _ = handle.await??;
    }

    sample_system.wait_and_sample().await?;

    Ok(())
}

async fn prep_systems(target: BenchTarget, address: &str, path: &str) -> Result<()> {
    let json_data = fs::read_to_string(path).await?;
    let json_data: Value = serde_json::from_str(&json_data)?;
    match target {
        BenchTarget::MeiliSearch => meilisearch::prep(address, json_data).await,
        BenchTarget::Lnx => lnx::prep(address, json_data).await,
    }
}

async fn get_terms(path: &str) -> Result<Vec<String>> {
    let json_data = fs::read_to_string(path).await?;
    Ok(serde_json::from_str(&json_data)?)
}
