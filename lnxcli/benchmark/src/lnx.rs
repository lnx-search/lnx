use std::sync::Arc;
use std::time::Instant;
use serde_json::Value;

use crate::Context;
use crate::sampler::SamplerHandle;

pub(crate) async fn prep(address: &str, data: Value) -> anyhow::Result<()> {
        let client = reqwest::Client::new();

    // Clear the existing docs
    let _ = client.delete(format!("{}/indexes/bench/documents/clear", address))
        .send()
        .await?;

    let start = Instant::now();
    let _ = client
        .post(format!("{}/indexes/bench/documents", address))
        .json(&data)
        .send()
        .await?
        .json()
        .await?;

    let delta = start.elapsed();
    info!("lnx took {:?} to process submitted documents", delta);

    Ok(())
}


pub(crate) async fn bench_standard(ctx:  Arc<Context>, sample: SamplerHandle) -> anyhow::Result<()> {
    Ok(())
}

pub(crate) async fn bench_typing(ctx:  Arc<Context>, sample: SamplerHandle) -> anyhow::Result<()> {
    Ok(())
}