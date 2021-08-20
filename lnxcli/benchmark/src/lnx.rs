use crate::sampler::SamplerHandle;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use crate::shared::{TargetUri, RequestClient, Query};

pub(crate) async fn prep(address: &str, data: Value) -> anyhow::Result<()> {
    let client = reqwest::Client::new();

    // Clear the existing docs
    let _ = client
        .delete(format!("{}/indexes/bench/documents/clear", address))
        .send()
        .await?;

    let start = Instant::now();
    let _ = client
        .post(format!("{}/indexes/bench/documents", address))
        .json(&data)
        .send()
        .await?;

    let _ = client
        .post(format!("{}/indexes/bench/commit", address))
        .send()
        .await?;

    let delta = start.elapsed();
    info!("lnx took {:?} to process submitted documents", delta);

    Ok(())
}

pub(crate) async fn bench_standard(
    address: Arc<String>,
    sample: SamplerHandle,
    terms: Vec<String>,
) -> anyhow::Result<()> {
    crate::shared::start_standard(
        address,
        sample,
        terms,
        move |client, uri, query| {
            async {
                search(client, uri, query).await
            }
        }
    ).await
}

pub(crate) async fn bench_typing(
    address: Arc<String>,
    sample: SamplerHandle,
    terms: Vec<String>,
) -> anyhow::Result<()> {
    crate::shared::start_typing(
        address,
        sample,
        terms,
        move |client, uri, query| {
            async {
                search(client, uri, query).await
            }
        }
    ).await
}

async fn search(client: RequestClient, uri: TargetUri, query: Query) -> anyhow::Result<u16> {
    let mut uri = reqwest::Url::parse(uri.as_ref()).expect("get uri");

    uri.set_query(Some(&format!("query={}", query)));

    let r = client.get(uri).send().await?;

    Ok(r.status().as_u16())
}
