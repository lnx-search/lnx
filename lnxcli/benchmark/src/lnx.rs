use std::sync::Arc;
use std::time::Instant;
use serde_json::Value;
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
        .await?;

    let _ = client.post(format!("{}/indexes/bench/commit", address))
        .send()
        .await?;

    let delta = start.elapsed();
    info!("lnx took {:?} to process submitted documents", delta);

    Ok(())
}


pub(crate) async fn bench_standard(
    address: Arc<String>,
    mut sample: SamplerHandle,
    terms: Vec<String>,
) -> anyhow::Result<()> {
    let search_addr = format!("{}/indexes/bench/search", address);
    let client = reqwest::Client::new();
    sample.start_timing();

    for term in terms.iter() {
        let start = Instant::now();
        let status = search(&client, &search_addr, term.clone()).await?;
        let stop = start.elapsed();

        if status != 200 {
            sample.register_error(status);
        } else {
            sample.add_latency(stop);
        }
    }

    sample.finish();
    Ok(())
}


pub(crate) async fn bench_typing(
    address: Arc<String>,
    mut sample: SamplerHandle,
    terms: Vec<String>,
) -> anyhow::Result<()> {
    let search_addr = format!("{}/indexes/bench/search", address);
    let client = reqwest::Client::new();
    sample.start_timing();

    for term in terms.iter() {
        let mut chars = term.chars();
        let mut search_term = vec![];
        while let Some(c) = chars.next() {
            search_term.push(c);

            let query: String = search_term.iter().collect();

            let start = Instant::now();
            let status = search(&client, &search_addr, query).await?;
            let stop = start.elapsed();

            if status != 200 {
                sample.register_error(status);
            } else {
                sample.add_latency(stop);
            }
        }
    }

    sample.finish();
    Ok(())
}

async fn search(client: &reqwest::Client, uri: &str, query: String) -> anyhow::Result<u16> {
    let mut uri = reqwest::Url::parse(uri)
        .expect("get uri");

    uri.set_query(Some(&format!("query={}", query)));

    let r = client.get(uri)
        .send()
        .await?;

    Ok(r.status().as_u16())
}
