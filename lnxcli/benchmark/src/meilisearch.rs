use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Duration;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;


use crate::sampler::SamplerHandle;

#[derive(Debug, Deserialize)]
struct EnqueueResponseData {
    #[serde(rename = "updateId")]
    update_id: usize
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
#[serde(rename_all = "lowercase")]
enum CheckData {
    Processed {
        #[serde(rename = "processedAt")]
        processed_at: DateTime<Utc>,

        #[serde(rename = "enqueuedAt")]
        enqueued_at: DateTime<Utc>,

        #[serde(flatten)]
        _other: HashMap<String, Value>,
    },
    Enqueued {
        #[serde(flatten)]
        _other: HashMap<String, Value>,
    },
    Processing {
        #[serde(flatten)]
        _other: HashMap<String, Value>,
    },
}

pub(crate) async fn prep(address: &str, data: Value) -> anyhow::Result<()> {
    let client = reqwest::Client::new();

    // Clear the existing docs
    let _ = client.delete(format!("{}/indexes/bench/documents", address))
        .send()
        .await?;

    let data: EnqueueResponseData = client
        .post(format!("{}/indexes/bench/documents", address))
        .json(&data)
        .send()
        .await?
        .json()
        .await?;

    let status = data.update_id;
    let delta;

    loop {
        let data: CheckData = client
            .get(format!("{}/indexes/bench/updates/{}", address, status))
            .send()
            .await?
            .json()
            .await?;

        if let CheckData::Processed { processed_at, enqueued_at, .. } = data {
            delta = processed_at - enqueued_at;
            break
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    info!("MeiliSearch took {}.{}s to process submitted documents", delta.num_seconds(), delta.num_milliseconds() / 100i64);

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
        search(&client, &search_addr, term.clone()).await?;
        let stop = start.elapsed();
        sample.add_latency(stop);
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
            search(&client, &search_addr, query).await?;
            let stop = start.elapsed();
            sample.add_latency(stop);
        }
    }

    sample.finish();
    Ok(())
}


#[derive(Serialize)]
struct QueryPayload {
    q: String
}

async fn search(client: &reqwest::Client, uri: &str, query: String) -> anyhow::Result<()> {
    let r = client.post(uri)
        .json(&QueryPayload { q: query })
        .send()
        .await?;

    assert_eq!(r.status(), StatusCode::OK);

    Ok(())
}

