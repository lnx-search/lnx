use std::sync::Arc;
use serde::Deserialize;
use serde_json::Value;
use chrono::{DateTime, Utc};

use crate::Context;
use crate::sampler::SamplerHandle;
use std::collections::HashMap;
use tokio::time::Duration;

#[derive(Debug, Deserialize)]
struct EnqueueResponseData {
    #[serde(rename = "updateId")]
    update_id: usize
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Processed,
    Enqueued,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
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
    }
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

    info!("MeiliSearch took {:?} to process submitted documents", delta);

    Ok(())
}

pub(crate) async fn bench_standard(ctx: Arc<Context>, sample: SamplerHandle) -> anyhow::Result<()> {
    Ok(())
}

pub(crate) async fn bench_typing(ctx: Arc<Context>, sample: SamplerHandle) -> anyhow::Result<()> {
    Ok(())

}
