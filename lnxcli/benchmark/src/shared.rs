use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::sampler::SamplerHandle;

pub(crate) type RequestClient = reqwest::Client;
pub(crate) type TargetUri = Arc<String>;
pub(crate) type Query = String;

fn get_client_and_addr(address: Arc<String>, index: &str) -> (RequestClient, TargetUri) {
    let search_addr = Arc::new(format!("{}/indexes/{}/search", address, index));
    let client = reqwest::Client::new();

    (client, search_addr)
}

pub(crate) async fn start_standard<T: Future<Output = Result<u16>>>(
    address: Arc<String>,
    mut sample: SamplerHandle,
    terms: Vec<String>,
    index: &str,
    callback: fn(RequestClient, TargetUri, Query) -> T,
) -> Result<()> {
    let (client, search_addr) = get_client_and_addr(address, index);
    sample.start_timing();

    for term in terms.iter() {
        let start = Instant::now();
        let status = callback(client.clone(), search_addr.clone(), term.clone()).await?;
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

pub(crate) async fn start_typing<T: Future<Output = Result<u16>>>(
    address: Arc<String>,
    mut sample: SamplerHandle,
    terms: Vec<String>,
    index: &str,
    callback: fn(RequestClient, TargetUri, Query) -> T,
) -> Result<()> {
    let (client, search_addr) = get_client_and_addr(address, index);
    sample.start_timing();

    for term in terms.iter() {
        let mut chars = term.chars();
        let mut search_term = vec![];
        while let Some(c) = chars.next() {
            search_term.push(c);

            let query: String = search_term.iter().collect();

            let start = Instant::now();
            let status = callback(client.clone(), search_addr.clone(), query).await?;
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
