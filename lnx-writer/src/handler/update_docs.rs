use std::time::Instant;
use anyhow::Result;
use lnx_common::types::document::{DocId, Document};
use crate::error::DocumentError;
use serde::{Serialize, Deserialize};
use tantivy::doc;
use lnx_storage::templates::doc_store::DocumentUpdate;
use crate::handler::batcher;

const MAX_UPDATE_CONCURRENCY: usize = 128;


pub async fn update_documents(
    index_name: &str,
    documents: Vec<DocumentUpdate>,
) -> Result<()> {
    let index_store = lnx_controller::get(index_name)
        .ok_or_else(|| DocumentError::UnknownIndex(index_name.to_string()))?;

    let docs_len = documents.len();
    let start = Instant::now();
    batcher::batch(
        MAX_UPDATE_CONCURRENCY,
        index_store,
        documents,
        move |chunk, store| {
            async move {
                store.update_documents(chunk).await
            }
        }
    ).await?;

    info!(
        "Update {} document(s) in the database in {:?}.",
        docs_len,
        start.elapsed(),
    );

    Ok(())
}