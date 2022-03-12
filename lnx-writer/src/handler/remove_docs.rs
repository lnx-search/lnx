use std::time::Instant;

use hashbrown::HashSet;
use lnx_common::types::document::DocId;
use lnx_storage::templates::change_log::ChangeLogEntry;
use lnx_storage::types::Timestamp;

use super::batcher;
use crate::error::DocumentError;

pub const MAX_DELETE_CONCURRENCY: usize = 10;

pub async fn remove_documents(
    index_name: &str,
    documents: Vec<DocId>,
) -> Result<(), DocumentError> {
    let index_store = lnx_controller::get(index_name)
        .ok_or_else(|| DocumentError::UnknownIndex(index_name.to_string()))?;

    let docs_len = documents.len();

    let start = Instant::now();
    batcher::batch(
        MAX_DELETE_CONCURRENCY,
        index_store,
        documents,
        move |chunk, store| async move { store.remove_documents(chunk).await },
    )
    .await?;

    info!(
        "Removed {} document(s) from the database in {:?}.",
        docs_len,
        start.elapsed(),
    );

    Ok(())
}
