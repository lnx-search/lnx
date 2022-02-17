use anyhow::Result;
use itertools::Itertools;
use lnx_common::types::document::Document;
use lnx_storage::{engine, ChangeKind, ChangeLogEntry};
use tantivy::chrono::Utc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::error::DocumentError;

const MAX_INSERT_CONCURRENCY: usize = 10;

pub async fn add_documents(
    index_name: &str,
    documents: Vec<Document>,
) -> Result<(), DocumentError> {
    let indexes = engine().indexes();

    let index = indexes
        .get(index_name)
        .ok_or_else(|| DocumentError::UnknownIndex(index_name.to_string()))?
        .clone();

    let size = (documents.len() / MAX_INSERT_CONCURRENCY) + 1;
    let chunks = documents.into_iter().chunks(size);

    let mut handles = Vec::with_capacity(MAX_INSERT_CONCURRENCY);
    for chunk in &chunks {
        let chunk: Vec<Document> = chunk.collect();
        let store = index.clone();
        let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let ids: Vec<Uuid> = (0..chunk.len()).map(|_| Uuid::new_v4()).collect();

            let docs: Vec<(Uuid, Document)> = chunk
                .into_iter()
                .zip(ids.iter())
                .map(|(l, r)| (*r, l))
                .collect();

            store.docs().add_documents(docs).await?;

            let chunks = ids.chunks(5_000);

            for chunk in chunks {
                let change_log = ChangeLogEntry {
                    kind: ChangeKind::Append,
                    affected_docs: chunk.to_vec(),
                    timestamp: Utc::now().timestamp_millis(),
                };

                store.docs().append_changes(change_log).await?;
            }

            Ok(())
        });

        handles.push(handle);
    }

    // TODO rollback changes on fail?
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
