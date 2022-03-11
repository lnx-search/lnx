use std::time::Instant;

use anyhow::Result;
use hashbrown::HashSet;
use lnx_common::schema::Schema;
use lnx_common::types::document::{DocId, Document, TypeSafeDocument};
use lnx_storage::templates::change_log::ChangeLogEntry;
use lnx_storage::types::Timestamp;
use uuid::Uuid;

use super::error::DocumentError;
use crate::error::ValidationError;

const MAX_INSERT_CONCURRENCY: usize = 256;

pub async fn add_documents(
    index_name: &str,
    documents: Vec<Document>,
) -> Result<(), DocumentError> {
    let index = lnx_controller::get(index_name)
        .ok_or_else(|| DocumentError::UnknownIndex(index_name.to_string()))?;

    let docs_len = documents.len();
    let (tx, rx) = async_channel::bounded(MAX_INSERT_CONCURRENCY * 2);
    let mut handles = Vec::with_capacity(MAX_INSERT_CONCURRENCY);
    for _ in 0..MAX_INSERT_CONCURRENCY {
        let tasks = rx.clone();
        let local_index = index.clone();
        let handle = tokio::spawn(async move {
            let mut changed_segments = HashSet::new();
            while let Ok(docs) = tasks.recv().await {
                let tokens = local_index.add_documents(docs).await?;

                changed_segments.extend(tokens.into_iter());
            }

            for segment in changed_segments {
                local_index
                    .append_changes(ChangeLogEntry {
                        affected_segment: segment,
                        timestamp: Timestamp::default(),
                    })
                    .await?;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    let start = Instant::now();

    let documents = parse_and_validate_documents(index.ctx().schema(), documents)?;
    let chunks = documents.chunks(MAX_INSERT_CONCURRENCY);

    info!("Got {} document(s) to process...", docs_len);
    for chunk in chunks {
        // SAFETY: We know chunks is always going to outlive our tasks here.
        let chunk: &'static [(DocId, TypeSafeDocument)] =
            unsafe { std::mem::transmute(chunk) };
        if tx.send(chunk).await.is_err() {
            break;
        };
    }

    // WARNING: This must be explicitly called in order to prevent deadlocks.
    tx.close();

    // TODO rollback changes on fail?
    let mut failed_tasks = vec![];
    for handle in handles {
        let res = handle.await;
        match res {
            Ok(Ok(())) => {},
            Ok(Err(e)) => failed_tasks.push(e),
            Err(e) => failed_tasks.push(anyhow::Error::from(e)),
        };
    }

    let num_errors = failed_tasks.len();
    for error in failed_tasks {
        error!("{} Tasks failed with the error: {}", num_errors, error)
    }
    info!(
        "Added {} document(s) to the database in {:?}.",
        docs_len,
        start.elapsed(),
    );

    Ok(())
}

fn parse_and_validate_documents(
    schema: &Schema,
    docs: Vec<Document>,
) -> Result<Vec<(Uuid, TypeSafeDocument)>, DocumentError> {
    let mut processed_documents = Vec::with_capacity(docs.len());
    let mut invalid_documents = Vec::new();

    for (position, document) in docs.into_iter().enumerate() {
        match schema.validate_document(document) {
            Ok(doc) => processed_documents.push((Uuid::new_v4(), doc)),
            Err(errors) => invalid_documents.push(ValidationError { position, errors }),
        }
    }

    if !invalid_documents.is_empty() {
        Err(DocumentError::ValidationError(invalid_documents))
    } else {
        Ok(processed_documents)
    }
}
