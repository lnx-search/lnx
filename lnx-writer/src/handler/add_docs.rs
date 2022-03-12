use std::time::Instant;

use anyhow::Result;
use hashbrown::HashSet;
use lnx_common::schema::Schema;
use lnx_common::types::document::{DocId, Document, TypeSafeDocument};
use lnx_storage::templates::change_log::ChangeLogEntry;
use lnx_storage::types::Timestamp;
use uuid::Uuid;

use crate::error::{DocumentError, ValidationError};
use crate::handler::batcher;

const MAX_INSERT_CONCURRENCY: usize = 256;

pub async fn add_documents(
    index_name: &str,
    documents: Vec<Document>,
) -> Result<(), DocumentError> {
    let index_store = lnx_controller::get(index_name)
        .ok_or_else(|| DocumentError::UnknownIndex(index_name.to_string()))?;

    let docs_len = documents.len();
    let start = Instant::now();
    let docs = parse_and_validate_documents(index_store.ctx().schema(), documents)?;
    batcher::batch(
        MAX_INSERT_CONCURRENCY,
        index_store,
        docs,
        move |chunk, store| async move { store.add_documents(chunk).await },
    )
    .await?;
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
