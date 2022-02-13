use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use itertools::Itertools;
use lnx_common::types::document::Document;
use tokio::sync::mpsc;

use super::connection::session;
use crate::change_log::{
    ChangeLogEntry,
    ChangeLogIterator,
    ChangeLogStore,
    DocId,
    Timestamp,
};
use crate::doc_store::{DocStore, DocumentIterator};
use crate::impls::scylla_backed::connection::keyspace;
use crate::impls::scylla_backed::doc_wrapper::{
    ScyllaSafeDocument,
    DOCUMENT_PRIMARY_KEY,
};
use crate::ChangeKind;

static DOCUMENT_TABLE: &str = "documents";
static CHANGE_LOG_TABLE: &str = "index_changelog";

type ChangedRow = (i8, Vec<DocId>, chrono::Duration);

/// The primary storage interface for the Scylla system.
///
/// This manages the change log and the document store itself.
/// Documents are partitioned into their respective fields and stored
/// as individual blobs per field.
/// This allows us during indexing time to only request the data we
/// actually need rather than all doc fields. This can greatly reduce
/// the bandwidth required.
pub struct ScyllaPrimaryDataStore {
    /// The fields as defined in the index schema.
    ///
    /// If no fields are provided by the DocStore trait then these
    /// fields are used.
    schema_fields: Vec<String>,

    keyspace: String,
}

impl ScyllaPrimaryDataStore {
    pub fn with_fields(index_name: &str, fields: Vec<String>) -> Self {
        Self {
            keyspace: keyspace(index_name),
            schema_fields: fields,
        }
    }
}

#[async_trait]
impl ChangeLogStore for ScyllaPrimaryDataStore {
    async fn append_changes(&self, logs: ChangeLogEntry) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} (kind, affected_docs, timestamp) VALUES (?, ?, ?);",
            ks = self.keyspace,
            table = CHANGE_LOG_TABLE,
        );

        session()
            .query_prepared(
                &query,
                (logs.kind.as_i8(), logs.affected_docs, logs.timestamp),
            )
            .await?;

        Ok(())
    }

    async fn get_pending_changes(
        &self,
        from: Timestamp,
        chunk_size: usize,
    ) -> Result<ChangeLogIterator> {
        let query = format!(
            "SELECT kind, affected_docs, timestamp FROM {ks}.{table} WHERE timestamp > ?;",
            ks = self.keyspace,
            table = CHANGE_LOG_TABLE,
        );

        let iter = session().query_iter(&query, (from,)).await?;

        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            let mut typed_iter = iter.into_typed::<ChangedRow>();
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);

                while let Some(Ok(row)) = typed_iter.next().await {
                    chunk.push(ChangeLogEntry {
                        kind: ChangeKind::from(row.0),
                        affected_docs: row.1,
                        timestamp: row.2.num_seconds(),
                    });

                    if chunk.len() >= chunk_size {
                        break;
                    }
                }

                if tx.send(chunk).await.is_err() {
                    trace!("chunk send failed, cleaning up and shutting down task");
                    break;
                }
            }
        });

        let iterator = ChangeLogIterator::from_rx_and_handle(rx, handle);

        Ok(iterator)
    }

    async fn run_garbage_collection(&self, upto: Timestamp) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE timestamp < ?;",
            ks = self.keyspace,
            table = CHANGE_LOG_TABLE,
        );

        session().query_prepared(&query, (upto,)).await?;

        Ok(())
    }
}

#[async_trait]
impl DocStore for ScyllaPrimaryDataStore {
    async fn add_documents(&self, docs: Vec<(DocId, Document)>) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} ({pk}, {columns}) VALUES (?, {placeholders})",
            ks = self.keyspace,
            pk = DOCUMENT_PRIMARY_KEY,
            table = DOCUMENT_TABLE,
            columns = self.schema_fields.join(", "),
            placeholders = self.schema_fields.iter().map(|_| "?").join(", "),
        );

        for (doc_id, doc) in docs {
            let doc = ScyllaSafeDocument(doc_id, doc);
            session().query_prepared(&query, &doc).await?;
        }

        Ok(())
    }

    async fn remove_documents(&self, docs: Vec<DocId>) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE {pk} = ?",
            ks = self.keyspace,
            table = DOCUMENT_TABLE,
            pk = DOCUMENT_PRIMARY_KEY,
        );

        session().query_prepared(&query, (docs,)).await?;

        Ok(())
    }

    async fn fetch_documents(
        &self,
        fields: Option<Vec<String>>,
        docs: Vec<DocId>,
    ) -> Result<Vec<(DocId, Document)>> {
        let columns = fields.unwrap_or_else(|| self.schema_fields.clone());
        let query = format!(
            "SELECT {pk}, {columns} FROM {ks}.{table} WHERE {pk} = ?",
            pk = DOCUMENT_PRIMARY_KEY,
            columns = columns.join(", "),
            ks = self.keyspace,
            table = DOCUMENT_TABLE,
        );

        let mut retrieved_docs = Vec::with_capacity(docs.len());
        let mut results = session()
            .query_iter(&query, (docs,)) // TODO: Can we optimise this?
            .await?;

        while let Some(row) = results.next().await {
            let parts = ScyllaSafeDocument::from_row_and_layout(row?, columns.clone())?
                .into_parts();
            retrieved_docs.push(parts)
        }

        Ok(retrieved_docs)
    }

    async fn iter_documents(
        &self,
        fields: Option<Vec<String>>,
        chunk_size: usize,
    ) -> Result<DocumentIterator> {
        let columns = fields.unwrap_or_else(|| self.schema_fields.clone());
        let query = format!(
            "SELECT {pk}, {columns} FROM {ks}.{table};",
            pk = DOCUMENT_PRIMARY_KEY,
            columns = columns.join(", "),
            table = DOCUMENT_TABLE,
            ks = self.keyspace,
        );

        let mut iter = session().query_iter(&query, &[]).await?;

        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);

                while let Some(Ok(row)) = iter.next().await {
                    let row = match ScyllaSafeDocument::from_row_and_layout(
                        row,
                        columns.clone(),
                    ) {
                        Err(e) => {
                            error!("failed to handle chunk due to error {:?}", e);
                            return;
                        },
                        Ok(row) => row,
                    };

                    chunk.push(row.into_parts());

                    if chunk.len() >= chunk_size {
                        break;
                    }
                }

                if tx.send(chunk).await.is_err() {
                    trace!("chunk send failed, cleaning up and shutting down task");
                    break;
                }
            }
        });

        let iterator = DocumentIterator::from_rx_and_handle(rx, handle);

        Ok(iterator)
    }
}
