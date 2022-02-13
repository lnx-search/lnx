use std::sync::Arc;

use async_trait::async_trait;
use anyhow::Result;
use tokio::sync::mpsc;
use futures_util::StreamExt;

use lnx_common::types::document::Document;
use lnx_utils::{ToBytes, FromBytes};

use crate::change_log::{ChangeLog, ChangeLogIterator, ChangeLogStore, Timestamp, DocId};
use crate::ChangeKind;
use crate::doc_store::DocStore;
use crate::impls::scylla_backed::connection::Session;


static LAST_UPDATE_KEY: &[u8] = b"LAST_UPDATE_TIMESTAMP";
static GET_PENDING_CHANGES_QUERY: &str = r#"
    SELECT kind, affected_docs, timestamp
    FROM index_changelog
    WHERE timestamp > ?;
"#;
static ADD_PENDING_CHANGES: &str = r#"
    INSERT INTO index_changelog (kind, affected_docs, timestamp)
    VALUES (?, ?, ?);
"#;

type ChangedRow = (i8, Vec<DocId>, chrono::Duration);

pub struct ScyllaPrimaryDataStore {
    session: Arc<Session>,
}

#[async_trait]
impl ChangeLogStore for ScyllaPrimaryDataStore {
    async fn append_changes(&self, logs: ChangeLog) -> Result<()> {
        self.session.query_prepared(
            ADD_PENDING_CHANGES,
            (logs.kind.as_i8(), logs.affected_docs, logs.timestamp)
        ).await?;

        Ok(())
    }

    async fn get_pending_changes(&self, from: Timestamp, chunk_size: usize) -> Result<ChangeLogIterator> {
        let iter = self.session.query_iter(
            GET_PENDING_CHANGES_QUERY,
            (from,),
        ).await?;

        let (tx, rx) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            let mut typed_iter = iter.into_typed::<ChangedRow>();
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);

                while let Some(Ok(row)) = typed_iter.next().await {
                    chunk.push(ChangeLog {
                        kind: ChangeKind::from(row.0),
                        affected_docs: row.1,
                        timestamp: row.2.num_seconds()
                    });

                    if chunk.len() >= chunk_size {
                        break;
                    }
                }

                if let Err(_) = tx.send(chunk).await {
                    trace!("chunk send failed, cleaning up and shutting down task");
                    break;
                }
            }
        });

        let iterator = ChangeLogIterator::from_rx_and_handle(rx, handle);

        Ok(iterator)
    }
}