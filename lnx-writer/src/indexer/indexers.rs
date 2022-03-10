use anyhow::{anyhow, Result};
use lnx_common::types::document::{DocId, Document};
use lnx_storage::types::SegmentId;
use tokio::sync::mpsc;

use crate::helpers::CancellingJoinHandle;
use crate::indexer::task_handler::Task;

/// The structure used to submit tasks to the internal indexing system.
///
/// This will cancel the indexing job if the indexer is dropped without
/// finalising everything.
///
/// The indexer will also be consumed if the internal actor runs into an
/// error.
///
/// A weak indexer can be created from the main `Indexer` which allows
/// you to create multiple copies of the indexer without the dropping
/// behaviour and error handling behaviour.
pub struct Indexer {
    emitter: mpsc::Sender<Task>,
    handle: CancellingJoinHandle<Result<()>>,
}

impl Indexer {
    pub fn get_weak(&self) -> WeakIndexer {
        WeakIndexer {
            emitter: self.emitter.clone(),
        }
    }

    async fn send_event(&mut self, event: Task) -> Result<()> {
        let res = self.emitter.send(event).await;

        if res.is_err() {
            return if let Some(handle) = self.handle.0.take() {
                handle.await?
            } else {
                Err(anyhow!(
                    "Index writer has shut down. No new tasks can be added"
                ))
            };
        }

        Ok(())
    }

    /// Add a set of documents to the indexer.
    pub async fn add_documents(
        &mut self,
        docs: Vec<(DocId, SegmentId, Document)>,
    ) -> Result<()> {
        self.send_event(Task::AddDocuments(docs)).await
    }

    /// Remove a set of documents.
    pub async fn remove_segment(&mut self, segment: SegmentId) -> Result<()> {
        self.send_event(Task::RemoveSegment(segment)).await
    }

    /// Clear all documents.
    pub async fn clear_documents(&mut self) -> Result<()> {
        self.send_event(Task::ClearAllDocuments).await
    }

    pub fn finish(self) {
        drop(self); // Unnecessary but helps to show what's happening.
    }
}

#[derive(Clone)]
/// A Indexer which does not cancel the indexing job when dropped.
///
/// This is designed to be cheap to clone while ignoring errors
/// returned by the indexing actor.
pub struct WeakIndexer {
    emitter: mpsc::Sender<Task>,
}

impl WeakIndexer {
    async fn send_event(&self, event: Task) -> Result<()> {
        let res = self.emitter.send(event).await;

        if res.is_err() {
            Err(anyhow!(
                "Index writer has shut down. No new tasks can be added"
            ))
        } else {
            Ok(())
        }
    }

    /// Add a set of documents to the indexer.
    pub async fn add_documents(
        &self,
        docs: Vec<(DocId, SegmentId, Document)>,
    ) -> Result<()> {
        self.send_event(Task::AddDocuments(docs)).await
    }

    /// Remove a set of documents.
    pub async fn remove_segment(&self, segment: SegmentId) -> Result<()> {
        self.send_event(Task::RemoveSegment(segment)).await
    }

    /// Clear all documents.
    pub async fn clear_documents(&self) -> Result<()> {
        self.send_event(Task::ClearAllDocuments).await
    }
}
