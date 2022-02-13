use async_trait::async_trait;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use lnx_common::types::document::Document;

use super::change_log::{ChangeLogStore, DocId};

#[async_trait]
pub trait DocStore: ChangeLogStore {
    /// Adds a set of documents to the store.
    async fn add_documents(&self, docs: Vec<(DocId, Document)>) -> Result<()>;

    /// Removes a set of documents from the store.
    async fn remove_documents(&self, docs: Vec<DocId>) -> Result<()>;

    /// Fetches a set of documents with the provided Ids.
    ///
    /// If `fields` is `None` all fields on the document should be returned
    /// which are inline with the schema.
    async fn fetch_documents(
        &self,
        fields: Option<Vec<String>>,
        docs: Vec<DocId>,
    ) -> Result<Vec<(DocId, Document)>>;

    /// Used to get all documents from the store.
    ///
    /// This is only used when building an index from scratch and no
    /// existing nodes are available to supply a partial index.
    async fn iter_documents(
        &self,
        fields: Option<Vec<String>>,
        chunk_size: usize,
    ) -> Result<DocumentIterator>;
}


pub struct DocumentIterator {
    rx: mpsc::Receiver<Vec<(DocId, Document)>>,
    handle: JoinHandle<()>
}

impl DocumentIterator {
    /// Produces a iterator from a given receiver and tokio handle.
    ///
    /// The handle is used for task cleanup after the iterator has been dropped.
    pub fn from_rx_and_handle(
        rx: mpsc::Receiver<Vec<(DocId, Document)>>,
        handle: JoinHandle<()>,
    ) -> DocumentIterator {
        Self { rx, handle }
    }

    /// Get the next chunk from the query.
    pub async fn next(&mut self) -> Option<Vec<(DocId, Document)>> {
        self.rx.recv().await
    }
}

impl Drop for DocumentIterator {
    fn drop(&mut self) {
        self.rx.close();
        self.handle.abort();
    }
}