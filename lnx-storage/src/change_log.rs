use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub type Timestamp = i64;
pub type DocId = Uuid;

#[derive(Debug, Copy, Clone)]
/// The type of change that's taking place.
pub enum ChangeKind {
    /// The given document(s) have been deleted and should be
    /// purged from the index.
    Delete,

    /// The given document(s) have been updated so should be
    /// deleted and re-indexed.
    Update,

    /// The given document(s) are new and should be indexed.
    Append,

    /// Clear all documents, this also should clear the changelog.
    ClearAll,
}

impl ChangeKind {
    /// A i8 representation of the given fields.
    pub fn as_i8(&self) -> i8 {
        match self {
            ChangeKind::ClearAll => 3,
            ChangeKind::Delete => 2,
            ChangeKind::Update => 1,
            ChangeKind::Append => 0,
        }
    }
}

impl From<i8> for ChangeKind {
    fn from(v: i8) -> Self {
        if v == 0 {
            Self::Append
        } else if v == 1 {
            Self::Update
        } else if v == 2 {
            Self::Delete
        } else {
            Self::ClearAll
        }
    }
}

/// A given change log entry.
pub struct ChangeLogEntry {
    /// The type of change.
    pub kind: ChangeKind,

    /// The given documents that have been affected by this.
    pub affected_docs: Vec<DocId>,

    /// The time these changes were created.
    ///
    /// This is used for nodes to determine if they are behind or not.
    pub timestamp: Timestamp,
}

#[async_trait]
pub trait ChangeLogStore {
    /// Append a change to the change log system.
    async fn append_changes(&self, logs: ChangeLogEntry) -> Result<()>;

    /// Marks the document store as cleared purging all logs other than
    /// the clear documents change.
    async fn mark_documents_cleared(&self) -> Result<()>;

    /// Get a iterator of pending changes from the given timestamp.
    ///
    /// Logs should be chunked into parts upto the size of `chunk_size` but does
    /// not need to be exactly that.
    async fn get_pending_changes(
        &self,
        from: Timestamp,
        chunk_size: usize,
    ) -> Result<ChangeLogIterator>;

    /// Get the current amount of pending changes.
    async fn count_pending_changes(&self, from: Timestamp) -> Result<usize>;

    /// Purge the change logs upto the given timestamp.
    async fn run_garbage_collection(&self, upto: Timestamp) -> Result<()>;
}

/// A chunked iterator of changes.
pub struct ChangeLogIterator {
    rx: mpsc::Receiver<Vec<ChangeLogEntry>>,
    handle: JoinHandle<()>,
}

impl ChangeLogIterator {
    /// Produces a iterator from a given receiver and tokio handle.
    ///
    /// The handle is used for task cleanup after the iterator has been dropped.
    pub fn from_rx_and_handle(
        rx: mpsc::Receiver<Vec<ChangeLogEntry>>,
        handle: JoinHandle<()>,
    ) -> ChangeLogIterator {
        Self { rx, handle }
    }

    /// Get the next chunk from the logs.
    pub async fn next(&mut self) -> Option<Vec<ChangeLogEntry>> {
        self.rx.recv().await
    }
}

impl Drop for ChangeLogIterator {
    fn drop(&mut self) {
        self.rx.close();
        self.handle.abort();
    }
}
