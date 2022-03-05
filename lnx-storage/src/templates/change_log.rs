use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::{SegmentId, Timestamp};

/// A given change log entry.
#[derive(Debug)]
pub struct ChangeLogEntry {
    /// The given segment that have been affected by this.
    pub affected_segment: SegmentId,

    /// The time these changes were created.
    ///
    /// This is used for nodes to determine if they are behind or not.
    pub timestamp: Timestamp,
}

#[async_trait]
pub trait ChangeLogStore {
    /// Append a change to the change log system.
    async fn append_changes(&self, logs: ChangeLogEntry) -> Result<()>;

    /// Get a iterator of pending changes from the given timestamp.
    ///
    /// Logs should be chunked into parts upto the size of `chunk_size` but does
    /// not need to be exactly that.
    async fn get_pending_changes(&self, from: Timestamp) -> Result<ChangeLogIterator>;

    /// Get the current amount of pending changes.
    async fn count_pending_changes(&self, from: Timestamp) -> Result<usize>;
}

/// A chunked iterator of changes.
pub struct ChangeLogIterator {
    rx: mpsc::Receiver<ChangeLogEntry>,
    handle: JoinHandle<()>,
}

impl ChangeLogIterator {
    /// Produces a iterator from a given receiver and tokio handle.
    ///
    /// The handle is used for task cleanup after the iterator has been dropped.
    pub fn from_rx_and_handle(
        rx: mpsc::Receiver<ChangeLogEntry>,
        handle: JoinHandle<()>,
    ) -> ChangeLogIterator {
        Self { rx, handle }
    }

    /// Get the next chunk from the logs.
    pub async fn next(&mut self) -> Option<ChangeLogEntry> {
        self.rx.recv().await
    }
}

impl Drop for ChangeLogIterator {
    fn drop(&mut self) {
        self.rx.close();
        self.handle.abort();
    }
}
