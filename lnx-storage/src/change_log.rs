use async_trait::async_trait;
use anyhow::Result;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type Timestamp = i64;
pub type DocId = Uuid;

#[derive(Debug, Copy, Clone)]
pub enum ChangeKind {
    Delete,
    Update,
    Append,
}

impl ChangeKind {
    pub fn as_i8(&self) -> i8 {
        match self {
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
        } else {
            Self::Delete
        }
    }
}

pub struct ChangeLog {
    pub kind: ChangeKind,
    pub affected_docs: Vec<DocId>,
    pub timestamp: Timestamp,
}

#[async_trait]
pub trait ChangeLogStore {
    async fn append_changes(&self, logs: ChangeLog) -> Result<()>;

    async fn get_pending_changes(&self, from: Timestamp, chunk_size: usize) -> Result<ChangeLogIterator>;
}

pub struct ChangeLogIterator {
    rx: mpsc::Receiver<Vec<ChangeLog>>,
    handle: JoinHandle<()>
}

impl ChangeLogIterator {
    pub fn from_rx_and_handle(
        rx: mpsc::Receiver<Vec<ChangeLog>>,
        handle: JoinHandle<()>,
    ) -> ChangeLogIterator {
        Self { rx, handle }
    }

    pub async fn next(&mut self) -> Option<Vec<ChangeLog>> {
        self.rx.recv().await
    }
}

impl Drop for ChangeLogIterator {
    fn drop(&mut self) {
        self.rx.close();
        self.handle.abort();
    }
}