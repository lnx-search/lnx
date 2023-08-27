use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use lnx_tools::hashers::NoOpRandomState;

pub mod metastore;

use crate::ops;


#[derive(Default)]
/// The indexing queue is responsible for managing each indexes' writers.
pub struct IndexingQueue {
    /// A mapping of indexes and their relevant ingestion queue.
    indexes: Arc<RwLock<HashMap<u64, IndexingBucket, NoOpRandomState>>>,
}

impl IndexingQueue {
    /// Adds a new index to the indexer queue.
    ///
    /// This will automatically shutdown any existing index.
    pub async fn add_index(&self, index_id: u64) -> IndexingOpsListener {
        let (bucket, listener) = IndexingBucket::new(index_id);

        let res = {
            let mut lock = self.indexes.write();
            lock.insert(index_id, bucket)
        };

        if let Some(existing) = res {
            let _ = existing.ops.send_async(ops::IndexingOp::shutdown()).await;
        }

        listener
    }

    /// Attempts to remove an existing index from the queue.
    ///
    /// This will automatically shutdown any existing index.
    pub async fn remove_index(&self, index_id: u64) {
        let res = {
            let mut lock = self.indexes.write();
            lock.remove(&index_id)
        };

        if let Some(existing) = res {
            let _ = existing.ops.send_async(ops::IndexingOp::shutdown()).await;
        }
    }
}


/// A indexing queue bucket for a specific index.
///
/// This contains the actual
pub struct IndexingBucket {
    /// The index ID tied to the bucket.
    index_id: u64,
    /// The indexes' ops channel.
    ops: flume::Sender<ops::IndexingOp>,
}

impl IndexingBucket {
    /// Creates a new indexing bucket.
    pub fn new(index_id: u64) -> (Self, IndexingOpsListener) {
        let (tx, rx) = flume::bounded(5);

        let listener = IndexingOpsListener { ops: rx };
        let slf = Self {
            index_id,
            ops: tx,
        };


        (slf, listener)
    }
}


/// A listener for indexing ops.
pub struct IndexingOpsListener {
    ops: flume::Receiver<ops::IndexingOp>
}

impl IndexingOpsListener {
    /// Gets the next indexing op.
    ///
    /// Returns `None` if the sender has closed.
    pub fn next(&self) -> Option<ops::IndexingOp> {
        self.ops.recv().ok()
    }

    /// Gets the next indexing op within a deadline.
    ///
    /// Returns `TimeoutRes::Closed` if the sender has closed.
    /// Returns `TimeoutRes::Timeout` if no op was received within the deadline.
    pub fn next_timeout(&self, dur: Duration) -> TimeoutRes {
        match self.ops.recv_timeout(dur) {
            Ok(op) => TimeoutRes::Op(op),
            Err(flume::RecvTimeoutError::Timeout) => TimeoutRes::Timeout,
            Err(flume::RecvTimeoutError::Disconnected) => TimeoutRes::Closed
        }
    }
}


pub enum TimeoutRes {
    /// The indexing ops listener has closed.
    Closed,
    /// No ops were received within the timeout deadline.
    Timeout,
    /// The indexing op received
    Op(ops::IndexingOp),
}