use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::Context;
use tokio::sync::oneshot;

use crate::actors::indexer::IndexingOp;

/// The pipeline that manages a set of actors in order
/// to process a set of documents with tantivy.
///
/// These pipelines control only two things, insertion and deletion of documents
/// in the index.
pub struct IndexingPipeline {
    /// The timing actor kill switch.
    timing_actor_stop: Arc<AtomicBool>,
    /// The queue of operations for the given indexing pipeline.
    indexing_ops: flume::Sender<IndexingOp>,
}

impl IndexingPipeline {


    /// Performs a graceful shutdown of the indexing pipeline.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        // Shutdown the timing actor
        self.timing_actor_stop.store(true, Ordering::Relaxed);

        // Tell the indexing actor to shutdown.
        let (tx, rx) = oneshot::channel();
        self.indexing_ops
            .send_async(IndexingOp::Shutdown(tx))
            .await
            .context("Send indexing op to indexer")?;
        let _ = rx.await;

        Ok(())
    }
}