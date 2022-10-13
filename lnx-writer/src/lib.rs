#[macro_use]
extern crate tracing;

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

mod clock;
mod finalizer;
mod indexer;
mod writer;

pub use clock::clock;
pub use finalizer::{subscribe, SegmentStatus};
pub use writer::Writer;

/// Initialise the writer global state.
pub async fn init(node_id: u32, tmp_path: &Path) {
    finalizer::init(tmp_path).await;
    clock::init(node_id).await;
}

#[derive(Default, Debug, Clone)]
/// Statistics about the writer handler.
pub struct WriterStatistics {
    segments_produced: Arc<AtomicUsize>,
    documents_ingested: Arc<AtomicUsize>,
    deletes_registered: Arc<AtomicUsize>,
}

impl WriterStatistics {
    #[inline]
    /// The number of segments the writer has created during it's lifetime.
    pub fn segments_produced(&self) -> usize {
        self.segments_produced.load(Ordering::Relaxed)
    }

    #[inline]
    /// The number of documents the writer has processed during it's lifetime.
    pub fn documents_ingested(&self) -> usize {
        self.documents_ingested.load(Ordering::Relaxed)
    }

    #[inline]
    /// The number of deletes the writer has registered during it's lifetime.
    pub fn deletes_registered(&self) -> usize {
        self.deletes_registered.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn inc_segments(&self) {
        self.segments_produced.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_documents_by(&self, n: usize) {
        self.documents_ingested.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_deletes_by(&self, n: usize) {
        self.deletes_registered.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("Tantivy Error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Failed to create new temporary indexing directory due to error: {0}")]
    TempDirectoryFailure(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("IO Error: {0}")]
    IoError(#[from] io::Error),

    #[error("{0}")]
    Other(String),
}
