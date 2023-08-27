use lnx_document::DocBlockReader;

use crate::tracker::IngestionCompleter;

/// A single operation processed by an indexer.
pub enum IndexingOp {
    /// Indexes a single block of documents.
    IndexBlock {
        reader: DocBlockReader,
        tracker: Option<IngestionCompleter>,
    },
    /// The indexer should change the number of threads it can use for indexing.
    AdjustCPUShares {
        num_threads: usize,
    },
    /// The indexer should shutdown.
    Shutdown,
}

impl IndexingOp {
    /// Creates a new index op.
    pub fn index(reader: DocBlockReader) -> Self {
        Self::IndexBlock { reader, tracker: None }
    }

    /// Creates a new index op with a given ingestion completer.
    pub fn index_with_tracker(
        reader: DocBlockReader,
        tracker: IngestionCompleter,
    ) -> Self {
        Self::IndexBlock { reader, tracker: Some(tracker) }
    }

    /// Creates cpu shares adjustment op.
    pub fn adjust_cpu_shared(num_threads: usize) -> Self {
        Self::AdjustCPUShares { num_threads }
    }

    /// Creates a new shutdown op.
    pub fn shutdown() -> Self {
        Self::Shutdown
    }
}