use std::time::Instant;

use lnx_document::DocBlockReader;
use lnx_schema::indexing::IndexingSchema;
use smallvec::SmallVec;
use tantivy::query::Query;
use tantivy::Term;
use tokio::sync::oneshot;
use tracing::{debug, error, info};

use crate::document::{DocMetadata, IndexingDoc};
use crate::IndexWriter;

/// The main document indexing actor.
///
/// This actor has no responsibility, meaning it will
/// exit at the first sign of an error. This is so it's
/// parent can manage re-creating it which normally involves
/// creating a new index and re-starting from the last checkpoint.
pub struct IndexingActor {
    schema: IndexingSchema,
    writer: IndexWriter,
    indexing_ops: flume::Receiver<IndexingOp>,
    needs_to_commit: bool,
}

impl IndexingActor {
    /// Spawns a new indexing actor.
    pub fn spawn(
        name: String,
        schema: IndexingSchema,
        writer: IndexWriter,
        indexing_ops: flume::Receiver<IndexingOp>,
    ) {
        let actor = Self {
            schema,
            writer,
            indexing_ops,
            needs_to_commit: false
        };

        std::thread::Builder::new()
            .name(name)
            .spawn(move || actor.start())
            .expect("Spawn thread");
    }

    /// Starts the actor running.
    ///
    /// TODO: Setup tracing instrumentation for logging info.
    fn start(mut self) -> anyhow::Result<()> {
        while let Ok(op) = self.indexing_ops.recv() {
            match self.handle_op(op) {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
                Err(e) => {
                    error!(error = ?e, "Failed to complete indexing operation due to error");
                }
            }
        }
        Ok(())
    }

    /// Handles a single specific indexing operation.
    ///
    /// Returns a flag indicating if the indexer should gracefully exit or not.
    fn handle_op(&mut self, op: IndexingOp) -> anyhow::Result<bool> {
        match op {
            IndexingOp::Add { reader, metadata } => {
                self.needs_to_commit = true;
                let num_docs = metadata.len();
                let start = Instant::now();
                self.index_doc_block(reader, metadata)?;
                debug!(num_docs = num_docs, elapsed = ?start.elapsed(), "Indexed documents")
            },
            IndexingOp::DeleteByTerm(term) => {
                self.needs_to_commit = true;
                self.writer.delete_term(term);
                debug!("Deleted document by term");
            },
            IndexingOp::DeleteByQuery(query) => {
                self.needs_to_commit = true;
                self.writer.delete_query(query)?;
                debug!("Deleted document by query");
            },
            IndexingOp::Commit(ack) => {
                if self.needs_to_commit {
                    self.writer.commit()?;
                }

                debug!("Indexing actor is commit successful");
                let _ = ack.send(());
                self.needs_to_commit = false;
            }
            IndexingOp::Shutdown(ack) => {
                info!("Indexing actor is shutting down");
                if self.needs_to_commit {
                    self.writer.commit()?;
                }
                let _ = ack.send(());
                return Ok(true)
            },
        };

        Ok(false)
    }

    fn index_doc_block(
        &mut self,
        reader: DocBlockReader,
        metadata: SmallVec<[DocMetadata; 2]>,
    ) -> anyhow::Result<()> {
        for (i, metadata) in metadata.into_iter().enumerate() {
            let view = IndexingDoc::new(
                metadata,
                reader.clone(),
                i,
                &self.schema,
            );

            self.writer.add_document(view)?;
        }
        Ok(())
    }
}

/// A single indexing task.
pub enum IndexingOp {
    /// Add a set of documents to be indexed.
    Add {
        /// The block reader.
        reader: DocBlockReader,
        /// The metadata for the given document.
        metadata: SmallVec<[DocMetadata; 2]>,
    },
    /// Delete documents by a given term.
    DeleteByTerm(Term),
    /// Delete documents by queries.
    DeleteByQuery(Box<dyn Query>),
    /// Commit the current changes on the index.
    Commit(oneshot::Sender<()>),
    /// A *graceful* shutdown.
    Shutdown(oneshot::Sender<()>),
}
