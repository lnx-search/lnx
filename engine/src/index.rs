use anyhow::{Error, Result};
use parking_lot::RwLock;
use slab::Slab;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crossbeam::queue::SegQueue;
use crossbeam::channel;

use tokio::sync::oneshot;

use tantivy::schema::Schema;
use tantivy::{Document, IndexWriter};
use tantivy::{Index, IndexBuilder};

use crate::structures::{IndexStorageType, LoadedIndex};

/// A writing operation to be sent to the `IndexWriterWorker`.
#[derive(Debug)]
enum WriterOp {
    /// Commits the current changes and flushes to storage.
    Commit,

    /// Removes any changes since the last commit.
    Rollback,

    /// Adds a document to the index.
    AddDocument(Document),

    /// Removes all documents from the index.
    DeleteAll,

    /// Shutdown the handler.
    __Shutdown,
}

/// A background task that applies write operations to the index.
///
/// This system uses the actor model receiving a stream of messages
/// and processes them in order of being sent.
///
/// Messages are ran in a new thread.
pub struct IndexWriterWorker {
    index_name: String,
    writer: IndexWriter,
    waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    rx: channel::Receiver<(WriterOp)>,
}

impl IndexWriterWorker {
    fn start(mut self) {
        loop {
            if self.process_messages() {
                break;
            };

            // Wake up waiters once a message has been removed.
            while let Some(waiter) = self.waiters.pop() {
                let _ = waiter.send(());
            }
        }

        // Unlock waiters so that they dont deadlock the system.
        while let Some(waiter) = self.waiters.pop() {
            let _ = waiter.send(());
        }
    }

    fn process_messages(&mut self) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            match self.handle_msg(msg) {
                Err(e) => error!(
                    "[ WRITER @ {} ] failed handling writer operation on index due to error: {:?}",
                    &self.index_name, e,
                ),
                Ok(true) => return true,
                _ => {}
            }
        };

        false
    }

    fn handle_msg(&mut self, op: WriterOp) -> Result<bool> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown => return Ok(true),
            WriterOp::Commit => (self.writer.commit()?, "COMMIT"),
            WriterOp::Rollback => (self.writer.rollback()?, "ROLLBACK"),
            WriterOp::AddDocument(docs) => (self.writer.add_document(docs), "ADD-DOCUMENT"),
            WriterOp::DeleteAll => (self.writer.delete_all_documents()?, "DELETE-ALL"),
        };

        info!(
            "[ WRITER @ {} ][ {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(false)
    }
}


pub struct IndexWriterHandler {
    writer_thread: std::thread::JoinHandle<()>,
    writer_waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    writer_sender: crossbeam::channel::Sender<WriterOp>,
}

impl IndexWriterHandler {
    fn create(index_name: String, writer: IndexWriter) -> Self {
        let waiters = Arc::new(SegQueue::new());
        let (tx, rx) = channel::bounded(20);
        let worker = IndexWriterWorker {
            index_name,
            writer,
            waiters: waiters.clone(),
            rx,
        };

        let handle = std::thread::spawn(move || { worker.start() });

        Self {
            writer_thread: handle,
            writer_sender: tx,
            writer_waiters: waiters,
        }
    }

    async fn send_op(&self, op: WriterOp) -> anyhow::Result<()> {
        let mut op = op;
        loop {
            op = match self.writer_sender.try_send(op) {
                Ok(()) =>
                    return Ok(()),
                Err(channel::TrySendError::Disconnected(v)) =>
                    return Err(Error::msg("writer worker has shutdown")),
                Err(channel::TrySendError::Full(v)) => v,
            };

            let (resolve, waiter) = oneshot::channel();
            self.writer_waiters.push(resolve);
            let _ = waiter.await;
        }
    }
}

pub struct IndexHandler {
    name: String,
    index: Index,
    schema: Schema,
    writer: IndexWriterHandler,
}

impl IndexHandler {
    pub async fn build_loaded(loader: LoadedIndex) -> Result<Self> {
        let index = IndexBuilder::default().schema(loader.schema.clone());

        let index = match loader.storage_type {
            IndexStorageType::TempFile => index.create_from_tempdir()?,
            IndexStorageType::Memory => index.create_in_ram()?,
            IndexStorageType::FileSystem(path) => index.create_in_dir(path)?,
        };

        let writer = index.writer_with_num_threads(
            loader.writer_threads,
            loader.writer_buffer,
        )?;

        let worker_handler = IndexWriterHandler::create(
            loader.name.clone(),
            writer,
        );

        Ok(Self {
            name: loader.name,
            index,
            schema: loader.schema,
            writer: worker_handler,
        })
    }
}
