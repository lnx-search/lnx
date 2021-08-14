use anyhow::{Error, Result};
use parking_lot::RwLock;
use slab::Slab;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crossbeam::channel;
use crossbeam::queue::SegQueue;

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
    /// Starts processing messages until a shutdown operation is sent.
    ///
    /// This processes operations in waves before waking up waiters,
    /// this means all operations currently in the queue will be processed
    /// first before any waiters are woken up to send more data.
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

    /// Purges all pending operations from the receiver.
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
        }

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

/// A simple wrapper handler around a set of queues and a worker.
///
/// This manages creating the waiters and scheduling the operations
/// in a new thread.
pub struct IndexWriterHandler {
    index_name: String,
    writer_thread: std::thread::JoinHandle<()>,
    writer_waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    writer_sender: crossbeam::channel::Sender<WriterOp>,
}

impl IndexWriterHandler {
    /// Creates a new writer handler from a given index name and
    /// a given index writer.
    ///
    /// This creates a bounded queue with a capacity of 20 and
    /// spawns a worker in a new thread.
    fn create(index_name: String, writer: IndexWriter) -> Self {
        let name = index_name.clone();
        let waiters = Arc::new(SegQueue::new());
        let (tx, rx) = channel::bounded(20);
        let worker = IndexWriterWorker {
            index_name: index_name.clone(),
            writer,
            waiters: waiters.clone(),
            rx,
        };

        let handle = std::thread::Builder::new()
            .name(format!("index-worker-{}", &index_name))
            .spawn(move || {
            let id = std::thread::current().id();
            info!(
                "[ WRITER @ {} ] writer thread started with id {:?}",
                name, id
            );
            worker.start()
        }).expect("spawn worker thread");

        Self {
            index_name,
            writer_thread: handle,
            writer_sender: tx,
            writer_waiters: waiters,
        }
    }

    /// Sends a message to the writer worker
    ///
    /// If there is space in the queue this will complete immediately
    /// otherwise this will wait until it's woken up again.
    async fn send_op(&self, op: WriterOp) -> anyhow::Result<()> {
        let mut op = op;
        loop {
            op = match self.writer_sender.try_send(op) {
                Ok(()) => return Ok(()),
                Err(channel::TrySendError::Disconnected(_)) => {
                    return Err(Error::msg("writer worker has shutdown"))
                }
                Err(channel::TrySendError::Full(v)) => v,
            };

            debug!(
                "[ WRITER @ {} ] operation queue full, waiting for wakeup",
                &self.index_name
            );

            let (resolve, waiter) = oneshot::channel();
            self.writer_waiters.push(resolve);
            let _ = waiter.await;
        }
    }
}

/// A search engine index.
///
/// Each index maintains a rayon thread pool which searches are executed
/// as well as an worker thread which is used to interact with the index writer.
///
/// The amount of threads `n` is determined by the the `max_concurrency` parameter.
pub struct IndexHandler {
    /// The name of the index.
    name: String,

    /// The internal tantivy index.
    index: Index,

    /// The internal tantivy schema.
    schema: Schema,

    /// The execution thread pool.
    thread_pool: rayon::ThreadPool,

    /// A writer actor to handle the index writer.
    writer: IndexWriterHandler,
}

impl IndexHandler {
    /// Creates a new index handler from a given loaded index.
    ///
    /// This constructs both the Tantivy index, thread pool and worker thread.
    ///
    /// Important note about performance:
    /// - The concurrency limit should be set according to the machine
    /// this system is being deployed on hence being a required field.
    /// The amount of threads spawned is equal the the max  concurrency + 1
    /// as well as the tokio runtime threads.
    pub fn build_loaded(loader: LoadedIndex) -> Result<Self> {
        let index = IndexBuilder::default().schema(loader.schema.clone());

        let index = match loader.storage_type {
            IndexStorageType::TempFile => index.create_from_tempdir()?,
            IndexStorageType::Memory => index.create_in_ram()?,
            IndexStorageType::FileSystem(path) => index.create_in_dir(path)?,
        };

        let writer = index.writer_with_num_threads(loader.writer_threads, loader.writer_buffer)?;

        let worker_handler = IndexWriterHandler::create(loader.name.clone(), writer);

        let thread_pool = {
            let name_ref = loader.name.as_str();

            rayon::ThreadPoolBuilder::new()
                .num_threads(loader.max_concurrency as usize)
                .thread_name(|n| format!("index-{}-worker-{}", name_ref, n))
                .build()?
        };

        Ok(Self {
            name: loader.name,
            index,
            thread_pool,
            schema: loader.schema,
            writer: worker_handler,
        })
    }
}
