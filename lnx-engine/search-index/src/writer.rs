use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Result};
use crossbeam::channel::{self, RecvTimeoutError};
use crossbeam::queue::SegQueue;
use serde::{Deserialize, Serialize};
use sysinfo::SystemExt;
use tantivy::schema::{Field, Schema};
use tantivy::{IndexWriter, Opstamp, Term};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use crate::corrections::SymSpellCorrectionManager;
use crate::helpers::{FrequencyCounter, PersistentFrequencySet, Validate};
use crate::stop_words::{PersistentStopWordManager, StopWordManager};
use crate::storage::StorageBackend;
use crate::structures::{DocumentPayload, IndexContext, INDEX_STORAGE_PATH};

type OpPayload = (WriterOp, Option<oneshot::Sender<Result<()>>>);
type OpReceiver = channel::Receiver<OpPayload>;
type OpSender = channel::Sender<OpPayload>;
type WaitersQueue = Arc<SegQueue<oneshot::Sender<()>>>;
type ShutdownWaker = async_channel::Sender<()>;
type ShutdownReceiver = async_channel::Receiver<()>;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub(crate) struct WriterContext {
    /// The amount of bytes to allocate to the writer buffer.
    #[serde(default)]
    writer_buffer: usize,

    /// The amount of worker threads to dedicate to a writer.
    #[serde(default = "defaults::default_writer_threads")]
    writer_threads: usize,

    /// The auto-commit duration, if no documents have been added within this period
    /// then the system will automatically commit and index them. In Seconds.
    #[serde(default)]
    auto_commit: usize,
}

mod defaults {
    /// The max number of threads to default to.
    ///
    /// If the cpu count is higher than this, it will not go beyond this
    /// value.
    const MAX_DEFAULT_THREAD_COUNT: usize = 8;

    /// Size of the margin for the heap. A segment is closed when the remaining memory
    /// in the heap goes below MARGIN_IN_BYTES.
    pub const MARGIN_IN_BYTES: usize = 1_000_000;

    /// We impose the memory per thread to be at least 3 MB.
    pub const HEAP_SIZE_MIN: usize = ((MARGIN_IN_BYTES as u32) * 3u32) as usize;
    pub const HEAP_SIZE_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

    /// The default amount of writer threads to use if left out of
    /// the index creation payload.
    pub fn default_writer_threads() -> usize {
        let cpu_count = num_cpus::get();

        if cpu_count > MAX_DEFAULT_THREAD_COUNT {
            MAX_DEFAULT_THREAD_COUNT
        } else {
            cpu_count
        }
    }
}

impl WriterContext {
    /// Computes a target buffer size if it's bellow the minimum
    /// required size.
    ///
    /// This tries to allocate 10% of the total memory of the system
    /// otherwise defaulting to the minimum required buffer size should it
    /// be bellow the minimum or above the amount of free memory.
    fn with_safe_buffer(&self) -> Result<WriterContext> {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();

        let num_threads = self.writer_threads;
        let mut buffer = self.writer_buffer;

        let min_buffer = defaults::HEAP_SIZE_MIN * num_threads;
        if buffer < min_buffer {
            let total_mem = sys.total_memory();
            let mut target_buffer_size = (total_mem as f64 * 0.10) as u64;

            if target_buffer_size < min_buffer as u64 {
                target_buffer_size = min_buffer as u64;
            }

            let free_mem = sys.free_memory();
            if free_mem < target_buffer_size {
                info!(
                    "target buffer size of {}KB cannot be reached due \
                    to not enough free memory, defaulting to {}KB",
                    target_buffer_size,
                    buffer / 1_000,
                );

                buffer = min_buffer;
            } else {
                buffer = (target_buffer_size * 1_000) as usize;
            }
        }

        let absolute_max = defaults::HEAP_SIZE_MAX * num_threads;
        if buffer > absolute_max {
            buffer = absolute_max;
        }

        let free_mem = sys.free_memory();
        if buffer > (free_mem * 1000) as usize {
            return Err(Error::msg(format!(
                "cannot allocate {}KB due to system not having enough free memory. (Free: {}KB)",
                buffer / 1_000,
                free_mem
            )));
        }

        Ok(Self {
            writer_threads: num_threads,
            writer_buffer: buffer,
            auto_commit: self.auto_commit,
        })
    }
}

impl Validate for WriterContext {
    fn validate(&self) -> Result<()> {
        if self.writer_threads == 0 {
            return Err(Error::msg(
                "writer buffer bellow minimum. Buffer size must be at least 1.",
            ));
        }

        Ok(())
    }
}


pub struct MetaStore {
    key: String,
    value: Vec<u8>,
    responder: mpsc::Sender<Result<()>>,
}


/// A writing operation to be sent to the `IndexWriterWorker`.
#[derive(Debug)]
pub(super) enum WriterOp {
    /// Commits the current changes and flushes to storage.
    Commit,

    /// Removes any changes since the last commit.
    Rollback,

    /// Adds a set of stopwords
    AddStopWords(Vec<String>),

    /// Removes a set of stopwords
    RemoveStopWords(Vec<String>),

    /// Removes all stopwords.
    ClearStopWords,

    /// Adds a document to the index.
    AddDocument(DocumentPayload),

    /// Adds multiple documents to the index.
    AddManyDocuments(Vec<DocumentPayload>),

    /// Deletes any documents matching the given term.
    DeleteTerm(Term),

    /// Removes all documents from the index.
    DeleteAll,

    /// A simple Ping to check if the worker is alive still after creation.
    __Ping,

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
    using_fast_fuzzy: bool,
    fuzzy_fields: Vec<Field>,
    waiters: WaitersQueue,
    schema: Schema,
    writer: IndexWriter,
    auto_commit: u64,
    rx: OpReceiver,
    shutdown: ShutdownWaker,
    frequencies: PersistentFrequencySet,
    corrections: SymSpellCorrectionManager,
    stop_words: PersistentStopWordManager,
}

impl IndexWriterWorker {
    /// Starts processing messages until a shutdown operation is sent.
    ///
    /// This processes operations in waves before waking up waiters,
    /// this means all operations currently in the queue will be processed
    /// first before any waiters are woken up to send more data.
    fn start(mut self) {
        let mut op_since_last_commit = false;
        loop {
            while let Ok((op, waker)) = self.rx.try_recv() {
                op_since_last_commit = true;
                self.handle_message(op, waker);
            }

            // Wake up waiters once a message has been removed.
            while let Some(waiter) = self.waiters.pop() {
                let _ = waiter.send(());
            }

            if (self.auto_commit == 0) | !op_since_last_commit {
                info!(
                    "[ WRITER @ {} ] parking writer until new events present",
                    &self.index_name
                );
                if let Ok((op, waker)) = self.rx.recv() {
                    op_since_last_commit = true;
                    self.handle_message(op, waker);
                } else {
                    info!(
                        "[ WRITER @ {} ] writer actor channel dropped, shutting down...",
                        &self.index_name
                    );
                    break;
                }

                continue;
            }

            match self.rx.recv_timeout(Duration::from_secs(self.auto_commit)) {
                Err(RecvTimeoutError::Timeout) => {
                    info!("[ WRITER @ {} ] running auto commit", &self.index_name);

                    // We know we wont shutdown.
                    let _ = self.handle_message(WriterOp::Commit, None);
                    op_since_last_commit = false;
                },
                Err(RecvTimeoutError::Disconnected) => {
                    info!(
                        "[ WRITER @ {} ] writer actor channel dropped, shutting down...",
                        &self.index_name
                    );
                    break;
                },
                Ok((op, waker)) => {
                    self.handle_message(op, waker);
                },
            }
        }

        // Unlock waiters so that they dont deadlock the system.
        while let Some(waiter) = self.waiters.pop() {
            let _ = waiter.send(());
        }

        let _ = self.writer.wait_merging_threads();
        let _ = self.shutdown.try_send(());
        info!("[ WRITER @ {} ] shutdown complete!", &self.index_name);
    }

    fn handle_message(
        &mut self,
        op: WriterOp,
        waker: Option<oneshot::Sender<Result<()>>>,
    ) {
        trace!(
            "[ WRITER @ {} ] handling operation: {:?}",
            &self.index_name, op
        );
        match self.handle_op(op) {
            Err(e) => {
                if let Some(w) = waker {
                    let _ = w.send(Err(e));
                }
            },
            _ => {
                if let Some(w) = waker {
                    let _ = w.send(Ok(()));
                }
            },
        }
    }

    /// Handles adding a document to the writer and returning it's
    /// transaction operation and id.
    ///
    /// This simply parses the payload into a document after checking
    /// if the system is using fast fuzzy or not.
    ///
    /// If fast fuzzy is set to `true` this also performs
    /// the the relevant word frequency adjustments for the
    /// corrections manager.
    fn handle_add_document(
        &mut self,
        document: DocumentPayload,
    ) -> Result<(Opstamp, &'static str)> {
        if !self.using_fast_fuzzy {
            let doc = document.parse_into_document(&self.schema)?;
            return Ok((self.writer.add_document(doc), "ADD-DOCUMENT"));
        }

        let to_index = document.get_text_values(&self.schema, &self.fuzzy_fields);
        for text in to_index {
            self.frequencies.process_sentence(&text);
        }

        let doc = document.parse_into_document(&self.schema)?;
        Ok((self.writer.add_document(doc), "ADD-DOCUMENT"))
    }

    fn handle_op(&mut self, op: WriterOp) -> Result<()> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown => {
                // This is a bit of a hack but for consistency we follow
                // the same drop behaviour.
                let (tx, rx) = channel::bounded(0);
                drop(tx);

                let rx = mem::replace(&mut self.rx, rx);
                drop(rx);
                return Ok(());
            },
            WriterOp::__Ping => return Ok(()),
            WriterOp::Commit => {
                self.frequencies.commit()?;
                self.corrections.adjust_index_frequencies(&self.frequencies);
                (self.writer.commit()?, "COMMIT")
            },
            WriterOp::Rollback => (self.writer.rollback()?, "ROLLBACK"),
            WriterOp::AddDocument(document) => self.handle_add_document(document)?,
            WriterOp::AddManyDocuments(documents) => {
                for document in documents {
                    let (transaction_id, type_) = self.handle_add_document(document)?;
                    debug!(
                        "[ WRITER @ {} ][ TRANSACTION {} ] completed operation {}",
                        &self.index_name, transaction_id, type_
                    );
                }

                return Ok(());
            },
            WriterOp::DeleteTerm(term) => (self.writer.delete_term(term), "DELETE-TERM"),
            WriterOp::DeleteAll => {
                self.frequencies.clear_frequencies();
                self.frequencies.commit()?;
                (self.writer.delete_all_documents()?, "DELETE-ALL")
            },
            WriterOp::AddStopWords(words) => {
                self.stop_words.add_stop_words(words);
                self.stop_words.commit()?;
                return Ok(());
            },
            WriterOp::RemoveStopWords(words) => {
                self.stop_words.remove_stop_words(words);
                self.stop_words.commit()?;
                return Ok(());
            },
            WriterOp::ClearStopWords => {
                self.stop_words.clear_stop_words();
                self.stop_words.commit()?;
                return Ok(());
            },
        };

        debug!(
            "[ WRITER @ {} ][ TRANSACTION {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn start_writer(
    name: String,
    conn: StorageBackend,
    stop_word_manager: StopWordManager,
    waiters: WaitersQueue,
    schema: Schema,
    auto_commit: usize,
    using_fast_fuzzy: bool,
    fuzzy_fields: Vec<Field>,
    writer: IndexWriter,
    op_receiver: OpReceiver,
    shutdown: ShutdownWaker,
    corrections: SymSpellCorrectionManager,
) -> Result<()> {
    let stop_words = PersistentStopWordManager::new(conn.clone(), stop_word_manager)?;
    let frequency_set = PersistentFrequencySet::new(conn)?;
    corrections.adjust_index_frequencies(&frequency_set);

    let worker = IndexWriterWorker {
        frequencies: frequency_set,
        index_name: name,
        auto_commit: auto_commit as u64,
        waiters,
        using_fast_fuzzy,
        fuzzy_fields,
        schema,
        writer,
        rx: op_receiver,
        shutdown,
        corrections,
        stop_words,
    };

    worker.start();

    Ok(())
}

/// A simple wrapper handler around a set of queues and a worker.
///
/// This manages creating the waiters and scheduling the operations
/// in a new thread.
pub(crate) struct Writer {
    index_name: String,
    op_sender: OpSender,
    shutdown_waiter: ShutdownReceiver,
    writer_waiters: WaitersQueue,
}

impl Writer {
    /// Creates a new writer handler from a given index name, index and context.
    ///
    /// This creates a bounded queue with a capacity of 20, builds the tantivy index
    /// writer with n threads and spawns a worker in a new thread.
    pub(crate) fn create(ctx: &IndexContext) -> Result<Self> {
        let index_name = ctx.name.clone();
        let (op_sender, op_receiver) = channel::bounded::<OpPayload>(20);
        let (shutdown, shutdown_waiter) = async_channel::bounded(1);

        let writer = {
            let writer_ctx = ctx.writer_ctx.with_safe_buffer()?;

            debug!(
                "[ WRITER @ {} ] index writer setup threads={}, heap={}B ",
                &ctx.name, writer_ctx.writer_threads, writer_ctx.writer_buffer,
            );

            ctx.index.writer_with_num_threads(
                writer_ctx.writer_threads,
                writer_ctx.writer_buffer,
            )?
        };

        let waiters = WaitersQueue::default();
        let task = {
            let name = index_name.clone();
            let conn = ctx.storage.clone();
            let stop_word_manager = ctx.stop_words.clone();
            let corrections = ctx.correction_manager.clone();
            let waiter_queue = waiters.clone();
            let schema = ctx.schema();
            let using_fast_fuzzy = ctx.query_ctx.use_fast_fuzzy;
            let fuzzy_fields = ctx.fuzzy_search_fields().clone();
            let auto_commit = ctx.writer_ctx.auto_commit;

            move || {
                start_writer(
                    name,
                    conn,
                    stop_word_manager,
                    waiter_queue,
                    schema,
                    auto_commit,
                    using_fast_fuzzy,
                    fuzzy_fields,
                    writer,
                    op_receiver,
                    shutdown,
                    corrections,
                )
            }
        };

        info!("[ WRITER @ {} ] starting writer worker.", &ctx.name);
        let handle = std::thread::Builder::new()
            .name(format!("{}-writer-worker", &ctx.name))
            .spawn(task)
            .map_err(|_e| {
                Error::msg(format!(
                    "failed to spawn writer worker thread for index {}",
                    &ctx.name
                ))
            })?;

        if op_sender.send((WriterOp::__Ping, None)).is_err() {
            handle.join().expect("join worker")?;

            info!(
                "[ WRITER @ {} ] worker is okay, startup successful!",
                &ctx.name
            );
        }

        Ok(Self {
            index_name,
            op_sender,
            shutdown_waiter,
            writer_waiters: waiters,
        })
    }

    /// Sends a message to the writer worker
    ///
    /// If there is space in the queue this will complete immediately
    /// otherwise this will wait until it's woken up again.
    pub(crate) async fn send_op(&self, op: WriterOp) -> anyhow::Result<()> {
        let (waker, waker_waiter) = oneshot::channel();
        let mut payload: OpPayload = (op, Some(waker));
        loop {
            payload = match self.op_sender.try_send(payload) {
                Ok(()) => {
                    break;
                },
                Err(channel::TrySendError::Disconnected(_)) => {
                    return Err(Error::msg("writer worker has shutdown"))
                },
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

        waker_waiter.await??;

        debug!(
            "[ WRITER @ {} ] operation queue full, waiting for wakeup",
            &self.index_name
        );

        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> anyhow::Result<()> {
        self.send_op(WriterOp::__Shutdown).await?;

        let _ = self.shutdown_waiter.recv().await;

        Ok(())
    }

    pub(crate) async fn destroy(&self) -> anyhow::Result<()> {
        self.shutdown().await?;

        let dir = format!("{}/{}", INDEX_STORAGE_PATH, &self.index_name);
        let index_dir = Path::new(&dir);
        if index_dir.exists() {
            tokio::fs::remove_dir_all(index_dir).await?;
        }

        Ok(())
    }
}
