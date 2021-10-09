use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Result};
use crossbeam::channel;
use crossbeam::queue::SegQueue;
use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, Schema};
use tantivy::{IndexWriter, Opstamp, Term};
use tokio::sync::oneshot;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WriterContext {
    /// The amount of bytes to allocate to the writer buffer.
    writer_buffer: usize,

    /// The amount of worker threads to dedicate to a writer.
    writer_threads: usize,
}

impl Validate for WriterContext {
    fn validate(&self) -> Result<()> {
        if self.writer_threads == 0 {
            return Err(Error::msg(
                "writer buffer bellow minimum. Buffer size must be at least 1.",
            ));
        }

        let min_buffer = 30_000 * self.writer_threads;
        if self.writer_buffer < min_buffer {
            return Err(Error::msg(format!(
                "writer buffer bellow minimum. \
                Buffer size must be at least {} \
                (30,000 * {} threads).",
                min_buffer, self.writer_threads,
            )));
        }

        Ok(())
    }
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

        let _ = self.writer.wait_merging_threads();
        let _ = self.shutdown.try_send(());
        info!("[ WRITER @ {} ] shutdown complete!", &self.index_name);
    }

    /// Purges all pending operations from the receiver.
    fn process_messages(&mut self) -> bool {
        while let Ok((msg, waker)) = self.rx.try_recv() {
            debug!(
                "[ WRITER @ {} ] handling operation {:?}",
                &self.index_name, msg
            );
            match self.handle_msg(msg) {
                Err(e) => {
                    if let Some(w) = waker {
                        let _ = w.send(Err(e));
                    }
                },
                Ok(true) => {
                    if let Some(w) = waker {
                        let _ = w.send(Ok(()));
                    }
                    return true;
                },
                _ => {
                    if let Some(w) = waker {
                        let _ = w.send(Ok(()));
                    }
                },
            }
        }

        false
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

    fn handle_msg(&mut self, op: WriterOp) -> Result<bool> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown => return Ok(true),
            WriterOp::__Ping => return Ok(false),
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

                return Ok(false);
            },
            WriterOp::DeleteAll => (self.writer.delete_all_documents()?, "DELETE-ALL"),
            WriterOp::DeleteTerm(term) => (self.writer.delete_term(term), "DELETE-TERM"),
            WriterOp::AddStopWords(words) => {
                self.stop_words.add_stop_words(words);
                self.stop_words.commit()?;
                return Ok(false);
            },
            WriterOp::RemoveStopWords(words) => {
                self.stop_words.remove_stop_words(words);
                self.stop_words.commit()?;
                return Ok(false);
            },
            WriterOp::ClearStopWords => {
                self.stop_words.clear_stop_words();
                self.stop_words.commit()?;
                return Ok(false);
            },
        };

        debug!(
            "[ WRITER @ {} ][ TRANSACTION {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(false)
    }
}

fn start_writer(
    name: String,
    conn: StorageBackend,
    stop_word_manager: StopWordManager,
    waiters: WaitersQueue,
    schema: Schema,
    using_fast_fuzzy: bool,
    fuzzy_fields: Vec<Field>,
    writer: IndexWriter,
    op_receiver: OpReceiver,
    shutdown: ShutdownWaker,
    corrections: SymSpellCorrectionManager,
) -> Result<()> {
    let stop_words = PersistentStopWordManager::new(conn.clone(), stop_word_manager)?;
    let frequency_set = PersistentFrequencySet::new(conn)?;
    let worker = IndexWriterWorker {
        frequencies: frequency_set,
        index_name: name.clone(),
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
            let num_threads = ctx.writer_ctx.writer_threads;
            let writer_buffer = ctx.writer_ctx.writer_buffer;

            debug!(
                "[ WRITER @ {} ] index writer setup threads={}, heap={}B ",
                &ctx.name, num_threads, writer_buffer,
            );

            ctx.index.writer_with_num_threads(
                num_threads,
                writer_buffer,
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

            move || {
                start_writer(
                    name,
                    conn,
                    stop_word_manager,
                    waiter_queue,
                    schema,
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
