use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use lnx_common::types::document::Document;
use lnx_storage::DocId;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tantivy::Index as InnerIndex;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;

use super::helpers::CancellingJoinHandle;
use super::helpers::serde::{BufferSize, NumThreads};
use super::indexer::{start_indexing, Task};

static INDEXER_CONFIG_KEY: &str = "INDEXER_CONFIG";
static INDEXER_HANDLER: OnceCell<IndexerHandler> = OnceCell::new();
type Indexes = HashMap<String, Index>;

pub struct Index {
    inner: Arc<InnerIndex>,
    cancel: JoinHandle<()>,
}

impl Index {
    pub fn new(index: InnerIndex, cancel: JoinHandle<()>) -> Self {
        Self {
            inner: Arc::new(index),
            cancel,
        }
    }
}

impl Deref for Index {
    type Target = InnerIndex;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

/// Starts the indexer handler.
#[instrument(name = "indexer-starter", skip(base_indexes))]
pub fn start(config: IndexerHandlerConfig, base_indexes: Indexes) {
    info!("Starting indexer...");
    let handler = IndexerHandler::start(config, base_indexes);
    let _ = INDEXER_HANDLER.set(handler);
}

/// Gets the indexer handler.
///
/// Panics if the global state hasn't been initialised yet.
pub fn get() -> &'static IndexerHandler {
    INDEXER_HANDLER.get().unwrap()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerHandlerConfig {
    /// The maximum number of indexers that can run concurrency.
    ///
    /// Node that this does not control how many threads each indexer uses.
    #[serde(default = "IndexerHandlerConfig::default_indexer_concurrency")]
    pub max_indexer_concurrency: NonZeroUsize,
}

impl IndexerHandlerConfig {
    fn default_indexer_concurrency() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct IndexerConfig {
    #[serde(default)]
    /// The number of threads to use on the index writer.
    num_threads: NumThreads,

    #[serde(default)]
    /// The per-thread buffer size.
    buffer_size: BufferSize,
}

enum Event {
    AddIndex(String, Index),
    RemoveIndex(String),
    BeginIndexing(String, mpsc::Receiver<Task>, oneshot::Sender<CancellingJoinHandle<Result<()>>>),
}

#[derive(Clone)]
/// The controller for all index writers.
///
/// Index writers are created and destroyed as an when they're required
/// and not kept open all the time, this minimises the chance of a lock
/// accidentally being left.
pub struct IndexerHandler {
    events: mpsc::Sender<Event>,
}

impl IndexerHandler {
    pub fn start(cfg: IndexerHandlerConfig, indexes: Indexes) -> Self {
        let (tx, rx) = mpsc::channel(4);
        let inst = Self { events: tx };
        tokio::spawn(run_actor(cfg, indexes, rx));

        inst
    }

    async fn send_event(&self, event: Event) -> Result<()> {
        let res = self.events.send(event).await;

        if res.is_err() {
            return Err(anyhow!(
                "The indexer handler actor has shut down, no new indexes can be added."
            ));
        }

        Ok(())
    }

    pub async fn add_index(
        &self,
        index_name: &str,
        index: InnerIndex,
        cancel: JoinHandle<()>,
    ) -> Result<()> {
        self.send_event(Event::AddIndex(
            index_name.to_string(),
            Index::new(index, cancel),
        ))
        .await
    }

    pub async fn remove_index(&self, index_name: &str) -> Result<()> {
        self.send_event(Event::RemoveIndex(index_name.to_string()))
            .await
    }

    #[instrument(name = "create-indexer", skip(self))]
    pub async fn begin_indexing(&self, index_name: &str) -> Result<Indexer> {
        info!("Creating indexer...");

        let (tx, rx) = mpsc::channel(1);
        let (handle_tx, handle_rx) = oneshot::channel();
        self.send_event(Event::BeginIndexing(index_name.to_string(), rx, handle_tx))
            .await?;

        let handle = handle_rx.await?;
        let indexer = Indexer { emitter: tx, handle };

        Ok(indexer)
    }
}

/// A handle to a indexer actor to begin processing documents.
///
/// Note that an indexer may not start processing tasks immediately
/// as it may be waiting on another indexer to finish.
pub struct Indexer {
    emitter: mpsc::Sender<Task>,
    handle: CancellingJoinHandle<Result<()>>,
}

impl Indexer {
    async fn send_event(&mut self, event: Task) -> Result<()> {
        let res = self.emitter.send(event).await;

        if res.is_err() {
            return if let Some(handle) = self.handle.0.take() {
                handle.await?
            } else {
                Err(anyhow!("Index writer has shut down. No new tasks can be added"))
            };
        }

        Ok(())
    }

    /// Add a set of documents to the indexer.
    pub async fn add_documents(&mut self, docs: Vec<(DocId, Document)>) -> Result<()> {
        self.send_event(Task::AddDocuments(docs)).await
    }

    /// Remove a set of documents.
    pub async fn remove_documents(&mut self, docs: Vec<DocId>) -> Result<()> {
        self.send_event(Task::RemoveDocs(docs)).await
    }

    /// Clear all documents.
    pub async fn clear_documents(&mut self) -> Result<()> {
        self.send_event(Task::ClearAllDocuments).await
    }
}

#[instrument(name = "indexer-actor", skip(indexes, events))]
async fn run_actor(
    cfg: IndexerHandlerConfig,
    mut indexes: Indexes,
    mut events: mpsc::Receiver<Event>,
) {
    let limiter = Arc::new(Semaphore::new(cfg.max_indexer_concurrency.get()));
    while let Some(event) = events.recv().await {
        if let Err(e) =
            handle_event(limiter.clone(), &mut indexes, event)
                .await
        {
            error!("Failed to handle event due to error: {}", e);
        }
    }

    info!("Indexing actor is shutting down...");
}

async fn handle_event(
    limiter: Arc<Semaphore>,
    indexes: &mut Indexes,
    event: Event,
) -> Result<()> {
    match event {
        Event::AddIndex(name, index) => {
            indexes.insert(name, index);
        },
        Event::RemoveIndex(name) => {
            if let Some(index) = indexes.remove(&name) {
                info!("Stopping existing polling task on old index...");
                index.cancel.abort();
            };
        },
        Event::BeginIndexing(name, tasks_queue, returner) => {
            if let Some(index) = indexes.get(&name) {
                let handle = tokio::spawn(begin_indexing(
                    limiter,
                    name,
                    tasks_queue,
                    index.inner.clone(),
                ));

                let handle = CancellingJoinHandle::from(handle);
                let _ = returner.send(handle);
            }
        },
    };

    Ok(())
}

#[instrument(name = "indexer", skip(limiter, tasks_queue, index))]
async fn begin_indexing(
    limiter: Arc<Semaphore>,
    name: String,
    tasks_queue: mpsc::Receiver<Task>,
    index: Arc<InnerIndex>,
) -> Result<()> {
    info!("Waiting on permit to begin indexing documents");
    let _permit = limiter.acquire().await?;

    let indexes = lnx_storage::engine().indexes();
    let index_store = indexes
        .get(&name)
        .ok_or_else(|| anyhow!("Index ({}) exists for the indexer handler but not for the storage engine (This is a bug)", name.as_str()))?;

    let cfg = index_store
        .load::<IndexerConfig>(INDEXER_CONFIG_KEY)
        .await?
        .unwrap_or_default();

    let schema = index.schema();
    let writer = index.writer_with_num_threads(*cfg.num_threads, *cfg.buffer_size)?;

    info!("Beginning indexing process");
    let start = std::time::Instant::now();
    tokio::task::spawn_blocking(move || start_indexing(schema, writer, tasks_queue))
        .await??;
    info!("Indexing process took {:?} to complete.", start.elapsed());

    Ok(())
}
