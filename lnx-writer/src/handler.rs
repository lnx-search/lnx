use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use lnx_common::schema::FieldName;
use serde::{Deserialize, Serialize};
use tantivy::Index;
use tokio::sync::mpsc::Permit;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

use super::helpers::serde::{BufferSize, NumThreads};
use super::indexer::{start_indexing, Task};

static INDEXER_CONFIG_KEY: &str = "INDEXER_CONFIG";
type Indexes = HashMap<String, Arc<Index>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerHandlerConfig {
    /// The maximum number of indexers that can run concurrency.
    ///
    /// Node that this does not control how many threads each indexer uses.
    #[serde(default = "IndexerHandlerConfig::default_indexer_concurrency")]
    max_indexer_concurrency: NonZeroUsize,
}

impl IndexerHandlerConfig {
    fn default_indexer_concurrency() -> NonZeroUsize {
        unsafe { NonZeroUsize::new_unchecked(1) }
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
    BeginIndexing(String, mpsc::Receiver<Task>),
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
}

#[instrument(name = "indexer-actor", skip(indexes, events))]
async fn run_actor(
    cfg: IndexerHandlerConfig,
    mut indexes: Indexes,
    mut events: mpsc::Receiver<Event>,
) {
    let mut active_indexers = vec![];
    let limiter = Arc::new(Semaphore::new(cfg.max_indexer_concurrency.get()));
    while let Some(event) = events.recv().await {
        if let Err(e) =
            handle_event(limiter.clone(), &mut active_indexers, &mut indexes, event)
                .await
        {
            error!("Failed to handle event due to error: {}", e);
        }
    }
}

async fn handle_event(
    limiter: Arc<Semaphore>,
    active_indexers: &mut Vec<JoinHandle<Result<()>>>,
    indexes: &mut Indexes,
    event: Event,
) -> Result<()> {
    match event {
        Event::AddIndex(name, index) => {
            indexes.insert(name, Arc::new(index));
        },
        Event::RemoveIndex(name) => {
            indexes.remove(&name);
        },
        Event::BeginIndexing(name, tasks_queue) => {
            if let Some(index) = indexes.get(&name) {
                let handle = tokio::spawn(begin_indexing(
                    limiter,
                    name,
                    tasks_queue,
                    index.clone(),
                ));
                active_indexers.push(handle);
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
    index: Arc<Index>,
) -> Result<()> {
    info!("Waiting on permit to begin indexing documents");
    let _permit = limiter.acquire().await?;

    let name = FieldName(name);
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
