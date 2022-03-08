use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use lnx_common::index::base::Index;
use lnx_common::index::context::IndexContext;
use lnx_common::index::polling::PollingMode;
use lnx_storage::stores::IndexStore;
use lnx_storage::templates::doc_store::DocStore;
use lnx_storage::templates::meta_store::MetaStore;
use once_cell::sync::OnceCell;
use tokio::time::interval;
use lnx_storage::templates::engine_store::EngineStore;
use scylladb_backend::ScyllaIndexStore;
use crate::backends::BackendSelector;
use crate::engine::Engine;


static INDEXES: OnceCell<DashMap<String, IndexStore>> = OnceCell::new();
static ENGINE: OnceCell<Engine> = OnceCell::new();

#[inline]
/// Gets the index store for the given index if it exists.
pub fn get(index_name: &str) -> Option<IndexStore> {
    Some(INDEXES.get()?.get(index_name)?.deref().clone())
}

#[inline]
/// Removes the index and returns it if it exists.
///
/// This can be used to shutdown and cleanup the index.
pub fn remove(index_name: &str) -> Option<IndexStore> {
    Some(INDEXES.get()?.remove(index_name)?.1)
}

#[inline]
/// Creates a new index from the given context, index, polling mode and
/// storage backend configuration.
pub async fn new(
    ctx: IndexContext,
    index: Index,
) -> anyhow::Result<()> {
    let engine = ENGINE.get_or_init(|| todo!());
    let doc_store: Arc<dyn DocStore> = match engine.config() {
        BackendSelector::Scylla(cfg) => {
            Arc::new(ScyllaIndexStore::setup(ctx.clone(), cfg.engine_replication.clone()).await?)
        }
    };

    let store = IndexStore::new(ctx.clone(), index, doc_store);

    let indexes = INDEXES.get_or_init(DashMap::new);
    indexes.insert(ctx.name().to_string(), store);

    Ok(())
}

/// Starts the background poller that tracks any remote updates to indexes
/// and their settings.
///
/// The given interval time determines how often settings are checked and adjusted.
///
/// This should only be called once.
pub async fn start_poller(period: Duration) -> JoinHandle<()> {
    let store = ENGINE.get_or_init(|| todo!());

    tokio::spawn(async move {
        let mut interval = interval(period);
        loop {
            interval.tick().await;

            if let Err(e) = check_and_update_indexes(store).await {
                error!("Failed to handle check due to error: {}", e);
            }
        }
    })
}

#[instrument(name="check-and-update-indexes", skip_all)]
async fn check_and_update_indexes(store: &'static Box<dyn EngineStore>) -> anyhow::Result<()> {
    let indexes = store.fetch_indexes().await?;
    let existing_indexes = INDEXES.get_or_init(DashMap::new);

    for item in existing_indexes.iter() {
        if let Some((name, settings)) = indexes.iter().find(|(k, _)| k == item.key()) {
            todo!()
        } else {
            existing_indexes.remove(item.key());
        }
    }

    Ok(())
}