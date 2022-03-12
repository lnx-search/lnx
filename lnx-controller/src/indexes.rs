use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use lnx_common::index::context::IndexContext;
use lnx_storage::stores::IndexStore;
use lnx_storage::templates::doc_store::DocStore;
use once_cell::sync::OnceCell;
use scylladb_backend::ScyllaIndexStore;
use tokio::task::JoinHandle;
use tokio::time::interval;
use uuid::Uuid;
use lnx_common::configuration::NODE_ID_KEY;

use crate::backends::BackendSelector;
use crate::engine::Engine;

static INDEXES: OnceCell<DashMap<String, IndexStore>> = OnceCell::new();

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
pub async fn new(mut ctx: IndexContext) -> anyhow::Result<()> {
    let engine = crate::engine::get();

    let index = ctx.get_or_create_index(engine.base_path())?;   // TODO: Spawn blocking?
    let meta_store = ctx.get_or_create_metastore(engine.base_path())?;   // TODO: Spawn blocking?

    let id = match meta_store.get(NODE_ID_KEY)? {
        Some(id) => Uuid::from_slice(&id)?,
        None => {
            let new_id = Uuid::new_v4();
            meta_store.insert(NODE_ID_KEY, new_id.as_bytes().as_slice())?;

            new_id
        },
    };

    info!("I am node {}!", &id);
    ctx.set_node_id(id);    // Just a little hacky.

    let doc_store: Arc<dyn DocStore> = match engine.config() {
        BackendSelector::Scylla(cfg) => Arc::new(
            ScyllaIndexStore::setup(ctx.clone(), cfg.engine_replication.clone()).await?,
        ),
    };

    let store = IndexStore::new(ctx.clone(), index, doc_store, meta_store);

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
    let engine = crate::engine::get();

    tokio::spawn(async move {
        let mut interval = interval(period);
        loop {
            interval.tick().await;

            if let Err(e) = check_and_update_indexes(engine).await {
                error!("Failed to handle check due to error: {}", e);
            }
        }
    })
}

#[instrument(name = "check-and-update-indexes", skip_all)]
async fn check_and_update_indexes(engine: &'static Engine) -> anyhow::Result<()> {
    let indexes = engine.fetch_indexes().await?;
    let existing_indexes = INDEXES.get_or_init(DashMap::new);

    for item in existing_indexes.iter() {
        if let Some((_, settings)) = indexes.iter().find(|(k, _)| k == item.key()) {
            let existing_schema = item.value().ctx().schema();

            if existing_schema.search_fields_eq(settings.schema()) {
                adjust_search_fields(settings.clone()).await?;
            }

            if existing_schema.boost_fields_eq(settings.schema()) {
                adjust_boost_fields(settings.clone()).await?;
            }
        } else if let Some((_, store)) = existing_indexes.remove(item.key()) {
            store.destroy(engine.base_path()).await?;
        }
    }

    for (name, ctx) in indexes {
        if !existing_indexes.contains_key(&name) {
            new(ctx).await?;
        }
    }

    Ok(())
}

async fn adjust_search_fields(ctx: IndexContext) -> anyhow::Result<()> {
    todo!()
}

async fn adjust_boost_fields(ctx: IndexContext) -> anyhow::Result<()> {
    todo!()
}
