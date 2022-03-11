use anyhow::Result;
use lnx_storage::stores::IndexStore;

mod configure;
mod indexers;
mod task_handler;

use configure::IndexerConfig;
pub use indexers::{Indexer, WeakIndexer};
use lnx_common::configuration::TASK_BACKLOG_SIZE;

use crate::helpers::CancellingJoinHandle;

static SETTINGS_STORE_KEY: &str = "indexer_config";

pub async fn new(store: IndexStore) -> Result<Indexer> {
    let cfg: Option<IndexerConfig> = store.load(SETTINGS_STORE_KEY).await?;

    if cfg.is_none() {
        warn!("Indexer config for this index has not been set globally. Is the index fully initialised? Using defaults.");
    }

    let (tx, rx) = tokio::sync::mpsc::channel(TASK_BACKLOG_SIZE);

    let handle = tokio::task::spawn_blocking(move || {
        let index = store.index();
        let inner_index = index.inner();
        let schema = inner_index.schema();

        let cfg = cfg.unwrap_or_default();
        let writer =
            inner_index.writer_with_num_threads(*cfg.num_threads, *cfg.buffer_size)?;

        task_handler::start_indexing(schema, writer, rx)?;

        Ok::<_, anyhow::Error>(())
    });

    Ok(Indexer::new(tx, CancellingJoinHandle::from(handle)))
}
