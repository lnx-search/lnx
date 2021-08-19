use anyhow::{Error, Result};
use hashbrown::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::index::IndexHandler;
use crate::storage::StorageManager;
use crate::structures::IndexDeclaration;
use crate::correction::load_dictionaries;

pub type LeasedIndex = Arc<IndexHandler>;

/// A manager for a collection of indexes.
///
/// This will store index definitions as persistent json files and will
/// load any existing indexes at creation time.
pub struct SearchEngine {
    storage: StorageManager,
    indexes: RwLock<HashMap<String, Arc<IndexHandler>>>,
}

impl SearchEngine {
    /// Creates a new search engine loading the existing index metadata
    /// from the given directory.
    pub async fn create(dir: &str, max_edit_distance: i64) -> Result<Self> {
        tokio::task::spawn_blocking(move || load_dictionaries(max_edit_distance)).await??;

        let storage = StorageManager::with_directory(dir.to_string()).await?;
        let loaded_indexes = storage.load_all().await?;

        // load previously defined indexes on restart.
        let mut indexes = HashMap::with_capacity(loaded_indexes.len());
        for loader in loaded_indexes {
            let name = loader.name.clone();
            let index = IndexHandler::build_loaded(loader).await?;

            indexes.insert(name, Arc::new(index));
        }

        Ok(Self {
            storage,
            indexes: RwLock::new(indexes),
        })
    }

    /// Adds a declared index to the search engine.
    ///
    /// This will set it in the index storage and then build the index handlers.
    pub async fn add_index(&self, index: IndexDeclaration, override_if_exists: bool) -> Result<()> {
        let remove = {
            let lock = self.indexes.read().await;
            if lock.contains_key(&index.name) {
                if !override_if_exists {
                    debug!("[ ENGINE ] index already exists, ignoring override");
                    return Err(Error::msg("index already exists"));
                }
                true
            } else {
                false
            }
        };

        if remove {
            debug!("[ ENGINE ] index already exists, purging and re-creating");
            let mut lock = self.indexes.write().await;
            debug!("[ ENGINE ] lock acquired");

            if let Some(index) = lock.remove(&index.name) {
                index.shutdown().await?;
                self.storage.remove_index_meta(&index.name).await?;
                debug!("[ ENGINE ] index correctly shutdown");
            };
        }

        let copy_index = index.clone();
        let loaded = copy_index.into_schema();
        let name = loaded.name.clone();
        let index_handler = Arc::new(IndexHandler::build_loaded(loaded).await?);

        // We must make sure to only save the metadata if the original making succeeded.
        self.storage.store_index_meta(&index).await?;

        {
            let mut lock = self.indexes.write().await;
            lock.insert(name, index_handler);
            debug!("[ ENGINE ] index re-create success");
        };

        Ok(())
    }

    /// Removes an index from the engine.
    ///
    /// This will wait until all searches are complete before shutting
    /// down the index.
    pub async fn remove_index(&self, index_name: &str) -> Result<()> {
        let value = { self.indexes.write().await.remove(index_name) };

        if value.is_none() {
            return Err(Error::msg("this index does not exit"));
        }

        let value = value.unwrap();

        self.storage.remove_index_meta(&value.name).await?;

        // This just shuts down the system, we still require the ref
        // count to actually fully drop the index.
        value.shutdown().await?;

        Ok(())
    }

    /// Gets an index from the search engine.
    pub async fn get_index(&self, index_name: &str) -> Option<LeasedIndex> {
        let lock = self.indexes.read().await;
        Some(lock.get(index_name)?.clone())
    }

    pub async fn reset(&self) -> Result<()> {
        let mut lock = self.indexes.write().await;

        for (name, v) in lock.drain() {
            info!("[ CONTROLLER ] clearing {}", &name);
            v.shutdown().await?;
        }

        self.storage.clear_all().await?;

        Ok(())
    }
}
