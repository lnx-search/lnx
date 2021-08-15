use anyhow::Result;
use parking_lot::RwLock;
use hashbrown::HashMap;

use crate::storage::StorageManager;
use crate::index::IndexHandler;


pub struct SearchEngine {
    storage: StorageManager,
    indexes: RwLock<HashMap<String, IndexHandler>>,
}

impl SearchEngine {
    pub async fn create(dir: &str) -> Result<Self> {
        let storage = StorageManager::with_directory(dir.to_string()).await?;
        let loaded_indexes = storage.load_all().await?;

        // load previously defined indexes on restart.
        let mut indexes = HashMap::with_capacity(loaded_indexes.len());
        for loader in loaded_indexes {
            let name = loader.name.clone();
            let index = IndexHandler::build_loaded(loader)?;

            indexes.insert(name, index);
        }

        Ok(Self {
            storage,
            indexes: RwLock::new(indexes),
        })
    }
}

