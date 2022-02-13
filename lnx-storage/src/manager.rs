use std::collections;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::{ArcSwap, Guard};
use hashbrown::HashMap;
use lnx_common::schema::{FieldName, Schema};
use lnx_utils::{FromBytes, ToBytes};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

use crate::configure::Config;
use crate::{DocStore, EngineStore, MetaStore};

static STORAGE_ENGINE_ROOT: &str = "engine";
static STORAGE_ENGINE: OnceCell<StorageManager> = OnceCell::new();

/// Initialises the global storage engine.
pub async fn init_with_config(cfg: Config) -> Result<()> {
    let inst = StorageManager::from_config(cfg).await?;
    let _ = STORAGE_ENGINE.set(inst);
    Ok(())
}

/// Gets a reference to the global storage engine.
///
/// Panics if the engine has not yet been initialised.
pub fn engine() -> &'static StorageManager {
    STORAGE_ENGINE.get().unwrap()
}

/// The storage manager wrapping the different backend implementations.
pub struct StorageManager {
    /// The storage backend config.
    cfg: Config,

    /// A sled database to store local data in order for the
    /// node to know some information about it's previous state.
    local_storage_db: sled::Db,

    /// The engine storage.
    ///
    /// This stores all of our index data and configuration.
    /// Any changes made via this will be seen by all other nodes.
    engine_store: Box<dyn EngineStore>,

    /// A mapping to each index's respective stores.
    index_stores: ArcSwap<HashMap<FieldName, IndexStore>>,
}

impl Deref for StorageManager {
    type Target = Box<dyn EngineStore>;

    fn deref(&self) -> &Self::Target {
        &self.engine_store
    }
}

impl StorageManager {
    async fn from_config(cfg: Config) -> Result<Self> {
        let db_path = cfg.storage_path.join(STORAGE_ENGINE_ROOT);
        let db = sled::Config::new()
            .use_compression(true)
            .mode(sled::Mode::HighThroughput)
            .path(&db_path)
            .open()?;

        cfg.backend.setup().await?;
        let engine = cfg.backend.get_engine_store();

        let inst = Self {
            cfg,
            local_storage_db: db,
            engine_store: engine,
            index_stores: ArcSwap::from_pointee(HashMap::new()),
        };

        inst.load_existing_indexes().await?;

        Ok(inst)
    }

    async fn load_existing_indexes(&self) -> Result<()> {
        let indexes = self.engine_store.fetch_indexes().await?;

        let mut loaded = HashMap::with_capacity(indexes.len());
        for index in indexes {
            let docs = self
                .cfg
                .backend
                .get_doc_store(&index.index_name, &index.schema);

            let meta = self
                .cfg
                .backend
                .get_meta_store(&index.index_name, &self.local_storage_db)?;

            let store = IndexStore {
                index_name: index.index_name,
                schema: index.schema,
                additional_settings: RwLock::new(index.additional_settings),
                doc_store: docs,
                meta_store: meta,
            };

            loaded.insert(store.index_name.clone(), store);
        }

        self.index_stores.store(Arc::new(loaded));

        Ok(())
    }

    #[inline]
    /// Gets a handle of the current indexes and their stores.
    pub fn indexes(&self) -> Guard<Arc<HashMap<FieldName, IndexStore>>> {
        self.index_stores.load()
    }
}

/// A Index's storage and configuration settings.
///
/// Additional settings can be added via the `store` and `load` method.
pub struct IndexStore {
    index_name: FieldName,
    schema: Schema,
    additional_settings: RwLock<collections::HashMap<String, Vec<u8>>>,
    doc_store: Arc<dyn DocStore>,
    meta_store: Arc<dyn MetaStore>,
}

impl IndexStore {
    #[inline]
    pub fn docs(&self) -> &Arc<dyn DocStore> {
        &self.doc_store
    }

    #[inline]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta_store
    }

    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Updates the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    pub async fn store<T: ToBytes>(&self, key: &str, settings: T) -> Result<()> {
        let mut lock = self.additional_settings.write();
        lock.insert(key.to_string(), settings.to_bytes()?);

        engine()
            .update_settings(&self.index_name, lock.clone())
            .await?;

        Ok(())
    }

    /// Removes the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    pub async fn remove(&self, key: &str) -> Result<()> {
        let mut lock = self.additional_settings.write();
        lock.remove(key);

        engine()
            .update_settings(&self.index_name, lock.clone())
            .await?;

        Ok(())
    }

    /// Loads the latest current settings for the given key.
    ///
    /// These changes are reflected in the database.
    pub async fn load<T: FromBytes>(&self, key: &str) -> Result<Option<T>> {
        let settings = engine().fetch_latest_settings(&self.index_name).await?;

        let mut lock = self.additional_settings.write();

        let item = settings.get(key).map(|v| T::from_bytes(v)).transpose()?;

        (*lock) = settings;

        Ok(item)
    }
}
