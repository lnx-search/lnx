use std::collections;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arc_swap::{ArcSwap, Guard};
use hashbrown::HashMap;
use lnx_common::schema::{FieldName, Schema};
use lnx_utils::{FromBytes, ToBytes};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

use crate::configure::Config;
use crate::{DocStore, EngineStore, MetaStore, PollingMode, ReplicationInfo};
use crate::engine_store::IndexData;

static STORAGE_ENGINE_ROOT: &str = "engine";
static INDEX_STORAGE_ROOT: &str = "engine";
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
    index_stores: ArcSwap<HashMap<String, IndexStore>>,
}

impl Deref for StorageManager {
    type Target = Box<dyn EngineStore>;

    fn deref(&self) -> &Self::Target {
        &self.engine_store
    }
}

impl StorageManager {
    #[instrument(name = "storage-manager-init", skip(cfg))]
    async fn from_config(cfg: Config) -> Result<Self> {
        let db_path = cfg.storage_path.join(STORAGE_ENGINE_ROOT);
        tokio::fs::create_dir_all(&db_path).await?;

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

        info!("Loading existing indexes...");
        inst.load_existing_indexes().await?;

        Ok(inst)
    }

    async fn load_existing_indexes(&self) -> Result<()> {
        info!("Running engine store setup...");
        self.engine_store.setup().await?;
        info!("Engine store setup complete!");

        let indexes = self.engine_store.fetch_indexes().await?;
        info!("Found {} existing indexes.", indexes.len());

        let mut loaded = HashMap::with_capacity(indexes.len());
        for index in indexes {
            info!("Ensuring keyspace replication: {:?}", &index.replication);
            index
                .replication
                .build_index_keyspace(index.index_name.as_str())
                .await?;

            let docs = self
                .cfg
                .backend
                .get_doc_store(&index.index_name, &index.schema);

            let meta = self
                .cfg
                .backend
                .get_meta_store(&index.index_name, &self.local_storage_db)?;

            let path = self
                .cfg
                .storage_path
                .join(INDEX_STORAGE_ROOT)
                .join(lnx_utils::index_id(&index.index_name).to_string());

            let store = IndexStore {
                output_path: path,
                index_name: index.index_name,
                schema: index.schema,
                additional_settings: Arc::from(RwLock::new(index.additional_settings)),
                doc_store: docs,
                meta_store: meta,
                polling_mode: index.polling_mode,
            };

            loaded.insert(store.index_name.0.clone(), store);
        }

        self.index_stores.store(Arc::new(loaded));

        Ok(())
    }

    #[instrument(name = "add-new-index", skip(self, schema, additional_settings))]
    pub async fn add_new_index(
        &self,
        name: &str,
        schema: Schema,
        replication: ReplicationInfo,
        polling_mode: PollingMode,
        additional_settings: collections::HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        info!("Ensuring keyspace replication: {:?}", &replication);
        replication.build_index_keyspace(name).await?;

        self.engine_store.store_index(IndexData {
            index_name: FieldName(name.to_string()),
            schema: schema.clone(),
            replication: replication.clone(),
            polling_mode,
            additional_settings: additional_settings.clone(),
        }).await?;

        let docs = self.cfg.backend.get_doc_store(name, &schema);

        info!("Running doc store setup...");
        let instant = Instant::now();
        docs.setup().await?;
        info!("Success! Took {:?}", instant.elapsed());

        let meta = self
            .cfg
            .backend
            .get_meta_store(name, &self.local_storage_db)?;

        info!("Running meta store setup...");
        let instant = Instant::now();
        meta.setup().await?;
        info!("Success! Took {:?}", instant.elapsed());

        let path = self
            .cfg
            .storage_path
            .join(INDEX_STORAGE_ROOT)
            .join(lnx_utils::index_id(name).to_string());

        let store = IndexStore {
            output_path: path,
            index_name: FieldName(name.to_string()),
            polling_mode,
            schema,
            additional_settings: Arc::from(RwLock::new(additional_settings)),
            doc_store: docs,
            meta_store: meta,
        };

        let mut indexes = self.index_stores.load().as_ref().clone();
        indexes.insert(name.to_string(), store);
        self.index_stores.store(Arc::new(indexes));

        Ok(())
    }

    #[inline]
    /// Gets a handle of the current indexes and their stores.
    pub fn indexes(&self) -> Guard<Arc<HashMap<String, IndexStore>>> {
        self.index_stores.load()
    }
}

#[derive(Clone)]
/// A Index's storage and configuration settings.
///
/// Additional settings can be added via the `store` and `load` method.
pub struct IndexStore {
    index_name: FieldName,
    schema: Schema,
    output_path: PathBuf,
    polling_mode: PollingMode,
    additional_settings: Arc<RwLock<collections::HashMap<String, Vec<u8>>>>,
    doc_store: Arc<dyn DocStore>,
    meta_store: Arc<dyn MetaStore>,
}

impl IndexStore {
    #[inline]
    pub fn polling_mode(&self) -> PollingMode {
        self.polling_mode
    }

    #[inline]
    pub fn file_path(&self) -> &Path {
        &self.output_path
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.index_name.as_str()
    }

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
    #[instrument(name = "index-settings-store", skip(self, settings), fields(index_name = %self.name()))]
    pub async fn store<T: ToBytes>(&self, key: &str, settings: T) -> Result<()> {
        let mut lock = self.additional_settings.write();
        lock.insert(key.to_string(), settings.to_bytes()?);

        let instant = Instant::now();
        engine()
            .update_settings(&self.index_name, lock.clone())
            .await?;
        info!("Settings store took {:?}", instant.elapsed());

        Ok(())
    }

    /// Removes the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-remove", skip(self), fields(index_name = %self.name()))]
    pub async fn remove(&self, key: &str) -> Result<()> {
        let mut lock = self.additional_settings.write();
        lock.remove(key);

        let instant = Instant::now();
        engine()
            .update_settings(&self.index_name, lock.clone())
            .await?;
        info!("Settings update took {:?}", instant.elapsed());

        Ok(())
    }

    /// Loads the latest current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-load", skip(self), fields(index_name = %self.name()))]
    pub async fn load<T: FromBytes>(&self, key: &str) -> Result<Option<T>> {
        let instant = Instant::now();
        let settings = engine()
            .fetch_latest_settings(&self.index_name)
            .await?;
        info!("Settings load took {:?}", instant.elapsed());

        let mut lock = self.additional_settings.write();

        let item = settings.get(key).map(|v| T::from_bytes(v)).transpose()?;

        (*lock) = settings;

        Ok(item)
    }
}
