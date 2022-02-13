use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::RwLock;
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use once_cell::sync::OnceCell;

use lnx_common::schema::{Schema, FieldName};
use lnx_utils::{FromBytes, ToBytes};

use crate::{DocStore, EngineStore, MetaStore};
use crate::configure::Config;


static STORAGE_ENGINE_ROOT: &str = "engine";
static STORAGE_ENGINE: OnceCell<StorageManager> = OnceCell::new();

pub async fn init_with_config(cfg: Config) -> Result<()> {
    let inst = StorageManager::from_config(cfg).await?;
    let _ = STORAGE_ENGINE.set(inst);
    Ok(())
}

pub fn engine() -> &'static StorageManager {
    STORAGE_ENGINE.get().unwrap()
}

pub struct StorageManager {
    cfg: Config,
    local_storage_db: sled::Db,
    engine_store: Box<dyn EngineStore>,
    index_stores: ArcSwap<HashMap<FieldName, IndexStore>>
}

impl Deref for StorageManager {
    type Target = Box<dyn EngineStore>;

    fn deref(&self) -> &Self::Target {
        &self.engine_store
    }
}

impl StorageManager {
    pub async fn from_config(cfg: Config) -> Result<Self> {
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
            index_stores: ArcSwap::from_pointee(HashMap::new())
        };

        inst.load_existing_indexes().await?;

        Ok(inst)
    }

    async fn load_existing_indexes(&self) -> Result<()> {
        let indexes = self.engine_store.fetch_indexes().await?;

        let mut loaded = HashMap::with_capacity(indexes.len());
        for index in indexes {
            let docs = self.cfg
                .backend
                .get_doc_store(&index.index_name, &index.schema);

            let meta = self.cfg
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
}


pub struct IndexStore {
    index_name: FieldName,
    schema: Schema,
    additional_settings: RwLock<HashMap<String, Vec<u8>>>,
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

    pub fn store<T: ToBytes>(&self, key: &str, settings: T) -> bincode::Result<()> {
        self.additional_settings
            .write()
            .insert(key.to_string(), settings.to_bytes()?);

        Ok(())
    }

    pub fn read<T: FromBytes>(&self, key: &str) -> bincode::Result<Option<T>> {
        let settings = self.additional_settings
            .read()
            .get(key)
            .map(|v| T::from_bytes(&v));

        Option::transpose(settings)
    }
}

