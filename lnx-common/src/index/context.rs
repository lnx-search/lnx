use std::borrow::Cow;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tantivy::directory::MmapDirectory;

use crate::configuration::{INDEX_KEYSPACE_PREFIX, TANTIVY_DATA_FOLDER};
use crate::index::base::Index;
use crate::index::polling::PollingMode;
use crate::schema::Schema;

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexContext {
    name: Cow<'static, String>,
    schema: Cow<'static, Schema>,
    polling_mode: PollingMode,
    storage_config: Option<Cow<'static, serde_json::Value>>,
}

impl IndexContext {
    pub fn new(
        name: String,
        schema: Schema,
        polling_mode: PollingMode,
        storage_cfg: Option<serde_json::Value>
    ) -> Self {
        Self {
            name: Cow::Owned(name),
            schema: Cow::Owned(schema),
            polling_mode,
            storage_config: storage_cfg.map(Cow::Owned),
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    #[inline]
    pub fn id(&self) -> u64 {
        crc32fast::hash(self.name.as_bytes()) as u64
    }

    #[inline]
    pub fn polling_mode(&self) -> PollingMode {
        self.polling_mode
    }

    #[inline]
    pub fn storage_config(&self) -> Option<&serde_json::Value> {
        self.storage_config.as_ref().map(|v| v.as_ref())
    }

    #[inline]
    pub fn keyspace(&self) -> String {
        format!(
            "{prefix}_{index}",
            prefix = INDEX_KEYSPACE_PREFIX,
            index = self.id()
        )
    }

    #[inline]
    pub fn root_storage_path(&self, base_path: &Path) -> PathBuf {
        base_path.join(self.id().to_string())
    }

    /// Gets an existing index or creates a new index otherwise.
    pub fn get_or_create_index(&self, base_path: &Path) -> anyhow::Result<Index> {
        let target_path = self.root_storage_path(base_path).join(TANTIVY_DATA_FOLDER);

        std::fs::create_dir_all(&target_path)?;

        let dir = MmapDirectory::open(&target_path)?;
        let does_exist = tantivy::Index::exists(&dir)?;

        let index = if does_exist {
            tantivy::Index::open(dir)
        } else {
            tantivy::Index::open_or_create(dir, self.schema().as_tantivy_schema())
        }?;

        let ref_schema = index.schema();
        self.schema().validate_with_tantivy_schema(&ref_schema)?;

        Ok(Index::new(self.clone(), index))
    }

    /// Removes the folder that would contain the index local data if it exists.
    pub fn clear_local_data(&self, base_path: &Path) -> std::io::Result<()> {
        std::fs::remove_dir_all(self.root_storage_path(base_path))
    }
}
