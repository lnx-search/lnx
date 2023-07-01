use anyhow::Result;
use lnx_metastore::Metastore;

#[derive(Clone)]
/// A wrapper that provides simple access abstractions to the underlying metastore.
pub struct StorageMetastore(Metastore);

impl StorageMetastore {
    /// Creates a new wrapper for a given metastore.
    pub fn from_core(metastore: Metastore) -> Result<Self> {
        let metastore = metastore.open_database("lnx-storage-service")?;
        Ok(Self(metastore))
    }

    /// Loads the latest checkpoint position for a given writer.
    pub fn load_last_checkpoint(&self, writer_key: u64) -> Result<Option<u64>> {
        self.0.get(&writer_key)
    }

    /// Sets a new checkpoint position for a given writer.
    pub fn set_checkpoint(
        &self,
        writer_key: u64,
        checkpoint_position: u64,
    ) -> Result<()> {
        self.0.put(&writer_key, &checkpoint_position)
    }
}
