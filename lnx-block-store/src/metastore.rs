use anyhow::Result;
use lnx_metastore::Metastore;
use crate::FileKey;


static METASTORE_COMMIT_DATABASE: &str = "lnx_metastore__block_storage_commit_checkpoints";


#[derive(Clone)]
/// The specific metastore operations for the block storage system.
///
/// This wraps the [Metastore] type internally.
pub struct BlockStorageMetastore {
    inner: Metastore,
}

impl BlockStorageMetastore {
    /// Creates a new [BlockStorageMetastore] from a existing [Metastore].
    pub fn from_metastore(metastore: Metastore) -> Result<Self> {
        let inner = metastore.open_database(METASTORE_COMMIT_DATABASE)?;
        Ok(Self { inner })
    }
    
    /// Sets a file's commit checkpoint position.
    pub async fn set_file_commit_checkpoint_async(&self, file_key: FileKey, pos: u64) -> Result<()> {
        self.inner.put_async(file_key, pos).await
    }

    /// Gets a file's commit checkpoint position.
    /// 
    /// This is the position that we guarantee we've indexed and committed upto.
    pub async fn get_file_commit_checkpoint_async(&self, file_key: FileKey) -> Result<Option<u64>> {
        self.inner.get_async(file_key).await
    }
    
    /// Sets a file's commit checkpoint position.
    pub fn set_file_commit_checkpoint(&self, file_key: FileKey, pos: u64) -> Result<()> {
        self.inner.put(&file_key, &pos)
    }

    /// Gets a file's commit checkpoint position.
    /// 
    /// This is the position that we guarantee we've indexed and committed upto.
    pub fn get_file_commit_checkpoint(&self, file_key: FileKey) -> Result<Option<u64>> {
        self.inner.get(&file_key)
    }    
}