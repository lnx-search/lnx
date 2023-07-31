use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use lnx_metastore::Metastore;
use parking_lot::RwLock;
use tokio::time::Instant;
use tracing::{info, instrument};

use crate::metastore::BlockStorageMetastore;
use crate::{BlockStoreReader, FileKey, Readers, StorageShardMailbox, DEFAULT_FILE_EXT};

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// The root path for all doc storage entries to be inserted at.
    pub base_path: PathBuf,
    /// The number of storage shards to spawn.
    pub num_shards: usize,
}

#[derive(Clone)]
/// The managing service that controls all active doc store writers.
pub struct BlockStoreService {
    /// The internal metastore storage for things like commit checkpoints.
    metastore: BlockStorageMetastore,
    /// The loaded block store readers.
    ///
    /// This uses a [NoOpRandomState] here as the `FileKey` is suitable for being
    /// used as the hash result itself.
    readers: Arc<RwLock<Readers>>,
    /// The config used by the service.
    config: Arc<ServiceConfig>,
    /// The active writing shards.
    writing_shards: Arc<Vec<StorageShardMailbox>>,
}

impl BlockStoreService {
    #[instrument(name = "block-store-service-create", skip(metastore))]
    /// Creates a new block storage service.
    pub async fn create(metastore: Metastore, config: ServiceConfig) -> Result<Self> {
        let metastore = BlockStorageMetastore::from_metastore(metastore)?;

        let readers = {
            let base_path = config.base_path.clone();
            let metastore = metastore.clone();
            lnx_executor::start_blocking_thread(move || {
                crate::reader::get_file_readers(&metastore, &base_path)
            })
            .await
            .expect("Join thread")?
        };

        let mut shards = Vec::with_capacity(config.num_shards);
        for shard_id in 0..config.num_shards {
            let shard =
                crate::shard::start_shard(shard_id, config.base_path.clone()).await?;
            shards.push(shard);
        }

        Ok(Self {
            metastore,
            readers: Arc::new(RwLock::new(readers)),
            config: Arc::new(config),
            writing_shards: Arc::new(shards),
        })
    }

    #[inline]
    /// Gets a context manager for adding multiple blocks of data
    /// to the writer.
    pub fn get_storage_context(&self, shard_id: usize) -> &StorageShardMailbox {
        &self.writing_shards[shard_id]
    }

    #[instrument(name = "reload-reader", skip(self))]
    /// Attempts to reload a given reader.
    ///
    /// If the reader is not already loaded (like with a new file) it will
    /// be loaded.
    pub fn reload_reader(&self, file_key: FileKey) -> Result<()> {
        let start = Instant::now();
        let path = self
            .config
            .base_path
            .join(format!("{file_key}.{DEFAULT_FILE_EXT}"));
        let checkpoint = self.metastore.get_file_commit_checkpoint(file_key)?;

        let reader = BlockStoreReader::open(&path, checkpoint)?;
        self.readers.write().insert(file_key, reader);

        info!(elapsed = ?start.elapsed(), "Reloaded reader");

        Ok(())
    }

    #[instrument(name = "set-commit-checkpoint", skip(self))]
    /// Sets a new file commit checkpoint and reloads the reader.
    pub fn set_file_commit_checkpoint(&self, file_key: FileKey, pos: u64) -> Result<()> {
        self.metastore.set_file_commit_checkpoint(file_key, pos)?;
        self.reload_reader(file_key)
    }
}
