use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use crate::StorageShardMailbox;


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
    config: Arc<ServiceConfig>,
    shards: Arc<Vec<StorageShardMailbox>>,
}

impl BlockStoreService {
    /// Creates a new block storage service.
    pub async fn create(config: ServiceConfig) -> Result<Self> {
        let mut shards = Vec::with_capacity(config.num_shards);
        for shard_id in 0..config.num_shards {
            let shard = crate::shard::start_shard(shard_id, config.base_path.clone()).await?;
            shards.push(shard);
        }

        Ok(Self {
            config: Arc::new(config),
            shards: Arc::new(shards),
        })
    }

    #[inline]
    /// Gets a context manager for adding multiple blocks of data
    /// to the writer.
    pub fn get_storage_context(&self, shard_id: usize) -> &StorageShardMailbox {
        &self.shards[shard_id]
    }
}
