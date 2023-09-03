mod fifo;
mod lru;

use std::sync::Arc;

use lnx_document::DocBlockReader;

/// A cache structure used for keeping blocks in memory.
pub trait Cache: Sized {
    /// Creates a new cache with a given capacity in bytes.
    fn with_capacity(capacity: u64) -> Self;

    /// Inserts a new block into the cache.
    fn put(&self, block_id: u64, reader: DocBlockReader);

    /// Attempts to get a block in the cache if it exists.
    fn get(&self, block_id: u64) -> Option<DocBlockReader>;
}

/// A sharded cache for increased performance when working with
/// higher concurrency levels.
///
/// This will produce 1 shard for every 8 cpu cores + 1.
pub struct ShardedCache<C: Cache> {
    shards: Arc<Vec<C>>,
}

impl<C: Cache> Cache for ShardedCache<C> {
    fn with_capacity(capacity: u64) -> Self {
        let num_cores = num_cpus::get();
        let num_shards = (num_cores / 8) + 1;

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(C::with_capacity(capacity / num_shards as u64));
        }

        Self {
            shards: Arc::new(shards),
        }
    }

    #[inline]
    fn put(&self, block_id: u64, reader: DocBlockReader) {
        let shards = self.shards.as_slice();

        let shard_id = block_id % shards.len() as u64;
        shards[shard_id as usize].put(block_id, reader)
    }

    #[inline]
    fn get(&self, block_id: u64) -> Option<DocBlockReader> {
        let shards = self.shards.as_slice();

        let shard_id = block_id % shards.len() as u64;
        shards[shard_id as usize].get(block_id)
    }
}

/// A cache structure that has selectable algorithms for evicting data.
pub enum CacheSelector {
    /// A FIFO queue based cache.
    Fifo(ShardedCache<fifo::FifoCache>),
    /// A LRU based cache.
    Lru(ShardedCache<lru::LruCache>),
}

impl CacheSelector {
    /// Creates a new FIFO cache with a given capacity in bytes.
    pub fn fifo_with_capacity(capacity: u64) -> Self {
        Self::Fifo(ShardedCache::with_capacity(capacity))
    }

    /// Creates a new LRU cache with a given capacity in bytes.
    pub fn lru_with_capacity(capacity: u64) -> Self {
        Self::Lru(ShardedCache::with_capacity(capacity))
    }

    #[inline]
    /// Inserts a new block into the cache.
    pub fn put(&self, block_id: u64, reader: DocBlockReader) {
        match self {
            CacheSelector::Fifo(cache) => cache.put(block_id, reader),
            CacheSelector::Lru(cache) => cache.put(block_id, reader),
        }
    }

    #[inline]
    /// Attempts to get a block in the cache if it exists.
    pub fn get(&self, block_id: u64) -> Option<DocBlockReader> {
        match self {
            CacheSelector::Fifo(cache) => cache.get(block_id),
            CacheSelector::Lru(cache) => cache.get(block_id),
        }
    }
}
