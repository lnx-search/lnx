use ahash::RandomState;
use lnx_document::DocBlockReader;

/// A single cache shard.
///
/// Internally this wraps a [moka::sync::Cache] instance.
pub struct LruCache {
    inner: moka::sync::Cache<u64, DocBlockReader, RandomState>,
}

impl super::Cache for LruCache {
    fn with_capacity(capacity: u64) -> Self {
        let inner = moka::sync::Cache::builder()
            .thread_pool_enabled(false)
            .max_capacity(capacity)
            .weigher(|_block_id: &u64, reader: &DocBlockReader| {
                reader.buffer_usage() as u32
            })
            .build_with_hasher(RandomState::new());

        Self { inner }
    }

    fn put(&self, block_id: u64, reader: DocBlockReader) {
        self.inner.insert(block_id, reader);
    }

    fn get(&self, block_id: u64) -> Option<DocBlockReader> {
        self.inner.get(&block_id)
    }
}
