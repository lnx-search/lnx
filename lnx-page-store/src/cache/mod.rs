use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use parking_lot::{Mutex, RwLock};

use crate::PageFileId;
use crate::cache::evictions::PendingEvictions;
use crate::cache::mem_block::{PageIndex, PageSize, VirtualMemoryBlock};
use crate::cache::page_file::PageFileCacheLayer;

mod evictions;
mod mem_block;
mod page_file;

type LivePagesLfu = moka::sync::Cache<(PageFileId, PageIndex), (), ahash::RandomState>;
type LayerEvictionSenders =
    ahash::HashMap<PageFileId, crossbeam_channel::Sender<PageIndex>>;

/// The page file cache
///
/// This system is built around reserving blocks of virtual memory and then incrementally
/// populating and evicting pages in a similar fashion to a memory mapped file.
///
/// Each page file has its own virtual memory reservation which holds only the actual page
/// data (i.e. no metadata, no structure overhead, etc...) and stores each block contiguously
/// allowing the system to return contiguous blocks of memory on reads without additional copies.
///
/// The caching strategy used is a LFU (Least Frequently Used) policy provided by `moka`.
/// The system itself may not immediately free memory when it has been marked for eviction
/// due to reads holding a reference to the memory currently, therefore, it is recommended
/// to allow some buffer zone of memory allowance as the system is not guaranteed to stay
/// within the bounds set constantly.
///
pub struct PageFileCache {
    live_pages: LivePagesLfu,
    page_size: PageSize,
    layer_eviction_senders: Arc<Mutex<LayerEvictionSenders>>,
}

impl PageFileCache {
    /// Create a new [PageFileCache] with a given amount of memory capacity
    /// and with a given page size.
    ///
    /// `memory_allowance` is in bytes.
    ///
    /// The cache will create `memory_allowance / page_size` number of pages.
    pub fn new(memory_allowance: u64, page_size: PageSize) -> Self {
        let num_pages = memory_allowance / page_size as u64;
        let target_memory_usage = num_pages * page_size as u64;
        tracing::info!(
            "page file cache has capacity for {num_pages} {page_size:?} pages ({memory_target} total)",
            memory_target =
                humansize::format_size(target_memory_usage, humansize::DECIMAL),
        );

        let layer_eviction_senders =
            Arc::new(Mutex::new(LayerEvictionSenders::default()));

        let live_pages = moka::sync::Cache::builder()
            .max_capacity(num_pages)
            .eviction_listener({
                let layer_eviction_senders = layer_eviction_senders.clone();
                move |key: Arc<(PageFileId, PageIndex)>, _, cause| {
                    if matches!(cause, RemovalCause::Size | RemovalCause::Expired) {
                        return;
                    }

                    let (file_id, page) = key.as_ref();

                    let mut lock = layer_eviction_senders.lock();
                    let did_error = if let Some(tx) = lock.get(file_id) {
                        tx.send(*page).is_err()
                    } else {
                        return;
                    };

                    if did_error {
                        lock.remove(file_id);
                    }
                }
            })
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .build_with_hasher(ahash::RandomState::new());

        Self {
            live_pages,
            page_size,
            layer_eviction_senders,
        }
    }

    /// Creates a new page file entry within the cache and allow it to hold
    /// upto `num_pages`.
    pub fn create_page_file_layer(
        &self,
        file_id: PageFileId,
        num_pages: usize,
    ) -> io::Result<Arc<PageFileCacheLayer>> {
        let (pending_evictions, tx) = PendingEvictions::new();
        let memory = VirtualMemoryBlock::allocate(num_pages, self.page_size)?;

        self.layer_eviction_senders.lock().insert(file_id, tx);

        let layer = PageFileCacheLayer {
            file_id,
            live_pages: self.live_pages.clone(),
            memory,
            pending_evictions,
        };

        Ok(Arc::new(layer))
    }

    fn register_file_layer_with_listener(
        &self,
        file_id: PageFileId,
        tx: crossbeam_channel::Sender<PageIndex>,
    ) {
        let mut lock = self.layer_eviction_senders.lock();
        lock.insert(file_id, tx);
    }
}

#[derive(Debug, Clone)]
/// Metrics collected by the file cache.
///
/// This includes all allocated page files.
pub struct GlobalCacheMetrics {
    /// Global metrics collected.
    pub global: BaseCacheMetrics,
    /// Cache metrics on the page file level.
    ///
    /// This is a mapping of page file ID to metrics.
    pub page_files: BTreeMap<u64, BaseCacheMetrics>,
}

#[derive(Debug, Copy, Clone)]
/// Core metrics collected for individual page files
/// and globally.
pub struct BaseCacheMetrics {
    /// The average hit ratio on the cache.
    pub hit_ratio: f32,
    /// The average miss ratio on the cache.
    pub miss_ratio: f32,
    /// The number of pages currently allocated.
    pub pages_allocated: usize,
    /// The total amount of virtual memory reserved in KB.
    pub virtual_memory_allocated_kb: u64,
    /// The total number of pages written to the cache.
    pub total_page_writes: u64,
    /// The total number of pages scheduled for eviction
    /// due to memory pressure.
    pub total_page_scheduled_revertible_evictions: u64,
    /// The total number of pages scheduled for eviction
    /// due to the page being dirty (old.)
    pub total_page_scheduled_dirty_evictions: u64,
    /// The total number of pages that were scheduled for a (revertible) eviction
    /// and actually ended up being freed.
    pub total_page_realised_revertible_evictions: u64,
    /// The total number of pages that were scheduled for eviction due to being
    /// dirty and actually ended up being freed.
    pub total_page_realised_dirty_evictions: u64,
}
