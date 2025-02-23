//! A page cache dynamically allocates virtual memory to allow partial or complete
//! caching of file contents while maintaining alignment and "contiguous" memory regions.
//!
//! This system handles incrementally locking ranges of the cache files, freeing unused pages
//! and managing the overall memory attempting to be used by the system.
//!
//! Internally this is done with a lot of unsafe and mmap calls.
//!
//! # WARNING!
//!
//! If you're reading this bit of the code, I am sorry, this is a _very_ complicated
//! system which does a ton of unsafe in order to minimise overhead since this is the
//! hottest bit of any code within the lnx project. Please take great care reading all
//! documentation & annotations around the code in this section because most types do
//! more than what they let on, particularly mutexes.
//!

mod block;
mod gc;
mod page;
mod prepared_read;
mod utils;

use std::hash::Hash;
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ahash::RandomState;

pub use self::prepared_read::PreparedRead;
use self::utils::NoOpRandomState;
use crate::page_cache::block::VirtualFileBlock;

/// A page cache holds files partially or full in memory using virtual memory
/// addressing.
///
/// This system is composed of a two stage cache using both an LFU and LRU eviction policy.
/// This is used as a primary-secondary configuration where evictions from the primary (LFU) cache
/// are inserted into the secondary (LRU) cache where they have another chance to be re-used
/// while allowing the system to freely evict pages with low contention when it needs to
/// create larger allocations for file reads.
///
/// Internally the cache describes the memory regions used in the primary as the "cache memory"
/// and the secondary as the "working memory". This is because the working memory is what is used
/// for holding onto allocations that probably won't stay in the primary cache, or for doing
/// large reads of multiple pages into contiguous blocks.
///
/// ### Note about working memory behaviour
///
/// If there is not enough working memory to fit an allocation, the system will return an
/// "OutOfMemory" error, this is done to prevent accidental OOMs.
///
///
///
pub struct FilePageCache {
    random_state: RandomState,
    /// The primary LFU cache (Cache memory.)
    primary: moka::sync::Cache<u64, (), NoOpRandomState>,
    /// The secondary LRU cache (Working Memory.)
    secondary: moka::sync::Cache<u64, (), NoOpRandomState>,
    /// The file blocks backing pages.
    file_blocks: dashmap::DashMap<u64, FileBlockState, NoOpRandomState>,
}

impl FilePageCache {
    /// Creates a [PreparedRead] which allows the system to load any pre-cached
    /// pages and work out what pages need to be loaded from disk.
    ///
    /// The prepared read will hold write guards to the pages it needs to write to
    /// which will prevent other reads from writing or accessing the page.
    ///
    /// NOTE:
    /// It is important to note that reading the file cache != no memory write, because this
    /// system has to somewhat seamlessly allow filling in blanks in the cache, the
    /// prepared reads may require writing certain pages (which you have probably gathered by now.)
    ///
    pub fn prepare_read<K: Hash>(
        &self,
        file: K,
        range: Range<usize>,
    ) -> io::Result<PreparedRead> {
        todo!()
    }

    #[inline(never)]
    fn prepare_read_inner(
        &self,
        file_id: u64,
        range: Range<usize>,
    ) -> io::Result<PreparedRead> {
        todo!()
    }
}

struct FileBlockState {
    file_id: u64,
    generation_counter: AtomicU64,
    block: Arc<VirtualFileBlock>,
}

impl FileBlockState {
    fn prepare_read(&self, bytes_range: Range<usize>) -> io::Result<PreparedRead> {
        let generation_id = self.generation_counter.fetch_add(1, Ordering::Relaxed);

        let generation = TrackedGeneration {
            file_id: self.file_id,
            generation_id,
        };

        PreparedRead::from_block_and_generation(
            self.block.clone(),
            bytes_range,
            generation,
        )
    }
}

/// A generation with a unique monotonic ID.
///
/// When this object is dropped it triggers a GC notification.
pub struct TrackedGeneration {
    file_id: u64,
    generation_id: u64,
}

impl Drop for TrackedGeneration {
    fn drop(&mut self) {
        gc::mark_dead_generation(self.file_id, self.generation_id);
    }
}
