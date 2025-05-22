use std::fmt::{Debug, Formatter};
use std::ops::{Deref, Range};
use std::sync::Arc;

use tokio::sync::Notify;

use super::LivePagesLfu;
use super::mem_block::{
    PageFreePermit,
    PageIndex,
    PageOrRetry,
    PageWritePermit,
    PrepareDirtyEvictionError,
    PrepareWriteError,
    ReadResult,
    TryFreeError,
    VirtualMemoryBlock,
};
use crate::PageFileId;
use crate::cache::evictions::PendingEvictions;

/// The global waker that notifies pending readers when a page has been written.
///
/// This might wake for page that the reader does not care about, but typically
/// the read rate on disk is not expected to be high enough to warrant independent
/// waker for each file (especially since that can ramp up the memory pressure on smaller cached
/// with lots of small files.)
///
/// This does use a slightly sharded approach to reduce contention and incorrect wakes
/// by taking the file ID and selecting the waker based on the hash mod of that.
static PAGE_WRITE_WAKER: [Notify; 32] = [const { Notify::const_new() }; 32];

/// The [PageFileCacheLayer] adds an in-memory LFU cache to a page file.
///
/// The cached pages within the file are controlled by a global LFU which allows
/// for optimal caching across several page files.
pub struct PageFileCacheLayer {
    pub(super) file_id: PageFileId,
    pub(super) live_pages: LivePagesLfu,
    pub(super) memory: VirtualMemoryBlock,
    pub(super) pending_evictions: PendingEvictions,
}

impl PageFileCacheLayer {
    /// Prepare a new read of pages.
    ///
    /// Once all pages are confirmed to be allocated the read can be completed.
    pub fn prepare_read(self: &Arc<Self>, page_range: Range<PageIndex>) -> PreparedRead {
        // Register the access within the cache's policy.
        for page in iter_pages(page_range.clone()) {
            self.live_pages.get(&(self.file_id, page));
        }

        self.pending_evictions.try_cleanup(&self.memory);

        let inner = self.memory.prepare_read(page_range.clone());
        PreparedRead {
            parent: self.clone(),
            inner,
            page_range,
        }
    }

    /// Dirty all pages in the cache for the given file.
    ///
    /// This method can block due to syscall and waiting on page locks to become free.
    pub fn dirty_page_range(&self, range: Range<PageIndex>) {
        let mut retries = Vec::new();
        for page in iter_pages(range) {
            self.live_pages.remove(&(self.file_id, page));

            match self.memory.try_dirty_page(PageOrRetry::Page(page)) {
                Ok(permit) => {
                    self.free_or_add_to_backlog(permit);
                },
                Err(
                    PrepareDirtyEvictionError::AlreadyFree
                    | PrepareDirtyEvictionError::AlreadyDirty
                    | PrepareDirtyEvictionError::OperationStale,
                ) => {},
                Err(PrepareDirtyEvictionError::PageLocked(retry)) => {
                    retries.push(retry);
                },
            }
        }

        for retry in retries {
            match self.memory.dirty_page(PageOrRetry::Retry(retry)) {
                Ok(permit) => {
                    self.free_or_add_to_backlog(permit);
                },
                Err(
                    PrepareDirtyEvictionError::AlreadyFree
                    | PrepareDirtyEvictionError::AlreadyDirty
                    | PrepareDirtyEvictionError::OperationStale,
                ) => {},
                Err(PrepareDirtyEvictionError::PageLocked(_)) => {
                    unreachable!(
                        "dirty_page should not happen as locks are acquired by blocking"
                    );
                },
            }
        }
    }

    /// Run the cleanup step explicitly to clean up pages.
    ///
    /// Unlike the maintenance task that occur during reads, this will wait
    /// for locks to become available to ensure the backlog is processed.
    pub fn run_cleanup(&self) {
        self.pending_evictions.cleanup(&self.memory);
    }

    fn free_or_add_to_backlog(&self, permit: PageFreePermit) {
        match self.memory.try_free(&permit) {
            Ok(()) => {},
            Err(TryFreeError::PermitExpired) => {},
            Err(TryFreeError::InUse | TryFreeError::Locked | TryFreeError::Io(_)) => {
                self.pending_evictions.process_page_dirty_permit(permit);
            },
        }
    }

    fn register_page_write_in_cache(&self, page_id: PageIndex) {
        self.live_pages.insert((self.file_id, page_id), ());
    }
}

impl Drop for PageFileCacheLayer {
    fn drop(&mut self) {
        for page in 0..self.memory.num_pages() {
            self.live_pages.remove(&(self.file_id, PageIndex(page)));
        }
    }
}

/// A prepared read marks a caller's intent to read a span of pages
/// and ensures that a read can only be performed once all pages that need to be read
/// are both allocated and valid.
pub struct PreparedRead<'cache> {
    parent: Arc<PageFileCacheLayer>,
    inner: super::mem_block::PreparedRead<'cache>,
    page_range: Range<PageIndex>,
}

impl<'cache> PreparedRead<'cache> {
    /// Produces an iterator of [PageWritePermit] for any of the pages
    /// with outstanding writes that are able to acquire the individual page locks.
    pub fn get_outstanding_write_permits(
        &self,
    ) -> impl Iterator<Item = PageWritePermit> {
        self.inner.outstanding_writes().iter().filter_map(|page| {
            // If the lock is acquired or already allocated we can ignore
            // the error as the next `try_finish` call will resolve any outstanding
            // writes left.
            let result = self.inner.parent().try_prepare_for_write(*page);

            match result {
                Ok(permit) => Some(permit),
                Err(PrepareWriteError::Locked) => None,
                Err(PrepareWriteError::AlreadyAllocated) => None,
                Err(PrepareWriteError::EvictionReverted) => {
                    self.parent.register_page_write_in_cache(*page);
                    None
                },
            }
        })
    }

    /// Write the provided byte array to the page guarded by the permit.
    pub fn write_page(&self, permit: PageWritePermit, bytes: &[u8]) {
        self.parent.register_page_write_in_cache(permit.page());
        self.inner.parent().write_page(permit, bytes);
        self.get_waker().notify_waiters();
    }

    /// Returns a future that waits until a write has been completed on the page file.
    pub fn wait_for_signal(&self) -> impl Future<Output = ()> {
        let waker = self.get_waker();
        waker.notified()
    }

    /// Attempt to finish the read if all pages are allocated and valid.
    pub fn try_finish(mut self) -> Result<ReadRef, Self> {
        match self.inner.try_finish() {
            Err(inner) => {
                self.inner = inner;
                Err(self)
            },
            Ok(result) => {
                Ok(ReadRef {
                    parent: self.parent,
                    // Safety:
                    // We can extend the lifetime as long as `parent` is kept alive, as that is
                    // what actually owns the memory we are now referencing.
                    inner: unsafe {
                        std::mem::transmute::<ReadResult<'_>, ReadResult<'static>>(
                            result,
                        )
                    },
                })
            },
        }
    }

    fn get_waker(&self) -> &'static Notify {
        let notify_id = self.parent.file_id.0 as usize % PAGE_WRITE_WAKER.len();
        &PAGE_WRITE_WAKER[notify_id]
    }
}

#[repr(C)]
/// A reference pointer to a span of memory pages.
///
/// This struct can be cheaply cloned as it just increments a ref-count.
pub struct ReadRef {
    parent: Arc<PageFileCacheLayer>,
    // NOTE: This is not actually `'static`, it lives only for as long as `parent`.
    inner: ReadResult<'static>,
}

impl Debug for ReadRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let start = self.inner.as_ref();
        write!(
            f,
            "ReadRef(start={:?}, len={})",
            start.as_ptr(),
            start.len()
        )
    }
}

impl Clone for ReadRef {
    fn clone(&self) -> Self {
        Self {
            parent: self.parent.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl AsRef<[u8]> for ReadRef {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Deref for ReadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

unsafe impl stable_deref_trait::StableDeref for ReadRef {}

fn iter_pages(range: Range<PageIndex>) -> impl Iterator<Item = PageIndex> {
    (range.start.0..range.end.0).map(PageIndex)
}
