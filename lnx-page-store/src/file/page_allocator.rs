use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::PageId;
use crate::layout::allocation_table;

/// The page allocator assigns pages to write ops as they come in.
///
/// Multiple pages can be written to at once, but access to the allocator is serialized
/// internally.
pub struct PageAllocator {
    live_page_bitset: Arc<Mutex<allocation_table::SimpleBitSet>>,
}

impl PageAllocator {
    /// Create a new allocator using an initial bitset.
    pub fn new(base_bitset: allocation_table::SimpleBitSet) -> Self {
        Self {
            live_page_bitset: Arc::new(Mutex::new(base_bitset)),
        }
    }

    /// Returns the number of pages marked as allocated.
    pub fn num_allocated(&self) -> usize {
        let lock = self.live_page_bitset.lock();
        lock.num_set()
    }

    /// Attempt to reserve `n_pages` on the page file.
    ///
    /// `None` is returned if there is not enough capacity in the file.
    pub fn try_reserve(&self, n_pages: usize) -> Option<ReservedPagesGuard> {
        let mut lock = self.live_page_bitset.lock();
        let pages = lock.reserve_next_n_free(n_pages)?;
        Some(ReservedPagesGuard {
            committed: false,
            pages,
            parent: self.live_page_bitset.clone(),
        })
    }

    /// Create the pages provided by the iterator.
    pub fn free(&self, pages: impl Iterator<Item = PageId>) {
        let mut lock = self.live_page_bitset.lock();
        for page in pages {
            lock.clear(page.0 as usize);
        }
    }
}

/// A guard that holds onto a set of reserved pages.
///
/// If this guard is dropped before `commit` is called, the pages
/// will be returned backed to the allocator and allowed to be reserved
/// by another operation.
pub struct ReservedPagesGuard {
    committed: bool,
    pages: SmallVec<[usize; 8]>,
    parent: Arc<Mutex<allocation_table::SimpleBitSet>>,
}

impl ReservedPagesGuard {
    /// Iterate over the reserved pages.
    pub fn iter_pages(&self) -> impl Iterator<Item = PageId> + '_ {
        self.pages.iter().map(|idx| PageId(*idx as u32))
    }

    /// Commit the reserved pages.
    ///
    /// These pages will no longer be freed up when the
    /// guard is dropped.
    pub fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for ReservedPagesGuard {
    fn drop(&mut self) {
        if !self.committed {
            let mut lock = self.parent.lock();
            for page in self.pages.iter() {
                lock.clear(*page);
            }
        }
    }
}
