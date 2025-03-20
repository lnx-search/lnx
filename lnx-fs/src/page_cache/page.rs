use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::page_cache::block::PageId;

pub(super) type PageWriteLockGuard<'a> = parking_lot::MutexGuard<'a, ()>;
pub(super) type PageMutex = parking_lot::Mutex<()>;

/// Some metadata about the current state of a page in the file cache.
///
/// This attempts to be as small as possible in order to minimise overhead.
pub(super) struct PageState {
    /// The write lock guard for synchronizing writes.
    lock: PageMutex,
    /// The flags set for the given page.
    flags: AtomicPageFlags,
}

impl PageState {
    fn empty() -> Self {
        Self {
            lock: PageMutex::default(),
            flags: AtomicPageFlags::default(),
        }
    }

    #[inline]
    /// Returns whether the page is locked for writing.
    pub(super) fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }

    #[inline]
    /// Returns the page flags.
    pub(super) fn flags(&self) -> PageFlags {
        self.flags.load()
    }

    /// Mark a page as free if it is already marked as dirty.
    /// 
    /// This is useful in situation where the page is going to be imediately written to again 
    /// and you just need to cancel the pending GC operation.
    pub(super) unsafe fn mark_free_if_dirty(&self) {
        let flags = self.flags();
        if flags.is_dirty() {
            self.mark_free_unchecked();
        }
    }
    
    /// Marks the page as free and reset flags without checks.
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn mark_free_unchecked(&self) {
        self.flags.set_free()
    }

    /// Marks the page as free and reset flags without checks.
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn mark_allocated_unchecked(&self) {
        self.flags.set_allocated()
    }

    /// Marks the page as dirty, tagging the operation with the given generation..
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn mark_dirty_unchecked(&self, generation: u64) {
        self.flags.set_dirty(generation)
    }

    /// Attempts to acquire the lock for the given page if it is not already locked.
    ///
    /// Returns `None` if the lock is already acquired by someone else.
    ///
    /// # Safety
    ///
    /// This lifetime returned by this method is not truly `'static`, instead, it is up to
    /// the called to ensure this guard does _not_ live longer than the parent mutex.
    pub(super) unsafe fn try_acquire_static_write_guard(
        &self,
    ) -> Option<PageWriteLockGuard<'static>> {
        let result = self.try_acquire_write_guard();
        mem::transmute::<Option<PageWriteLockGuard<'_>>, Option<PageWriteLockGuard<'static>>>(result)
    }
    
    /// Acquire the page lock guard for writing.
    pub(super) fn acquire_write_guard(&self) -> PageWriteLockGuard<'_> {
        self.lock.lock()
    }

    /// Attempt to acquire the page lock guard for writing otherwise return None 
    /// if it is already locked.
    pub(super) fn try_acquire_write_guard(&self) -> Option<PageWriteLockGuard<'_>> {
        self.lock.try_lock()
    }
}

/// A fixed size table of page states.
pub(super) struct PageStateTable {
    table: Box<[PageState]>,
}

impl PageStateTable {
    /// Creates a new page state table with the given num pages.
    pub(super) fn new(num_pages: usize) -> Self {
        let mut table = Vec::with_capacity(num_pages);

        for _ in 0..num_pages {
            table.push(PageState::empty());
        }

        Self {
            table: table.into_boxed_slice(),
        }
    }

    /// Returns the number of entries in the page table.
    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    /// Returns the page state for the given page ID.
    pub(super) fn at(&self, idx: PageId) -> &PageState {
        &self.table[idx]
    
    }

    /// Calculates the page index based on the pointer.
    ///
    /// This assumes the pointer passed to the table belongs to the table, otherwise
    /// an invalid index can be returned.
    pub(super) fn pointer_to_index(&self, page: *const PageState) -> PageId {
        let start = self.table.as_ptr().addr();
        let end = page as usize;
        let diff = end - start;
        diff / size_of::<PageState>()
    }
}

#[derive(Default)]
/// A set of flags marking what state the page is in, represented by an
/// atomic `u8`.
pub(super) struct AtomicPageFlags(AtomicU64);

impl AtomicPageFlags {
    /// Performs a relaxed load of the page flags.
    pub(super) fn load(&self) -> PageFlags {
        PageFlags(self.0.load(Ordering::Relaxed))
    }

    fn set_free(&self) {
        self.0.store(0, Ordering::Release);
    }

    fn set_allocated(&self) {
        self.0.store(PageFlags::ALLOCATED, Ordering::Release);
    }

    fn set_dirty(&self, generation: u64) {
        self.0.store(generation, Ordering::Release);
    }
}

/// The page state flags.
pub(super) struct PageFlags(u64);

impl PageFlags {
    const ALLOCATED: u64 = u64::MAX;
    const UNALLOCATED: u64 = 0;

    /// Returns if the page is allocated or not.
    pub(super) fn is_allocated(&self) -> bool {
        self.0 == Self::ALLOCATED
    }

    /// Returns if the page is free/unallocated or not.
    pub(super) fn is_free(&self) -> bool {
        self.0 == Self::UNALLOCATED
    }
    
    /// Returns if the page is waiting to be freed by the gc.
    pub(super) fn is_dirty(&self) -> bool {
        self.0 != Self::ALLOCATED
            && self.0 != Self::UNALLOCATED
    }
    
    /// The 
    pub(super) fn dirty_marker_generation(&self) -> Option<u64> {
        if self.is_dirty() {
            Some(self.0)
        } else {
            None
        }
    }
}

impl Debug for PageFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageFlags(is_allocated={}, is_dirty={})", self.is_allocated(), self.is_dirty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_flags() {
        let flags = AtomicPageFlags::default();
        assert_eq!(size_of::<PageState>(), 16);

        let v = flags.load();
        assert!(!v.is_allocated());
        assert!(!v.is_dirty());

        flags.set_allocated();

        let v = flags.load();
        assert!(v.is_allocated());
        assert!(!v.is_dirty());

        flags.set_dirty(1);

        let v = flags.load();
        assert_eq!(v.dirty_marker_generation(), Some(1));
        assert!(!v.is_allocated());
        assert!(v.is_dirty());

        flags.set_free();
        
        let v = flags.load();
        assert_eq!(v.dirty_marker_generation(), None);
        assert!(!v.is_allocated());
        assert!(!v.is_dirty());
    }
}
