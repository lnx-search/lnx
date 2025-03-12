use std::mem;
use std::sync::atomic::{AtomicU8, Ordering};

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

    /// Marks the page as free and reset flags without checks.
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn set_free_unchecked(&self) {
        self.flags.set_free()
    }

    /// Marks the page as free and reset flags without checks.
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn set_allocated_unchecked(&self) {
        self.flags.set_allocated()
    }

    /// Marks the page as free and reset flags without checks.
    ///
    /// # Safety
    ///
    /// The caller must hold the page lock before calling this method.
    pub(super) unsafe fn set_to_be_freed_unchecked(&self) {
        self.flags.set_to_be_freed()
    }

    /// Attempts to acquire the lock for the given page if it is not already locked.
    ///
    /// Returns `None` if the lock is already acquired by someone else.
    ///
    /// # Safety
    ///
    /// This lifetime returned by this method is not truly `'static`, instead, it is up to
    /// the called to ensure this guard does _not_ live longer than the parent mutex.
    pub(super) unsafe fn try_acquire_write_guard(
        &self,
    ) -> Option<PageWriteLockGuard<'static>> {
        if let Some(guard) = self.lock.try_lock() {
            let false_lifetime = mem::transmute::<
                PageWriteLockGuard<'_>,
                PageWriteLockGuard<'static>,
            >(guard);
            Some(false_lifetime)
        } else {
            None
        }
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
pub(super) struct AtomicPageFlags(AtomicU8);

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

    fn set_to_be_freed(&self) {
        self.0.fetch_or(PageFlags::TO_BE_FREED, Ordering::Release);
    }
}

/// The page state flags.
pub(super) struct PageFlags(u8);

impl PageFlags {
    const ALLOCATED: u8 = 1 << 0;
    const TO_BE_FREED: u8 = 1 << 1;

    /// Returns if the page is allocated or not.
    pub(super) fn is_allocated(&self) -> bool {
        self.0 & Self::ALLOCATED != 0
    }

    /// Returns if the page is waiting to be freed by the gc.
    pub(super) fn is_to_be_freed(&self) -> bool {
        self.0 & Self::TO_BE_FREED != 0
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_atomic_flags() {
        let flags = AtomicPageFlags::default();
        
        let v = flags.load();
        assert!(!v.is_allocated());
        assert!(!v.is_to_be_freed());

        flags.set_allocated();
        
        let v = flags.load();
        assert!(v.is_allocated());
        assert!(!v.is_to_be_freed());
        
        flags.set_to_be_freed();
        
        let v = flags.load();
        assert!(v.is_allocated());
        assert!(v.is_to_be_freed());
        
        flags.set_free();

        let v = flags.load();
        assert!(!v.is_allocated());
        assert!(!v.is_to_be_freed());   
    }
}