use std::sync::atomic::Ordering;

use super::flags::{AtomicPageFlags, PageFlags};

pub(super) type PageWriteLockGuard<'a> = parking_lot::MutexGuard<'a, ()>;
pub(super) type PageMutex = parking_lot::Mutex<()>;

#[derive(Default, Debug)]
pub(super) struct PageStateEntry {
    flags: AtomicPageFlags,
    lock: PageMutex,
}

impl PageStateEntry {
    /// Load the current page flags with [Ordering::Relaxed] ordering.
    pub(super) fn flags(&self) -> PageFlags {
        self.flags.load(Ordering::Relaxed)
    }

    /// Mark the page as free.
    pub(super) fn mark_free(&self, _guard: &PageWriteLockGuard) {
        self.flags.set_free();
    }

    /// Mark the page as allocated.
    pub(super) fn mark_allocated(&self, _guard: &PageWriteLockGuard, ticket_id: u64) {
        self.flags.set_allocated(ticket_id);
    }

    /// Mark the page as scheduled for eviction but able to be reverted.
    pub(super) fn mark_revertible_eviction_scheduled(
        &self,
        _guard: &PageWriteLockGuard,
        ticket_id: u64,
    ) {
        self.flags.set_revertible_eviction(ticket_id);
    }

    /// Mark the page as dirty.
    pub(super) fn mark_dirty(&self, _guard: &PageWriteLockGuard, ticket_id: u64) {
        self.flags.set_eviction(ticket_id);
    }

    /// Attempt to acquire the lock without blocking.
    ///
    /// Returns `None` if the lock is already in use.
    pub(super) fn try_acquire_lock(&self) -> Option<PageWriteLockGuard> {
        self.lock.try_lock()
    }

    /// Attempt to acquire the lock, blocking if the lock is already in use by someone else.
    pub(super) fn acquire_lock(&self) -> PageWriteLockGuard {
        self.lock.lock()
    }
}
