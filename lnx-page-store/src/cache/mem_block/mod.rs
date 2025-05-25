mod flags;
mod prepared;
mod raw;
mod state;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod ticket;

use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

pub use self::prepared::{PreparedRead, ReadResult};
pub use self::raw::{PageIndex, PageSize};
use self::state::PageWriteLockGuard;
use self::ticket::GenerationTicketMachine;
use crate::cache::mem_block::state::PageStateEntry;

static BLOCK_UID_GENERATOR: AtomicU64 = AtomicU64::new(0);

/// A block of virtual memory split into pages.
pub struct VirtualMemoryBlock {
    /// A unique ID assigned to the memory block to prevent misuse.
    uid: u64,
    /// The raw virtual memory.
    inner: raw::RawVirtualMemoryPages,
    /// The associate state for each page within the memory.
    state: Box<[state::PageStateEntry]>,
    /// The ticket machine tracks operations and ensures thread safe access to the pages
    /// using generational GC-like patterns.
    ticket_machine: GenerationTicketMachine,
}

impl VirtualMemoryBlock {
    /// Allocate a new [VirtualMemoryBlock] with `num_pages` pages with the target [PageSize].
    ///
    /// May error if there is not enough virtual memory capacity or huge pages are targeted
    /// but are not enabled in the system.
    pub fn allocate(num_pages: usize, page_size: PageSize) -> io::Result<Self> {
        let uid = BLOCK_UID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        let mut state = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            state.push(state::PageStateEntry::default());
        }

        let inner = raw::RawVirtualMemoryPages::allocate(num_pages, page_size)?;

        Ok(Self {
            uid,
            inner,
            state: state.into_boxed_slice(),
            ticket_machine: GenerationTicketMachine::default(),
        })
    }

    #[inline]
    /// Returns the number of pages in this block.
    pub fn num_pages(&self) -> usize {
        self.state.len()
    }

    /// Attempt to collapse the memory pages into transparent huge pages.
    pub fn try_collapse(&self) -> io::Result<()> {
        self.inner.try_collapse()
    }

    /// Attempt to free the target page.
    ///
    /// This call may fail if any one of the following is true:
    ///
    /// - There are still active readers that might be accessing the page.
    /// - The page has been mutated since meaning the [PageFreePermit] has now expired.
    /// - The page lock is already being held by another task.
    pub fn try_free(&self, permit: &PageFreePermit) -> Result<(), TryFreeError> {
        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);

        // We first try check the page flags before acquiring the lock,
        // since another operation might have already changed the flags
        // and our permit is now expired, so no point contesting the lock.
        if !flags_tagged_with_ticket(state, permit.ticket_id) {
            return Err(TryFreeError::PermitExpired);
        }

        // If there is a chance a reader may still be accessing the page, we abort.
        if permit.ticket_id >= self.ticket_machine.oldest_alive_ticket() {
            return Err(TryFreeError::InUse);
        }

        let guard = state.try_acquire_lock().ok_or(TryFreeError::Locked)?;

        // Acquire the lock and check flags again in case they changed. Now we have the lock
        // we can be sure they won't change as long as we hold the guard.
        if !flags_tagged_with_ticket(state, permit.ticket_id) {
            return Err(TryFreeError::PermitExpired);
        }

        // Page is still marked for eviction and tagged with our generation ID, we can free the page.
        // Safety:
        // - We checked to ensure that no generations or ticket guards are still active
        //   for readers that came before the permit.
        // - We mark the page as freed, preventing reads from occurring until a write passes.
        let result = unsafe { self.inner.free(permit.page) };

        // If the madvise call fails, we should be able to retry. It should also not invalidate
        // our existing data, so it is safe to not mark the page as free and wait for a retry.
        if let Err(e) = result {
            return Err(TryFreeError::Io(e));
        } else {
            state.mark_free(&guard);
        }

        drop(guard);

        Ok(())
    }

    /// Attempt to mark the page as dirty and get back a [PageFreePermit].
    ///
    /// This call may fail if it cannot acquire the page lock or the operation
    /// has become stale, the event the lock cannot be acquired, a retry is provided
    /// within the error so operation ordering can be maintained.
    pub fn try_dirty_page(
        &self,
        target: PageOrRetry,
    ) -> Result<PageFreePermit, PrepareDirtyEvictionError> {
        let permit = self.get_or_reserve_free_permit(target);

        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);
        let guard = match state.try_acquire_lock() {
            Some(guard) => guard,
            None => {
                let flags = state.flags();

                // We do not return an early error if the page dirty because
                // we have to be sure the ordering is correct, and we cannot do
                // that unless we have the page lock.
                return if flags.is_stale(permit.ticket_id) {
                    // The operation has been superseded.
                    Err(PrepareDirtyEvictionError::OperationStale)
                } else {
                    Err(PrepareDirtyEvictionError::PageLocked(EvictRetry(permit)))
                };
            },
        };
        self.dirty_page_inner(state, permit, &guard)
    }

    /// Marks a page as dirty.
    ///
    /// Unlike [self.try_dirty_page] this method will wait for the page lock
    /// to become available.
    pub fn dirty_page(
        &self,
        target: PageOrRetry,
    ) -> Result<PageFreePermit, PrepareDirtyEvictionError> {
        let permit = self.get_or_reserve_free_permit(target);

        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);
        let guard = state.acquire_lock();
        self.dirty_page_inner(state, permit, &guard)
    }

    fn dirty_page_inner(
        &self,
        state: &PageStateEntry,
        permit: PageFreePermit,
        guard: &PageWriteLockGuard,
    ) -> Result<PageFreePermit, PrepareDirtyEvictionError> {
        let flags = state.flags();
        if flags.is_free() {
            return Err(PrepareDirtyEvictionError::AlreadyFree);
        } else if flags.is_dirty() {
            return Err(PrepareDirtyEvictionError::AlreadyDirty);
        } else if flags.is_stale(permit.ticket_id) {
            return Err(PrepareDirtyEvictionError::OperationStale);
        }

        // We must always issue a new permit once we have the guard because
        // we may be performing a retry on an operation, which we know logically
        // is valid in terms of order of operations, but new readers may be active
        // so we need to get a new ticket ID.
        let permit = self.issue_new_free_permit(permit.page);

        let state = self.state_at(permit.page);
        state.mark_dirty(&guard, permit.ticket_id);

        Ok(permit)
    }

    /// Attempt to mark the page for eviction and get back a [PageFreePermit].
    ///
    /// This call may fail if it cannot acquire the page lock or the operation
    /// has become stale, the event the lock cannot be acquired, a retry is provided
    /// within the error so operation ordering can be maintained.
    pub fn try_mark_for_revertible_eviction(
        &self,
        target: PageOrRetry,
    ) -> Result<PageFreePermit, PrepareRevertibleEvictionError> {
        let permit = self.get_or_reserve_free_permit(target);

        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);

        let guard = match state.try_acquire_lock() {
            Some(guard) => guard,
            None => {
                let flags = state.flags();

                // We do not return an early error if the page dirty because
                // we have to be sure the ordering is correct, and we cannot do
                // that unless we have the page lock.
                return if flags.is_stale(permit.ticket_id) {
                    // The operation has been superseded.
                    Err(PrepareRevertibleEvictionError::OperationStale)
                } else {
                    Err(PrepareRevertibleEvictionError::PageLocked(EvictRetry(
                        permit,
                    )))
                };
            },
        };

        let flags = state.flags();
        if flags.is_free() {
            return Err(PrepareRevertibleEvictionError::AlreadyFree);
        } else if flags.is_dirty() {
            return Err(PrepareRevertibleEvictionError::Dirty);
        } else if flags.is_stale(permit.ticket_id) {
            return Err(PrepareRevertibleEvictionError::OperationStale);
        }

        // We must always issue a new permit once we have the guard because
        // we may be performing a retry on an operation, which we know logically
        // is valid in terms of order of operations, but new readers may be active
        // so we need to get a new ticket ID.
        let permit = self.issue_new_free_permit(permit.page);

        let state = self.state_at(permit.page);
        state.mark_revertible_eviction_scheduled(&guard, permit.ticket_id);

        Ok(permit)
    }

    /// Write the provided set of bytes to the target page specified by the [PageFreePermit].
    ///
    /// This operation is technically infallible as it is just a memcpy under the hood and atomic
    /// flag change.
    ///
    /// This method can panic if the permit UID does not match with current memory block
    /// or if the `data` is not the same size as the configured [PageSize].
    pub fn write_page(&self, permit: PageWritePermit, data: &[u8]) {
        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);
        let mut page_mem = self.inner.get_mut_page(permit.page);

        assert_eq!(
            data.len(),
            self.inner.page_size() as usize,
            "data length is not equal to the page size."
        );

        unsafe {
            let uninit_mem = page_mem.access_uninit();
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                uninit_mem.as_mut_ptr() as *mut u8,
                data.len(),
            );
        }

        state.mark_allocated(&permit.page_lock_guard, permit.ticket_id);
    }

    /// Attempt to get access to a page for writing and obtain a guard to allow
    /// writing to the page at a later point in time.
    ///
    /// This call can return a [PrepareWriteError] in the event the page is already allocated
    /// or the page lock could not be acquired.
    pub fn try_prepare_for_write(
        &self,
        page: PageIndex,
    ) -> Result<PageWritePermit, PrepareWriteError> {
        let state = self.state_at(page);
        let flags = state.flags();

        if flags.is_allocated() && !flags.is_marked_for_eviction() {
            return Err(PrepareWriteError::AlreadyAllocated);
        }

        let page_lock_guard =
            state.try_acquire_lock().ok_or(PrepareWriteError::Locked)?;

        let flags = state.flags();
        if flags.is_allocated() && !flags.is_marked_for_eviction() {
            return Err(PrepareWriteError::AlreadyAllocated);
        }

        let ticket_id = self.ticket_machine.increment_ticket_id();

        // If the page is allocated but marked for eviction, we can simply
        // revert the eviction and say the page is already allocated.
        if flags.is_allocated() && flags.is_marked_for_eviction() {
            state.mark_allocated(&page_lock_guard, ticket_id);
            return Err(PrepareWriteError::AlreadyAllocated);
        }

        Ok(PageWritePermit {
            uid: self.uid,
            page,
            ticket_id,
            page_lock_guard,
        })
    }

    /// Prepare to read the given range of pages.
    ///
    /// This will allow the caller to select pages and write any pages that need to be
    /// allocated before the read is safe.
    pub fn prepare_read(&self, range: Range<PageIndex>) -> PreparedRead {
        assert!(
            range.start.0 < self.state.len()
                && range.end.0 <= self.state.len()
                && range.start <= range.end,
            "invalid page range provided, this is a bug"
        );

        let ticket_guard = self.ticket_machine.get_next_ticket();
        PreparedRead::for_page_range(ticket_guard, self, range)
    }

    /// Reads a range of pages and returns the slice of memory spanning those pages.
    ///
    /// # Safety
    /// It is the callers responsibility to ensure:
    /// - The target range of pages do not change as long as this slice lives.
    /// - All pages are currently allocated
    /// - The pages being selected lay within the boundaries of the block.
    unsafe fn read_pages(&self, range: Range<PageIndex>) -> raw::RawPagePtr {
        self.inner.read_pages(range)
    }

    #[cfg(test)]
    fn for_test_get_raw_page_ptr(&self, index: PageIndex) -> raw::RawMutPagePtr {
        self.inner.get_mut_page(index)
    }

    #[cfg(test)]
    fn for_test_advance_ticket_counter(&self, by: usize) {
        for _ in 0..by {
            self.ticket_machine.increment_ticket_id();
        }
    }

    fn get_page_flags(&self, index: PageIndex) -> flags::PageFlags {
        self.state_at(index).flags()
    }

    fn state_at(&self, index: PageIndex) -> &PageStateEntry {
        &self.state[index.0]
    }

    fn get_or_reserve_free_permit(&self, target: PageOrRetry) -> PageFreePermit {
        match target {
            PageOrRetry::Retry(retry) => retry.0,
            PageOrRetry::Page(page) => self.issue_new_free_permit(page),
        }
    }

    fn issue_new_free_permit(&self, page: PageIndex) -> PageFreePermit {
        let ticket_id = self.ticket_machine.increment_ticket_id();
        PageFreePermit {
            uid: self.uid,
            ticket_id,
            page,
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// An error describing why a target page could not be freed.
pub enum TryFreeError {
    #[error("permit expired")]
    /// The permit has expired and should be ignored.
    PermitExpired,
    #[error("page locked")]
    /// The page is currently locked by another task.
    Locked,
    #[error("page is still potentially still in use")]
    /// The page may still be referenced by readers and is therefore
    /// no safe to free the page.
    InUse,
    #[error("{0}")]
    /// An IO error prevented the operation from completing.
    ///
    /// This should be retried.
    Io(io::Error),
}

#[derive(Debug, thiserror::Error)]
/// The system could not schedule a page eviction due to a given reason.
pub enum PrepareDirtyEvictionError {
    #[error("page locked")]
    /// The page is currently locked and cannot be marked.
    ///
    /// A retry value is provided if the operation wants to retry.
    PageLocked(EvictRetry),
    #[error("operation is stale")]
    /// The operation attempting to be applied is stale and newer operations
    /// have since superseded it.
    OperationStale,
    #[error("page already free")]
    /// The page is already free.
    AlreadyFree,
    #[error("page already dirty")]
    /// The page is already marked as dirty.
    AlreadyDirty,
}

#[derive(Debug, thiserror::Error)]
/// The system could not schedule a page eviction due to a given reason.
pub enum PrepareRevertibleEvictionError {
    #[error("page locked")]
    /// The page is currently locked and cannot be marked.
    ///
    /// A retry value is provided if the operation wants to retry.
    PageLocked(EvictRetry),
    #[error("operation is stale")]
    /// The operation attempting to be applied is stale and newer operations
    /// have since superseded it.
    OperationStale,
    #[error("page already free")]
    /// The page is already free.
    AlreadyFree,
    #[error("page dirty")]
    /// The page is dirty and cannot be marked as revertible.
    Dirty,
}

#[derive(Debug)]
/// A snapshot of the state in order to retry an eviciton operation
/// at a later stage without breaking ordering of events.
pub struct EvictRetry(PageFreePermit);

/// An enum selecting either a page to evict or a retry value.
pub enum PageOrRetry {
    /// Apply a new operation to the page with no existing retry.
    Page(PageIndex),
    /// Try to apply a previous attempt.
    Retry(EvictRetry),
}

#[derive(Debug, thiserror::Error)]
/// An error preventing the block from issuing a [PageWritePermit].
pub enum PrepareWriteError {
    #[error("page locked")]
    /// The page is currently locked by another task.
    Locked,
    #[error("page already allocated")]
    /// The page is already allocated and does not need to be written.
    AlreadyAllocated,
    #[error("page already allocated due to eviction reverted")]
    /// The page is already allocated and does not need to be written
    /// because it was able to revert a scheduled revertible eviction.
    EvictionReverted,
}

#[derive(Debug)]
/// A [PageFreePermit] represents a free operation that has been queued
/// and can later be used to complete the freeing of the page providing
/// no operations since have invalidated the permit.
///
/// This contains a UID to the memory block that produced it, the target page
/// and a generation ID used to check if any new operations have since invalidated the page.
pub struct PageFreePermit {
    uid: u64,
    page: PageIndex,
    ticket_id: u64,
}

#[derive(Debug)]
/// A [PageWritePermit] represents a pending write operation to a target page.
///
/// This permit allows an operation to reserve its spot and prevent other tasks
/// from reforming needless additional IO or frees of the page.
///
/// Unlike the rest of the operations, this permit holds onto the acquired page lock.
pub struct PageWritePermit<'guard> {
    uid: u64,
    page: PageIndex,
    ticket_id: u64,
    page_lock_guard: PageWriteLockGuard<'guard>,
}

impl PageWritePermit<'_> {
    pub(super) fn page(&self) -> PageIndex {
        self.page
    }
}

fn flags_tagged_with_ticket(
    state: &state::PageStateEntry,
    expected_generation: u64,
) -> bool {
    let flags = state.flags();
    if let Some(active_generation) = flags.extract_ticket_id() {
        active_generation == expected_generation
    } else {
        false
    }
}
