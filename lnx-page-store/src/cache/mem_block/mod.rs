use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::mem_block::prepared::PreparedRead;
use crate::cache::mem_block::state::PageWriteLockGuard;
use crate::cache::mem_block::ticket::GenerationTicketMachine;

mod flags;
mod prepared;
mod raw;
mod state;
mod ticket;

pub use self::raw::{PageIndex, PageSize};

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
    /// Allocate a new [VirtualMemoryBlock] with capacity for at least `size_bytes`
    /// and allocate pages with the target [PageSize].
    ///
    /// This will align the `size_bytes` _up_ to the nearest page size. I.e. 128 bytes
    /// in size will be rounded up to a single 8KB page if [PageSize::Standard].
    ///
    /// May error if there is not enough virtual memory capacity or huge pages are targetted
    /// but are not enabled in the system.
    pub fn allocate(size_bytes: usize, page_size: PageSize) -> io::Result<Self> {
        let uid = BLOCK_UID_GENERATOR.fetch_add(1, Ordering::Relaxed);

        let mut num_pages = size_bytes / page_size as usize;
        if num_pages % page_size as usize != 0 {
            num_pages += 1;
        }

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
    ) -> Result<PageFreePermit, TryEvictError> {
        let permit = self.get_or_reserve_free_permit(target);

        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let guard = self.try_lock_and_check_for_eviction(&permit)?;

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
    ) -> Result<PageFreePermit, TryEvictError> {
        let permit = self.get_or_reserve_free_permit(target);

        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let guard = self.try_lock_and_check_for_eviction(&permit)?;

        // We must always issue a new permit once we have the guard because
        // we may be performing a retry on an operation, which we know logically
        // is valid in terms of order of operations, but new readers may be active
        // so we need to get a new ticket ID.
        let permit = self.issue_new_free_permit(permit.page);

        let state = self.state_at(permit.page);
        state.mark_revertible_eviction_scheduled(&guard, permit.ticket_id);

        Ok(permit)
    }

    /// Attempt to get access to a page for writing and obtain a guard to allow
    /// writing to the page at a later point in time.
    /// 
    /// This call can return a [TryWriteError] in the event the page is already allocated
    /// or the page lock could not be acquired.
    pub fn try_prepare_for_write(
        self: &Arc<Self>,
        page: PageIndex,
    ) -> Result<PageWritePermit, TryWriteError> {
        let state = self.state_at(page);
        let flags = state.flags();

        if flags.is_allocated() && !flags.is_marked_for_eviction() {
            return Err(TryWriteError::AlreadyAllocated);
        }

        let page_lock_guard = state.try_acquire_lock().ok_or(TryWriteError::Locked)?;

        let flags = state.flags();
        if flags.is_allocated() && !flags.is_marked_for_eviction() {
            return Err(TryWriteError::AlreadyAllocated);
        }

        let ticket_id = self.ticket_machine.increment_ticket_id();

        // If the page is allocated but marked for eviction, we can simply
        // revert the eviction and say the page is already allocated.
        if flags.is_allocated() && flags.is_marked_for_eviction() {
            state.mark_allocated(&page_lock_guard, ticket_id);
            return Err(TryWriteError::AlreadyAllocated);
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
    pub fn prepare_read(self: &Arc<Self>, range: Range<PageIndex>) -> PreparedRead {
        assert!(
            range.start.0 < self.state.len()
                && range.end.0 <= self.state.len()
                && range.start <= range.end,
            "invalid page range provided, this is a bug"
        );

        let ticket_guard = self.ticket_machine.get_next_ticket();
        PreparedRead::for_page_range(ticket_guard, self.clone(), range)
    }

    /// Attempts to acquire the page lock and validates that the provided
    /// [PageFreePermit] has not become stale or redundant.
    ///
    /// A [PageFreePermit] is considered stale/redundant if:
    /// - The lock was acquired and the page is already free.
    /// - The lock was not acquired and the page flags contain a ticket ID that is newer than
    ///   the ticket ID contained by the permit.
    fn try_lock_and_check_for_eviction(
        &self,
        permit: &PageFreePermit,
    ) -> Result<PageWriteLockGuard, TryEvictError> {
        let state = self.state_at(permit.page);

        if let Some(guard) = state.try_acquire_lock() {
            let flags = state.flags();
            if flags.is_free() {
                Err(TryEvictError::AlreadyFree)
            } else if flags.is_stale(permit.ticket_id) {
                Err(TryEvictError::OperationStale)
            } else {
                Ok(guard)
            }
        } else {
            let flags = state.flags();

            if flags.is_stale(permit.ticket_id) {
                // The operation has been superseded.
                Err(TryEvictError::OperationStale)
            } else {
                // We do not implement Copy or Clone in order to prevent misuse.
                Err(TryEvictError::PageLocked(EvictRetry(PageFreePermit {
                    uid: permit.uid,
                    ticket_id: permit.ticket_id,
                    page: permit.page,
                })))
            }
        }
    }

    fn state_at(&self, index: PageIndex) -> &state::PageStateEntry {
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
pub enum TryEvictError {
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
pub enum TryWriteError {
    #[error("page locked")]
    /// The page is currently locked by another task.
    Locked,
    #[error("page already allocated")]
    /// The page is already allocated and does not need to be written.
    AlreadyAllocated,
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
