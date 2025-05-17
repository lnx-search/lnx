use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

mod flags;
mod generation;
mod raw;
mod state;

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
    /// A monotonic counter assigning new generation IDs for every operation
    /// performed on the virtual memory block.
    ///
    /// This is used to manage the GC collection of memory when it is not in use.
    generation: AtomicU64,
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
            generation: AtomicU64::new(0),
        })
    }

    /// Attempt to free the target page.
    ///
    /// This call may fail if any one of the following is true:
    ///
    /// - The page has been mutated since meaning the [PageFreePermit] has now expired.
    /// - The page lock is already being held by another task.
    ///
    /// # Safety
    /// The caller must ensure that no readers currently hold a reference to the target page
    /// otherwise this will cause the reads to become UB.
    pub unsafe fn try_free(&self, permit: &PageFreePermit) -> Result<(), TryFreeError> {
        assert_eq!(
            permit.uid, self.uid,
            "uid of permit does not match uid of memory block, this likely means there is a bug",
        );

        let state = self.state_at(permit.page);

        // We first try check the page flags before acquiring the lock,
        // since another operation might have already changed the flags
        // and our permit is now expired, so no point contesting the lock.
        if !generation_is_active(state, permit.generation) {
            return Err(TryFreeError::PermitExpired);
        }

        let Some(guard) = state.try_acquire_lock() else {
            return Err(TryFreeError::Locked);
        };

        // Acquire the lock and check flags again in case they changed. Now we have the lock
        // we can be sure they won't change as long as we hold the guard.
        if !generation_is_active(state, permit.generation) {
            return Err(TryFreeError::PermitExpired);
        }

        // Page is still marked for eviction and tagged with our generation ID, we can free the page.
        // Safety:
        // - The caller is responsible for ensuring no active readers still hold the memory.
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
    /// This call may fail if it cannot acquire the page lock.
    pub fn try_dirty_page(
        &self,
        page: PageIndex,
    ) -> Result<PageFreePermit, PageLockedError> {
        let state = self.state_at(page);
        let Some(guard) = state.try_acquire_lock() else {
            return Err(PageLockedError);
        };
        let permit = self.make_free_permit(page);
        state.mark_dirty(&guard, permit.generation);
        Ok(permit)
    }

    /// Attempt to mark the page for eviction and get back a [PageFreePermit].
    ///
    /// This call may fail if it cannot acquire the page lock.
    pub fn try_mark_for_revertible_eviction(
        &self,
        page: PageIndex,
    ) -> Result<PageFreePermit, PageLockedError> {
        let state = self.state_at(page);
        let Some(guard) = state.try_acquire_lock() else {
            return Err(PageLockedError);
        };
        let permit = self.make_free_permit(page);
        state.mark_revertible_eviction_scheduled(&guard, permit.generation);
        Ok(permit)
    }

    /// Prepare to read the given range of pages.
    ///
    /// This will allow the caller to select pages and write any pages that need to be
    /// allocated before the read is safe.
    pub fn prepare_read(&self, range: Range<PageIndex>) -> () {
        // Maybe issues;
        // - We increment the generation here, so we advance the state machine
        // - Something calls dirty_page..., task tries to immediately act on the permit
        //   because this op hasn't completed yet.
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);

        for page in iter_pages(range) {
            let state = self.state_at(page);
            let flags = state.flags();

            if flags.is_allocated() {}
        }
    }

    fn state_at(&self, index: PageIndex) -> &state::PageStateEntry {
        &self.state[index.0]
    }

    fn make_free_permit(&self, page: PageIndex) -> PageFreePermit {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);
        PageFreePermit {
            uid: self.uid,
            generation,
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
    #[error("{0}")]
    /// An IO error prevented the operation from completing.
    ///
    /// This should be retried.
    Io(io::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("the target page is currently locked")]
/// The system could not acquire the page lock.
pub struct PageLockedError;

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
    generation: u64,
}

fn generation_is_active(
    state: &state::PageStateEntry,
    expected_generation: u64,
) -> bool {
    let flags = state.flags();
    if let Some(active_generation) = flags.extract_generation() {
        active_generation != expected_generation
    } else {
        false
    }
}

// Eventually we can implement the `Step` trait when it is stable.
fn iter_pages(range: Range<PageIndex>) -> impl Iterator<Item = PageIndex> {
    (range.start.0..range.end.0).map(PageIndex)
}
