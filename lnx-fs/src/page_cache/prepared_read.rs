use std::hash::{Hash, Hasher};
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{io, mem};

use smallvec::SmallVec;
use stable_deref_trait::StableDeref;
use tokio::sync::Notify;

use super::block::{MutPageRef, PageId, VirtualFileBlock};
use super::page::{PageState, PageWriteLockGuard};
use super::TrackedGeneration;
use crate::config::PageSize;

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

#[derive(Debug, thiserror::Error)]
#[error("prepared read still has outstanding write requests")]
/// An error that occurred while attempting to complete the [PreparedRead]
/// operation.
///
/// The read still needs some pages to be written before the read can be
/// safely completed. You need to call `outstanding_writes()` again and
/// complete them.
pub struct OutstandingWritesError;

/// A read of the file cache which may or may not have all the pages required already cached.
///
/// This can contain multiple pages of the file and represents the read as a contiguous slice,
/// but cannot have the memory read until the pages that need to be read from disk have been fetched
/// and written to the memory pages held by this read.
///
/// Once all the prerequisites are complete, you can call `finish` to retrieve the read reference
/// to the memory pages which is a cheap to clone access to the memory data.
///
///
pub struct PreparedRead {
    /// The operation guard.
    op_guard: OpGuards,
    /// The write locks acquired by the read for pages.
    ///
    /// WARNING:
    /// The lifetimes are not truly 'static, we're cheating here because
    /// we _know_ the guard will live a shorter lifespan than the mutex.
    /// This is to avoid atomic overhead.
    write_requests: SmallVec<[WriteRequest; 4]>,
    /// Any pages which are currently locked, but not by us.
    ///
    /// We need to wait for these locks to release before we can read the data,
    /// and we need to ensure all these locks were completed successfully.
    inflight_locks: SmallVec<[*const PageState; 4]>,
    /// Returns the pages read.
    page_range: Range<PageId>,
    /// The number of bytes to skip from the _first_ page of the memory.
    page_relative_offset: usize,
    /// The total length of the read which is added to the `raw_mem_ptr`
    /// in order to get the end pointer.
    total_read_len: usize,
    /// The raw pointer located at the start of the page attempting to be
    /// read by this operation.
    ///
    /// Accessing the memory behind this pointer is UB until all pages are
    /// guaranteed to be allocated and contiguous.
    raw_mem_ptr: *const u8,
}

impl PreparedRead {
    pub(super) fn from_block_and_generation(
        file_block: Arc<VirtualFileBlock>,
        bytes_range: Range<usize>,
        generation: TrackedGeneration,
    ) -> io::Result<Self> {
        let waker = get_waker(generation.file_id);

        let mut write_requests = SmallVec::new();
        let mut inflight_locks = SmallVec::new();

        let page_size = file_block.page_size();
        let page_range = get_page_range(bytes_range, page_size);

        let page_relative_offset = page_range.start % page_size.num_bytes();
        let total_read_len = page_range.len();

        let op_guard = OpGuards {
            generation: Arc::new(generation),
            block: file_block.clone(),
        };

        let raw_mem_ptr = unsafe { file_block.get_page_ptr(page_range.start) };

        for page_id in page_range.clone() {
            let state = file_block.page_at(page_id);
            let flags = state.flags();

            // The page is allocated, it was previously marked to be freed.
            if flags.is_allocated() && flags.is_to_be_freed() {
                unsafe {
                    if let Some(_guard) = state.try_acquire_write_guard() {
                        // Load the flags again just to make sure now we have the guard.
                        let flags = state.flags();

                        // Page is still allocated, we can unset the flag without re-reading.
                        if flags.is_allocated() {
                            // Reset the page marker so it is no longer marked to be freed,
                            // we can do this because the allocation is still valid.
                            //
                            // # Safety
                            // We hold the write lock for the page and can safely update the flag.
                            state.set_allocated_unchecked();
                        }
                    }
                }
            } else if flags.is_allocated() {
                continue;
            }

            // Page has been deallocated, we need to read from disk.
            unsafe {
                // No active write is in flight if we can acquire the lock.
                if let Some(guard) = state.try_acquire_write_guard() {
                    // # Safety
                    // We hold the page lock for the target `page_id`, which means we are allowed
                    // to acquire a mutable reference to the page.
                    let mut_page = file_block.get_mut_page(page_id);

                    let write_request = WriteRequest {
                        op_guard: op_guard.clone(),
                        lock_guard: guard,
                        waker,
                        target_page: page_id,
                        page_size: file_block.page_size(),
                        mut_page,
                    };

                    write_requests.push(write_request);
                } else {
                    // Write already in process, we just need to wait until the lock gets released.
                    inflight_locks.push(state as *const PageState);
                }
            }
        }

        Ok(Self {
            op_guard,
            write_requests,
            inflight_locks,
            page_range,
            page_relative_offset,
            total_read_len,
            raw_mem_ptr,
        })
    }

    #[inline]
    /// The ID of the file this operation is mutating.
    pub fn file_id(&self) -> u64 {
        self.op_guard.generation.file_id
    }

    #[inline]
    /// The generation ID tied to this operation.
    pub fn generation_id(&self) -> u64 {
        self.op_guard.generation.generation_id
    }

    /// Returns the next write request that must be completed before the read is valid.
    ///
    /// TODO: This API _sucks_ I mean, really sucks.
    pub fn next_outstanding_write(&mut self) -> Option<WriteRequest> {
        self.write_requests.pop()
    }

    #[inline]
    /// Returns the pages read.
    pub fn page_range(&self) -> Range<usize> {
        self.page_range.clone()
    }

    /// Waits for all pending writes to blank pages to be completed
    /// then returns the now complete [ReadRef].
    pub async fn try_finish(&mut self) -> Result<ReadRef, OutstandingWritesError> {
        let waker = get_waker(self.op_guard.generation.file_id);

        let waker_future = waker.notified();
        tokio::pin!(waker_future);

        // Wait until all writes are completed or until we have other writes
        // the reader needs to process.
        while !self.inflight_locks.is_empty() {
            waker_future.as_mut().enable();

            self.check_inflight_locks();
            self.err_if_outstanding_writes()?;

            waker_future.as_mut().await;
            waker_future.set(waker.notified());
        }

        // # Safety
        // We know the offset will be valid because the offset is calculated by aligning to page
        // boundaries when the prepared read is created.
        let mem_ptr_with_offset =
            unsafe { self.raw_mem_ptr.add(self.page_relative_offset) };

        Ok(ReadRef {
            _op_guard: self.op_guard.clone(),
            mem_ptr: mem_ptr_with_offset,
            mem_len: self.total_read_len,
        })
    }

    /// Goes through each inflight page lock and checks if it has been completed/released.
    ///
    /// If the lock has been release but the page was not allocated, it attempts to acquire
    /// the lock and add it to the write locks being controlled by this prepared read.
    fn check_inflight_locks(&mut self) {
        let waker = get_waker(self.op_guard.generation.file_id);

        // Check the pages initially to see if they have already been completed.
        for page_state in mem::take(&mut self.inflight_locks) {
            // # Safety
            // The page state will always live as long as this object as it keeps
            // a ref count to the parent file block which owns the pages.
            let page = unsafe { &(*page_state) };

            // The page is still locked for writing.
            if page.is_locked() {
                self.inflight_locks.push(page_state);
                continue;
            }

            let flags = page.flags();

            // The page was written correctly, we don't need to do anything else now.
            if flags.is_allocated() {
                continue;
            }

            // Attempt to acquire the write guard so we can ensure we re attempt to write the page.
            if let Some(lock_guard) = unsafe { page.try_acquire_write_guard() } {
                let file_block = self.file_block();

                let page_id = unsafe { file_block.pointer_to_page_id(page_state) };
                let mut_page = unsafe { file_block.get_mut_page(page_id) };

                let request = WriteRequest {
                    op_guard: self.op_guard.clone(),
                    lock_guard,
                    waker,
                    target_page: page_id,
                    page_size: file_block.page_size(),
                    mut_page,
                };

                // Add the guard to the read's owned lock guards.
                self.write_requests.push(request);
                // Add the inflight locks back
                self.inflight_locks.push(page_state);
            }
        }
    }

    #[inline]
    /// Returns an `Err(OutstandingWritesError)` if there are any write locks currently
    /// still held by the prepared read.
    fn err_if_outstanding_writes(&self) -> Result<(), OutstandingWritesError> {
        if self.write_requests.is_empty() {
            Ok(())
        } else {
            Err(OutstandingWritesError)
        }
    }

    fn file_block(&self) -> &VirtualFileBlock {
        self.op_guard.block.as_ref()
    }
}

/// A guarded write handle to a specific page in the file cache.
///
/// This request must be "completed"/written to before a [PreparedRead] can
/// be completed (if it contains any write requests.)
///
/// The byte range to read from the file is provided by the write request.
pub struct WriteRequest {
    /// The operation guard.
    op_guard: OpGuards,
    /// The page write lock guard.
    lock_guard: PageWriteLockGuard<'static>,
    /// The write waker for the file.
    waker: &'static Notify,
    /// The page being targeted.
    target_page: usize,
    /// The page size to be read.
    page_size: PageSize,
    /// The mutable page memory pointer.
    mut_page: MutPageRef,
}

impl WriteRequest {
    #[inline]
    /// Returns the target bytes range to read from the file.
    pub fn bytes_range(&self) -> Range<usize> {
        let start = self.target_page * self.page_size.num_bytes();
        let end = start + self.page_size.num_bytes();
        start..end
    }

    /// Copy the bytes from `buffer` to the page.
    ///
    /// The length of `buffer` must be equal to the page size
    pub fn write(self, buffer: &[u8]) {
        assert_eq!(
            buffer.len(),
            self.mut_page.size(),
            "buffer length does not match page size"
        );

        // # Safety
        // The buffer is checked above to ensure it matches the page sizer.
        unsafe { self.write_unchecked(buffer) }
    }

    /// Copy the bytes from `buffer` to the page.
    ///
    /// Unlike `write`, the buffer length can be _less than_ the length of the page.
    ///
    /// # Safety
    ///
    /// You _must_ ensure the bytes not written to the page cannot be accessed by reads as
    /// these are technically uninitialized bytes from the perspective of the reader.
    pub unsafe fn write_partial(self, buffer: &[u8]) {
        assert!(
            buffer.len() <= self.mut_page.size(),
            "buffer length cannot exceed page size"
        );

        // # Safety
        // The buffer is checked above to ensure it does not exceed the page size,
        // protection of the uninitialized bytes are the responsibility of the caller.
        self.write_unchecked(buffer)
    }

    /// Copy the bytes from `buffer` into the page without any checks.
    ///
    /// # Safety
    ///
    /// The buffer must not be longer than the size of the page, and great care
    /// must be taken to ensure if the buffer is _less than_ the page size, that
    /// no reads can access the uninitialized part of the page.
    unsafe fn write_unchecked(mut self, buffer: &[u8]) {
        self.mut_page.write(buffer);
        self.waker.notify_waiters();
    }
}

#[derive(Clone)]
/// A cheap to clone reference to a set of cached memory.
pub struct ReadRef {
    /// The operation guard.
    _op_guard: OpGuards,
    /// The memory read containing the raw pointer.
    ///
    /// This acts as a read-only view of the file which can span multiple page.
    mem_ptr: *const u8,
    /// The length of the read reference.
    mem_len: usize,
}

impl Deref for ReadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // # Safety
        // This ref assumes the pages are all initialised because ReadRef can only
        // be produced by the PreparedRead.
        // The `generation` ensures no pages are freed before this read is dropped.
        unsafe { std::slice::from_raw_parts(self.mem_ptr, self.mem_len) }
    }
}

// Safety:
// We _always_ pre-allocate all the virtual memory upfront, once the pointer
// is created we can never move it, hence this is safe to declare as stable.
unsafe impl StableDeref for ReadRef {}
unsafe impl Send for ReadRef {}

#[derive(Clone)]
/// The guards that prevent a page and file block being dropped
/// before it is safe to do so.
struct OpGuards {
    generation: Arc<TrackedGeneration>,
    block: Arc<VirtualFileBlock>,
}

fn get_waker(file_id: u64) -> &'static Notify {
    let notify_id = file_id as usize % PAGE_WRITE_WAKER.len();
    &PAGE_WRITE_WAKER[notify_id]
}

fn get_page_range(range: Range<usize>, page_size: PageSize) -> Range<PageId> {
    let page_start = range.start / page_size.num_bytes();
    let mut page_end = range.end / page_size.num_bytes();
    if range.end % page_size.num_bytes() != 0 {
        page_end += 1;
    }
    page_start..page_end
}
