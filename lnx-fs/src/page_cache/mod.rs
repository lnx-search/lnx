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
mod utils;
mod page;

use std::hash::Hash;
use std::io;
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::mem;
use ahash::RandomState;
use smallvec::SmallVec;
use stable_deref_trait::StableDeref;
use tokio::sync::Notify;

use crate::page_cache::page::{PageState, PageWriteLockGuard};
use self::utils::NoOpRandomState;


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
    file_blocks: dashmap::DashMap<u64, (), NoOpRandomState>,
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
    pub fn prepare_read<K: Hash>(&self, file: K, range: Range<usize>) -> io::Result<PreparedRead> {
                
        todo!()
    }
    
    fn prepare_read_inner(
        &self,
        file_id: u64,
        
    ) {
        todo!()
    }
}


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
    /// The generation assigned to the prepared read.
    generation: Arc<TrackedGeneration>,
    /// The write locks acquired by the read for pages.
    /// 
    /// WARNING:
    /// The lifetimes are not truly 'static, we're cheating here because
    /// we _know_ the guard will live a shorter lifespan than the mutex.
    /// This is to avoid atomic overhead.
    write_locks: SmallVec<[WriteRequest; 4]>,
    /// Any pages which are currently locked, but not by us.
    /// 
    /// We need to wait for these locks to release before we can read the data,
    /// and we need to ensure all these locks were completed successfully. 
    inflight_locks: SmallVec<[*const PageState; 4]>,
    /// The memory read containing the raw pointer, length and ref count
    /// for the entire file block.
    /// 
    /// This acts as a read-only view of the file which can span multiple page.
    page_ref: RawFileMemRef,
}

impl PreparedRead {
    /// The writes that must be completed by the user for this read to be completed.
    /// 
    /// This does _not_ include writes currently in progress by _other_ reads.
    pub fn outstanding_writes(&self) -> &[WriteRequest] {
        &self.write_locks
    }
    
    /// Waits for all pending writes to blank pages to be completed
    /// then returns the now complete [ReadRef].
    pub async fn try_finish(&mut self) -> Result<ReadRef, OutstandingWritesError> {
        let notify_id = self.generation.file_id as usize % PAGE_WRITE_WAKER.len();
        let waker = &PAGE_WRITE_WAKER[notify_id];
        
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
        
        Ok(ReadRef {
            generation: self.generation.clone(),
            page_ref: self.page_ref.clone(),
        })        
    }

    /// Goes through each inflight page lock and checks if it has been completed/released.
    /// 
    /// If the lock has been release but the page was not allocated, it attempts to acquire
    /// the lock and add it to the write locks being controlled by this prepared read.
    fn check_inflight_locks(&mut self) {
        // Check the pages initially to see if they have already been completed.
        for page_state in mem::take(&mut self.inflight_locks) {
            // # Safety
            // The page state will always live as long as this object as it keeps
            // a ref count to the parent file block which owns the pages.
            let page = unsafe { &(*page_state) };

            // The page is still locked for writing.
            if page.is_locked() {
                self.inflight_locks.push(page_state);
                continue
            }

            let flags = page.flags();

            // The page was written correctly, we don't need to do anything else now.
            if flags.is_allocated() {
                continue
            }

            // Attempt to acquire the write guard so we can ensure we re attempt to write the page.
            if let Some(lock_guard) = unsafe { page.try_acquire_write_guard() } {
                // Add the guard to the read's owned lock guards.
                // TODO: self.write_locks.push(lock_guard);
                // Add the inflight locks back
                self.inflight_locks.push(page_state);
            }
        }
    }

    #[inline]
    /// Returns an `Err(OutstandingWritesError)` if there are any write locks currently
    /// still held by the prepared read.
    fn err_if_outstanding_writes(&self) -> Result<(), OutstandingWritesError> {
        if self.write_locks.is_empty() {
            Ok(())
        } else {
            Err(OutstandingWritesError)
        }
    }
}

/// A guarded write handle to a specific page in the file cache.
/// 
/// This request must be "completed"/written to before a [PreparedRead] can
/// be completed (if it contains any write requests.)
/// 
/// The byte range to read from the file is provided by the write request.
pub struct WriteRequest {
    /// The page write lock guard.
    guard: PageWriteLockGuard<'static>,
    /// The range of bytes from the file that need to be read.
    bytes_range: Range<usize>,
    /// The mutable page memory pointer.
    mut_page: MutRawPageRef,
}

impl WriteRequest {
    #[inline]
    /// Returns the bytes range to read from the file.
    pub fn bytes_range(&self) -> Range<usize> {
        self.bytes_range.clone()
    }
    
    /// Copy the bytes from `buffer` to the page.
    /// 
    /// The length of `buffer` must be equal to the page size.
    pub fn write(&mut self, buffer: &[u8]) {
        assert_eq!(buffer.len(), self.mut_page.mem_len, "buffer length does not match page size");
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
    pub unsafe fn write_partial(&mut self, buffer: &[u8]) {
        assert!(buffer.len() <= self.mut_page.mem_len, "buffer length cannot exceed page size");
        
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
    unsafe fn write_unchecked(&mut self, buffer: &[u8]) {
        std::ptr::copy_nonoverlapping(
            buffer.as_ptr(), 
            self.mut_page.mem_ptr, 
            buffer.len(),
        )
    }
}

pub struct ReadRef {
    /// The generation assigned to the read.
    generation: Arc<TrackedGeneration>,
    /// The memory read containing the raw pointer, length and ref count
    /// for the entire file block.
    ///
    /// This acts as a read-only view of the file which can span multiple page.
    page_ref: RawFileMemRef,
}

impl Deref for ReadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // # Safety
        // This ref assumes the pages are all initialised because ReadRef can only
        // be produced by the PreparedRead. 
        // The `generation` ensures no pages are freed before this read is dropped.
        unsafe { self.page_ref.as_slice() }
    }
}

// Safety:
// We _always_ pre-allocate all the virtual memory upfront, once the pointer
// is created we can never move it, hence this is safe to declare as stable.
unsafe impl StableDeref for ReadRef {}
unsafe impl Send for ReadRef {}


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

#[derive(Clone)]
struct RawFileMemRef {
    mem_ptr: *const u8,
    mem_len: usize,
}

impl RawFileMemRef {
    unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.mem_ptr, self.mem_len)
    }
}

/// A mutable pointer to the page data expected to be written.
struct MutRawPageRef {
    mem_ptr: *mut u8,
    mem_len: usize,    
}