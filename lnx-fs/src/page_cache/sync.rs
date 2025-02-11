use std::fmt::Debug;
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use stable_deref_trait::StableDeref;

use super::block::{PageId, VirtualFileBlock};

/// A threadsafe, shared file block.
///
/// A file block is a contiguous slice of virtual memory that may be entirely or only
/// partially allocated in physical memory.
pub(crate) struct SyncFileBlock {
    /// The inner state of the block.
    inner: Arc<InnerState>,
}

impl SyncFileBlock {
    /// Creates a new [SyncFileBlock] with a given file ID and [VirtualFileBlock].
    pub(crate) fn new(file_id: u64, file_block: VirtualFileBlock) -> Self {
        Self {
            inner: Arc::new(InnerState {
                file_id,
                mut_state: Mutex::new(MutableInnerState {
                    block: file_block,
                    generation_id: 0,
                }),
                latest_generation: ArcSwap::from_pointee(TrackedGeneration {
                    file_id,
                    generation_id: 0,
                }),
            }),
        }
    }

    /// Writes a new page into the block for the given page ID.
    ///
    /// Errors if the page is already allocated or if the buffer length is not
    /// correctly aligned to the page size.
    pub(crate) fn write_page(
        &self,
        page_idx: PageId,
        buffer: &[u8],
    ) -> io::Result<WeakReadRef> {
        self.inner.write_page(page_idx, buffer)
    }

    /// Truly frees the memory of a given page.
    ///
    /// # Safety   
    ///    
    /// This is unsafe because it does _not_ check if any reads are currently
    /// accessing this page or not. If is the case, then this all reads for that page become UB.
    pub(crate) unsafe fn free_page(&self, page_idx: PageId) -> io::Result<()> {
        self.inner.free_page(page_idx)
    }
}

/// The mutable state of the file block.
///
/// Access to this state is guaranteed to be serialized.
struct MutableInnerState {
    block: VirtualFileBlock,
    generation_id: u64,
}

struct InnerState {
    /// The unique assigned ID of the file.
    file_id: u64,
    /// The inner mutable state.
    mut_state: Mutex<MutableInnerState>,
    /// The monotonic ID counter for generation IDs.
    latest_generation: ArcSwap<TrackedGeneration>,
}

impl InnerState {
    /// Write a page of memory to the block.
    ///
    /// Errors if the block is not aligned to the page size, or if page is already allocated.
    fn write_page(
        self: &Arc<InnerState>,
        page_idx: PageId,
        buffer: &[u8],
    ) -> io::Result<WeakReadRef> {
        let mut lock = self.mut_state.lock();

        // Increment the generation ID.
        lock.generation_id += 1;
        let generation_id = lock.generation_id;

        let mem = lock.block.write_page(page_idx, buffer)?;
        let mem_ptr = mem.as_ptr();
        let mem_len = mem.len();

        drop(lock);

        // Make the new generation visible to readers.
        let generation = TrackedGeneration {
            file_id: self.file_id,
            generation_id,
        };
        self.latest_generation.store(Arc::new(generation));

        Ok(WeakReadRef {
            slice: MemSlicePtr {
                ptr: mem_ptr,
                len: mem_len,
            },
            parent: Arc::clone(self),
        })
    }

    unsafe fn free_page(&self, page_idx: PageId) -> io::Result<()> {
        let mut lock = self.mut_state.lock();
        lock.block.free_page(page_idx)?;
        Ok(())
    }
}

/// A specialized weak reference to a read-only page.
///
/// Unlike the [ReadRef] this does _not_ count towards the generation
/// reference count.
///
/// To explain further, the core of the memory reclamation model is that a page can
/// be safely freed once _all_ generations between when the page was _written_ and when it was
/// _marked for freeing_ have a zero ref count, this is because we know any operation before
/// or after this range cannot possibly see the data, but generations _within_ that range _might_
/// be reading that page.
///
/// The issue is if we do not have short-lived reads, we can never free the pages quickly, because
/// the long-lived reads will keep the ref count alive. Specifically, the `page_table` where
/// available pages are kept, will always live for a long time, in-fact, if a page is used often
/// it might live for as long as the program itself does. These causes use a _lot_ of issues
/// if the table held a strong reference in this table because if we allocated pages `[1, 2, 3]`
/// and store the strong reference in our page table, then free page `2`, until pages `1` and `3`
/// are _also_ freed, page `2` will never be dropped because out generation IDs look like:
///
/// - `GEN-1` -> write page 1
/// - `GEN-2` -> write page 2
/// - `GEN-3` -> write page 3
/// - `GEN-4` -> mark page 2
///     *  _GEN-1 to GEN-3 must all have zero ref count to be safe to free._
///
pub(crate) struct WeakReadRef {
    /// The read only slice pointing to the page memory.
    slice: MemSlicePtr,
    /// The parent object holding the true ownership of the page data.
    parent: Arc<InnerState>,
}

impl WeakReadRef {
    /// Upgrades the [WeakReadRef] into a [ReadRef] using the latest
    /// visible generation.
    /// 
    /// # Safety
    /// 
    /// The called must guarantee that the page `self` occupies is not marked to be freed,
    /// otherwise this can cause immediate UB when the page is freed and a [ReadRef] still
    /// attempts to read the data.
    /// 
    /// The reason why is that the GC uses the generation counter to track what reads can
    /// _possibly_ have access to the page, and will defer freeing the page until it knows
    /// _under normal circumstances_ that can see the page about to be freed.
    /// 
    /// This method can void that assumption though, hence the possible UB. This _won't_ cause
    /// a use-after-free because the pointer will remain alive and valid, but that data at the
    /// pointer location makes no guarantees about the contents once a page has been freed. 
    pub(crate) unsafe fn upgrade(&self) -> ReadRef {
        let generation = self.parent.latest_generation.load_full();
        ReadRef {
            slice: self.slice,
            parent: self.parent.clone(),
            generation
        }
    }
}

/// A strong reference to page(s) data.
pub(crate) struct ReadRef {
    /// The read only slice pointing to the page memory.
    slice: MemSlicePtr,
    /// The parent object holding the true ownership of the page data.
    parent: Arc<InnerState>,
    /// The shared generation pointer of the read.
    generation: Arc<TrackedGeneration>,
}

impl Clone for ReadRef {
    fn clone(&self) -> Self {
        Self {
            slice: self.slice,
            parent: self.parent.clone(),
            generation: self.generation.clone(),
        }
    }
}

impl Deref for ReadRef {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        // Safety:
        // As long as `parent` and `generation` live,
        unsafe { self.slice.as_slice() }
    }
}

// Safety:
// We _always_ pre-allocate all the virtual memory upfront, once the pointer
// is created we can never move it, hence this is safe to declare as stable.
unsafe impl StableDeref for ReadRef {}
unsafe impl Send for ReadRef {}

#[derive(Copy, Clone)]
struct MemSlicePtr {
    ptr: *const u8,
    len: usize,
}

impl MemSlicePtr {
    unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.ptr, self.len)
    }
}

/// A generation with a unique monotonic ID.
///
/// When this object is dropped it triggers a GC notification.
pub struct TrackedGeneration {
    file_id: u64,
    generation_id: u64,
}

impl Drop for TrackedGeneration {
    fn drop(&mut self) {
        crate::gc::mark_dead_generation(self.file_id, self.generation_id);
    }
}
