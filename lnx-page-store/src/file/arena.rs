use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use offset_allocator::{Allocation, Allocator};
use parking_lot::Mutex;

use super::ALLOC_PAGE_SIZE;

#[derive(Clone)]
/// The arena allocator produces sets of pages for use in reading and writing of
/// temporary buffers.
///
/// All pages are aligned to the page boundary (normally 4KB) which meets the
/// requirement for O_DIRECT reads and writes.
///
/// This arena can be cheaply cloned, it is guarded by a lock internally.
pub struct ArenaAllocator {
    allocator: Arc<Mutex<Fragment>>,
    size: usize,
}

impl ArenaAllocator {
    /// Creates a new [ArenaAllocator] with capacity for a given `num_pages` with [ALLOC_PAGE_SIZE]
    /// number of bytes each.
    pub fn new(num_pages: usize) -> Self {
        let mem = memmap2::MmapOptions::default()
            .len(num_pages * ALLOC_PAGE_SIZE)
            .map_anon()
            .expect("Failed to allocate memory");

        let fragment = Fragment {
            allocator: Allocator::new(num_pages as u32),
            mem,
        };

        Self {
            allocator: Arc::new(Mutex::new(fragment)),
            size: num_pages * ALLOC_PAGE_SIZE,
        }
    }

    /// Try to allocate `num_pages` of memory and get back a reference to the allocated
    /// slice, or return `None` if there is not enough free memory available to allocate
    /// a contiguous slice.
    pub fn alloc(&self, num_pages: usize) -> Option<ArenaBuffer> {
        let mut lock = self.allocator.lock();
        let (allocation, ptr, len) = lock.alloc(num_pages)?;
        Some(ArenaBuffer {
            guard: SingleOrShared::Single(AllocationGuard {
                allocator: self.allocator.clone(),
                allocation,
            }),
            ptr,
            len,
        })
    }

    /// Returns the pointer to the memory block the arena allocates on.
    pub fn mem_ptr(&self) -> *mut u8 {
        let mut lock = self.allocator.lock();
        lock.mem.as_mut_ptr()
    }

    /// Returns the total size of the memory block the arena allocates on.
    pub fn mem_size(&self) -> usize {
        self.size
    }
}

struct Fragment {
    allocator: Allocator,
    mem: memmap2::MmapMut,
}

impl Fragment {
    fn alloc(&mut self, num_pages: usize) -> Option<(Allocation, *mut u8, usize)> {
        let allocation = self.allocator.allocate(num_pages as u32)?;
        let ptr = unsafe {
            self.mem
                .as_mut_ptr()
                .add((allocation.offset as usize) * ALLOC_PAGE_SIZE)
        };
        Some((allocation, ptr, num_pages * ALLOC_PAGE_SIZE))
    }

    fn free(&mut self, allocation: Allocation) {
        self.allocator.free(allocation);
    }
}

/// An allocated buffer of N KB aligned to the page boundary (normally 4KB)
///
/// The buffer is returned to the arena once dropped.
///
/// It is not recommended to hold onto this buffer for large periods of time or
/// leak this buffer as it can severely impact performance.
pub struct ArenaBuffer {
    guard: SingleOrShared<AllocationGuard>,
    ptr: *mut u8,
    len: usize,
}

impl ArenaBuffer {
    /// Puts the allocation guard into an Arc and returns
    /// a copy. This can be used to ensure the allocation
    /// lives longer than this buffer which is useful
    /// in situations like io_uring.
    pub fn share_guard(&mut self) -> Arc<AllocationGuard> {
        let guard = mem::replace(&mut self.guard, SingleOrShared::None);
        match guard {
            SingleOrShared::None => unreachable!("variant should never bit hit"),
            SingleOrShared::Single(single) => {
                let shared = Arc::new(single);
                self.guard = SingleOrShared::Shared(shared.clone());
                shared
            },
            SingleOrShared::Shared(shared) => {
                self.guard = SingleOrShared::Shared(shared.clone());
                shared
            },
        }
    }

    /// Returns the buffer as an immutable slice.
    pub fn as_slice(&self) -> &[u8] {
        self
    }

    /// Returns the buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut *self
    }
}

unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

impl Debug for ArenaBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let slice: &[u8] = self.as_ref();
        write!(f, "{slice:?}")
    }
}

impl AsRef<[u8]> for ArenaBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Deref for ArenaBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for ArenaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// The allocation guard holds the lifetime of the allocation,
/// once dropped it will return the memory back to the arena.
pub struct AllocationGuard {
    allocator: Arc<Mutex<Fragment>>,
    allocation: Allocation,
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        let mut fragment = self.allocator.lock();
        fragment.free(self.allocation);
    }
}

enum SingleOrShared<T> {
    None,
    Single(T),
    Shared(Arc<T>),
}
