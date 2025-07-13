use std::ptr;
use std::sync::Arc;

use offset_allocator::{Allocation, Allocator};
use parking_lot::Mutex;

use super::ALLOC_PAGE_SIZE;
use super::utils::SingleOrShared;

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
    fn alloc(
        &mut self,
        num_pages: usize,
    ) -> Option<(Allocation, ptr::NonNull<u8>, usize)> {
        let allocation = self.allocator.allocate(num_pages as u32)?;
        let ptr = unsafe {
            let ptr = self
                .mem
                .as_mut_ptr()
                .add((allocation.offset as usize) * ALLOC_PAGE_SIZE);
            ptr::NonNull::new_unchecked(ptr)
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
    ptr: ptr::NonNull<u8>,
    len: usize,
}

impl ArenaBuffer {
    /// Puts the allocation guard into an Arc and returns
    /// a copy. This can be used to ensure the allocation
    /// lives longer than this buffer which is useful
    /// in situations like io_uring.
    pub(super) fn share_guard(&mut self) -> Arc<AllocationGuard> {
        self.guard.share()
    }

    /// Returns mutable buffer pointer.
    pub(super) fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns buffer pointer.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns the size of the allocation.
    pub(super) fn capacity(&self) -> usize {
        self.len
    }
}

unsafe impl Send for ArenaBuffer {}
unsafe impl Sync for ArenaBuffer {}

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

#[cfg(test)]
mod tests {
    use std::alloc::alloc;

    use super::*;

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::many_pages(6)]
    fn test_arena_creation(#[case] num_pages: usize) {
        let arena = ArenaAllocator::new(num_pages);
        assert_eq!(arena.mem_size(), 4096 * num_pages);
    }

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::many_pages(6)]
    #[case::all_pages(10)]
    #[should_panic]
    #[case::too_many_pages(11)]
    fn test_single_arena_alloc(#[case] num_pages: usize) {
        let arena = ArenaAllocator::new(10);
        let _alloc = arena.alloc(num_pages).expect("Failed to allocate memory");
    }

    #[test]
    fn test_many_arena_alloc() {
        let arena = ArenaAllocator::new(10);

        let alloc1 = arena.alloc(2).expect("Failed to allocate memory");
        assert_eq!(alloc1.capacity(), 4096 * 2);
        let alloc2 = arena.alloc(8).expect("Failed to allocate memory");
        assert_eq!(alloc2.capacity(), 4096 * 8);

        drop(alloc2);

        let _alloc3 = arena.alloc(2).expect("Failed to allocate memory");
        let _alloc4 = arena.alloc(2).expect("Failed to allocate memory");

        let result = arena.alloc(5);
        assert!(result.is_none());

        let _alloc5 = arena.alloc(4).expect("Failed to allocate memory");
        drop(alloc1);

        let _alloc6 = arena.alloc(2).expect("Failed to allocate memory");
    }

    #[test]
    fn test_allocation_ptr_behaviour() {
        let arena = ArenaAllocator::new(10);

        let alloc1 = arena.alloc(2).unwrap();
        assert!(ptr::addr_eq(arena.mem_ptr(), alloc1.as_mut_ptr()));

        let alloc2 = arena.alloc(2).unwrap();

        let start_ptr = arena.mem_ptr();
        unsafe { assert!(ptr::addr_eq(start_ptr.add(4096 * 2), alloc2.as_ptr())) }
    }

    #[test]
    fn test_defer_free() {
        let arena = ArenaAllocator::new(10);

        let mut alloc1 = arena.alloc(10).unwrap();
        let guard = alloc1.share_guard();

        let result = arena.alloc(1);
        assert!(result.is_none());

        drop(alloc1);
        drop(guard);

        let result = arena.alloc(1);
        assert!(result.is_some());
    }
}
