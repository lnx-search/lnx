use std::alloc::Layout;
use std::any::Any;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::{alloc, ptr, slice};

use super::ALLOC_PAGE_SIZE;
use super::arena::ArenaBuffer;
use super::utils::SingleOrShared;

const MIN_ALIGN: usize = 4096;

/// A memory buffer that meets the minimum required alignment
/// requirements of DMA `O_DIRECT` operations.
pub struct DmaBuffer {
    inner: Alloc,
    capacity: usize,
}

impl DmaBuffer {
    /// Create a new [DmaBuffer] using a new block of memory
    /// taken from the system allocator.
    pub fn alloc_sys(num_pages: usize) -> Self {
        if num_pages == 0 {
            return Self::alloc_empty();
        }
        let buffer = SysBuffer::new(num_pages * ALLOC_PAGE_SIZE);
        Self::new(Alloc::Sys(buffer), num_pages * ALLOC_PAGE_SIZE)
    }

    /// Alloc a new empty buffer.
    pub fn alloc_empty() -> Self {
        Self::new(Alloc::Empty, 0)
    }

    /// Creates a new buffer using the provided [ArenaBuffer].
    pub fn from_arena(arena_buffer: ArenaBuffer) -> Self {
        let capacity = arena_buffer.capacity();
        Self::new(Alloc::Arena(arena_buffer), capacity)
    }

    fn new(inner: Alloc, capacity: usize) -> Self {
        debug_assert_eq!(capacity % MIN_ALIGN, 0);
        Self { inner, capacity }
    }

    /// Produces a guard which can defer the deallocation of
    /// the buffer but does not allow shared interactions with the buffer.
    pub fn share_guard(&mut self) -> Arc<dyn Any + Send + Sync> {
        match &mut self.inner {
            Alloc::Empty => Arc::new(()) as _,
            Alloc::Sys(buf) => buf.share_guard() as _,
            Alloc::Arena(buf) => buf.share_guard() as _,
        }
    }

    /// Returns the capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Deref for DmaBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let ptr = match &self.inner {
            Alloc::Empty => return &[],
            Alloc::Sys(buf) => buf.as_ptr(),
            Alloc::Arena(buf) => buf.as_ptr(),
        };
        unsafe { slice::from_raw_parts(ptr, self.capacity) }
    }
}

impl DerefMut for DmaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let ptr = match &mut self.inner {
            Alloc::Empty => return &mut [],
            Alloc::Sys(buf) => buf.as_mut_ptr(),
            Alloc::Arena(buf) => buf.as_mut_ptr(),
        };
        unsafe { slice::from_raw_parts_mut(ptr, self.capacity) }
    }
}

enum Alloc {
    Empty,
    Sys(SysBuffer),
    Arena(ArenaBuffer),
}

pub(crate) struct SysBuffer {
    pub(self) data: ptr::NonNull<u8>,
    pub(self) size: usize,
    pub(self) guard: SingleOrShared<SysBufferDropGuard>,
}

unsafe impl Send for SysBuffer {}
unsafe impl Sync for SysBuffer {}

impl SysBuffer {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, MIN_ALIGN).unwrap();
        let data = unsafe { alloc::alloc_zeroed(layout) };
        let data = ptr::NonNull::new(data).expect("failed to allocate buffer");
        Self {
            data,
            size: layout.size(),
            guard: SingleOrShared::Single(SysBufferDropGuard { data, layout }),
        }
    }

    fn share_guard(&mut self) -> Arc<SysBufferDropGuard> {
        self.guard.share()
    }

    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    fn alloc_size(&self) -> usize {
        self.size
    }
}

struct SysBufferDropGuard {
    pub(self) data: ptr::NonNull<u8>,
    pub(self) layout: Layout,
}

unsafe impl Send for SysBufferDropGuard {}
unsafe impl Sync for SysBufferDropGuard {}

impl Drop for SysBufferDropGuard {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}
