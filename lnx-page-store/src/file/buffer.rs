use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::{alloc, ptr, slice};

use super::ALLOC_PAGE_SIZE;
use super::arena::ArenaBuffer;

const MIN_ALIGN: usize = 4096;

/// A memory buffer that meets the minimum required alignment
/// requirements of DMA `O_DIRECT` operations.
pub struct DmaBuffer {
    inner: Alloc,
    len: usize,
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
        Self {
            inner: Alloc::Sys(buffer),
            len: 0,
            capacity: num_pages * ALLOC_PAGE_SIZE,
        }
    }

    /// Alloc a new empty buffer.
    pub fn alloc_empty() -> Self {
        Self {
            inner: Alloc::Empty,
            len: 0,
            capacity: 0,
        }
    }

    /// Creates a new buffer using the provided [ArenaBuffer].
    pub fn from_arena(arena_buffer: ArenaBuffer) -> Self {
        Self {
            capacity: arena_buffer.alloc_size(),
            inner: Alloc::Arena(arena_buffer),
            len: 0,
        }
    }

    fn new(inner: Alloc, capacity: usize) -> Self {
        debug_assert_eq!(capacity % MIN_ALIGN, 0);
        Self {
            inner,
            len: 0,
            capacity,
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
        unsafe { slice::from_raw_parts(ptr, self.len) }
    }
}

impl DerefMut for DmaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let ptr = match &mut self.inner {
            Alloc::Empty => return &mut [],
            Alloc::Sys(buf) => buf.as_mut_ptr(),
            Alloc::Arena(buf) => buf.as_mut_ptr(),
        };
        unsafe { slice::from_raw_parts_mut(ptr, self.len) }
    }
}

enum Alloc {
    Empty,
    Sys(SysBuffer),
    Arena(ArenaBuffer),
}

#[derive(Debug)]
pub(crate) struct SysBuffer {
    pub(self) data: ptr::NonNull<u8>,
    pub(self) layout: Layout,
}

unsafe impl Send for SysBuffer {}
unsafe impl Sync for SysBuffer {}

impl SysBuffer {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, MIN_ALIGN).unwrap();
        let data = unsafe { alloc::alloc(layout) };
        let data = ptr::NonNull::new(data).expect("failed to allocate buffer");
        Self { data, layout }
    }

    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    fn alloc_size(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for SysBuffer {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}
