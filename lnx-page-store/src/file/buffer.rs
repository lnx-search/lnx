use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::{alloc, ptr};

use super::ALLOC_PAGE_SIZE;
use super::arena::ArenaBuffer;

/// A memory buffer that meets the minimum required alignment
/// requirements of DMA `O_DIRECT` operations.
pub struct DmaBuffer {
    inner: Alloc,
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
        }
    }

    /// Alloc a new empty buffer.
    pub fn alloc_empty() -> Self {
        Self {
            inner: Alloc::Empty,
        }
    }

    /// Creates a new buffer using the provided [ArenaBuffer].
    pub fn from_arena(arena_buffer: ArenaBuffer) -> Self {
        Self {
            inner: Alloc::Arena(arena_buffer),
        }
    }
}

impl Deref for DmaBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            Alloc::Empty => &[],
            Alloc::Sys(alloc) => alloc.as_slice(),
            Alloc::Arena(alloc) => alloc.as_slice(),
        }
    }
}

impl DerefMut for DmaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            Alloc::Empty => &mut [],
            Alloc::Sys(alloc) => alloc.as_mut_slice(),
            Alloc::Arena(alloc) => alloc.as_mut_slice(),
        }
    }
}

enum Alloc {
    Empty,
    Sys(SysBuffer),
    Arena(ArenaBuffer),
}

#[derive(Debug)]
pub(crate) struct SysBuffer {
    data: ptr::NonNull<u8>,
    layout: Layout,
}

impl SysBuffer {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let data = unsafe { alloc::alloc(layout) };
        let data = ptr::NonNull::new(data).expect("failed to allocate buffer");
        Self { data, layout }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.layout.size()) }
    }
}

impl Drop for SysBuffer {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}
