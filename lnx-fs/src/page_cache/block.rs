use std::io;
use std::io::ErrorKind;
use std::ops::Range;

use memmap2::UncheckedAdvice;
use tracing::{debug, info, warn};

/// The threshold the file size must be for us to explicitly
/// use huge pages for the allocation.
///
/// NOTE:
/// Just because a file is bellow this threshold does not mean huge pages
/// won't be used, in fact, it is most likely that the linux kernel will use
/// transparent huge pages for the smaller blocks if there is a valid reason to.
///
/// We explicitly use huge pages beyond this threshold to get more consistent behaviour
/// and minimise the amount of pages the kernel needs to scan when looking to make
/// transparent huge pages.
const HUGE_PAGE_THRESHOLD: usize = 2 << 30;
const MAP_HUGE_2MB: u8 = 21;

const PAGE_SIZE_2MB: usize = 2 << 20;
const PAGE_SIZE_8KB: usize = 8 << 10;


/// A mutable raw file block.
pub(super) struct FileBlock {
    /// The allocated anonymous memory block of the file.
    ///
    /// Most of this is likely zeroed memory.
    mem: memmap2::MmapMut,
    /// A bitvec indicating what pages of the memory are currently written to
    /// and allocated. Pages not allocated will be zeroed.
    allocated_pages: bitvec::vec::BitVec,
    /// The size of the pages being allocated, this is used
    /// to calculate the actual memory usage of the block.
    ///
    /// This is used because huge pages might consume more memory
    /// than we have currently written to, and if we are able to
    /// we can re-use this memory, but we want to avoid using more
    /// than the configured max while being aware of this behaviour
    /// to prevent swapping.
    page_size: usize,
    /// The total number of pages allocated.
    pages_allocated: usize,
}

impl FileBlock {
    pub(super) fn allocate(size: usize) -> io::Result<Self> {
        use bitvec::prelude::*;


        let mut page_size = PAGE_SIZE_8KB;
        let mem = if size >= HUGE_PAGE_THRESHOLD {
            page_size = PAGE_SIZE_2MB;
            try_open_huge_mmap_else_default(size)?
        } else {
            let mut options = memmap2::MmapOptions::new();
            options.len(size);
            options.map_anon()?
        };

        let num_pages = get_num_pages(size, page_size);
        let allocated_pages = bitvec![usize, Lsb0; 0; num_pages];

        Ok(Self {
            mem,
            allocated_pages,
            page_size,
            pages_allocated: 0,
        })
    }

    #[inline]
    /// Returns the amount of bytes allocated.
    ///
    /// This is the number of allocated pages multiplied by
    /// the page size.
    pub fn memory_usage(&self) -> usize {
        self.pages_allocated * self.page_size
    }

    #[inline]
    /// Returns the amount of virtual address space allocated.
    pub fn virtual_address_space_usage(&self) -> usize {
        self.allocated_pages.len() * self.page_size
    }
    
    /// Copies a given slice into memory.
    ///
    /// # Safety
    ///
    /// This routine assumes the buffer, written at `pos` will not go out of bounds
    /// of the memory block. If this turns out to _not_ be true, the function will panic
    /// and the internal state will be left in an undefined state making any reads or writes
    /// to the block afterward undefined behaviour.
    pub unsafe fn write_at(&mut self, pos: usize, buffer: &[u8]) {
        self.mem[pos..pos+buffer.len()].copy_from_slice(buffer);
        
        for idx in self.get_alloc_page_range(pos, pos + buffer.len()) {
            self.mark_page_allocated(idx);
        }
    }
    
    /// Frees the pages within the provided range.
    /// 
    /// # Safety
    /// 
    /// This routine is unsafe because the call to mmadvise is unsafe,
    /// once a page has been freed it become undefined behaviour to read
    /// the data until a new write has replaced the content in the pages.
    pub unsafe fn free(&mut self, start: usize, end: usize) -> io::Result<()> {
        let (offset, len) = self.get_free_page_range(start, end);
        self.mem.unchecked_advise_range(
            UncheckedAdvice::Free,
            offset,
            len,
        )
    }

    fn mark_page_allocated(&mut self, page_idx: usize) {
        let old = self.allocated_pages.replace(page_idx, true);
        self.pages_allocated += !old as usize;
    }

    fn get_alloc_page_range(&self, start: usize, end: usize) -> Range<usize> {
        let page_idx_start = start / self.page_size;
        let mut page_idx_end = end / self.page_size;
        if (end % self.page_size) != 0 {
            page_idx_end += 1;
        }        
        page_idx_start..page_idx_end
    }
    
    fn get_free_page_range(&self, start: usize, end: usize) -> (usize, usize) {
        // Unlike get_alloc_page_range, we only include pages that are completely
        // within the range bounds.
        let mut page_idx_start = start / self.page_size;
        if (start % self.page_size) != 0 {
            page_idx_start += 1;
        }

        (page_idx_start * self.page_size, end - start)
    }
}

fn try_open_huge_mmap_else_default(size: usize) -> io::Result<memmap2::MmapMut> {
    let mut options = memmap2::MmapOptions::new();
    options.len(align_up_to_2mb(size));
    options.huge(Some(MAP_HUGE_2MB));
    match options.map_anon() {
        Err(e) if e.kind() == ErrorKind::OutOfMemory  => {
            debug!("System attempted to use huge page table but not enough huge pages were available");
        },
        other => return other,
    };

    debug!("Falling back to standard page sizes for mmap");
    let mut options = memmap2::MmapOptions::new();
    options.len(size);
    options.map_anon()        
}

/// Aligns the value _up_ to the nearest 2MB huge page size
fn align_up_to_2mb(value: usize) -> usize {
    let remainder = value % PAGE_SIZE_2MB;
    if remainder == 0 {
        value
    } else {
        value + PAGE_SIZE_2MB - remainder
    }
}

fn get_num_pages(size: usize, page_size: usize) -> usize {
    let mut num_pages = size / page_size;
    if size % page_size != 0 {
        num_pages += 1;
    }
    num_pages
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_align_up_2mb() {
        assert_eq!(align_up_to_2mb(0), 0);
        assert_eq!(align_up_to_2mb(2), PAGE_SIZE_2MB);
        assert_eq!(align_up_to_2mb(PAGE_SIZE_2MB), PAGE_SIZE_2MB);
        assert_eq!(align_up_to_2mb(PAGE_SIZE_2MB + 1), 2 * PAGE_SIZE_2MB);        
    }
    
    #[test]
    fn test_num_pages() {
        assert_eq!(get_num_pages(0, PAGE_SIZE_8KB), 0);
        assert_eq!(get_num_pages(1, PAGE_SIZE_8KB), 1);
        assert_eq!(get_num_pages(16 << 10, PAGE_SIZE_8KB), 2);
        assert_eq!(get_num_pages((16 << 10) + 5, PAGE_SIZE_8KB), 3);

        assert_eq!(get_num_pages(0, PAGE_SIZE_2MB), 0);
        assert_eq!(get_num_pages(1, PAGE_SIZE_2MB), 1);
        assert_eq!(get_num_pages(16 << 20, PAGE_SIZE_2MB), 8);
        assert_eq!(get_num_pages((16 << 20) + 5, PAGE_SIZE_2MB), 9);
    }
    
    #[test]
    fn test_block_allocate() {
        let block = FileBlock::allocate(4 << 30)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 4 << 30);
    }
}