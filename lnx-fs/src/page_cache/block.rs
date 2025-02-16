use std::io::ErrorKind;
use std::ops::{Index, Range};
use std::{cmp, io};

use memmap2::UncheckedAdvice;

use crate::config::PageSize;
use crate::page_cache::page::{PageState, PageStateTable};

pub type PageId = usize;

/// A mutable raw file block backed by virtual address space.
pub(super) struct VirtualFileBlock {
    /// The allocated anonymous memory block of the file.
    ///
    /// Most of this is likely zeroed memory.
    mem: memmap2::MmapMut,
    /// A set of page states for the memory.
    page_state_table: PageStateTable,
    /// The size of the pages being allocated, this is used
    /// to calculate the actual memory usage of the block.
    page_size: PageSize,
    /// The total number of pages allocated.
    pages_allocated: usize,
}

impl VirtualFileBlock {
    pub(super) fn allocate(size: usize, page_size: PageSize) -> io::Result<Self> {
        let num_pages = get_num_pages(size, page_size as usize);

        let mem = memmap2::MmapOptions::new()
            .len(num_pages * page_size.num_bytes())
            .map_anon()?;

        let page_state_table = PageStateTable::new(num_pages);

        Ok(Self {
            mem,
            page_state_table,
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
        self.pages_allocated * self.page_size.num_bytes()
    }

    #[inline]
    /// Returns the amount of virtual address space allocated.
    pub fn virtual_address_space_usage(&self) -> usize {
        self.page_state_table.len() * self.page_size.num_bytes()
    }

    #[inline]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    #[inline]
    /// Returns the page state for the given page.
    pub fn page_at(&self, page_id: PageId) -> &PageState {
        self.page_state_table.at(page_id)
    }

    /// Returns a mutable page reference for the given page ID.
    ///
    /// # Safety
    /// 
    /// The caller must ensure that the page ID is valid for the given file block,
    /// that the caller holds the page write lock exclusively.
    ///
    /// This method is `&self` because mutability is controlled on the page-level,
    /// and no over values are allowed to be modified outside the memory pages themselves.
    pub unsafe fn get_mut_page(&self, page_id: PageId) -> MutPageRef {
        let state = self.page_at(page_id);
        let offset = page_id * self.page_size.num_bytes();

        let mem_ptr = self.mem.as_ptr().add(offset) as *mut u8;
        let mem_len = self.page_size.num_bytes();

        MutPageRef {
    
        }
    }

    /// Returns a page reference for the given page ID.
    ///
    /// # Safety
    /// 
    /// The caller must ensure that the page ID is valid for the given file block.
    pub unsafe fn get_page(&self, page_id: PageId) -> PageRef {
        let state = self.page_at(page_id);
        let offset = page_id * self.page_size.num_bytes();

        let mem_ptr = self.mem.as_ptr().add(offset);
        let mem_len = self.page_size.num_bytes();

        PageRef {

        }
    }

    /// Free the given page.
    ///
    /// # Safety
    /// 
    /// The caller must ensure that the page ID is valid for the given file block and that there are
    /// no other active readers or accesses to this page before being freed.
    pub unsafe fn free_page(&self, page_id: PageId) {
        let state = self.page_at(page_id);
        let offset = page_id * self.page_size.num_bytes();

        self.mem
            .unchecked_advise_range(
                UncheckedAdvice::Free,
                offset,
                self.page_size.num_bytes(),
            )
            .expect("madvise free call should not fail");

        state.set_free_unchecked();
    }
}

fn get_num_pages(size: usize, page_size: usize) -> usize {
    let mut num_pages = size / page_size;
    if size % page_size != 0 {
        num_pages += 1;
    }
    num_pages
}

pub(super) struct MutPageRef {
    mem_ptr: *mut u8,
    mem_len: usize,
}

impl MutPageRef {
    pub(super) unsafe fn mark_to_be_freed(&mut self) {
        todo!()
    }

    pub(super) unsafe fn write(&mut self, buffer: &[u8]) {
        todo!()
    }
}

pub(super) struct PageRef {
    mem_ptr: *const u8,
    mem_len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE_8KB: usize = 8 << 10;

    #[test]
    fn test_num_pages() {
        assert_eq!(get_num_pages(0, PAGE_SIZE_8KB), 0);
        assert_eq!(get_num_pages(1, PAGE_SIZE_8KB), 1);
        assert_eq!(get_num_pages(16 << 10, PAGE_SIZE_8KB), 2);
        assert_eq!(get_num_pages((16 << 10) + 5, PAGE_SIZE_8KB), 3);
    }

    #[test]
    fn test_block_allocate_large() {
        let block = VirtualFileBlock::allocate(64 << 30, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 64 << 30);
    }

    // #[test]
    // fn test_block_write_and_read() {
    //     let mut block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
    //         .expect("Allocate zeroed space");
    //     assert_eq!(block.memory_usage(), 0);
    //     assert_eq!(block.virtual_address_space_usage(), 8 << 10);
    // 
    //     let error = block.read_page(2).unwrap_err();
    //     assert_eq!(error.kind(), ErrorKind::InvalidInput);
    //     assert_eq!(error.to_string(), "page index is out of bounds");
    // 
    //     let error = block.read_page(0).unwrap_err();
    //     assert_eq!(error.kind(), ErrorKind::InvalidInput);
    //     assert_eq!(error.to_string(), "cannot read unallocated pages");
    // 
    //     let error = block
    //         .write_page(0, b"Hello, world!".as_slice())
    //         .unwrap_err();
    //     assert_eq!(error.kind(), ErrorKind::InvalidInput);
    //     assert_eq!(error.to_string(), "write buffer is not size of page");
    // 
    //     let data = vec![1; 8 << 10];
    //     let error = block.write_page(2, &data).unwrap_err();
    //     assert_eq!(error.kind(), ErrorKind::InvalidInput);
    //     assert_eq!(error.to_string(), "page index is out of bounds");
    // 
    //     block
    //         .write_page(0, &[1; 8 << 10])
    //         .expect("Write memory to page");
    // 
    //     let slice = block.read_page(0).unwrap();
    //     assert_eq!(
    //         slice,
    //         &[1; 8 << 10],
    //         "Read data should match written buffer"
    //     );
    // }
    // 
    // #[test]
    // fn test_block_free() {
    //     let mut block = VirtualFileBlock::allocate(1 << 20, PageSize::Size8KB)
    //         .expect("Allocate zeroed space");
    //     assert_eq!(block.memory_usage(), 0);
    //     assert_eq!(block.virtual_address_space_usage(), 1 << 20);
    // 
    //     block
    //         .write_page(0, &[1; 8 << 10])
    //         .expect("Write memory to page");
    // 
    //     let slice = block.read_page(0).unwrap();
    //     assert_eq!(
    //         slice,
    //         &[1; 8 << 10],
    //         "Read data should match written buffer"
    //     );
    // 
    //     block.free_page(0).unwrap();
    // 
    //     let error = block.read_page(0).unwrap_err();
    //     assert_eq!(error.kind(), ErrorKind::InvalidInput);
    //     assert_eq!(error.to_string(), "cannot read unallocated pages");
    // }
}
