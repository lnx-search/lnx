use std::ops::Index;
use std::io;

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
        })
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
    
    #[inline]
    /// Returns the page ID which the pointer is attached to.
    pub(super) unsafe fn pointer_to_page_id(&self, ptr: *const PageState) -> PageId {
        self.page_state_table.pointer_to_index(ptr)
    }
    
    /// Returns a read-only pointer starting from the given page ID.
    /// 
    /// # Safety
    /// 
    /// The caller must ensure that the page ID is valid for the given file block,
    /// and must ensure all access to the raw memory are initialised pages.
    /// 
    /// The caller must also ensure that any reads using this pointer do not go out
    /// of bounds of the file block itself.
    pub(super) unsafe fn get_page_ptr(&self, page_id: PageId) -> *const u8 {
        let offset = page_id * self.page_size.num_bytes();
        self.mem.as_ptr().add(offset)
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
    pub(super)  unsafe fn get_mut_page(&self, page_id: PageId) -> MutPageRef {
        let state = self.page_at(page_id);
        
        let mem_ptr = self.get_page_ptr(page_id) as *mut u8;
        let mem_len = self.page_size.num_bytes();

        MutPageRef {
            state: state as *const PageState,
            mem_ptr,
            mem_len,
        }
    }

    /// Free the given page.
    ///
    /// # Safety
    /// 
    /// The caller must ensure that the page ID is valid for the given file block and that there are
    /// no other active readers or accesses to this page before being freed.
    pub(super)  unsafe fn free_page(&self, page_id: PageId) {
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


/// A mutable reference to a memory page.
pub(super) struct MutPageRef {
    state: *const PageState,
    mem_ptr: *mut u8,
    mem_len: usize,
}

impl MutPageRef {
    #[inline]
    /// Returns the size of the page memory.
    pub fn size(&self) -> usize {
        self.mem_len
    }
    
    /// Marks the current page as "to be freed".
    ///
    /// # Safety
    /// 
    /// The caller must hold an exclusive lock to the current page state.    
    pub(super) unsafe fn mark_to_be_freed(&mut self) {
        (*self.state).set_to_be_freed_unchecked();
    }

    /// Marks the current page as "to be freed".
    ///
    /// # Safety
    /// 
    /// The caller must hold an exclusive lock to the current page state, and the
    /// buffer length must be less than or equal to the page size.
    /// In the event of a buffer size that is _less than_ the page size, it is the callers
    /// responsibility to ensure the bytes _not_ overwritten by the buffer are not read/accessed
    /// by readers.
    pub(super) unsafe fn write(&mut self, buffer: &[u8]) {
        std::ptr::copy_nonoverlapping(buffer.as_ptr(), self.mem_ptr, buffer.len());        
        (*self.state).set_allocated_unchecked();
    }
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
