use std::ops::Range;
use std::{cmp, io};

use memmap2::UncheckedAdvice;
use tracing::error;

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
    /// The size of the file.
    ///
    /// This may be smaller than the address space allocated.
    file_size: usize,
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
            file_size: size,
        })
    }

    #[inline]
    /// Returns the amount of virtual address space allocated.
    pub fn virtual_address_space_usage(&self) -> usize {
        self.page_state_table.len() * self.page_size.num_bytes()
    }

    #[inline]
    /// Returns the page size being used by the block.
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    #[inline]
    /// Returns the file size being used by the block.
    pub fn file_size(&self) -> usize {
        self.file_size
    }

    #[inline]
    /// Returns the allocated bytes size being used by the block.
    /// 
    /// This can differ from the file size because allocations are aligned to the
    /// page size.
    pub fn allocated_size(&self) -> usize {
        self.mem.len()
    }
    
    /// Returns the byte range of the file for the given page.
    ///
    /// This may not be exactly the page size as if it is the last page
    /// the file may be smaller than the total virtual address space allocated.
    pub fn page_to_bytes_range(&self, page_id: PageId) -> Range<usize> {
        let page_size_bytes = self.page_size().num_bytes();
        let read_start = page_id * page_size_bytes;
        let read_end = cmp::min(read_start + page_size_bytes, self.file_size());
        read_start..read_end
    }

    #[inline]
    /// Returns the number of pages in the block.
    pub fn num_pages(&self) -> usize {
        self.page_state_table.len()
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
        assert!(
            page_id < self.page_state_table.len(),
            "Attempting to read page out bounds"
        );
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
    pub(super) unsafe fn get_mut_page(&self, page_id: PageId) -> MutPageRef {
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
    /// Returns if the page was freed successfully or not.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the page ID is valid for the given file block and that there are
    /// no other active readers or accesses to this page before being freed.
    pub(super) unsafe fn free_page(&self, page_id: PageId) -> bool {
        let state = self.page_at(page_id);
        let offset = page_id * self.page_size.num_bytes();

        let result = self.mem.unchecked_advise_range(
            UncheckedAdvice::Free,
            offset,
            self.page_size.num_bytes(),
        );

        if let Err(error) = result {
            error!(error = ?error, "failed to free page due to error");
            return false;
        }

        state.mark_free_unchecked();

        true
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
    /// You must provide the generation attached to this operation in order
    /// to prevent use-after free situations where older GC callbacks can cleanup
    /// memory now in use.
    /// 
    /// # Safety
    ///
    /// The caller must hold an exclusive lock to the current page state.    
    pub(super) unsafe fn mark_to_be_freed(&mut self, generation: u64) {
        (*self.state).mark_dirty_unchecked(generation);
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
        (*self.state).mark_allocated_unchecked();
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

    #[test]
    fn test_block_write_and_read_single_page() {
        #[allow(unused_mut)]
        let mut block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.virtual_address_space_usage(), 8 << 10);

        unsafe {
            let ptr = block.get_page_ptr(0);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice.len(), 8 << 10);
            assert_eq!(slice, &[0; 8 << 10]);

            let mut page_ref = block.get_mut_page(0);
            assert_eq!(page_ref.size(), 8 << 10);
            page_ref.write(&[4; 8 << 10]);

            let state = block.page_at(0);
            let flags = state.flags();
            assert!(flags.is_allocated());
            assert!(!flags.is_dirty());
            assert!(!state.is_locked());

            let ptr = block.get_page_ptr(0);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice, &[4; 8 << 10]);
        };
    }

    #[test]
    fn test_block_write_and_read_many_pages() {
        #[allow(unused_mut)]
        let mut block = VirtualFileBlock::allocate(64 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.virtual_address_space_usage(), 64 << 10);

        unsafe {
            let ptr = block.get_page_ptr(4);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice.len(), 8 << 10);
            assert_eq!(slice, &[0; 8 << 10]);

            let mut page_ref = block.get_mut_page(4);
            assert_eq!(page_ref.size(), 8 << 10);
            page_ref.write(&[4; 8 << 10]);

            let state = block.page_at(4);
            let flags = state.flags();
            assert!(flags.is_allocated());
            assert!(!flags.is_dirty());
            assert!(!state.is_locked());

            let state = block.page_at(2);
            let flags = state.flags();
            assert!(!flags.is_allocated());
            assert!(!flags.is_dirty());
            assert!(!state.is_locked());

            let ptr = block.get_page_ptr(0);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice, &[0; 8 << 10]);

            let ptr = block.get_page_ptr(4);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice, &[4; 8 << 10]);
        };
    }

    #[test]
    #[should_panic]
    fn test_block_read_out_of_bounds() {
        let block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        unsafe {
            let _ptr = block.get_page_ptr(4);
        };
    }

    #[test]
    #[should_panic]
    fn test_block_write_out_of_bounds() {
        let block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        unsafe {
            let _ptr = block.get_mut_page(4);
        };
    }

    #[test]
    #[should_panic]
    fn test_block_free_out_of_bounds() {
        let block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        unsafe {
            let _ptr = block.free_page(4);
        };
    }

    #[test]
    fn test_block_free_single_page() {
        #[allow(unused_mut)]
        let mut block = VirtualFileBlock::allocate(1 << 20, PageSize::Size8KB)
            .expect("Allocate zeroed space");

        unsafe {
            let mut page_ref = block.get_mut_page(0);
            assert_eq!(page_ref.size(), 8 << 10);
            page_ref.write(&[4; 8 << 10]);

            let ptr = block.get_page_ptr(0);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice, &[4; 8 << 10]);

            block.free_page(0);

            let state = block.page_at(0);
            let flags = state.flags();
            assert!(!flags.is_allocated());
            assert!(!flags.is_dirty());
            assert!(!state.is_locked());
        }
    }

    #[test]
    fn test_block_free_many_page() {
        #[allow(unused_mut)]
        let mut block = VirtualFileBlock::allocate(1 << 20, PageSize::Size8KB)
            .expect("Allocate zeroed space");

        unsafe {
            let mut page_ref = block.get_mut_page(0);
            assert_eq!(page_ref.size(), 8 << 10);
            page_ref.write(&[4; 8 << 10]);

            let ptr = block.get_page_ptr(0);
            let slice = std::slice::from_raw_parts(ptr, block.page_size().num_bytes());
            assert_eq!(slice, &[4; 8 << 10]);

            block.free_page(0);

            let state = block.page_at(0);
            let flags = state.flags();
            assert!(!flags.is_allocated());
            assert!(!flags.is_dirty());
            assert!(!state.is_locked());

            // We can't actually check if the page was truly freed by the OS or not because
            // Linux doesn't actually give us any guarantee that the page is immediately freed
            // and zeroed or not.
        }
    }
}
