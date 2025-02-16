use std::io::ErrorKind;
use std::ops::{Index, Range};
use std::{cmp, io};

use memmap2::UncheckedAdvice;

use crate::config::PageSize;
use crate::page_cache::page::PageStateTable;

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

    /// Reads a slice of bytes between the provided range.
    ///
    /// This method will check that all pages being read are currently allocated,
    /// if the read does not lay within the allocated page ranges an error is returned.
    pub fn read_page(&self, page_idx: PageId) -> io::Result<&[u8]> {
        self.ensure_valid_page(page_idx)?;

        if !self.is_allocated(page_idx) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "cannot read unallocated pages",
            ));
        }

        let range = self.get_page_range(page_idx);

        // # Safety
        // The bounds of the read are already checked by `ensure_valid_range`.
        let slice = unsafe { self.mem.get_unchecked(range) };

        Ok(slice)
    }

    /// Copies a given slice into memory.
    ///
    /// The start and end of the slice must be aligned to the set [PageSize], otherwise
    /// this method will return an error.
    pub fn write_page(&mut self, page_idx: PageId, buffer: &[u8]) -> io::Result<&[u8]> {
        self.ensure_valid_page(page_idx)?;

        if self.is_allocated(page_idx) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "page already allocated",
            ));
        }

        // The length of the buffer must match the page size
        if buffer.len() != self.page_size.num_bytes() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "write buffer is not size of page",
            ));
        }

        let range = self.get_page_range(page_idx);

        // # Safety
        // The bounds of the read are already checked by `ensure_valid_range`.
        unsafe {
            let slice_mut = self.mem.get_unchecked_mut(range.clone());
            slice_mut.copy_from_slice(buffer);
        }

        self.mark_page_allocated(page_idx);

        // # Safety
        // The bounds of the read are already checked by `ensure_valid_range` and
        // fully allocated after write.
        unsafe {
            let slice = self.mem.get_unchecked(range);
            Ok(slice)
        }
    }

    /// Frees the pages within the provided range.
    ///
    /// This method expects the bounds are aligned to the configured [PageSize]
    /// otherwise an error is returned.
    pub fn free_page(&mut self, page_idx: PageId) -> io::Result<()> {
        self.ensure_valid_page(page_idx)?;

        if !self.is_allocated(page_idx) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "page not allocated",
            ));
        }

        let range = self.get_page_range(page_idx);

        // # Safety
        // Although this is not an unsafe method, the advice can cause UB.
        // This is safe to use because we track what pages we have allocated
        // and unallocated preventing reads on a page that is marked as free.
        //
        // The reason why it is UB to read the page after this call is because
        // the kernel does not guarantee that the reader will see a zero filled buffer
        // or the old data originally in the buffer, and you have no way of knowing.
        self.mem.unchecked_advise_range(
            UncheckedAdvice::Free,
            range.start,
            range.len(),
        )?;

        self.mark_page_free(page_idx);

        Ok(())
    }

    fn ensure_valid_page(&self, page_idx: PageId) -> io::Result<()> {
        if page_idx >= self.allocated_pages.len() {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "page index is out of bounds",
            ))
        } else {
            Ok(())
        }
    }

    fn get_page_range(&self, page_idx: PageId) -> Range<usize> {
        let start = page_idx * self.page_size.num_bytes();
        let end = (page_idx + 1) * self.page_size.num_bytes();
        start..cmp::min(end, self.mem.len())
    }

    fn mark_page_allocated(&mut self, page_idx: PageId) {
        let old = self.allocated_pages.replace(page_idx, true);
        self.pages_allocated += !old as usize;
    }

    fn mark_page_free(&mut self, page_idx: PageId) {
        let old = self.allocated_pages.replace(page_idx, false);
        self.pages_allocated -= old as usize;
    }

    pub fn is_allocated(&self, page_idx: PageId) -> bool {
        *self.allocated_pages.index(page_idx)
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

    #[test]
    fn test_block_write_and_read() {
        let mut block = VirtualFileBlock::allocate(8 << 10, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 8 << 10);

        let error = block.read_page(2).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "page index is out of bounds");

        let error = block.read_page(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "cannot read unallocated pages");

        let error = block
            .write_page(0, b"Hello, world!".as_slice())
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "write buffer is not size of page");

        let data = vec![1; 8 << 10];
        let error = block.write_page(2, &data).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "page index is out of bounds");

        block
            .write_page(0, &[1; 8 << 10])
            .expect("Write memory to page");

        let slice = block.read_page(0).unwrap();
        assert_eq!(
            slice,
            &[1; 8 << 10],
            "Read data should match written buffer"
        );
    }

    #[test]
    fn test_block_free() {
        let mut block = VirtualFileBlock::allocate(1 << 20, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 1 << 20);

        block
            .write_page(0, &[1; 8 << 10])
            .expect("Write memory to page");

        let slice = block.read_page(0).unwrap();
        assert_eq!(
            slice,
            &[1; 8 << 10],
            "Read data should match written buffer"
        );

        block.free_page(0).unwrap();

        let error = block.read_page(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "cannot read unallocated pages");
    }
}
