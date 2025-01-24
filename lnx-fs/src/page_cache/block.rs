use std::io;
use std::io::ErrorKind;
use std::ops::{Index, Range};

use memmap2::UncheckedAdvice;

use crate::config::PageSize;

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
    page_size: PageSize,
    /// The total number of pages allocated.
    pages_allocated: usize,
}

impl FileBlock {
    pub(super) fn allocate(size: usize, page_size: PageSize) -> io::Result<Self> {
        use bitvec::prelude::*;
        
        let mem = memmap2::MmapOptions::new()
            .len(size)
            .map_anon()?;

        let num_pages = get_num_pages(size, page_size as usize);
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
        self.pages_allocated * self.page_size.num_bytes()
    }

    #[inline]
    /// Returns the amount of virtual address space allocated.
    pub fn virtual_address_space_usage(&self) -> usize {
        self.allocated_pages.len() * self.page_size.num_bytes()
    }

    #[inline]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }
    
    /// Reads a slice of bytes between the provided range.
    /// 
    /// This method will check that all pages being read are currently allocated,
    /// if the read does not lay within the allocated page ranges an error is returned.
    pub fn read_at(&self, range: Range<usize>) -> io::Result<&[u8]> {
        self.ensure_valid_range(range.start, range.end)?;
        
        let is_safe_read = self.get_pages(range.start, range.end)
            .all(|idx| self.is_allocated(idx));
        
        if !is_safe_read {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "cannot read unallocated pages"
            ));
        }
        
        // # Safety
        // The bounds of the read are already checked by `ensure_valid_range`.
        let slice = unsafe { self.mem.get_unchecked(range) };
        
        Ok(slice)
    }
    
    /// Copies a given slice into memory.
    ///
    /// The start and end of the slice must be aligned to the set [PageSize], otherwise
    /// this method will return an error. 
    pub fn write_at(&mut self, start: usize, buffer: &[u8]) -> io::Result<()> {
        self.ensure_valid_range(start, start + buffer.len())?;

        // # Safety
        // The bounds of the read are already checked by `ensure_valid_range`.
        unsafe {
            let slice_mut = self.mem.get_unchecked_mut(start..start + buffer.len());
            slice_mut.copy_from_slice(buffer);
        }
        
        for idx in self.get_pages(start, start + buffer.len()) {
            self.mark_page_allocated(idx);
        }
        
        Ok(())
    }
    
    /// Frees the pages within the provided range.
    /// 
    /// This method expects the bounds are aligned to the configured [PageSize]
    /// otherwise an error is returned.
    pub fn free(&mut self, range: Range<usize>) -> io::Result<()> {
        self.ensure_valid_range(range.start, range.end)?;
        
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
        
        for idx in self.get_pages(range.start, range.end) {
            self.mark_page_free(idx);
        }
        
        Ok(())   
    }
    
    fn ensure_valid_range(&self, start: usize, end: usize) -> io::Result<()> {
        if !self.page_size.is_aligned(start) || !self.page_size.is_aligned(end) {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "range bounds are not aligned to the page size",
            ))
        } else if start >= self.mem.len() || end > self.mem.len()  {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "index range is out of bounds",
            ))
        } else {
            Ok(())
        }
    }
    
    fn get_pages(&self, start: usize, end: usize) -> Range<usize> {
        let start = start / self.page_size.num_bytes();
        let end = end / self.page_size.num_bytes();
        start..end
    }

    fn mark_page_allocated(&mut self, page_idx: usize) {
        let old = self.allocated_pages.replace(page_idx, true);
        self.pages_allocated += !old as usize;
    }
    
    fn mark_page_free(&mut self, page_idx: usize) {
        let old = self.allocated_pages.replace(page_idx, false);
        self.pages_allocated -= old as usize;
    }
    
    fn is_allocated(&self, page_idx: usize) -> bool {
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
    use std::time::Duration;
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
        let block = FileBlock::allocate(64 << 30, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 64 << 30);
    }
    
    #[test]
    fn test_block_write_and_read() {
        let mut block = FileBlock::allocate(1 << 20, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 1 << 20);

        let error = block.read_at(0..13).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "range bounds are not aligned to the page size");
        
        let error = block.read_at(0..8 << 20).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "index range is out of bounds");
        
        let error = block.read_at(0..block.page_size().num_bytes()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "cannot read unallocated pages");

        let error = block.write_at(0, b"Hello, world!".as_slice()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "range bounds are not aligned to the page size");
        
        let data = vec![1; 2 << 20];
        let error = block.write_at(0, &data).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "index range is out of bounds");
        
        block.write_at(0, &[1; 8 << 10])
            .expect("Write memory to page");        
        
        let slice = block.read_at(0..8 << 10).unwrap();
        assert_eq!(slice, &[1; 8 << 10], "Read data should match written buffer");
    }

    #[test]
    fn test_block_free() {
        let mut block = FileBlock::allocate(1 << 20, PageSize::Size8KB)
            .expect("Allocate zeroed space");
        assert_eq!(block.memory_usage(), 0);
        assert_eq!(block.virtual_address_space_usage(), 1 << 20);

        block.write_at(0, &[1; 8 << 10])
            .expect("Write memory to page");

        let slice = block.read_at(0..8 << 10).unwrap();
        assert_eq!(slice, &[1; 8 << 10], "Read data should match written buffer");
        
        block.free(0..8<<10).unwrap();

        let error = block.read_at(0..8<<10).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "cannot read unallocated pages");
    }
}