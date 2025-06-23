mod page_allocator;
mod writer;

use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::PageGroupId;
use crate::file::page_allocator::PageAllocator;
use crate::layout::{
    PAGES_PER_FILE,
    allocation_table,
    file_metadata,
    log,
    page_metadata,
};

/// The page file holds [PAGES_PER_FILE] number of pages of a fixed size (but variable at
/// creation time.)
pub struct PageFile {
    /// The open file that the page file is backed on.
    file: std::fs::File,
    /// The metadata header of the page file,
    file_metadata: file_metadata::VersionedPageFileMetadata,
    /// The amount of bytes the file has allocated on disk.
    ///
    /// This number is increased as writes occur or when
    /// pre-allocation is enabled.
    file_allocated_size: AtomicU64,
    /// The currently allocated metadata blocks, each block contains
    /// 64 metadata entries.
    metadata_blocks: RwLock<Vec<page_metadata::PageMetadataX63Bock>>,
    /// The latest snapshot of the page file and what pages it has allocated.
    ///
    /// This does not necessarily include the most recent changes, only changes
    /// that have been rolled up from the operations log.
    allocated_pages_snapshot: Mutex<allocation_table::PageAllocationTable>,
    /// The page allocator for the page file.
    ///
    /// This holds changes to the file that might not yet have been commited.
    page_allocator: PageAllocator,
}

impl PageFile {
    /// Returns the metadata associated with the page file.
    pub fn metadata(&self) -> &file_metadata::VersionedPageFileMetadata {
        &self.file_metadata
    }

    /// Returns the approximate number of allocated pages on the file.
    ///
    /// The reason this is approximate is it can include pages that have
    /// been reserved by writes that have not yet been commited, hence it is
    /// possible (but unlikely) that the write could fail and the pages could
    /// be marked as free once again.
    pub fn num_allocated_pages(&self) -> usize {
        self.page_allocator.num_allocated()
    }

    /// Returns the amount of disk space allocated for the page file so far.
    pub fn file_allocated_size(&self) -> u64 {
        self.file_allocated_size.load(Ordering::Relaxed)
    }

    /// Attempts to create a new writer with a given group ID and a given byte capacity.
    ///
    /// If the file does not have enough space to contain the file; `None` is returned.
    pub fn try_create_writer(
        &self,
        group_id: PageGroupId,
        total_size: u64,
    ) -> Option<writer::PageGroupWriter<'_>> {
        let page_size = self.metadata().page_size();
        let mut num_pages_required = (total_size / page_size as u64) as usize;
        if total_size % page_size as u64 != 0 {
            num_pages_required += 1;
        }

        // We can never support the target file size because we don't have enough log entries
        // available that add up to or beyond the total size.
        if num_pages_required > log::MAX_LOG_ENTRIES {
            return None;
        }

        let reserved_pages = self.page_allocator.try_reserve(num_pages_required)?;

        todo!()
    }
}
