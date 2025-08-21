//! The metadata snapshot file holds the page metadata tied to a given page file.
//!
//! This file is not modified on every op, instead it is updated when the op log is rolled up.

mod dirty_marker;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;

use crate::file::page_table::dirty_marker::DirtyMarkerTable;
use crate::layout::file_metadata::Encryption;
use crate::layout::page_metadata::PageMetadata;
use crate::{PageFileId, PageId};

// TODO: Situation: bad actor can do replay attack on the page table
//       Solution: Attach associated data for the _PAGE DATA_ with the page ID
//                 AND revision AND page group ID.

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub struct MetadataHeader {
    /// The unique ID of the page file this metadata file is linked to.
    pub page_file_id: PageFileId,
    /// The number of pages in the page file.
    pub num_pages: usize,
    /// Signals if the data in the log is encrypted or not.
    pub encryption: Encryption,
}

/// The global page table, this holds the in memory state of all
/// currently allocated pages.
pub struct GlobalPageTable {
    groups_to_page_file: papaya::HashMap<u64, PageLookup>,
    page_tables: papaya::HashMap<PageFileId, LocalPageTable>,
}

/// The page lookup allows the system to locate the pages of a given
/// group.
///
/// Pages are connected in a linked list so once you have one page,
/// you can find the next page down the line.
struct PageLookup {
    first_page_file_id: PageFileId,
    first_page_id: PageId,
}

/// A [LocalPageTable] contains a lookup table mapping [PageId]s to their
/// respective [PageMetadata] entries for a page file.
struct LocalPageTable {
    blocks: Vec<PageMetadata>,
    dirty_marker_table: DirtyMarkerTable,
}

#[cfg(all(test, not(feature = "test-miri")))]
mod misc_tests {
    use super::*;

    #[test]
    fn test_lookup_size() {
        assert_eq!(size_of::<PageLookup>(), 8);
    }
}
