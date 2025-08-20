//! The metadata snapshot file holds the page metadata tied to a given page file.
//!
//! This file is not modified on every op, instead it is updated when the op log is rolled up.

#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod dirty_marker;

use crate::file::page_table::dirty_marker::DirtyMarkerTable;
use crate::PageFileId;
use crate::layout::file_metadata::Encryption;

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
    groups_to_page_file: papaya::HashMap<u64, ()>,
    dirty_marker_table: DirtyMarkerTable,
}


pub struct LocalPageTable {
    id: PageFileId,
    pages: Vec<()>
}