//! The metadata snapshot file holds the page metadata tied to a given page file.
//!
//! This file is not modified on every op, instead it is updated when the op log is rolled up.

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
