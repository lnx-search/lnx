//! The page data file holds onto the raw data of pages.

use crate::PageFileId;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub struct MetadataHeader {
    /// The unique ID of the page file this metadata file is linked to.
    pub page_file_id: PageFileId,
    /// The number of pages in the page file.
    pub num_pages: usize,
}
