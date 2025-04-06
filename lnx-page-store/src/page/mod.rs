mod encode;
mod flags;

use std::borrow::Cow;
use std::fmt::{Debug, Formatter};

pub use self::encode::PageEncoderDecoder;
pub use self::flags::PageFlags;
use crate::{BlockId, PageId};

/// A type alias for a static archived page.
pub type PageRef = rkyv::Archived<Page<'static>>;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// A page holds some chunk of data for a given block.
pub struct Page<'a> {
    /// The ID of the page.
    id: PageId,
    /// The page data checksum.
    checksum: u32,
    /// The block this page contains data for.
    block: BlockId,
    #[rkyv(with = rkyv::with::AsOwned)]
    /// The raw data contained within the page.
    data: Cow<'a, [u8]>,
}

impl<'a> Debug for Page<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("checksum", &self.checksum)
            .field("block", &self.block)
            .finish()
    }
}

impl<'a> Page<'a> {
    /// Create a new [Page] from the given id, block and data.
    ///
    /// The checksum will be automatically calculated.
    pub fn new(id: PageId, block: BlockId, data: &'a [u8]) -> Self {
        let checksum = crc32fast::hash(&data);

        Self {
            id,
            block,
            checksum,
            data: Cow::Borrowed(data),
        }
    }

    #[inline]
    /// The ID of the page.
    pub fn id(&self) -> PageId {
        self.id
    }

    #[inline]
    /// The block this page contains data for.
    pub fn block(&self) -> BlockId {
        self.block
    }

    #[inline]
    /// Returns the CRC32 checksum of the data.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    #[inline]
    /// Returns the size of the page when serialized.
    pub fn serialize_size(&self) -> usize {
        size_of::<rkyv::Archived<Self>>()
            + size_of::<u32>()   // Included because AsOwned, makes Cow<'_, [u8]> become a Vec<u8>.
            + self.data.len()
    }
}

impl Debug for ArchivedPage<'static> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("checksum", &self.checksum)
            .field("block", &self.block)
            .finish()
    }
}

impl ArchivedPage<'static> {
    #[inline]
    /// The ID of the page.
    pub fn id(&self) -> PageId {
        PageId(self.id.0.to_native())
    }

    #[inline]
    /// The block this page contains data for.
    pub fn block(&self) -> BlockId {
        BlockId(self.block.0.to_native())
    }

    #[inline]
    /// Returns the CRC32 checksum of the data.
    pub fn checksum(&self) -> u32 {
        self.checksum.to_native()
    }

    #[inline]
    /// The slice of page data.
    pub fn data(&self) -> &[u8] {
        self.data.as_ref()
    }

    #[inline]
    /// Compares the checksum stored in the page with the calculated checksum
    /// of the data.
    pub fn checksums_match(&self) -> bool {
        let actual = crc32fast::hash(self.data());
        actual == self.checksum
    }
}
