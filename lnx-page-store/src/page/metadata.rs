use std::fmt::Debug;

use rkyv::rancor;
use rkyv::ser::Positional;
use rkyv::ser::writer::Buffer;

use super::mem::PageEncodeBuffer;
use super::version::LayoutVersion;
use crate::{BlockId, PageId};

/// The total size of a single page (8KB)
pub const PAGE_SIZE: usize = 8 << 10;

/// A type alias for a static archived page metadata entry.
pub type DiskPageMetadataRef = rkyv::Archived<DiskPageMetadata>;

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
/// Metadata about the page and the ata stored within it.
pub struct DiskPageMetadata {
    /// The ID of the page.
    pub(super) id: PageId,
    /// The page data checksum.
    pub(super) checksum: u32,
    /// The block this page contains data for.
    pub(super) block: BlockId,
    /// The revision is a monotonic ID for each page within a block that
    /// tracks the number of observed updates to the block.
    pub(super) revision: u32,
    /// The layout version used for this page.
    ///
    /// This is to validate that a different version was not used to decode the page.
    pub(super) layout_version: LayoutVersion,
    /// The length of the buffer within the page.
    pub(super) data_len: u16,
}

impl DiskPageMetadata {
    pub(super) const SERIALIZED_SIZE: usize = size_of::<rkyv::Archived<Self>>();

    pub(super) fn write_to(
        &self,
        buffer: &mut PageEncodeBuffer,
    ) -> Result<(), rancor::Error> {
        let remaining = buffer.remaining_mut();
        let writer = Buffer::from(remaining);
        let writer = rkyv::api::high::to_bytes_in(self, writer)?;
        let num_bytes_written = writer.pos();

        buffer.advance_cursor(num_bytes_written);

        Ok(())
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
    /// Returns the revision of the block this page was assigned.
    pub fn revision(&self) -> u32 {
        self.revision
    }

    #[inline]
    /// Returns the layout version this page was created with.
    pub fn layout_version(&self) -> LayoutVersion {
        self.layout_version
    }

    #[inline]
    /// The length of initialized bytes within the page.
    pub fn data_len(&self) -> usize {
        self.data_len as usize
    }
}

impl ArchivedDiskPageMetadata {
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
    /// Returns the revision of the block this page was assigned.
    pub fn revision(&self) -> u32 {
        self.revision.to_native()
    }

    #[inline]
    /// Returns the layout version this page was created with.
    pub fn layout_version(&self) -> LayoutVersion {
        self.layout_version.into()
    }

    #[inline]
    /// The length of initialized bytes within the page.
    pub fn data_len(&self) -> usize {
        self.data_len.to_native() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mostly just used for coverage.
    #[test]
    fn test_metadata_getters() {
        let metadata = DiskPageMetadata {
            id: PageId(1),
            checksum: 2,
            block: BlockId(3),
            revision: 4,
            layout_version: LayoutVersion::V1,
            data_len: 5,
        };

        assert_eq!(metadata.id(), PageId(1));
        assert_eq!(metadata.checksum(), 2);
        assert_eq!(metadata.block(), BlockId(3));
        assert_eq!(metadata.revision(), 4);
        assert_eq!(metadata.layout_version(), LayoutVersion::V1);
        assert_eq!(metadata.data_len(), 5);
    }
}
