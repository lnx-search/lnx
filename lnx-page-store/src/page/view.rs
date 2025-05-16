use std::fmt::{Debug, Formatter};

use rkyv::rancor;

use super::metadata::{DiskPageMetadata, DiskPageMetadataRef};

/// A page is a fixed 8KB size of data that holds basic metadata with some inner
/// chunk of data representing part of a block.
pub struct DiskPageView<'buf> {
    /// The decoded page metadata.
    metadata: &'buf DiskPageMetadataRef,
    /// The decoded page data.
    data: &'buf [u8],
}

impl<'buf> Debug for DiskPageView<'buf> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DiskPageView(id={:?}, block_id={:?}, revision={}, checksum={})",
            self.metadata.id(),
            self.metadata.block(),
            self.metadata.revision(),
            self.metadata.checksum(),
        )
    }
}

impl<'buf> DiskPageView<'buf> {
    /// Decodes the page view from the given buffer.
    ///
    /// This requires the provided buffer is correctly aligned and starts
    /// with the metadata object first.
    pub fn decode(buf: &'buf [u8]) -> Result<Self, rancor::Error> {
        let metadata_bytes = &buf[..DiskPageMetadata::SERIALIZED_SIZE];
        let remaining_bytes = &buf[DiskPageMetadata::SERIALIZED_SIZE..];

        let metadata = rkyv::access::<DiskPageMetadataRef, _>(metadata_bytes)?;
        let data = &remaining_bytes[..metadata.data_len()];

        Ok(Self { metadata, data })
    }

    /// Returns a reference to the page metadata.
    pub fn metadata(&self) -> &'buf DiskPageMetadataRef {
        self.metadata
    }

    /// Returns the slice of data stored within the page.
    pub fn data(&self) -> &'buf [u8] {
        self.data
    }
}
