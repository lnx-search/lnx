use std::borrow::Cow;

use rkyv::rancor;

use super::mem::PageEncodeBuffer;
use super::metadata;
use super::version::LayoutVersion;
use crate::{BlockId, PageId};

/// An owned, mutable disk page, used for constructing and writing new pages.
pub struct DiskPageBuilder<'buf> {
    pub(crate) metadata: metadata::DiskPageMetadata,
    data: Cow<'buf, [u8]>,
}

impl<'buf> DiskPageBuilder<'buf> {
    pub fn new(
        id: PageId,
        block: BlockId,
        revision: u32,
        layout_version: LayoutVersion,
        data: Cow<'buf, [u8]>,
    ) -> Self {
        assert!(
            data.len() <= layout_version.max_data_size(),
            "Page data exceeds maximum size allowance",
        );

        let checksum = crc32fast::hash(&data);

        let metadata = metadata::DiskPageMetadata {
            id,
            checksum,
            block,
            revision,
            layout_version,
            data_len: data.len() as u16,
        };

        Self { metadata, data }
    }

    /// Encode the page into the provided [PageEncodeBuffer].
    ///
    /// This will automatically apply and reserved space in the buffer or padding
    /// bytes based on the set [LayoutVersion].
    pub(crate) fn encode(
        &self,
        buffer: &mut PageEncodeBuffer,
    ) -> Result<(), rancor::Error> {
        let layout_bytes = self.metadata.layout_version().to_bytes();

        buffer.write_bytes(&layout_bytes);
        buffer.write_bytes(&[0; 6]); // Pad reserved bytes for alignment.
        self.metadata.write_to(buffer)?;
        buffer.write_bytes(self.data.as_ref());

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::metadata::PAGE_SIZE;
    
    #[rstest::rstest]
    #[case(LayoutVersion::V1, Cow::Borrowed(b"hello, world".as_ref()))]
    #[case(LayoutVersion::V1, Cow::Borrowed(b"".as_ref()))]
    #[case(LayoutVersion::V1, Cow::Owned(vec![1; LayoutVersion::V1.max_data_size()]))]
    #[should_panic]
    #[case(LayoutVersion::V1, Cow::Owned(vec![1; PAGE_SIZE]))]
    fn test_page_builder(
        #[case] layout_version: LayoutVersion,
        #[case] data: Cow<'static, [u8]>,
    ) {
        let builder = DiskPageBuilder::new(
            PageId(u32::MIN),
            BlockId(u64::MAX),
            u32::MIN,
            layout_version,
            data,
        );
        
        let mut buffer = PageEncodeBuffer::default();
        builder
            .encode(&mut buffer)
            .expect("encode page data into buffer");
        
                
    }
}