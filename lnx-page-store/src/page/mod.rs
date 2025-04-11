use std::borrow::Cow;
use rkyv::rancor;
use crate::{BlockId, PageId};
use crate::page::mem::PageEncodeBuffer;
use crate::page::version::LayoutVersion;

mod version;
mod metadata;
mod mem;
mod view;

/// The total size of a page (metadata included) on disk.
pub const PAGE_SIZE: usize = 8 << 10;

/// An owned, mutable disk page, mostly used for constructing and writing new pages.
pub struct DiskPage<'buf> {
    metadata: metadata::DiskPageMetadata, 
    data: Cow<'buf, [u8]>,
}

impl<'buf> DiskPage<'buf> {
    pub fn new(
        id: PageId,
        block: BlockId,
        revision: u32,
        layout_version: LayoutVersion,
        data: Cow<'buf, [u8]>,
    ) -> Self {        
        assert!(
            data.len() < layout_version.max_data_size(),
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
        
        Self {
            metadata,
            data,
        }
    }
    
    /// Encode the page into the provided [PageEncodeBuffer].
    /// 
    /// This will automatically apply and reserved space in the buffer or padding
    /// bytes based on the set [LayoutVersion].
    pub fn encode(&self, buffer: &mut PageEncodeBuffer) -> Result<(), rancor::Error> {
        let layout_bytes = self.metadata.layout_version().to_bytes();
        
        buffer.write_bytes(&layout_bytes);
        buffer.write_bytes(&[0; 6]);  // Pad reserved bytes for alignment.
        self.metadata.write_to(buffer)?;        
        buffer.write_bytes(self.data.as_ref());
        
        Ok(())
    }
}
