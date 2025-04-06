mod standard;

use std::mem::MaybeUninit;
use std::ops::Deref;

use super::{PAGE_SIZE, Page, PageFlags, PageRef};

/// The bytes prefix of each page buffer
const HEADER_SIZE: usize = size_of::<PageFlags>() + size_of::<u32>();

/// An encoder/decoder determines how a page is laid out on disk.
///
/// This integration exists in order to eventually support encryption at rest.
pub(super) trait PageEncoderDecoder {
    /// The page flags the encoder/decoder expects a page to have in order
    /// to be decoded.
    fn required_flags(&self) -> PageFlags;

    /// The fixed amount of overhead the encode/decoder has when serializing the data.
    ///
    /// This is used to ensure the right size of `buffer` is provided to the encoder
    /// and page sizes are managed correctly.
    fn overhead(&self) -> usize;

    /// Encode the given page information into the buffer slice.
    ///
    /// The page must fit within the provided buffer otherwise an error should be returned.
    fn encode(
        &self,
        page: &Page,
        buffer: &mut [MaybeUninit<u8>],
    ) -> anyhow::Result<usize>;

    /// Decode the given buffer into the page information.
    fn decode<'buf>(&self, buffer: &'buf mut [u8]) -> anyhow::Result<&'buf PageRef>;
}

#[derive(Debug, Clone)]
/// A fixed-size buffer holding [PAGE_SIZE] bytes.
pub(crate) struct PageBuffer {
    buf: Box<[MaybeUninit<u8>]>,
    len: usize,
}

impl Default for PageBuffer {
    fn default() -> Self {
        Self {
            buf: Box::new_uninit_slice(PAGE_SIZE),
            len: 0,
        }
    }
}

impl PageBuffer {
    /// Returns the initialised bytes of the page.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr() as *const u8, self.len) }
    }

    /// Clear the page buffer.
    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    fn get_writable_buffer(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.buf[HEADER_SIZE..]
    }

    fn write_header(&mut self, flags: PageFlags, len: usize) {
        let [b1, b2] = flags.to_bytes();
        self.buf[0].write(b1);
        self.buf[1].write(b2);

        let [b1, b2, b3, b4] = (len as u32).to_le_bytes();
        self.buf[2].write(b1);
        self.buf[3].write(b2);
        self.buf[4].write(b3);
        self.buf[5].write(b4);

        self.len += HEADER_SIZE;
    }

    /// Advances the internal length by `len`.
    ///
    /// # Safety
    ///
    /// The value provided must not result in the internal cursor
    /// exceeding the range of initialized bytes.
    unsafe fn advance_len(&mut self, len: usize) {
        assert!(
            self.buf.len() > (self.len + len),
            "length would exceed allocation"
        );
        self.len += len;
    }
}

/// A dynamic [PageEncoderDecoder] for encoding or decoding pages.
pub struct AnyPageEncoder(Box<dyn PageEncoderDecoder>);

impl<T> From<T> for AnyPageEncoder
where
    T: PageEncoderDecoder + 'static,
{
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl AnyPageEncoder {
    /// Split a given buffer into chunks based on the [PAGE_SIZE] and encoder overhead.
    pub fn chunk_buffer<'buf>(
        &self,
        buf: &'buf [u8],
    ) -> impl Iterator<Item = &'buf [u8]> {
        // TODO: This doesn't quite account for the few bytes of padding rkyv can do, so we are
        //       _technically_ not making 100% use of the disk space here... Although it is likely
        //       8 bytes at most.
        let chunk_size = self.0.overhead() + Page::OVERHEAD + HEADER_SIZE;
        buf.chunks(chunk_size)
    }

    /// Encode a new page and writes the full layout to the provided buffer.
    pub fn encode_page(
        &self,
        page: &Page,
        buffer: &mut PageBuffer,
    ) -> anyhow::Result<()> {
        let write_buffer = buffer.get_writable_buffer();

        let mut flags = PageFlags::default();
        flags |= self.0.required_flags();

        let bytes_written = self.0.encode(page, write_buffer)?;

        // # Safety
        // We've written N bytes as part of the encoding and can safely
        // treat them as initialised now.
        unsafe { buffer.advance_len(bytes_written) };

        buffer.write_header(flags, bytes_written);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::encode::standard::StdEncoderDecoder;
    use crate::{BlockId, PageId};

    #[test]
    fn test_page_buffer_empty() {
        let buffer = PageBuffer::default();
        assert!(buffer.as_bytes().is_empty());
    }

    #[test]
    fn test_page_buffer_write() {
        let mut buffer = PageBuffer::default();
        buffer.write_header(PageFlags::ENCODING_STD_V1, 20);
        assert_eq!(buffer.as_bytes(), &[2, 0, 20, 0, 0, 0]);

        let mut buffer = PageBuffer::default();
        buffer.write_header(PageFlags::ENCODING_STD_V1, 20);

        let write = buffer.get_writable_buffer();
        write[0].write(16);
        unsafe { buffer.advance_len(1) };
        assert_eq!(buffer.as_bytes(), &[2, 0, 20, 0, 0, 0, 16]);
    }

    #[test]
    #[should_panic]
    fn test_page_buffer_assert() {
        let mut buffer = PageBuffer::default();
        unsafe { buffer.advance_len(16 << 10) };
    }

    #[test]
    fn test_encoder_write_page() {
        let encoder = AnyPageEncoder::from(StdEncoderDecoder);

        let page = Page::new(PageId(1), BlockId(2), b"hello");

        let mut buffer = PageBuffer::default();
        encoder.encode_page(&page, &mut buffer).unwrap();
        assert_eq!(
            buffer.as_bytes().len(),
            page.serialize_size() + HEADER_SIZE + StdEncoderDecoder.overhead()
        );
    }
}
