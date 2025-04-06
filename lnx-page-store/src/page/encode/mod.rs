mod std;

use super::{Page, PageFlags, PageRef};

/// An encoder/decoder determines how a page is laid out on disk.
///
/// This integration exists in order to eventually support encryption at rest.
pub trait PageEncoderDecoder {
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
    fn encode(&self, page: &Page, buffer: &mut [u8]) -> anyhow::Result<usize>;

    /// Decode the given buffer into the page information.
    fn decode<'buf>(&self, buffer: &'buf mut [u8]) -> anyhow::Result<&'buf PageRef>;
}
