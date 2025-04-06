use std::mem::MaybeUninit;

use anyhow::bail;
use rkyv::ser::Positional;
use rkyv::ser::writer::Buffer;

use super::PageEncoderDecoder;
use crate::page::{Page, PageFlags, PageRef};

/// A standard encoder/decoder for pages using rkyv.
///
/// This serializes a given page into the provided buffer with no encryption or
/// additional checks.
pub struct StdEncoderDecoder;

impl PageEncoderDecoder for StdEncoderDecoder {
    #[inline]
    fn required_flags(&self) -> PageFlags {
        PageFlags::ENCODING_STD_V1
    }

    #[inline]
    fn overhead(&self) -> usize {
        0
    }

    fn encode(
        &self,
        page: &Page,
        buffer: &mut [MaybeUninit<u8>],
    ) -> anyhow::Result<usize> {
        let mut writer = Buffer::from(buffer);
        writer = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(page, writer)
            .map_err(|e| anyhow::Error::msg("unable to serialize page").context(e))?;
        Ok(writer.pos())
    }

    fn decode<'buf>(&self, buffer: &'buf mut [u8]) -> anyhow::Result<&'buf PageRef> {
        let page = rkyv::access::<PageRef, rkyv::rancor::Error>(buffer)
            .map_err(|e| anyhow::Error::msg("page corrupted").context(e))?;

        if !page.checksums_match() {
            bail!("page corrupted, calculated checksum does not match")
        }

        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlockId, PageId};

    #[test]
    fn test_encode() {
        let page = Page::new(PageId(1), BlockId(4), b"hello, world");
        let mut buffer = vec![MaybeUninit::new(0); page.serialize_size() + 15];

        let encoder = StdEncoderDecoder;
        let size = encoder
            .encode(&page, &mut buffer)
            .expect("encode page to buffer");
        assert_eq!(size, page.serialize_size());
        assert_eq!(
            unsafe {
                std::mem::transmute::<&[MaybeUninit<u8>], &[u8]>(
                    &buffer[page.serialize_size()..],
                )
            },
            &[0; 15]
        );
    }

    #[test]
    fn test_encode_fails_buffer_too_small() {
        let page = Page::new(PageId(1), BlockId(0), b"hello, world");
        let mut buffer = vec![MaybeUninit::new(0); 12];

        let encoder = StdEncoderDecoder;
        let err = encoder
            .encode(&page, &mut buffer)
            .expect_err("encode page should fail");
        assert_eq!(
            err.to_string(),
            "overflowed buffer while writing 4 bytes into buffer of length 12 (capacity is 12)",
        );
    }

    static PAGE_BYTES: &[u8] = &[
        104, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108,
        100, // b"hello, world"
        0, 0, 0, 0, 1, 0, 0, 0, // page id
        58, 114, 171, 255, // checksum
        4, 0, 0, 0, 0, 0, 0, 0, // block id
        224, 255, 255, 255, 12, 0, 0, 0, // data len
    ];

    static INVALID_CHECKSUM_PAGE_BYTES: &[u8] = &[
        104, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108,
        100, // b"hello, world"
        0, 0, 0, 0, 1, 0, 0, 0, // page id
        58, 114, 171, 0, // checksum
        4, 0, 0, 0, 0, 0, 0, 0, // block id
        224, 255, 255, 255, 12, 0, 0, 0, // data len
    ];

    static CORRUPT_PAGE_BYTES: &[u8] = &[
        104, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108,
        100, // b"hello, world"
        0, 0, 0, 0, // padding
        1, 0, 0, 0, // page id
        58, 114, 171, 255, // checksum
        4, 0, 0, 0, 0, 0, 0, 0, // block id
        12, 255, 255, 255, // bad pointer!
        12, 0, 0, 0, // data len
    ];

    #[test]
    fn test_decode() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(PAGE_BYTES);

        let decoder = StdEncoderDecoder;
        let page = decoder.decode(&mut buffer).expect("decode page");
        assert_eq!(page.id(), PageId(1));
        assert_eq!(page.checksum(), 4289425978);
        assert_eq!(page.block(), BlockId(4));
        assert_eq!(page.data(), b"hello, world");
        assert!(page.checksums_match());
    }

    #[test]
    fn test_decode_corrupt() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(CORRUPT_PAGE_BYTES);

        let decoder = StdEncoderDecoder;
        let err = decoder
            .decode(&mut buffer)
            .expect_err("page should be corrupted");
        assert!(err.to_string().starts_with("subtree pointer overran range"));
    }

    #[test]
    fn test_decode_checksum_match() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(INVALID_CHECKSUM_PAGE_BYTES);

        let decoder = StdEncoderDecoder;
        let err = decoder
            .decode(&mut buffer)
            .expect_err("page should be corrupted");
        assert_eq!(
            err.to_string(),
            "page corrupted, calculated checksum does not match"
        );
    }
}
