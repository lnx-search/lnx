//! page encoding decoding tests
//!
//! This is checking that the base page data is serialized correctly and can
//! be read from the desired layout version.
//!
//!
//!
//!
use rstest::rstest;

mod layout_v1 {
    use std::borrow::Cow;

    use super::*;
    use crate::page::metadata::DiskPageMetadata;
    use crate::page::{DiskPageBuilder, LayoutVersion, PAGE_SIZE, PageEncodeBuffer};
    use crate::{BlockId, PageId};

    #[rstest]
    #[case(0)]
    #[case(16)]
    #[case(59)]
    #[case(2 << 10)]
    #[case(LayoutVersion::V1.max_data_size())]
    fn test_encode(#[case] buffer_size: usize) {
        let buffer = vec![1; buffer_size];
        let page_builder = DiskPageBuilder::new(
            PageId(0),
            BlockId(1),
            0,
            LayoutVersion::V1,
            Cow::Owned(buffer),
        );

        let mut buffer = PageEncodeBuffer::default();

        page_builder
            .encode(&mut buffer)
            .expect("encode page should fit and serialize into fixed buffer");

        let bytes_written = buffer.as_ref();
        assert_eq!(
            bytes_written.len(),
            DiskPageMetadata::SERIALIZED_SIZE + 8 + buffer_size
        );
    }

    #[rstest]
    #[case(PAGE_SIZE)]
    #[case(LayoutVersion::V1.max_data_size() + 1)]
    #[should_panic]
    fn test_encode_panic_on_too_big(#[case] buffer_size: usize) {
        let buffer = vec![1; buffer_size];
        let _page_builder = DiskPageBuilder::new(
            PageId(0),
            BlockId(1),
            0,
            LayoutVersion::V1,
            Cow::Owned(buffer),
        );
    }

    fn test_decode() {}
}
