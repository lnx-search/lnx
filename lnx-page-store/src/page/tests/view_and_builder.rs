//! page view encoding decoding tests
//!
//! This is checking that the base page data is serialized correctly and can
//! be read from the desired layout version.
use rstest::rstest;

/// Test the decoding/encoding of the raw page views ignoring any relevance to the layout versions
/// minus the max data allowance.
mod raw_view {
    use std::borrow::Cow;

    use super::*;
    use crate::page::metadata::DiskPageMetadata;
    use crate::page::{
        DiskPageBuilder,
        DiskPageView,
        LayoutVersion,
        PAGE_SIZE,
        PageEncodeBuffer,
    };
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

    #[rstest]
    #[case(PageId(0), BlockId(0), 0, 0)]
    #[case(PageId(1), BlockId(1), 1, 1 << 10)]
    #[case(PageId(1), BlockId(1), 1, 7 << 10)]
    #[should_panic]
    #[case(PageId(1), BlockId(1), 1, 12 << 10)]
    #[should_panic]
    #[case(PageId(1), BlockId(1), 1, 8 << 10)]
    fn test_decode(
        #[case] page_id: PageId,
        #[case] block_id: BlockId,
        #[case] revision: u32,
        #[case] buffer_size: usize,
    ) {
        let buffer = vec![1; buffer_size];
        let page_builder = DiskPageBuilder::new(
            page_id,
            block_id,
            revision,
            LayoutVersion::V1,
            Cow::Owned(buffer),
        );

        let mut buffer = PageEncodeBuffer::default();

        page_builder
            .encode(&mut buffer)
            .expect("encode page should fit and serialize into fixed buffer");

        let bytes_written = buffer.as_ref();

        let view = DiskPageView::decode(&bytes_written[8..])
            .unwrap_or_else(|e| panic!("page should be decoded from bytes: {e}"));
        assert_eq!(view.metadata().layout_version(), LayoutVersion::V1);
        assert_eq!(view.metadata().block(), block_id);
        assert_eq!(view.metadata().id(), page_id);
        assert_eq!(view.metadata().revision(), revision);
    }
}
