use std::borrow::Cow;

use rstest::rstest;

use crate::page::version::VersionProcessorRegistry;
use crate::page::{
    DiskPageBuilder,
    IntegrityCheckConditions,
    LayoutVersion,
    PAGE_SIZE,
    PageDecodeError,
    PageEncodeBuffer,
    decode_page,
    encode_page,
};
use crate::{BlockId, PageId};

#[test]
fn test_encode_page_missing_registry() {
    let registry = VersionProcessorRegistry::default();

    let mut buffer = PageEncodeBuffer::default();

    let page_builder = DiskPageBuilder::new(
        PageId(0),
        BlockId(0),
        0,
        LayoutVersion::V1Enc,
        Cow::Owned(vec![1; 10]),
    );
    let error = encode_page(&registry, page_builder, &mut buffer)
        .expect_err("encoder should error because of registry missing version");
    assert_eq!(
        error.to_string(),
        PageDecodeError::ProcessorNotFound(LayoutVersion::V1Enc).to_string()
    );
}

#[test]
fn test_decode_page_registry_missing_version() {
    let registry = VersionProcessorRegistry::for_test();
    let mut page_data = encode_inner(&registry, LayoutVersion::V1Enc, 512);

    let checks = IntegrityCheckConditions {
        block_id: BlockId(1),
        page_id: PageId(0),
    };
    let registry = VersionProcessorRegistry::with_default_processors();
    let error = decode_page(&registry, page_data.as_mut_slice(), checks)
        .expect_err("system should not decode page with unknown version");
    assert_eq!(
        error.to_string(),
        PageDecodeError::ProcessorNotFound(LayoutVersion::V1Enc).to_string()
    );
}

#[test]
fn test_decode_page_unknown_version() {
    let registry = VersionProcessorRegistry::with_default_processors();
    let checks = IntegrityCheckConditions {
        block_id: BlockId(1),
        page_id: PageId(0),
    };

    let mut invalid_page = vec![64; PAGE_SIZE];
    let error = decode_page(&registry, &mut invalid_page, checks)
        .expect_err("system should not decode page with unknown version");
    assert_eq!(
        error.to_string(),
        PageDecodeError::UnknownLayoutVersion("0x4040".into()).to_string()
    );
}

#[rstest]
#[case(LayoutVersion::V1, 0)]
#[case(LayoutVersion::V1, 512)]
#[case(LayoutVersion::V1, 4 << 10)]
#[case(LayoutVersion::V1, 7 << 10)]
#[should_panic]
#[case(LayoutVersion::V1, 8 << 10)]
#[case(LayoutVersion::V1Enc, 0)]
#[case(LayoutVersion::V1Enc, 512)]
#[case(LayoutVersion::V1Enc, 4 << 10)]
#[case(LayoutVersion::V1Enc, 7 << 10)]
#[should_panic]
#[case(LayoutVersion::V1Enc, 8 << 10)]
fn test_encode(#[case] layout_version: LayoutVersion, #[case] buffer_size: usize) {
    let registry = VersionProcessorRegistry::for_test();

    encode_inner(&registry, layout_version, buffer_size);
}

#[rstest]
#[case(LayoutVersion::V1, 0, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1, 512, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1, 4 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1, 7 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1, 8 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1, 0, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1, 512, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(1) })]
#[should_panic]
#[case(LayoutVersion::V1, 512, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(1) })]
#[case(LayoutVersion::V1Enc, 0, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1Enc, 512, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1Enc, 4 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1Enc, 7 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1Enc, 8 << 10, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1Enc, 0, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(0) })]
#[should_panic]
#[case(LayoutVersion::V1Enc, 512, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(1) })]
#[should_panic]
#[case(LayoutVersion::V1Enc, 512, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(1) })]
fn test_encode_decode(
    #[case] layout_version: LayoutVersion,
    #[case] buffer_size: usize,
    #[case] checks: IntegrityCheckConditions,
) {
    let registry = VersionProcessorRegistry::for_test();

    let mut buffer = encode_inner(&registry, layout_version, buffer_size);

    let page = decode_page(&registry, buffer.as_mut_slice(), checks)
        .expect("decode page from data");

    assert_eq!(page.metadata().layout_version(), layout_version);
    assert_eq!(page.metadata().block(), BlockId(0));
    assert_eq!(page.metadata().id(), PageId(0));
    assert_eq!(page.data(), vec![1; buffer_size]);
}

#[rstest]
#[case(LayoutVersion::V1, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(0) })]
#[case(LayoutVersion::V1, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(1) })]
#[case(LayoutVersion::V1, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(1) })]
#[should_panic]
#[case(LayoutVersion::V1, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
#[case(LayoutVersion::V1Enc, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(0) })]
#[case(LayoutVersion::V1Enc, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(1) })]
#[case(LayoutVersion::V1Enc, IntegrityCheckConditions { block_id: BlockId(1), page_id: PageId(1) })]
#[should_panic]
#[case(LayoutVersion::V1Enc, IntegrityCheckConditions { block_id: BlockId(0), page_id: PageId(0) })]
fn test_integrity_check_fails(
    #[case] layout_version: LayoutVersion,
    #[case] checks: IntegrityCheckConditions,
) {
    let registry = VersionProcessorRegistry::for_test();

    let mut buffer = encode_inner(&registry, layout_version, 512);

    let error = decode_page(&registry, buffer.as_mut_slice(), checks)
        .expect_err("page should not pass integrity check");

    assert!(
        matches!(error, PageDecodeError::IntegrityCheckFailed),
        "integrity check error should be returned, got: {:?}",
        error,
    );
}

fn encode_inner(
    registry: &VersionProcessorRegistry,
    layout_version: LayoutVersion,
    buffer_size: usize,
) -> PageEncodeBuffer {
    let mut buffer = PageEncodeBuffer::default();

    let page_builder = DiskPageBuilder::new(
        PageId(0),
        BlockId(0),
        0,
        layout_version,
        Cow::Owned(vec![1; buffer_size]),
    );

    encode_page(registry, page_builder, &mut buffer).expect("encode page data");

    buffer
}
