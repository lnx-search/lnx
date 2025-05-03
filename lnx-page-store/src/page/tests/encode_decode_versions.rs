use rstest::rstest;
use std::borrow::Cow;
use crate::page::{decode_page, encode_page, DiskPageBuilder, IntegrityCheckConditions, LayoutVersion, PageDecodeError, PageEncodeBuffer};
use crate::page::version::VersionProcessorRegistry;
use crate::{BlockId, PageId};
use super::*;

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
fn test_encode(
    #[case] layout_version: LayoutVersion,
    #[case] buffer_size: usize,
) {
    let registry = VersionProcessorRegistry::for_test();

    encode_inner(
        &registry,
        layout_version,
        buffer_size,
    );
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

    let mut buffer = encode_inner(
        &registry,
        layout_version,
        buffer_size,
    );    
    
    let page = decode_page(
        &registry,
        buffer.as_mut_slice(),
        checks,
    ).expect("decode page from data");

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

    let mut buffer = encode_inner(
        &registry,
        layout_version,
        512,
    );

    let error = decode_page(
        &registry,
        buffer.as_mut_slice(),
        checks,
    ).expect_err("page should not pass integrity check");
    
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
    encode_page(
        &registry,
        page_builder,
        &mut buffer,
    ).expect("encode page data");
    
    buffer
}