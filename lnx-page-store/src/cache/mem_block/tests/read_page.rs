use std::collections::BTreeSet;
use std::ops::Range;

use crate::cache::mem_block::{PageIndex, PageOrRetry, PageSize, VirtualMemoryBlock};

#[rstest::rstest]
#[case::read_8kb_x1_page(1, PageSize::Std8KB)]
#[case::read_8kb_x2_page(2, PageSize::Std8KB)]
#[case::read_8kb_x4_page(4, PageSize::Std8KB)]
#[case::read_8kb_x1024_page(1024, PageSize::Std8KB)]
#[case::read_32kb_x1_page(1, PageSize::Std32KB)]
#[case::read_32kb_x2_page(2, PageSize::Std32KB)]
#[case::read_32kb_x4_page(4, PageSize::Std32KB)]
#[case::read_32kb_x1024_page(1024, PageSize::Std32KB)]
#[case::read_64kb_x1_page(1, PageSize::Std64KB)]
#[case::read_64kb_x2_page(2, PageSize::Std64KB)]
#[case::read_64kb_x4_page(4, PageSize::Std64KB)]
#[case::read_64kb_x1024_page(1024, PageSize::Std64KB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x1_huge_page(1, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x2_huge_page(2, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x4_huge_page(4, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x512_huge_page(1024, PageSize::Huge2MB)]
fn test_memory_read_empty_block(#[case] num_pages: usize, #[case] page_size: PageSize) {
    let block = VirtualMemoryBlock::allocate(num_pages, page_size)
        .expect("virtual memory block should be created");
    let mut prepared_read = block.prepare_read(PageIndex(0)..PageIndex(num_pages));
    prepared_read = prepared_read
        .try_finish()
        .expect_err("read should not be completed as no pages are allocated");
    assert_eq!(prepared_read.outstanding_writes().len(), num_pages);
}

#[rstest::rstest]
#[case::read_8kb_x1_page(1, PageSize::Std8KB, 0..1)]
#[case::read_8kb_x2_page(2, PageSize::Std8KB, 0..1)]
#[case::read_8kb_x4_page(4, PageSize::Std8KB, 1..3)]
#[case::read_8kb_x1024_page(1024, PageSize::Std8KB, 123..125)]
#[case::read_32kb_x1_page(1, PageSize::Std32KB, 0..1)]
#[case::read_32kb_x2_page(2, PageSize::Std32KB, 0..1)]
#[case::read_32kb_x4_page(4, PageSize::Std32KB, 1..2)]
#[case::read_32kb_x1024_page(1024, PageSize::Std32KB, 123..125)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x1_huge_page(1, PageSize::Huge2MB, 0..1)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x2_huge_page(2, PageSize::Huge2MB, 0..1)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x4_huge_page(4, PageSize::Huge2MB, 1..3)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_2mb_x512_huge_page(1024, PageSize::Huge2MB, 123..125)]
fn test_memory_read_sub_span_empty_block(
    #[case] num_pages: usize,
    #[case] page_size: PageSize,
    #[case] span: Range<usize>,
) {
    let block = VirtualMemoryBlock::allocate(num_pages, page_size)
        .expect("virtual memory block should be created");
    let mut prepared_read =
        block.prepare_read(PageIndex(span.start)..PageIndex(span.end));
    prepared_read = prepared_read
        .try_finish()
        .expect_err("read should not be completed as no pages are allocated");
    assert_eq!(prepared_read.outstanding_writes().len(), span.len());
}

#[rstest::rstest]
// 8KB pages
#[case::read_one_filled_8kb_x1_page(1, PageSize::Std8KB, 0..1, 0..1)]
#[case::read_all_filled_8kb_x4_page(4, PageSize::Std8KB, 0..4, 0..4)]
#[case::read_one_filled_8kb_x4_page_start(4, PageSize::Std8KB, 0..4, 0..1)]
#[case::read_one_filled_8kb_x4_page_mid(4, PageSize::Std8KB, 0..4, 2..3)]
#[case::read_one_filled_8kb_x4_page_end(4, PageSize::Std8KB, 0..4, 3..4)]
#[case::read_two_filled_8kb_x4_page_start(4, PageSize::Std8KB, 0..4, 0..2)]
#[case::read_two_filled_8kb_x4_page_mid(4, PageSize::Std8KB, 0..4, 1..3)]
#[case::read_two_filled_8kb_x4_page_end(4, PageSize::Std8KB, 0..4, 2..4)]
#[should_panic]
#[case::read_out_of_bounds_8kb_x4(4, PageSize::Std8KB, 0..4, 3..6)]
// 32KB pages
#[case::read_one_filled_32kb_x1_page(1, PageSize::Std32KB, 0..1, 0..1)]
#[case::read_all_filled_32kb_x4_page(4, PageSize::Std32KB, 0..4, 0..4)]
#[case::read_one_filled_32kb_x4_page_start(4, PageSize::Std32KB, 0..4, 0..1)]
#[case::read_one_filled_32kb_x4_page_mid(4, PageSize::Std32KB, 0..4, 2..3)]
#[case::read_one_filled_32kb_x4_page_end(4, PageSize::Std32KB, 0..4, 3..4)]
#[case::read_two_filled_32kb_x4_page_start(4, PageSize::Std32KB, 0..4, 0..2)]
#[case::read_two_filled_32kb_x4_page_mid(4, PageSize::Std32KB, 0..4, 1..3)]
#[case::read_two_filled_32kb_x4_page_end(4, PageSize::Std32KB, 0..4, 2..4)]
#[should_panic]
#[case::read_out_of_bounds_32kb_x4(4, PageSize::Std32KB, 0..4, 3..6)]
// 2MB Huge pages
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_one_filled_2mb_x1_huge_page(1, PageSize::Huge2MB, 0..1, 0..1)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_all_filled_2mb_x4_huge_page(4, PageSize::Huge2MB, 0..4, 0..4)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_one_filled_2mb_x4_huge_page_start(4, PageSize::Huge2MB, 0..4, 0..1)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_one_filled_2mb_x4_huge_page_mid(4, PageSize::Huge2MB, 0..4, 2..3)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_one_filled_2mb_x4_huge_page_end(4, PageSize::Huge2MB, 0..4, 3..4)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_two_filled_2mb_x4_huge_page_start(4, PageSize::Huge2MB, 0..4, 0..2)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_two_filled_2mb_x4_huge_page_mid(4, PageSize::Huge2MB, 0..4, 1..3)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_two_filled_2mb_x4_huge_page_end(4, PageSize::Huge2MB, 0..4, 2..4)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[should_panic]
#[case::read_out_of_bounds_2mb_x4(4, PageSize::Huge2MB, 0..4, 3..6)]
fn test_memory_read_single_range_allocated(
    #[case] num_pages: usize,
    #[case] page_size: PageSize,
    #[case] allocate_pages: Range<usize>,
    #[case] read_pages: Range<usize>,
) {
    let block = create_block_with_allocation(allocate_pages, num_pages, page_size);

    let prepared_read =
        block.prepare_read(PageIndex(read_pages.start)..PageIndex(read_pages.end));
    let block = prepared_read
        .try_finish()
        .expect("prepared read should be completed as all pages are allocated");
    assert_eq!(
        block.len(),
        read_pages.len() * page_size as usize,
        "read length does not match expected length",
    );

    let data = vec![1; page_size as usize];

    let checksum = crc32fast::hash(&block);
    let mut expected_checksum_hasher = crc32fast::Hasher::new();
    for _ in 0..read_pages.len() {
        expected_checksum_hasher.update(&data);
    }
    let expected_checksum = expected_checksum_hasher.finalize();
    assert_eq!(
        checksum, expected_checksum,
        "read result does not match data written"
    );
}

#[rstest::rstest]
#[case::read_outstanding_writes_8kb_x4_page(4, PageSize::Std8KB, 0..2, 2..4)]
#[case::read_outstanding_writes_32kb_x4_page(4, PageSize::Std32KB, 0..2, 2..4)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::read_outstanding_writes_2mb_x4_page(4, PageSize::Huge2MB, 0..2, 2..4)]
fn test_memory_read_partially_allocated_err_outstanding_writes(
    #[case] num_pages: usize,
    #[case] page_size: PageSize,
    #[case] allocate_pages: Range<usize>,
    #[case] read_pages: Range<usize>,
) {
    let block =
        create_block_with_allocation(allocate_pages.clone(), num_pages, page_size);

    let prepared_read =
        block.prepare_read(PageIndex(read_pages.start)..PageIndex(read_pages.end));
    let prepared_read = prepared_read
        .try_finish()
        .expect_err("not all writes are allocated, finish should error");

    let mut pages_unallocated = BTreeSet::new();
    pages_unallocated.extend(read_pages.clone());
    for page in allocate_pages {
        pages_unallocated.remove(&page);
    }

    let pages_unallocated = pages_unallocated
        .into_iter()
        .map(PageIndex)
        .collect::<Vec<_>>();
    assert_eq!(prepared_read.outstanding_writes(), &pages_unallocated);
}

#[test]
fn test_reads_respect_dirty_marker() {
    let block = create_block_with_allocation(0..8, 8, PageSize::Std8KB);

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    let read = prepared_read.try_finish().unwrap();
    println!("read: {:?}", read);

    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    prepared_read.try_finish().expect_err(
        "reads after the permit should not be able to complete without writes",
    );

    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(4)))
        .unwrap();
    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(5)))
        .unwrap();

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    println!("read: {:?}", prepared_read);

    prepared_read.try_finish().expect_err(
        "reads after the permit should not be able to complete without writes",
    );
}

#[test]
fn test_reads_respect_marked_for_eviction_marker() {
    let block = create_block_with_allocation(0..8, 8, PageSize::Std8KB);

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    let read = prepared_read.try_finish().unwrap();
    println!("read: {:?}", read);

    let _permit = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    prepared_read.try_finish().expect_err(
        "reads after the permit should not be able to complete without writes",
    );

    let _permit = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(4)))
        .unwrap();
    let _permit = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(5)))
        .unwrap();

    let prepared_read = block.prepare_read(PageIndex(0)..PageIndex(8));
    println!("read: {:?}", prepared_read);

    prepared_read.try_finish().expect_err(
        "reads after the permit should not be able to complete without writes",
    );
}

fn create_block_with_allocation(
    pages: Range<usize>,
    num_pages: usize,
    page_size: PageSize,
) -> VirtualMemoryBlock {
    let block = VirtualMemoryBlock::allocate(num_pages, page_size)
        .expect("virtual memory block should be created");

    let data = vec![1; page_size as usize];

    for page in pages {
        let permit = block
            .try_prepare_for_write(PageIndex(page))
            .expect("get page write permit");
        block.write_page(permit, &data);
    }

    block
}
