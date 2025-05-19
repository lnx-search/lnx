use crate::cache::mem_block::*;

#[rstest::rstest]
#[case::std_8kb_x0_pages(0, PageSize::Std8KB)]
#[case::std_8kb_x1_pages(1, PageSize::Std8KB)]
#[case::std_8kb_x2_pages(2, PageSize::Std8KB)]
#[case::std_8kb_xn_pages(1310720, PageSize::Std8KB)]
#[case::std_32kb_x0_pages(0, PageSize::Std32KB)]
#[case::std_32kb_x1_pages(1, PageSize::Std32KB)]
#[case::std_32kb_x2_pages(2, PageSize::Std32KB)]
#[case::std_32kb_xn_pages(1310720, PageSize::Std32KB)]
#[case::std_64kb_x0_pages(0, PageSize::Std64KB)]
#[case::std_64kb_x1_pages(1, PageSize::Std64KB)]
#[case::std_64kb_x2_pages(2, PageSize::Std64KB)]
#[case::std_64kb_xn_pages(1310720, PageSize::Std64KB)]
#[case::std_128kb_x0_pages(0, PageSize::Std64KB)]
#[case::std_128kb_x1_pages(1, PageSize::Std64KB)]
#[case::std_128kb_x2_pages(2, PageSize::Std64KB)]
#[case::std_128kb_xn_pages(1310720, PageSize::Std64KB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::huge_2mb_x0_pages(0, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::huge_2mb_x1_pages(1, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::huge_2mb_x2_pages(2, PageSize::Huge2MB)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::huge_2mb_xn_pages(5120, PageSize::Huge2MB)]
fn test_create_memory_block(#[case] num_pages: usize, #[case] page_size: PageSize) {
    let block = VirtualMemoryBlock::allocate(num_pages, page_size)
        .expect("virtual memory block should be created");
    assert_eq!(block.num_pages(), num_pages);
}

#[rstest::rstest]
#[case::write_8kb_page_full(1, 0, PageSize::Std8KB, 8 << 10)]
#[case::write_8kb_page_out_of_many_full(3, 1, PageSize::Std8KB, 8 << 10)]
#[should_panic]
#[case::write_8kb_page_data_too_big(1, 0, PageSize::Std8KB, 16 << 10)]
#[should_panic]
#[case::write_8kb_page_too_small(1, 0, PageSize::Std8KB, 4 << 10)]
#[case::write_32kb_page_full(1, 0, PageSize::Std32KB, 32 << 10)]
#[case::write_32kb_page_out_of_many_full(3, 1, PageSize::Std32KB, 32 << 10)]
#[should_panic]
#[case::write_32kb_page_out_of_many_too_small(3, 1, PageSize::Std32KB, 4 << 10)]
#[should_panic]
#[case::write_32kb_page_data_too_big(1, 0, PageSize::Std32KB, 48 << 10)]
#[should_panic]
#[case::write_32kb_page_too_small(1, 0, PageSize::Std32KB, 4 << 10)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::write_huge_page_full(1, 0, PageSize::Huge2MB, 2 << 20)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[case::write_huge_page_out_of_many_full(3, 1, PageSize::Huge2MB, 2 << 20)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[should_panic]
#[case::write_huge_data_too_big(1, 0, PageSize::Huge2MB, 4 << 20)]
#[cfg_attr(not(feature = "test-huge-pages"), ignore)]
#[should_panic]
#[case::write_huge_data_too_small(1, 0, PageSize::Huge2MB, 1 << 20)]
fn test_write_page(
    #[case] num_pages: usize,
    #[case] write_page_at: usize,
    #[case] page_size: PageSize,
    #[case] data_size: usize,
) {
    let block = VirtualMemoryBlock::allocate(num_pages, page_size)
        .expect("virtual memory block should be created");

    let permit = block
        .try_prepare_for_write(PageIndex(write_page_at))
        .expect("write should be prepared successfully");
    let data = vec![1; data_size];
    block.write_page(permit, &data);

    let state = block.get_page_flags(PageIndex(write_page_at));
    assert!(state.is_allocated());
    assert!(!state.is_marked_for_eviction());
    assert!(!state.is_dirty());
    assert!(!state.is_free());
    assert_eq!(state.extract_ticket_id(), Some(0));

    check_page_bytes(&block, write_page_at, page_size);
}

#[test]
fn test_write_page_err_already_allocated() {
    let block = create_block_with_1_allocated_page();

    let err = block
        .try_prepare_for_write(PageIndex(0))
        .expect_err("write should not be permitted because page is already allocated");
    assert_eq!(
        err.to_string(),
        PrepareWriteError::AlreadyAllocated.to_string()
    );
}

#[test]
fn test_write_page_err_locked() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let permit = block
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");

    let err = block
        .try_prepare_for_write(PageIndex(0))
        .expect_err("write should not be permitted because page is locked");
    assert_eq!(err.to_string(), PrepareWriteError::Locked.to_string());

    drop(permit);

    let _permit = block
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");
}

#[should_panic]
#[test]
fn test_write_page_panic_check_uid() {
    let block1 = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");
    let block2 = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let permit = block1
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");

    // Panic here! The UIDs should not match.
    let data = vec![1; 8 << 10];
    block2.write_page(permit, &data);
}

#[test]
fn test_write_page_revert_eviction_marker() {
    let block = create_block_with_1_allocated_page();

    let _permit = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    let state = block.get_page_flags(PageIndex(0));
    assert!(state.is_allocated());
    assert!(state.is_marked_for_eviction());
    assert!(!state.is_dirty());
    assert!(!state.is_free());
    assert_eq!(state.extract_ticket_id(), Some(2));

    let err = block
        .try_prepare_for_write(PageIndex(0))
        .expect_err("write should revert eviction marker");
    assert_eq!(
        err.to_string(),
        PrepareWriteError::AlreadyAllocated.to_string()
    );

    let state = block.get_page_flags(PageIndex(0));
    assert!(state.is_allocated());
    assert!(!state.is_marked_for_eviction());
    assert!(!state.is_dirty());
    assert!(!state.is_free());
    assert_eq!(state.extract_ticket_id(), Some(3));
}

#[test]
fn test_write_page_cannot_revert_dirty_marker() {
    let block = create_block_with_1_allocated_page();

    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    let state = block.get_page_flags(PageIndex(0));
    assert!(!state.is_allocated());
    assert!(state.is_marked_for_eviction());
    assert!(state.is_dirty());
    assert!(!state.is_free());
    assert_eq!(state.extract_ticket_id(), Some(2));

    let _permit = block
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");

    let state = block.get_page_flags(PageIndex(0));
    assert!(!state.is_allocated());
    assert!(state.is_marked_for_eviction());
    assert!(state.is_dirty());
    assert!(!state.is_free());
    assert_eq!(state.extract_ticket_id(), Some(2));
}

#[test]
fn test_mark_dirty_page_ok() {
    let block = create_block_with_1_allocated_page();

    let permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    block.for_test_advance_ticket_counter(256);

    block
        .try_free(&permit)
        .expect("try free call should succeed with permit");
}

#[test]
fn test_mark_dirty_err_in_use() {
    let block = create_block_with_1_allocated_page();

    let permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    let err = block
        .try_free(&permit)
        .expect_err("call should error as generation is still live");
    assert_eq!(err.to_string(), TryFreeError::InUse.to_string());
}

#[test]
fn test_mark_dirty_err_already_free() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let err = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect_err("mark call should error");
    assert_eq!(
        err.to_string(),
        PrepareDirtyEvictionError::AlreadyFree.to_string()
    );
}

#[test]
fn test_mark_dirty_err_stale() {
    let block = create_block_with_1_allocated_page();

    let permit1 = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let permit2 = block
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");
    let data = vec![1; 8 << 10];
    block.write_page(permit2, &data);

    block.for_test_advance_ticket_counter(256);

    let err = block
        .try_free(&permit1)
        .expect_err("try free ticket check should use permit2's ID");
    assert_eq!(err.to_string(), TryFreeError::PermitExpired.to_string());
}

#[test]
fn test_mark_dirty_err_locked() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let write_permit = block.try_prepare_for_write(PageIndex(0)).unwrap();

    let err = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect_err("call should error because lock is held");
    drop(write_permit);
    assert_eq!(err.to_string(), "page locked");
}

#[test]
fn test_mark_dirty_retry() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let write_permit = block.try_prepare_for_write(PageIndex(0)).unwrap();

    let err = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .expect_err("call should error because lock is held");
    drop(write_permit);
    let PrepareDirtyEvictionError::PageLocked(retry) = err else {
        panic!("unexpected error")
    };

    let err = block
        .try_dirty_page(PageOrRetry::Retry(retry))
        .expect_err("call should still error because the page is already free");
    assert_eq!(err.to_string(), "page already free");
}

#[test]
fn test_mark_dirty_err_on_already_dirty_page() {
    let block = create_block_with_1_allocated_page();

    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let err = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .unwrap_err();
    assert_eq!(err.to_string(), "page already dirty");
}

#[test]
fn test_mark_for_revertible_eviction_ok() {
    let block = create_block_with_1_allocated_page();

    let permit = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    block.for_test_advance_ticket_counter(256);

    block
        .try_free(&permit)
        .expect("try free call should succeed with permit");
}

#[test]
fn test_mark_for_revertible_eviction_err_already_free() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let err = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect_err("mark call should error");
    assert_eq!(
        err.to_string(),
        PrepareRevertibleEvictionError::AlreadyFree.to_string()
    );
}

#[test]
fn test_mark_for_revertible_eviction_err_stale() {
    let block = create_block_with_1_allocated_page();

    let permit1 = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let _permit2 = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    block.for_test_advance_ticket_counter(256);

    let err = block
        .try_free(&permit1)
        .expect_err("try free ticket check should use permit2's ID");
    assert_eq!(err.to_string(), TryFreeError::PermitExpired.to_string());
}

#[test]
fn test_mark_for_revertible_eviction_err_on_dirty_page() {
    let block = create_block_with_1_allocated_page();

    let _permit = block
        .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
        .unwrap();

    let err = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .unwrap_err();
    assert_eq!(err.to_string(), "page dirty");
}

#[test]
fn test_mark_for_revertible_eviction_err_locked() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let write_permit = block.try_prepare_for_write(PageIndex(0)).unwrap();

    let err = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect_err("call should error because lock is held");
    drop(write_permit);
    assert_eq!(err.to_string(), "page locked");
}

#[test]
fn test_mark_for_revertible_eviction_retry() {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let write_permit = block.try_prepare_for_write(PageIndex(0)).unwrap();

    let err = block
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect_err("call should error because lock is held");
    drop(write_permit);
    let PrepareRevertibleEvictionError::PageLocked(retry) = err else {
        panic!("unexpected error")
    };

    let err = block
        .try_mark_for_revertible_eviction(PageOrRetry::Retry(retry))
        .expect_err("call should still error because the page is already free");
    assert_eq!(err.to_string(), "page already free");
}

#[should_panic]
#[test]
fn test_try_free_panic_uid() {
    let block1 = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");
    let block2 = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let permit = block1
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");

    let data = vec![1; 8 << 10];
    block1.write_page(permit, &data);

    let permit = block2
        .try_mark_for_revertible_eviction(PageOrRetry::Page(PageIndex(0)))
        .expect("mark page for eviction");

    // Panic! UIDs should not match
    block1.try_free(&permit).unwrap();
}

fn check_page_bytes(block: &VirtualMemoryBlock, page_at: usize, page_size: PageSize) {
    let mut ptr = block.for_test_get_raw_page_ptr(PageIndex(page_at));
    unsafe {
        let buf = ptr.access_uninit();
        let page = std::slice::from_raw_parts(buf.as_ptr() as *const u8, ptr.len());

        let data = vec![1; page_size as usize];
        assert_eq!(page, data);
    };
}

fn create_block_with_1_allocated_page() -> VirtualMemoryBlock {
    let block = VirtualMemoryBlock::allocate(1, PageSize::Std8KB)
        .expect("virtual memory block should be created");

    let permit = block
        .try_prepare_for_write(PageIndex(0))
        .expect("write should be prepared successfully");

    let data = vec![1; 8 << 10];
    block.write_page(permit, &data);

    block
}
