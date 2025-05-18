use crate::cache::mem_block::*;

#[rstest::rstest]
#[case::zero_sized_block(0, PageSize::Standard, 0)]
#[case::small_block(64, PageSize::Standard, 1)]
#[case::single_page_8kb_block(8 << 10, PageSize::Standard, 1)]
#[case::double_page_9kb_block(9 << 10, PageSize::Standard, 2)]
#[case::large_virtual_memory(10 << 30, PageSize::Standard, 1310720)]
#[cfg_attr(
    feature = "test-huge-pages",
    case::huge_zero_sized_block(0, PageSize::Huge, 0)
)]
#[cfg_attr(
    feature = "test-huge-pages",
    case::huge_small_block(64, PageSize::Huge, 1)
)]
#[cfg_attr(feature = "test-huge-pages", case::huge_single_page_8kb_block(8 << 10, PageSize::Huge, 1))]
#[cfg_attr(feature = "test-huge-pages", case::huge_double_page_9kb_block(4 << 20, PageSize::Huge, 2))]
#[cfg_attr(feature = "test-huge-pages", case::huge_large_virtual_memory(10 << 30, PageSize::Huge, 5120))]
fn test_create_memory_block(
    #[case] size: usize,
    #[case] page_size: PageSize,
    #[case] target_num_pages: usize,
) {
    let block = VirtualMemoryBlock::allocate(size, page_size)
        .expect("virtual memory block should be created");
    assert_eq!(block.num_pages(), target_num_pages);
}

#[test]
fn test_mark_dirty_page_ok() {}

#[test]
fn test_mark_dirty_err_already_free() {}

#[test]
fn test_mark_dirty_err_stale_before_lock() {}

#[test]
fn test_mark_dirty_err_stale_after_lock() {}

#[test]
fn test_mark_dirty_err_locked() {}

#[test]
fn test_mark_for_revertible_eviction_ok() {}

#[test]
fn test_mark_for_revertible_eviction_err_already_free() {}

#[test]
fn test_mark_for_revertible_eviction_err_stale_before_lock() {}

#[test]
fn test_mark_for_revertible_eviction_err_stale_after_lock() {}

#[test]
fn test_mark_for_revertible_eviction_err_locked() {}

#[test]
fn test_try_free_ok() {}

#[test]
fn test_try_free_err_permit_expired_before_lock() {}

#[test]
fn test_try_free_err_permit_expired_after_lock() {}

#[test]
fn test_try_free_err_page_in_use() {}

#[test]
fn test_try_free_err_locked() {}
