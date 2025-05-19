use std::ops::Range;

use smallvec::SmallVec;

use super::ticket::TicketGuard;
use super::{PageIndex, VirtualMemoryBlock};

type PageRanges = SmallVec<[PageIndex; 64]>;

/// A prepared read allows for reading multiple pages while incrementally
/// filling any gaps the span of memory may have with unallocated data.
pub struct PreparedRead<'block> {
    guard: TicketGuard,
    parent: &'block VirtualMemoryBlock,
    page_range: Range<PageIndex>,
    /// Sets of page ranges that need to be allocated before we can read.
    unknown_page_ranges: PageRanges,
}

impl<'block> PreparedRead<'block> {
    /// Create a new [PageIndex] for the given range of pages.
    pub(super) fn for_page_range(
        guard: TicketGuard,
        parent: &'block VirtualMemoryBlock,
        page_range: Range<PageIndex>,
    ) -> Self {
        

        Self {
            guard,
            parent,
            page_range,
            allocated_page_ranges,
            unknown_page_ranges,
        }
    }

    /// Try read all pages and produce a single contiguous slice.
    pub fn try_finish(&mut self) -> Result<BlockRead, TryFinishError> {
        for range in self.unknown_page_ranges.iter() {
            for page in iter_pages(range.clone()) {
                let flags = self.parent.get_page_flags(page);
                
                if flags.is_readable()
            }
        }
        
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
/// The read cannot be finished because one of the prerequisites has not been met.
pub enum TryFinishError {
    #[error("read has outstanding writes")]
    /// The read has outstanding writes.
    OutstandingWrites,
    #[error("inflight locks")]
    /// Some pages have locks currently inflight that must complete
    /// before the read can be finished.
    LocksInFlight,
}

pub struct BlockRead {}

fn get_unallocated_pages(
    block: &VirtualMemoryBlock,
    page_range: Range<PageIndex>,
) -> PageRanges {
    let mut allocated_page_ranges = SmallVec::new();
    let mut unknown_page_ranges = SmallVec::new();

    let mut start_state = page_range.start;
    let mut is_allocated = false;
    for page in iter_pages(page_range.clone()) {
        let flags = block.get_page_flags(page);

        let swap_ranges = flags.is_allocated() != is_allocated;

        if swap_ranges {
            let range = start_state..page;
            if !is_empty_range(&range) && !is_allocated {
                unknown_page_ranges.push(range);
            } else if !is_empty_range(&range) && is_allocated {
                allocated_page_ranges.push(range);
            }

            is_allocated = !is_allocated;
            start_state = page;
        }
    }

    let range = start_state..page_range.end;
    if !is_empty_range(&range) && !is_allocated {
        unknown_page_ranges.push(range);
    } else if !is_empty_range(&range) && is_allocated {
        allocated_page_ranges.push(range);
    }

    (allocated_page_ranges, unknown_page_ranges)
}

// Eventually we can implement the `Step` trait when it is stable.
fn iter_pages(range: Range<PageIndex>) -> impl Iterator<Item = PageIndex> {
    (range.start.0..range.end.0).map(PageIndex)
}

fn is_empty_range(range: &Range<PageIndex>) -> bool {
    range.start == range.end
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::cache::mem_block::PageSize;

    #[rstest::rstest]
    #[case::full_range(
        PageIndex(0)..PageIndex(8),
        &[PageIndex(0)..PageIndex(2), PageIndex(3)..PageIndex(6)],
        &[PageIndex(2)..PageIndex(3), PageIndex(6)..PageIndex(8)],
    )]
    #[case::range_1(PageIndex(0)..PageIndex(2), &[PageIndex(0)..PageIndex(2)], &[])]
    #[case::range_2(PageIndex(3)..PageIndex(6), &[PageIndex(3)..PageIndex(6)], &[])]
    #[case::range_3(PageIndex(2)..PageIndex(3), &[], &[PageIndex(2)..PageIndex(3)])]
    #[case::range_4(PageIndex(6)..PageIndex(8), &[], &[PageIndex(6)..PageIndex(8)])]
    #[case::range_5(PageIndex(6)..PageIndex(8), &[], &[PageIndex(6)..PageIndex(8)])]
    #[case::range_6(PageIndex(1)..PageIndex(4), &[PageIndex(1)..PageIndex(2), PageIndex(3)..PageIndex(4)], &[PageIndex(2)..PageIndex(3)])]
    fn test_get_allocated_and_unknown_pages(
        #[case] page_range: Range<PageIndex>,
        #[case] expected_allocated_pages: &[Range<PageIndex>],
        #[case] expected_unallocated_pages: &[Range<PageIndex>],
    ) {
        let block = VirtualMemoryBlock::allocate(8, PageSize::Standard)
            .expect("virtual memory block should be created");

        allocate_page(&block, 0);
        allocate_page(&block, 1);

        allocate_page(&block, 3);
        allocate_page(&block, 4);
        allocate_page(&block, 5);

        let (allocated, unallocated) =
            get_allocated_and_unknown_pages(&block, page_range);

        assert_eq!(allocated.as_slice(), expected_allocated_pages);
        assert_eq!(unallocated.as_slice(), expected_unallocated_pages);
    }

    fn allocate_page(block: &VirtualMemoryBlock, page: usize) {
        let permit = block
            .try_prepare_for_write(PageIndex(page))
            .expect("write should be prepared successfully");
        let data = vec![1; 8 << 10];
        block.write_page(permit, &data, 0);
    }
}
