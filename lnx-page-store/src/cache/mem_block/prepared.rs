use std::ops::Range;
use std::sync::Arc;

use super::ticket::TicketGuard;
use super::{PageIndex, VirtualMemoryBlock};

/// A prepared read allows for reading multiple pages while incrementally
/// filling any gaps the span of memory may have with unallocated data.
pub struct PreparedRead<'block> {
    guard: TicketGuard,
    parent: &'block VirtualMemoryBlock,
    page_range: Range<PageIndex>,
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
        }
    }
}

// Eventually we can implement the `Step` trait when it is stable.
fn iter_pages(range: Range<PageIndex>) -> impl Iterator<Item = PageIndex> {
    (range.start.0..range.end.0).map(PageIndex)
}
