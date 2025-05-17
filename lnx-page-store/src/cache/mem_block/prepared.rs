use std::ops::Range;
use std::sync::Arc;

use super::generation::TicketGuard;
use super::{PageIndex, VirtualMemoryBlock};

/// A prepared read allows for reading multiple pages while incrementally
/// filling any gaps the span of memory may have with unallocated data.
pub struct PreparedRead {
    guard: TicketGuard,
    parent: Arc<VirtualMemoryBlock>,
    page_range: Range<PageIndex>,
}

impl PreparedRead {
    /// Create a new [PageIndex] for the given range of pages.
    pub(super) fn for_page_range(
        guard: TicketGuard,
        parent: Arc<VirtualMemoryBlock>,
        page_range: Range<PageIndex>,
    ) -> Self {
        Self {
            guard,
            parent,
            page_range,
        }
    }
}
