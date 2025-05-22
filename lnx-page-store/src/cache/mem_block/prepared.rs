use std::fmt::{Debug, Formatter};
use std::ops::{Deref, Range};

use smallvec::SmallVec;

use super::ticket::TicketGuard;
use super::{PageIndex, VirtualMemoryBlock};

type PageSet = SmallVec<[PageIndex; 64]>;

/// A prepared read allows for reading multiple pages while incrementally
/// filling any gaps the span of memory may have with unallocated data.
pub struct PreparedRead<'block> {
    guard: TicketGuard,
    parent: &'block VirtualMemoryBlock,
    page_range: Range<PageIndex>,
    outstanding_write_pages: PageSet,
}

impl Debug for PreparedRead<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PreparedRead(page_range={:?})",
            self.page_range.start.0..self.page_range.end.0
        )
    }
}

impl<'block> PreparedRead<'block> {
    /// Create a new [PageIndex] for the given range of pages.
    pub(super) fn for_page_range(
        guard: TicketGuard,
        parent: &'block VirtualMemoryBlock,
        page_range: Range<PageIndex>,
    ) -> Self {
        let mut slf = Self {
            guard,
            parent,
            page_range: page_range.clone(),
            outstanding_write_pages: PageSet::new(),
        };

        for page in iter_pages(page_range) {
            let flags = slf.parent.get_page_flags(page);
            if !flags.is_readable() {
                slf.outstanding_write_pages.push(page);
            }
        }

        slf
    }

    /// Returns a reference slice of all outstanding page writes that must be
    /// completed before the read can be finished.
    pub fn outstanding_writes(&self) -> &[PageIndex] {
        self.outstanding_write_pages.as_slice()
    }

    /// Returns a reference to the parent [VirtualMemoryBlock].
    pub fn parent(&self) -> &'block VirtualMemoryBlock {
        self.parent
    }

    /// Try read all pages and produce a single contiguous slice.
    ///
    /// Returns an `Err(Self)` when there are outstanding writes still left to be
    /// completed.
    pub fn try_finish(mut self) -> Result<ReadResult<'block>, Self> {
        let has_outstanding_writes = self.check_outstanding_writes();
        if has_outstanding_writes {
            return Err(self);
        }

        // Safety:
        // We are now sure all pages we are referencing are allocated and know they all are within
        // bounds as every page has an associated state within the block.
        let ptr = unsafe { self.parent.read_pages(self.page_range.clone()) };

        Ok(ReadResult {
            ptr,
            guard: self.guard,
            parent: self.parent,
        })
    }

    fn check_outstanding_writes(&mut self) -> bool {
        self.outstanding_write_pages.retain(|page| {
            let flags = self.parent.get_page_flags(*page);
            !flags.is_readable()
        });
        !self.outstanding_write_pages.is_empty()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("outstanding operations prevent read completion")]
/// The read cannot be finished because one of the prerequisites has not been met.
pub struct OutstandingWrites;

/// A read reference to a span of pages.
///
/// This is an owned reference because it contains a guard that prevents
/// the pages being read from being modified or freed while this read exists.
pub struct ReadResult<'mem> {
    guard: TicketGuard,
    parent: &'mem VirtualMemoryBlock,
    ptr: super::raw::RawPagePtr,
}

impl Clone for ReadResult<'_> {
    fn clone(&self) -> Self {
        Self {
            guard: self.guard.clone_for_read(),
            parent: self.parent,
            ptr: self.ptr,
        }
    }
}

impl Debug for ReadResult<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockRead(num_pages={})", self.ptr.pages_spanned())
    }
}

impl Deref for ReadResult<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for ReadResult<'_> {
    fn as_ref(&self) -> &[u8] {
        unsafe { self.ptr.access() }
    }
}

// Eventually we can implement the `Step` trait when it is stable.
fn iter_pages(range: Range<PageIndex>) -> impl Iterator<Item = PageIndex> {
    (range.start.0..range.end.0).map(PageIndex)
}
