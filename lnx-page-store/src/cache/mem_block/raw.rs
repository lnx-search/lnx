use std::io;
use std::mem::MaybeUninit;

const STANDARD_PAGE_SIZE: usize = 8 << 10;
const HUGE_PAGE_SIZE: usize = 2 << 20;

#[repr(usize)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The size of memory pages in use.
pub enum PageSize {
    /// The default 8KB allocation size.
    Standard = STANDARD_PAGE_SIZE,
    /// Huge pages 2MB in size.
    Huge = HUGE_PAGE_SIZE,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
/// The unique ID of a page within the memory block.
pub struct PageIndex(pub(super) usize);

/// A raw block of virtual memory split into pages.
///
/// This structure makes no effort to ensure any safety of operations, its only job
/// is to organise the virtual memory allocated into the desired page layout and keep
/// track of their state.
pub(super) struct RawVirtualMemoryPages {
    memory: VirtualMemory,
    page_size: PageSize,
}

impl RawVirtualMemoryPages {
    /// Allocate a new block of virtual memory with capacity of at least the given `size`.
    ///
    /// The `size` is automatically aligned _up_ to the nearest `page_size`.
    pub(super) fn allocate(num_pages: usize, page_size: PageSize) -> io::Result<Self> {
        let memory = VirtualMemory::allocate(num_pages, page_size)?;
        Ok(Self { memory, page_size })
    }

    /// Returns the number of pages this memory contains.
    pub(super) fn num_pages(&self) -> usize {
        self.memory.len() / self.page_size as usize
    }

    /// Marks a page as available to be reclaimed by the OS.
    ///
    /// # Safety
    /// The caller must ensure that no reads still access this page
    /// and that no subsequent reads take place on this page until a write op
    /// has completed.
    ///
    /// This is because after a free operation, the memory is considered uninitialized.
    pub(super) unsafe fn free(&self, page: PageIndex) -> io::Result<()> {
        let start = self.resolve_pos(page);
        unsafe { self.memory.free(start, self.page_size as usize) }
    }

    /// Get mutable pointer access to a given page.
    pub(super) fn get_mut_page(&self, page: PageIndex) -> RawMutPagePtr {
        let span = self.get_spanning_ptr(page);
        RawMutPagePtr { span }
    }

    /// Get read-only pointer access to a given page.
    pub(super) fn get_page(&self, page: PageIndex) -> RawPagePtr {
        let span = self.get_spanning_ptr(page);
        RawPagePtr { span }
    }

    fn get_spanning_ptr<T>(&self, page: PageIndex) -> SpanningPagePtr<T> {
        let pos = self.resolve_pos(page);
        let len = self.page_size as usize;
        let ptr = self.memory.get_ptr_at(pos);
        SpanningPagePtr {
            page_size: self.page_size,
            ptr: ptr as *mut T,
            len,
        }
    }

    fn resolve_pos(&self, page: PageIndex) -> usize {
        page.0 * self.page_size as usize
    }
}

/// A raw mutable pointer to the given page memory.
pub(super) struct RawMutPagePtr {
    span: SpanningPagePtr<MaybeUninit<u8>>,
}

impl RawMutPagePtr {
    /// Access the memory located at the given page pointer.
    ///
    /// # Safety
    /// This slice of memory is always treated as potentially being
    /// uninitialised until the caller can prove otherwise.
    ///
    /// It is the caller's responsibility that the page is:
    ///
    /// - The pointer is valid at not dangling.
    /// - Not being accessed by any other writers and therefore not subject to being mutated
    ///   under the writer's nose / no racing writes.
    /// - No readers currently have access to the page or hold a reference to the page.
    /// - Will not be accessed by any other readers or writers as long as this slice lives.
    pub(super) unsafe fn access_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { std::slice::from_raw_parts_mut(self.span.ptr as *mut _, self.span.len) }
    }

    /// Join two contiguous pages writes together.
    ///
    /// `self` becomes a new single mutable slice spanning multiple pages.
    ///
    /// # Safety
    ///
    /// The two pointers must belong to the same block of virtual memory.
    pub(super) unsafe fn unsplit(&mut self, other: Self) -> Result<(), Self> {
        unsafe {
            self.span
                .unsplit(other.span)
                .map_err(|inner| Self { span: inner })
        }
    }

    /// Returns the number of pages this pointer spans.
    pub(super) fn pages_spanned(&self) -> usize {
        self.span.pages_spanned()
    }

    /// Returns the length of the page in bytes.
    pub(super) fn len(&self) -> usize {
        self.span.len
    }
}

/// A raw pointer to the given page memory.
pub(super) struct RawPagePtr {
    span: SpanningPagePtr<u8>,
}

impl RawPagePtr {
    /// Access the memory located at the given page pointer.
    ///
    /// # Safety
    /// It is the caller's responsibility that the page is:
    ///
    /// - The pointer is valid at not dangling.
    /// - Not being accessed by any writers and therefore not subject to being mutated
    ///   under the reader's nose.
    /// - Will not be accessed by any writers and mutated as long as this slice lives.
    /// - All bytes are initialised and valid to read.
    pub(super) unsafe fn access(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.span.ptr, self.span.len) }
    }

    /// Join two contiguous pages reads together.
    ///
    /// `self` becomes a new single slice spanning multiple pages.
    ///
    /// # Safety
    ///
    /// The two pointers must belong to the same block of virtual memory.
    pub(super) unsafe fn unsplit(&mut self, other: Self) -> Result<(), Self> {
        unsafe {
            self.span
                .unsplit(other.span)
                .map_err(|inner| Self { span: inner })
        }
    }

    /// Returns the number of pages this pointer spans.
    pub(super) fn pages_spanned(&self) -> usize {
        self.span.pages_spanned()
    }
}

struct SpanningPagePtr<T> {
    page_size: PageSize,
    ptr: *mut T,
    len: usize,
}

impl<T> SpanningPagePtr<T> {
    /// # Safety
    ///
    /// The two pointers must belong to the same block of virtual memory.
    unsafe fn unsplit(&mut self, other: Self) -> Result<(), Self> {
        let slf_page_end = unsafe { self.ptr.add(self.len) };
        let other_page_end = unsafe { other.ptr.add(other.len) };

        if slf_page_end == other.ptr {
            // `self` is the head of the slice.
            self.len += other.len;
            Ok(())
        } else if other_page_end == self.ptr {
            // `self` is the tail of the slice.
            self.ptr = other.ptr;
            self.len += other.len;
            Ok(())
        } else {
            Err(other)
        }
    }

    fn pages_spanned(&self) -> usize {
        self.len / self.page_size as usize
    }
}

struct VirtualMemory {
    mem: memmap2::MmapMut,
}

impl VirtualMemory {
    fn allocate(num_pages: usize, page_size: PageSize) -> io::Result<Self> {
        let mut map_options = memmap2::MmapOptions::new();
        map_options.len(num_pages * page_size as usize);

        if page_size == PageSize::Huge {
            map_options.huge(Some(21)); // MAP_HUGE_2MB
        }

        let mem = map_options.map_anon()?;

        Ok(Self { mem })
    }

    /// # Safety
    /// The caller must ensure that no reads are still held to this memory region
    /// and that no subsequent reads take place until memory is written again.
    ///
    /// This is because from this point on, the bytes in this region are considered
    /// uninitialized.
    pub(super) unsafe fn free(&self, start: usize, len: usize) -> io::Result<()> {
        use memmap2::UncheckedAdvice;

        self.mem
            .unchecked_advise_range(UncheckedAdvice::Free, start, len)
    }

    fn len(&self) -> usize {
        self.mem.len()
    }

    fn get_ptr_at(&self, pos: usize) -> *mut u8 {
        assert!(pos < self.mem.len());
        // Safety: We have pre-checked that the pos is within bounds.
        unsafe { self.mem.as_ptr().add(pos) as *mut u8 }
    }
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_page(2)]
    fn test_virtual_memory_allocation_standard_page(#[case] num_pages: usize) {
        VirtualMemory::allocate(num_pages, PageSize::Standard)
            .expect("virtual memory should be created");
    }

    #[cfg(feature = "test-huge-pages")]
    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_page(2)]
    fn test_virtual_memory_allocation_huge_page(#[case] num_pages: usize) {
        VirtualMemory::allocate(num_pages, PageSize::Huge)
            .expect("virtual memory should be created");
    }

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_pages(2)]
    fn test_raw_virtual_memory_pages_creation_standard_page(#[case] num_pages: usize) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Standard)
            .expect("virtual memory pages should be created");
        assert_eq!(pages.num_pages(), num_pages);
    }

    #[cfg(feature = "test-huge-pages")]
    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_pages(2)]
    fn test_raw_virtual_memory_pages_creation_huge_page(#[case] num_pages: usize) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Huge)
            .expect("virtual memory pages should be created");
        assert_eq!(pages.num_pages(), num_pages);
    }

    #[rstest::rstest]
    #[case::page_read(1, PageIndex(0))]
    #[case::page_read(2, PageIndex(1))]
    #[should_panic]
    #[case::page_read(2, PageIndex(2))]
    #[should_panic]
    #[case::page_out_of_bounds_panic_1(0, PageIndex(0))]
    #[should_panic]
    #[case::page_out_of_bounds_panic_2(0, PageIndex(2))]
    fn test_read_only_page_access(
        #[case] num_pages: usize,
        #[case] target_page_index: PageIndex,
    ) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Standard)
            .expect("virtual memory pages should be created");
        let ptr = pages.get_page(target_page_index);
        assert_eq!(ptr.pages_spanned(), 1);

        unsafe {
            assert_eq!(
                ptr.access(),
                vec![0; STANDARD_PAGE_SIZE],
                "memory should be zeroed"
            );
        }
    }

    #[rstest::rstest]
    #[case::page_read(1, PageIndex(0))]
    #[case::page_read(2, PageIndex(1))]
    #[should_panic]
    #[case::page_read(2, PageIndex(2))]
    #[should_panic]
    #[case::page_out_of_bounds_panic_1(0, PageIndex(0))]
    #[should_panic]
    #[case::page_out_of_bounds_panic_2(0, PageIndex(2))]
    fn test_mut_page_access(
        #[case] num_pages: usize,
        #[case] target_page_index: PageIndex,
    ) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Standard)
            .expect("virtual memory pages should be created");
        let mut ptr = pages.get_mut_page(target_page_index);
        assert_eq!(ptr.pages_spanned(), 1);

        unsafe {
            assert_eq!(
                ptr.access_uninit().len(),
                STANDARD_PAGE_SIZE,
                "memory should be zeroed"
            );
        }
    }

    #[test]
    fn test_ptr_unsplit() {
        let pages = RawVirtualMemoryPages::allocate(4, PageSize::Standard)
            .expect("virtual memory pages should be created");

        let mut ptr1 = pages.get_mut_page(PageIndex(0));
        let ptr2 = pages.get_mut_page(PageIndex(1));
        assert!(unsafe { ptr1.unsplit(ptr2).is_ok() });
        assert_eq!(ptr1.pages_spanned(), 2);

        let mut ptr1 = pages.get_mut_page(PageIndex(0));
        let ptr2 = pages.get_mut_page(PageIndex(2));
        assert!(unsafe { ptr1.unsplit(ptr2).is_err() });
        assert_eq!(ptr1.pages_spanned(), 1);

        let mut ptr1 = pages.get_page(PageIndex(0));
        let ptr2 = pages.get_page(PageIndex(1));
        assert!(unsafe { ptr1.unsplit(ptr2).is_ok() });
        assert_eq!(ptr1.pages_spanned(), 2);

        let mut ptr1 = pages.get_page(PageIndex(0));
        let ptr2 = pages.get_page(PageIndex(2));
        assert!(unsafe { ptr1.unsplit(ptr2).is_err() });
        assert_eq!(ptr1.pages_spanned(), 1);
    }
}
