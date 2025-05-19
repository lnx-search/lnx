use std::io;
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::ops::Range;

use memmap2::Advice;

#[repr(usize)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
/// The size of memory pages in use.
pub enum PageSize {
    #[default]
    /// The default 8KB page size.
    ///
    /// This dispatching of pages from the kernel will use the standard page size
    /// of the system (probably 4KB.) unless huge pages are enabled.
    Std8KB = 8 << 10,
    /// The 32KB page size.
    ///
    /// This dispatching of pages from the kernel will use the standard page size
    /// of the system (probably 4KB.) unless huge pages are enabled.
    Std32KB = 32 << 10,
    /// The 64KB page size.
    ///
    /// This dispatching of pages from the kernel will use the standard page size
    /// of the system (probably 4KB.) unless huge pages are enabled.
    Std64KB = 64 << 10,
    /// The 128KB page size.
    ///
    /// This dispatching of pages from the kernel will use the standard page size
    /// of the system (probably 4KB.) unless huge pages are enabled.
    Std128KB = 128 << 10,
    /// Use 2MB huge pages.
    ///
    /// This will only attempt to use huge pages and fail otherwise.
    Huge2MB = 2 << 20,
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

    /// Returns the size of the individual memory pages.
    pub(super) fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Returns the size of the individual memory pages.
    pub(super) fn try_collapse(&self) -> io::Result<()> {
        self.memory.collapse()
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
        let span = self.get_spanning_ptr(page.0..page.0 + 1);
        RawMutPagePtr { span }
    }

    /// Get read-only pointer access to a given span of pages.
    pub(super) fn read_pages(&self, pages: Range<PageIndex>) -> RawPagePtr {
        let span = self.get_spanning_ptr(pages.start.0..pages.end.0);
        RawPagePtr { span }
    }

    fn get_spanning_ptr<T>(&self, range: Range<usize>) -> SpanningPagePtr<T> {
        let pos = self.resolve_pos(PageIndex(range.start));
        let len = range.len() * self.page_size as usize;
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
    collapsable: bool,
    huge: bool,
}

impl VirtualMemory {
    fn allocate(num_pages: usize, page_size: PageSize) -> io::Result<Self> {
        // If huge pages are not enabled, and we have 2MB being targeted, return an error.
        // It is impractical to use 2MB as the page size if huge pages are not active.
        if page_size == PageSize::Huge2MB {
            let mem = try_open_huge(num_pages, page_size)?;
            return Ok(Self {
                mem,
                collapsable: false,
                huge: true,
            });
        }

        let collapsable = Advice::Collapse.is_supported();
        if let Ok(mem) = try_open_huge(num_pages, page_size) {
            return Ok(Self {
                mem,
                collapsable,
                huge: true,
            });
        }

        let mem = try_open_std(num_pages, page_size)?;
        Ok(Self {
            mem,
            collapsable,
            huge: false,
        })
    }

    fn collapse(&self) -> io::Result<()> {
        // TODO: If `huge: true`, can this even collapse say 32KB HP into 64KB HP?
        if self.collapsable {
            self.mem.advise(Advice::Collapse)
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "kernel does not support collapsing memory",
            ))
        }
    }

    /// # Safety
    /// The caller must ensure that no reads are still held to this memory region
    /// and that no subsequent reads take place until memory is written again.
    ///
    /// This is because from this point on, the bytes in this region are considered
    /// uninitialized.
    unsafe fn free(&self, start: usize, len: usize) -> io::Result<()> {
        use memmap2::UncheckedAdvice;

        // Note to future maintainers:
        // We use `MADV_DONTNEED` here over `MADV_FREE` following this little trail:
        // https://github.com/JuliaLang/julia/issues/51086
        // Primarily the note about RSS and OOM killer, MADV_DONTNEED is much more clear
        // about how the kernel attributes RSS usage to the process with this advice
        // and hopefully will prevent it killing the process when memory pressure is high.
        unsafe {
            self.mem
                .unchecked_advise_range(UncheckedAdvice::DontNeed, start, len)
        }
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

fn try_open_huge(num_pages: usize, page_size: PageSize) -> io::Result<memmap2::MmapMut> {
    let mut map_options = memmap2::MmapOptions::new();
    map_options.len(num_pages * page_size as usize);
    let shift_bits = (page_size as usize).ilog2() as u8;
    map_options.huge(Some(shift_bits));
    map_options.map_anon()
}

fn try_open_std(num_pages: usize, page_size: PageSize) -> io::Result<memmap2::MmapMut> {
    let mut map_options = memmap2::MmapOptions::new();
    map_options.len(num_pages * page_size as usize);
    map_options.map_anon()
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_page(2)]
    fn test_virtual_memory_allocation_standard_page(#[case] num_pages: usize) {
        VirtualMemory::allocate(num_pages, PageSize::Std8KB)
            .expect("virtual memory should be created");
    }

    #[cfg(feature = "test-huge-pages")]
    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_page(2)]
    fn test_virtual_memory_allocation_huge_page(#[case] num_pages: usize) {
        VirtualMemory::allocate(num_pages, PageSize::Huge2MB)
            .expect("virtual memory should be created");
    }

    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_pages(2)]
    fn test_raw_virtual_memory_pages_creation_standard_page(#[case] num_pages: usize) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Std8KB)
            .expect("virtual memory pages should be created");
        assert_eq!(pages.num_pages(), num_pages);
    }

    #[cfg(feature = "test-huge-pages")]
    #[rstest::rstest]
    #[case::zero_pages(0)]
    #[case::one_page(1)]
    #[case::two_pages(2)]
    fn test_raw_virtual_memory_pages_creation_huge_page(#[case] num_pages: usize) {
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Huge2MB)
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
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Std8KB)
            .expect("virtual memory pages should be created");
        let ptr =
            pages.read_pages(target_page_index..PageIndex(target_page_index.0 + 1));
        assert_eq!(ptr.pages_spanned(), 1);

        unsafe {
            assert_eq!(ptr.access(), vec![0; 8 << 10], "memory should be zeroed");
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
        let pages = RawVirtualMemoryPages::allocate(num_pages, PageSize::Std8KB)
            .expect("virtual memory pages should be created");
        let mut ptr = pages.get_mut_page(target_page_index);
        assert_eq!(ptr.pages_spanned(), 1);

        unsafe {
            assert_eq!(
                ptr.access_uninit().len(),
                8 << 10,
                "memory should be zeroed"
            );
        }
    }

    #[test]
    fn test_ptr_unsplit() {
        let pages = RawVirtualMemoryPages::allocate(4, PageSize::Std8KB)
            .expect("virtual memory pages should be created");

        let mut ptr1 = pages.get_mut_page(PageIndex(0));
        let ptr2 = pages.get_mut_page(PageIndex(1));
        assert!(unsafe { ptr1.unsplit(ptr2).is_ok() });
        assert_eq!(ptr1.pages_spanned(), 2);

        let mut ptr1 = pages.get_mut_page(PageIndex(0));
        let ptr2 = pages.get_mut_page(PageIndex(2));
        assert!(unsafe { ptr1.unsplit(ptr2).is_err() });
        assert_eq!(ptr1.pages_spanned(), 1);

        let mut ptr1 = pages.read_pages(PageIndex(0)..PageIndex(1));
        let ptr2 = pages.read_pages(PageIndex(1)..PageIndex(2));
        assert!(unsafe { ptr1.unsplit(ptr2).is_ok() });
        assert_eq!(ptr1.pages_spanned(), 2);

        let mut ptr1 = pages.read_pages(PageIndex(0)..PageIndex(1));
        let ptr2 = pages.read_pages(PageIndex(2)..PageIndex(3));
        assert!(unsafe { ptr1.unsplit(ptr2).is_err() });
        assert_eq!(ptr1.pages_spanned(), 1);
    }
}
