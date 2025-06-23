use super::PageFile;
use super::page_allocator::ReservedPagesGuard;

pub struct PageGroupWriter<'a> {
    page_file: &'a PageFile,
    reserved_pages: ReservedPagesGuard,
}
