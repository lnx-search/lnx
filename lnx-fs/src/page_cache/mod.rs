//! A page cache dynamically allocates virtual memory to allow partial or complete
//! caching of file contents while maintaining alignment and "contiguous" memory regions.
//!
//! This system handles incrementally locking ranges of the cache files, freeing unused pages
//! and managing the overall memory attempting to be used by the system.
//!
//! Internally this is done with a lot of unsafe and mmap calls.
//!

mod block;
mod sync;
mod gc;
mod utils;

/// A page cache holds files partially or full in memory using virtual memory
/// addressing.
pub struct FilePageCache {
}