//! The page store cache
//!
//! This system is built around reserving blocks of virtual memory and then incrementally
//! populating and evicting pages in a similar fashion to a memory mapped file.
//!
//! Each page file has its own virtual memory reservation which holds only the actual page
//! data (i.e. no metadata, no structure overhead, etc...) and stores each block contiguously
//! allowing the system to return contiguous blocks of memory on reads without additional copies.
//!
//! The caching strategy used is a LFU (Least Frequently Used) policy provided by `moka`.
//! The system itself may not immediately free memory when it has been marked for eviction
//! due to reads holding a reference to the memory currently, therefore, it is recommended
//! to allow some buffer zone of memory allowance as the system is not guaranteed to stay
//! within the bounds set constantly.
//!

mod mem_block;
