//! A page file is a collection of 8KB pages and their respective metadata.
//!
//! Each page file contains 1,000,000 pages, which brings the target page file size
//! inline with ~8GB.
//!
//! Page files are structured with the following layout:
//!
//! - Page File Metadata
//!     * Contains information like layout version.
//! - Allocation Table
//!     * A checkpoint bitset tracking what pages within the file
//!       are allocated.
//!     * This value is authenticated but not encrypted when encryption at
//!       rest is enabled.
//!     * The size of the table is aligned to 8KB, totalling 128KB.
//! - Page Operation Log
//!     * A fixed size log for completing transactional operations.
//!     * This acts similarly to a WAL.
//!     * Each entry is 64 bytes, and blocks of logs are aligned to 512B.
//!     * The log is containing up to `524,288` entries totalling 32MB of reserved
//!       space.
//! - Page Meta Table
//!     * Contains a fixed lookup table holding page metadata which are 64B in size.
//!     * Blocks are aligned on 4KB boundaries (63 x entries per block + offset)
//!     * 1 entry per page, so 1,000,000 entries totalling ~65MB.
//! - Page data
//!     * 8KB blocks of data.
//!     * Raw data, left as is.
//!
//! Overall the total overhead of the file is roughly ~98MB (128KB + 32MB + 65MB.)
//!

pub mod encrypt;
pub mod file_metadata;
pub mod integrity;
pub mod log;
pub mod page_metadata;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;

/// The total number of pages each page file holds onto.
pub const PAGES_PER_FILE: usize = 1_000_000;
