//! The operations log tracks changes to pages in the data files.

#[cfg(all(test, not(feature = "test-miri"), feature = "bench-lib-unstable"))]
pub mod benches;
mod file;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;

use std::collections::VecDeque;

use parking_lot::RwLock;

use crate::layout::file_metadata::Encryption;

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub struct MetadataHeader {
    /// The unique ID of the log file.
    ///
    /// NOTE: This ID changes every time the WAL is flushed, although the disk
    /// allocation stays the same, the file itself is seen as 'new'.
    pub log_file_id: u64,
    /// Signals if the data in the log is encrypted or not.
    pub encryption: Encryption,
}

/// The op log writer acts as a WAL for page metadata changes.
///
/// The log must only be written to once the page data itself is confirmed
/// to be persisted safely on disk (if the operation changes the data.)
///
/// The writer holds onto multiple log files which are rotated during the operation
/// of the system, once a log file is full or a given timeout has elapsed, the
/// log will be processed and the modified page files will have their metadata
/// entries updated. Once this is complete, the log will be marked as empty
/// and put back into rotation.
///
/// If the system cannot rollup the log in the background, it will force a writer
/// to rollup the log before being able to proceed.
pub struct OpLogWriter {
    /// Open log files for writing.
    files: RwLock<VecDeque<()>>,
    /// A backlog of events to write to the log.
    enqueued_logs: crossbeam_queue::SegQueue<()>,
}
