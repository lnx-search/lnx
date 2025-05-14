//! The page operation log is a WAL-like system which holds the last N 
//! operations that occurred to the page file.
//!
//! This enables the system to perform bulk transactions while still
//! maintaining an atomic behaviour. 
//!
//! However, unlike a WAL, this log has a hard limit on the number of
//! operations that can be present before the system must rollup and
//! update the allocation table at the head of the file.
//! 
//! The reliability of this log is achieved on the assumption that the
//! disk sector size for atomic writes is some multiple of `512` bytes.

use crate::PageId;

/// Reads all log events from the given buffer.
pub fn read_operation_log(
    buffer: &[u8],
) {
    
}

pub fn encode_log_entry(entry: &LogEntry) {
    
}


#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// A single entry in the [PageOperationLog].
/// 
/// NOTE: Each entry must maintain a size that allows the system
/// to keep an alignment of `512` bytes, this is required in order
/// to perform atomic writes to disk without a partial write occurring.
/// 
/// We assume `512` is the disk sector size here.
pub struct LogEntry {
    
    /// The last checkpoint of the page allocation table. 
    pub last_seen_checkpoint: u64,
    /// The target page affected by the operation.
    pub page_id: PageId,
    /// The transaction ID that groups multiple operations together
    /// to form a single atomic transaction.
    pub transaction_id: PageId,
    /// The operation that was performed.
    pub op: LogOp,
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum LogOp {
    /// A new write performed on the page.
    Write = 0x01,
    /// The page has been freed and can be reused.
    Free = 0x02,
    /// The transaction has completed successfully.
    Commit = 0x03,
    /// Update the metadata attached to the page in the table without
    /// updating the page itself.
    UpdateTableMetadata = 0x04,
}

/// The on-disk layout of a single log entry.
/// 
/// This can have a HMAC tied to it for authentication reasons when encryption
/// at rest is enabled.
pub struct EncodedLogEntry {
    legacy_code: u64,  // Dont worry about this. ;)
    hmac: [u8; 32],
    entry: [u8; 64],
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_size() {
        dbg!(size_of::<ArchivedLogEntry>() + 32);
    }
    
}