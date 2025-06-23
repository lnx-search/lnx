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

use rkyv::rancor;

use super::file_metadata::Encryption;
use super::integrity;
use crate::PageId;

/// The fixed size of a single log entry in bytes.
pub const LOG_ENTRY_SIZE: usize = 64;
/// The maximum number of log entries that can be in the file
/// before a log rollup must take place
pub const MAX_LOG_ENTRIES: usize = 524_288;

/// Try to decode a log entry from the provided set of bytes.
///
/// This will verify the integrity of the entry with either a HMAC
/// check or SHA256 checksum check depending on if `hmac_key` is `None` or not.
pub fn decode_log_entry<'buf>(
    mode: Encryption,
    encoded_entry: &'buf [u8],
    hmac_key: Option<&[u8]>,
) -> Result<&'buf rkyv::Archived<LogEntry>, DecodeLogEntryError> {
    if encoded_entry.len() != LOG_ENTRY_SIZE {
        return Err(DecodeLogEntryError::BufferWrongSize);
    }

    let (verified, bytes) = integrity::verify(mode, encoded_entry, hmac_key);

    if !verified {
        return Err(DecodeLogEntryError::VerificationFail);
    }

    rkyv::access::<_, rancor::Error>(bytes).map_err(DecodeLogEntryError::Deserialize)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a log entry.
pub enum DecodeLogEntryError {
    #[error("buffer wrong size")]
    /// The provided buffer is not [LOG_ENTRY_SIZE] in size.
    BufferWrongSize,
    #[error("HMAC or SHA256 verification check failed for entry")]
    /// The verification method specified by the [DecodeVerification] enum failed.
    VerificationFail,
    #[error("deserialize error: {0}")]
    /// The system could not parse and deserialize the log entry.
    Deserialize(rancor::Error),
}

/// Serializes and writes a [LogEntry] into the provided buffer.
///
/// If a `hmac_key` is not None, a hmac signature is calculated for each log entry.
///
/// The size of the log entry is always [LOG_ENTRY_SIZE] in size.
pub fn encode_log_entry(
    entry: &LogEntry,
    buffer: &mut [u8],
    hmac_key: Option<&[u8]>,
) -> Result<(), EncodeLogEntryError> {
    use rkyv::api::high;
    use rkyv::ser::writer::Buffer;

    if buffer.len() != LOG_ENTRY_SIZE {
        return Err(EncodeLogEntryError::BufferWrongSize);
    }

    let mut temp_buffer = [0; size_of::<ArchivedLogEntry>()];
    high::to_bytes_in::<_, rancor::Error>(entry, Buffer::from(&mut temp_buffer))
        .map_err(EncodeLogEntryError::Serialize)?;

    integrity::copy_with_check_bytes(&temp_buffer, buffer, hmac_key);

    Ok(())
}

#[derive(Debug, thiserror::Error)]
/// The log entry could not be encoded and written to the buffer.
pub enum EncodeLogEntryError {
    #[error("buffer wrong size")]
    /// The provided buffer is not [LOG_ENTRY_SIZE] in size.
    BufferWrongSize,
    #[error("{0}")]
    /// Rkyv failed to serialize the entry.
    ///
    /// This should always be infallible, but we avoid panicking.
    Serialize(rancor::Error),
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug)))]
/// A single entry in the [PageOperationLog].
///
/// NOTE: Each entry must maintain a size that allows the system
/// to keep an alignment of `512` bytes, this is required in order
/// to perform atomic writes to disk without a partial write occurring.
///
/// We assume `512` is the disk sector size here.
pub struct LogEntry {
    /// The current checkpoint of the page allocation table.
    pub checkpoint: u64,
    /// The transaction ID that groups multiple operations together
    /// to form a single atomic transaction.
    pub transaction_id: u64,
    /// The target page affected by the operation.
    pub page_id: PageId,
    /// The operation that was performed.
    pub op: LogOp,
    /// Padding bytes to ensure the entry is 64B.
    pub padding: [u8; 8],
}

#[repr(u32)]
#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug)))]
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

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn ensure_log_entry_32_bytes() {
        // WARNING! Changing this has side effects!
        assert_eq!(size_of::<ArchivedLogEntry>(), 32);
    }
}
