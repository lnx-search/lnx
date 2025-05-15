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

use crate::PageId;

/// The fixed size of a single log entry in bytes.
pub const LOG_ENTRY_SIZE: usize = 64;

#[derive(Debug, Copy, Clone)]
/// The verification to perform on the log entry.
pub enum DecodeVerification {
    /// Use SHA256 checksums.
    Sha256,
    /// Use a HMAC authenticated digest.
    Hmac,
    /// Treat a HMAC as a checksum and allow either HMAC or SHA256.
    ///
    /// DANGER! This should only be used for updating existing data to and from encryption
    /// at rest.
    DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256,
}

/// A decoder for parsing and validating individual entries in the Page Operation Log.
pub struct LogDecoder<'key> {
    /// The HMAC authentication key used to verify the log entries.
    ///
    /// If this is `None` then regular SHA256 checksums are used to verify the integrity
    /// (but not the authentication) of the data.
    pub hmac_key: Option<&'key [u8]>,
    /// The verification the decoder should perform on the bytes.
    pub verification: DecodeVerification,
}

impl LogDecoder<'_> {
    /// Try to decode a log entry from the provided set of bytes.
    ///
    /// This will verify the integrity of the entry with either a HMAC
    /// check or SHA256 checksum check depending on if `hmac_key` is `None` or not.
    pub fn decode_entry<'buf>(
        &self,
        encoded_entry: &'buf [u8],
    ) -> Result<&'buf rkyv::Archived<LogEntry>, DecodeLogEntryError> {
        if encoded_entry.len() != LOG_ENTRY_SIZE {
            return Err(DecodeLogEntryError::BufferWrongSize);
        }

        let (verified, bytes) = match self.verification {
            DecodeVerification::Sha256 => {
                super::integrity::verify_sha256_buffer(encoded_entry)
            },
            DecodeVerification::Hmac => super::integrity::verify_hmac_buffer(
                encoded_entry,
                self.hmac_key.expect(
                    "HMAC key should be provided when HMAC verification is enabled",
                ),
            ),
            DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256 => {
                super::integrity::dangerous_relaxed_hmac_or_sha256_check_buffer(
                    encoded_entry,
                    self.hmac_key,
                )
            },
        };

        if !verified {
            return Err(DecodeLogEntryError::VerificationFail);
        }

        rkyv::access::<_, rancor::Error>(bytes).map_err(DecodeLogEntryError::Deserialize)
    }
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

    super::integrity::copy_with_check_bytes(&temp_buffer, buffer, hmac_key);

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
    /// The target page affected by the operation.
    pub page_id: PageId,
    /// The transaction ID that groups multiple operations together
    /// to form a single atomic transaction.
    pub transaction_id: u64,
    /// The operation that was performed.
    pub op: LogOp,
}

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
