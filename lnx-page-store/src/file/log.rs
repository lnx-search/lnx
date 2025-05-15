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

use std::marker::PhantomData;

use hmac::{Hmac, Mac};
use rkyv::rancor;
use sha2::Sha256;

use crate::PageId;

/// The fixed size of a single log entry in bytes.
pub const LOG_ENTRY_SIZE: usize = 64;
type HmacSha256 = Hmac<sha2::Sha224>;

/// Verify the integrity of the log entry using the HMAC address
/// and do not treat the bytes like a SHA256 entry.
///
/// This should be treated as the default.
pub struct VerifyHmac;

/// Try to verify the HMAC otherwise try to treat it as a SHA256 checksum.
///
/// WARNING: This should only be used for upgrading to and from enabling and
/// disabling encryption at rest. As this option effectively disables
/// HMAC's functionality.
pub struct DangerVerifyHmacOrSha256;

/// A decoder for parsing and validating individual entries in the Page Operation Log.
pub struct LogDecoder<'key, M = VerifyHmac> {
    /// The HMAC authentication key used to verify the log entries.
    ///
    /// If this is `None` then regular SHA256 checksums are used to verify the integrity
    /// (but not the authentication) of the data.
    pub hmac_key: Option<&'key [u8]>,
    /// Allows the decoder to accept log entries with SHA256 checksums rather than HMACs
    /// when a `hmac_key` is provided.
    ///
    /// WARNING: This should only be used with the intention of updating existing
    /// data to enable/disable encryption at rest.
    pub either_hmac_or_sha256: bool,
    /// The mode the decoder should operate with.
    pub _mode: PhantomData<M>,
}

impl LogDecoder<'_, VerifyHmac> {
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

        let hmac_or_sha256 = &encoded_entry[..32];
        let encoded_log_bytes = &encoded_entry[32..];

        let mut check_sha = self.hmac_key.is_none();
        if let Some(key) = self.hmac_key {
            let mut mac =
                HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
            mac.update(encoded_log_bytes);
            if mac.verify_slice(hmac_or_sha256).is_err() {
                check_sha = true;
            }
        }

        if check_sha {
            let result = hash_sha256(encoded_log_bytes);
            if result.as_slice() != hmac_or_sha256 {
                return Err(DecodeLogEntryError::ChecksumFail);
            }
        }

        rkyv::access::<_, rancor::Error>(encoded_log_bytes)
            .map_err(DecodeLogEntryError::Deserialize)
    }
}

impl LogDecoder<'_, DangerVerifyHmacOrSha256> {
    /// Try to decode a log entry from the provided set of bytes.
    ///
    /// This will verify the integrity of the entry with either a HMAC
    /// check or SHA256 checksum check depending on if `hmac_key` is `None` or not.
    ///
    /// WARNING:
    ///
    /// This method is HIGHLY dangerous as it does _not_ truly check the authentication
    /// of the HMAC, an attacker can simply switch the bytes out with a valid SHA256
    /// checksum, and it will pass in this mode!
    pub fn decode_entry<'buf>(
        &self,
        encoded_entry: &'buf [u8],
    ) -> Result<&'buf rkyv::Archived<LogEntry>, DecodeLogEntryError> {
        if encoded_entry.len() != LOG_ENTRY_SIZE {
            return Err(DecodeLogEntryError::BufferWrongSize);
        }

        let hmac_or_sha256 = &encoded_entry[..32];
        let encoded_log_bytes = &encoded_entry[32..];

        if let Some(key) = self.hmac_key {
            let mut mac =
                HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
            mac.update(encoded_log_bytes);
            mac.verify_slice(hmac_or_sha256)
                .map_err(|_| DecodeLogEntryError::HmacVerificationFail)?;
        } else {
            let result = hash_sha256(encoded_log_bytes);
            if result.as_slice() != hmac_or_sha256 {
                return Err(DecodeLogEntryError::ChecksumFail);
            }
        }

        rkyv::access::<_, rancor::Error>(encoded_log_bytes)
            .map_err(DecodeLogEntryError::Deserialize)
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a log entry.
pub enum DecodeLogEntryError {
    #[error("buffer wrong size")]
    /// The provided buffer is not [LOG_ENTRY_SIZE] in size.
    BufferWrongSize,
    #[error("hmac verification check failed for entry")]
    /// The HMAC check failed.
    ///
    /// This can only happen when `hmac_key` is not None or `either_hmac_or_sha256` is `true`.
    HmacVerificationFail,
    #[error("sha256 checksum for entry does not match")]
    /// The SHA256 checksum check failed.
    ///
    /// This can only happen when `hmac_key` is None or `either_hmac_or_sha256` is `true`.
    ChecksumFail,
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
    hmac_key: Option<&[u8]>,
    buffer: &mut [u8],
) -> Result<(), EncodeLogEntryError> {
    use rkyv::api::high;
    use rkyv::ser::writer::Buffer;

    if buffer.len() != LOG_ENTRY_SIZE {
        return Err(EncodeLogEntryError::BufferWrongSize);
    }

    let mut temp_buffer = [0; size_of::<ArchivedLogEntry>()];
    high::to_bytes_in::<_, rancor::Error>(entry, Buffer::from(&mut temp_buffer))
        .map_err(EncodeLogEntryError::Serialize)?;

    if let Some(key) = hmac_key {
        let mut mac =
            HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(&temp_buffer);
        let result = mac.finalize().into_bytes();
        buffer[..32].copy_from_slice(result.as_slice());
    } else {
        let result = hash_sha256(&temp_buffer);
        buffer[..32].copy_from_slice(result.as_slice());
    }

    buffer[32..].copy_from_slice(&temp_buffer);

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
/// A single entry in the [PageOperationLog].
///
/// NOTE: Each entry must maintain a size that allows the system
/// to keep an alignment of `512` bytes, this is required in order
/// to perform atomic writes to disk without a partial write occurring.
///
/// We assume `512` is the disk sector size here.
pub struct LogEntry {
    /// The sequence ID is a monotonic ID based on the current checkpoint.
    ///
    /// This is used to ensure ordering of log events.
    pub sequence_id: u64,
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

fn hash_sha256(data: &[u8]) -> sha2::digest::Output<Sha256> {
    use sha2::Digest;
    let mut digest = Sha256::default();
    digest.update(data);
    digest.finalize()
}
