//! The page operation log is a WAL-like system which holds the last N
//! operations that occurred to the page file.
//!
//! This enables the system to perform bulk transactions while still
//! maintaining an atomic behaviour.
//!
//! The reliability of this log is achieved on the assumption that the
//! disk sector size for atomic writes is some multiple of `512` bytes.

use rkyv::rancor;
use rkyv::ser::Positional;

use super::file_metadata::Encryption;
use super::{encrypt, integrity};
use crate::PageFileId;
use crate::layout::encrypt::EncryptError;
use crate::layout::page_metadata::{ArchivedPageMetadata, PageMetadata};

/// The fixed size of a log block buffer in bytes.
pub const LOG_BLOCK_SIZE: usize = 512;

/// Try to decode a [LogBlock] from the provided set of bytes.
///
/// The size of the buffer is expected to be 512 bytes.
///
/// This will decrypt the  buffer if `cipher` is provided or attempt to
/// check the CRC32 checksum check depending on if not.
pub fn decode_log_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
) -> Result<LogBlock, DecodeLogBlockError> {
    if buffer.len() != LOG_BLOCK_SIZE {
        return Err(DecodeLogBlockError::BufferWrongSize);
    }

    let ctx_indices = [0..40, 40..512];
    let [context, blk] = buffer.get_disjoint_mut(ctx_indices).unwrap();

    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, blk, context)
            .map_err(|_| DecodeLogBlockError::DecryptionFail)?;
    } else {
        let verified = integrity::verify(Encryption::Disabled, None, blk, context);
        if !verified {
            return Err(DecodeLogBlockError::VerificationFail);
        }
    }

    let blk_len = u64::from_le_bytes(blk[..8].try_into().unwrap());
    let blk_data = &blk[8..8 + blk_len as usize];

    let view: &rkyv::Archived<LogBlock> = rkyv::access::<_, rancor::Error>(blk_data)
        .map_err(DecodeLogBlockError::Deserialize)?;

    rkyv::deserialize(view).map_err(DecodeLogBlockError::Deserialize)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a log entry.
pub enum DecodeLogBlockError {
    #[error("buffer wrong size")]
    /// The provided buffer is not [LOG_BLOCK_SIZE] in size.
    BufferWrongSize,
    #[error("buffer decryption failed")]
    /// The buffer could not be decrypted
    DecryptionFail,
    #[error("buffer verification failed")]
    /// The buffer could not be verified for integrity
    VerificationFail,
    #[error("deserialize error: {0}")]
    /// The system could not parse and deserialize the log entry.
    Deserialize(rancor::Error),
}

/// Serializes and writes a [LogBlock] into the provided buffer.
///
/// The size of the buffer is expected to be 512 bytes.
///
/// The size of the log entry is always [LOG_BLOCK_SIZE] in size.
pub fn encode_log_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    entry: &LogBlock,
    buffer: &mut [u8],
) -> Result<(), EncodeLogBlockError> {
    use rkyv::api::high;
    use rkyv::ser::writer::Buffer;

    if buffer.len() != LOG_BLOCK_SIZE {
        return Err(EncodeLogBlockError::BufferWrongSize);
    }

    // Buffer is split into [context, block_len, block_data].
    let ctx_indices = [0..40, 40..512];
    let blk_indices = [0..8, 8..472];
    let [context, blk] = buffer.get_disjoint_mut(ctx_indices).unwrap();
    let [blk_len, blk_data] = blk.get_disjoint_mut(blk_indices).unwrap();

    let writer = high::to_bytes_in::<_, rancor::Error>(entry, Buffer::from(blk_data))
        .map_err(EncodeLogBlockError::Serialize)?;
    let n_bytes_written = writer.pos();
    blk_len.copy_from_slice(&(n_bytes_written as u64).to_le_bytes());

    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, blk, context)
            .map_err(EncodeLogBlockError::EncryptionFail)?;
    } else {
        integrity::write_check_bytes(None, blk, context);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
/// The log entry could not be encoded and written to the buffer.
pub enum EncodeLogBlockError {
    #[error("buffer wrong size")]
    /// The provided buffer is not [LOG_BLOCK_SIZE] in size.
    BufferWrongSize,
    #[error("{0}")]
    /// Rkyv failed to serialize the entry.
    ///
    /// This should always be infallible, but we avoid panicking.
    Serialize(rancor::Error),
    #[error("failed to encrypt data: {0}")]
    /// The data could not be encrypted.
    EncryptionFail(EncryptError),
}

#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, rkyv(derive(Debug)))]
/// The log block holds onto `N` [LogEntry]s and some number of
/// page metadata entries.
///
/// The size of the block is up to `448` bytes but cannot go beyond.
pub struct LogBlock {
    #[rkyv(with = rkyv::with::AsBox)]
    pairs: Vec<EntryPair>,
}

impl LogBlock {
    /// The maximum number of bytes the block can grow to.
    pub const MAX_BYTES_SIZE: usize = 448;

    /// Attempts to push a log entry into the block.
    ///
    /// Returns `Err(entry)` if there is no space left in the block.
    pub fn push_entry(
        &mut self,
        entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> Result<(), (LogEntry, Option<PageMetadata>)> {
        let mut capacity_required = size_of::<ArchivedEntryPair>();
        if metadata.is_some() {
            capacity_required += size_of::<ArchivedPageMetadata>();
        }

        if self.remaining_capacity() < capacity_required {
            return Err((entry, metadata));
        }

        let metadata = metadata.map(Box::new);
        self.pairs.push(EntryPair {
            log: entry,
            metadata,
        });

        Ok(())
    }

    /// Reset the block back to its empty form.
    pub fn reset(&mut self) {
        self.pairs.clear();
    }

    /// Returns the remaining number of bytes the block can hold.
    pub(crate) fn remaining_capacity(&self) -> usize {
        let mut bytes_consumed = 0;
        for pair in self.pairs.iter() {
            bytes_consumed += size_of::<ArchivedEntryPair>();

            if pair.metadata.is_some() {
                bytes_consumed += size_of::<ArchivedPageMetadata>();
            }
        }
        Self::MAX_BYTES_SIZE - bytes_consumed
    }

    #[inline]
    pub(crate) fn num_entries(&self) -> usize {
        self.pairs.len()
    }

    #[inline]
    pub(crate) fn entries(&self) -> &[EntryPair] {
        &self.pairs
    }
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, rkyv(derive(Debug)))]
/// A pair of [LogEntry] and an optional page metadata entry.
pub struct EntryPair {
    /// The log entry itself.
    pub log: LogEntry,
    #[rkyv(with = rkyv::with::Niche)]
    /// The page metadata tied to the entry if applicable.
    pub metadata: Option<Box<PageMetadata>>,
}

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
/// A single entry in the `PageOperationLog`.
pub struct LogEntry {
    /// The current sequence ID of the page allocation table.
    pub sequence_id: u32,
    /// The sequence ID marking where the last flush occurred.
    pub last_flush_sequence_id: u32,
    /// The transaction ID that groups multiple operations together
    /// to form a single atomic transaction.
    pub transaction_id: u64,
    /// The number of entries this transaction encompasses.
    pub transaction_n_entries: u32,
    /// The target page file affected by the operation.
    pub page_file_id: PageFileId,
    /// The operation that was performed.
    pub op: LogOp,
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[cfg_attr(test, rkyv(derive(Debug), compare(PartialEq)))]
pub enum LogOp {
    /// A new write performed on the page.
    Write = 0x01,
    /// The page has been freed and can be reused.
    Free = 0x02,
    /// The entry is used to update the flushed sequence ID.
    Flush = 0x03,
    /// Update the metadata attached to the page in the table without
    /// updating the page itself.
    UpdateTableMetadata = 0x04,
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[test]
    fn ensure_log_entry_40_bytes() {
        // WARNING! Changing this has side effects!
        assert_eq!(size_of::<ArchivedLogEntry>(), 40);
    }

    #[test]
    fn ensure_log_pair_size() {
        assert_eq!(size_of::<ArchivedEntryPair>(), 48);
    }

    #[test]
    fn test_log_block_sizing_all_entries() {
        let mut block = LogBlock::default();
        for _ in 0..9 {
            block
                .push_entry(
                    LogEntry {
                        sequence_id: 0,
                        last_flush_sequence_id: 0,
                        transaction_id: 0,
                        transaction_n_entries: 0,
                        page_file_id: PageFileId(1),
                        op: LogOp::Write,
                    },
                    None,
                )
                .unwrap();
        }

        block
            .push_entry(
                LogEntry {
                    sequence_id: 0,
                    last_flush_sequence_id: 0,
                    transaction_id: 0,
                    transaction_n_entries: 0,
                    page_file_id: PageFileId(1),
                    op: LogOp::Write,
                },
                None,
            )
            .expect_err("block should be full");
    }

    #[test]
    fn test_log_block_sizing_all_with_metadata() {
        let mut block = LogBlock::default();
        for _ in 0..4 {
            block
                .push_entry(
                    LogEntry {
                        sequence_id: 0,
                        last_flush_sequence_id: 0,
                        transaction_id: 0,
                        transaction_n_entries: 0,
                        page_file_id: PageFileId(1),
                        op: LogOp::Write,
                    },
                    Some(PageMetadata::empty()),
                )
                .unwrap();
        }

        block
            .push_entry(
                LogEntry {
                    sequence_id: 0,
                    last_flush_sequence_id: 0,
                    transaction_id: 0,
                    transaction_n_entries: 0,
                    page_file_id: PageFileId(1),
                    op: LogOp::Write,
                },
                Some(PageMetadata::empty()),
            )
            .expect_err("block should be full");
    }
}
