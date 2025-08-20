use rkyv::rancor;
use rkyv::ser::writer::Buffer;

use crate::layout::encrypt;
use crate::{PageGroupId, PageId};

const ENTRIES_PER_BLOCK: usize = 63;
const EXPECTED_BUFFER_SIZE: usize = 4 << 10;

#[derive(Debug, Copy, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[rkyv(derive(Debug))]
/// Metadata about the page and the ata stored within it when serialized on disk.
pub struct PageMetadata {
    /// The block this page contains data for.
    pub(crate) group: PageGroupId,
    /// The revision is a monotonic ID for each page within a block that
    /// tracks the number of observed updates to the block.
    pub(crate) revision: u64,
    /// The ID of the page.
    pub(crate) id: PageId,
    /// The length of the buffer within the page.
    pub(crate) data_len: u32,
    /// Context bytes used for decrypting the page data.
    pub(crate) context: [u8; 40],
}

impl PageMetadata {
    pub(crate) fn is_empty(&self) -> bool {
        self.id == PageId(u32::MAX)
    }

    /// Creates a new [PageMetadata] entry representing an empty page.
    pub(crate) const fn empty() -> Self {
        Self {
            id: PageId(u32::MAX),
            group: PageGroupId(u64::MAX),
            revision: 0,
            data_len: 0,
            context: [0; 40],
        }
    }
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[rkyv(derive(Debug))]
/// A packed block of 63 metadata entries, forming a block of 4032B.
///
/// This is the storage layout used in order to account for encryption overhead
/// of the blocks while still maintaining 4KB alignment.
pub struct PageMetadataX63Bock {
    /// The packed page data.
    pub(crate) pages: [PageMetadata; ENTRIES_PER_BLOCK],
}

impl PageMetadataX63Bock {
    /// Creates a new [PageMetadataX63Bock] with all empty pages.
    pub(crate) const fn empty() -> Self {
        Self {
            pages: [const { PageMetadata::empty() }; ENTRIES_PER_BLOCK],
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// The provided buffer is too small to serialize the block.
pub enum EncodeError {
    #[error("provided buffer length is incorrect")]
    /// The provided buffer is not 4KB.
    IncorrectBufferSize,
    #[error("failed to encrypt data")]
    /// The data could not be encrypted.
    EncryptionFail,
}

/// Encode a set of N ([ENTRIES_PER_BLOCK]) page metadata entries and
/// write the result to the provided buffer.
///
/// Metadata entries are packed into blocks in order to maximise read and write
/// efficiency and maintain a 4KB alignment.
///
/// The provided buffer should be 4KB in size.
pub fn encode_page_metadata_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    entries: &PageMetadataX63Bock,
    buffer: &mut [u8],
) -> Result<(), EncodeError> {
    if buffer.len() != EXPECTED_BUFFER_SIZE {
        return Err(EncodeError::IncorrectBufferSize);
    }

    let [context, buffer] = buffer
        .get_disjoint_mut([0..40, 40..EXPECTED_BUFFER_SIZE])
        .unwrap();

    let writer = Buffer::from(&mut buffer[24..]);
    rkyv::api::high::to_bytes_in::<_, rancor::Panic>(entries, writer).unwrap();

    let checksum = crc32fast::hash(&buffer[24..]);
    buffer[..4].copy_from_slice(&checksum.to_le_bytes());

    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, associated_data, buffer, context)
            .map_err(|_| EncodeError::EncryptionFail)?;
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding a block of metadata pages.
pub enum DecodeError {
    #[error("provided buffer length is incorrect")]
    /// The provided buffer is too small.
    IncorrectBufferSize,
    #[error("failed to decrypt data")]
    /// The data could not be decrypted.
    DecryptionFailed,
    #[error("metadata corrupted")]
    /// The data was corrupted
    Corrupted,
    #[error("{0}")]
    /// The payload data is malformed and could not be deserialized.
    Deserialize(rancor::Error),
}

/// Decode a set of N ([ENTRIES_PER_BLOCK]) page metadata entries and
/// write the result to the provided buffer.
///
/// Metadata entries are packed into blocks in order to maximise read and write
/// efficiency and maintain a 4KB alignment.
///
/// The provided buffer should be 4KB in size.
pub fn decode_page_metadata_block(
    cipher: Option<&encrypt::Cipher>,
    associated_data: &[u8],
    buffer: &mut [u8],
) -> Result<Box<PageMetadataX63Bock>, DecodeError> {
    if buffer.len() != EXPECTED_BUFFER_SIZE {
        return Err(DecodeError::IncorrectBufferSize);
    }

    let [context, buffer] = buffer
        .get_disjoint_mut([0..40, 40..EXPECTED_BUFFER_SIZE])
        .unwrap();

    if let Some(cipher) = cipher {
        encrypt::decrypt_in_place(cipher, associated_data, buffer, context)
            .map_err(|_| DecodeError::DecryptionFailed)?;
    }

    let actual_checksum = crc32fast::hash(&buffer[24..]);
    let expected_checksum = u32::from_le_bytes(buffer[..4].try_into().unwrap());

    if actual_checksum != expected_checksum {
        return Err(DecodeError::Corrupted);
    }

    let view: &rkyv::Archived<PageMetadataX63Bock> =
        rkyv::access(buffer).map_err(DecodeError::Deserialize)?;

    rkyv::deserialize(view)
        .map(Box::new)
        .map_err(DecodeError::Deserialize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_metadata_size() {
        assert_eq!(size_of::<PageMetadata>(), 64);
        assert_eq!(size_of::<ArchivedPageMetadata>(), 64);
        assert_eq!(size_of::<ArchivedPageMetadataX63Bock>(), 4032);
    }
}
