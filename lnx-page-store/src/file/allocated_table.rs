use bytes::BufMut;
use rkyv::rancor;

use super::integrity::{self, DecodeVerification};
use crate::PageId;

/// Decode the allocation table contained within the given buffer after verifying the integrity
/// of the data.
///
/// HMAC or SHA256 checksum integrity checks are supported and will be validated according
/// to the specified [DecodeVerification], if these checks fail an error will be returned.
pub fn decode_allocation_table(
    buffer: &[u8],
    hmac_key: Option<&[u8]>,
    verification: DecodeVerification,
) -> Result<PageAllocationTable, DecodeAllocationTableError> {
    let (verified, buffer) = verification.verify(buffer, hmac_key);

    if !verified {
        return Err(DecodeAllocationTableError::VerificationFail);
    }

    let accessed = rkyv::access::<rkyv::Archived<PageAllocationTable>, _>(buffer)
        .map_err(DecodeAllocationTableError::Deserialize)?;
    rkyv::deserialize::<PageAllocationTable, _>(accessed)
        .map_err(DecodeAllocationTableError::Deserialize)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the system from decoding an allocation table
/// from a buffer.
pub enum DecodeAllocationTableError {
    #[error("HMAC or SHA256 verification check failed")]
    /// The verification method specified by the [DecodeVerification] enum failed.
    VerificationFail,
    #[error("deserialize error: {0}")]
    /// The system could not parse and deserialize the table.
    Deserialize(rancor::Error),
}

/// Encode the provided allocation table into a buffer.
///
/// This will attach a HMAC or SHA256 checksum to the start of the buffer to be used
/// for integrity checks when decoding.
pub fn encode_allocation_table(
    table: &PageAllocationTable,
    hmac_key: Option<&[u8]>,
    output_buffer: impl BufMut,
) -> Result<(), EncodeAllocationTableError> {
    let encoded_table = rkyv::to_bytes(table).map_err(EncodeAllocationTableError)?;
    integrity::copy_with_check_bytes(&encoded_table, output_buffer, hmac_key);
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
/// The allocation table could not be serialized.
pub struct EncodeAllocationTableError(rancor::Error);

#[derive(Default, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq))]
/// A table header signalling what pages are allocated and what point in the operations
/// log the table accounts for.
pub struct PageAllocationTable {
    /// The current number of operations this table accounts for, there might be newer
    /// operations that this table has yet to account for via the operations log.
    checkpoint: u64,
    /// The page pages allocated up to the specified generation.
    allocated_pages: SimpleBitSet,
}

impl PageAllocationTable {
    /// Creates a new [PageAllocationTable] which can `size` number of pages
    /// rounded up to the nearest `64`.
    pub fn new(size: usize) -> Self {
        Self {
            checkpoint: 0,
            allocated_pages: SimpleBitSet::new(size),
        }
    }

    /// Returns if the current page is allocated or not.
    pub fn is_allocated(&self, page_id: PageId) -> bool {
        self.allocated_pages.get(page_id.0 as usize)
    }

    /// Returns the current checkpoint of the page table.
    pub fn checkpoint(&self) -> u64 {
        self.checkpoint
    }

    /// Advance the table's checkpoint by a given amount.
    pub fn advance_checkpoint(&mut self, by: u64) {
        self.checkpoint += by;
    }

    /// Mark the page as allocated.
    pub fn mark_allocated(&mut self, page_id: PageId) {
        self.allocated_pages.set(page_id.0 as usize);
    }

    /// Mark the given page as free / available for reuse.
    pub fn mark_free(&mut self, page_id: PageId) {
        self.allocated_pages.clear(page_id.0 as usize);
    }
}

#[derive(Default, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq))]
/// A simple bit set which can be serialized and deserialized with rkyv.
///
/// The size of the bitset is always aligned to 64.
struct SimpleBitSet {
    data: Box<[u64]>,
}

impl SimpleBitSet {
    /// Creates a new [SimpleBitSet] with the given `size` rounded up to the nearest
    /// 64 entries.
    fn new(mut size: usize) -> Self {
        size += size % 64;

        let data = vec![0; size];

        Self {
            data: data.into_boxed_slice(),
        }
    }

    /// Returns the size of the bitset.
    fn size(&self) -> usize {
        self.data.len() * 64
    }

    /// Set the flag at a given index.
    fn set(&mut self, index: usize) {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] |= 1 << offset;
    }

    /// Clear the flag at a given index.
    fn clear(&mut self, index: usize) {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] &= !(1 << offset);
    }

    /// Read the flag at the given index.
    fn get(&self, index: usize) -> bool {
        let sector = index / 64;
        let offset = index % 64;
        self.data[sector] & (1 << offset) != 0
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(14)]
    #[case(512)]
    #[should_panic]
    #[case(0)]
    fn test_bitset(#[case] size: usize) {
        let mut bitset = SimpleBitSet::new(size);
        assert_eq!(bitset.size() % 64, 0);

        assert!(!bitset.get(0));
        assert!(!bitset.get(7));
        assert!(!bitset.get(size - 1));

        bitset.set(0);
        bitset.set(7);
        bitset.set(size - 1);

        assert!(bitset.get(0));
        assert!(!bitset.get(1));
        assert!(!bitset.get(6));
        assert!(bitset.get(7));
        assert!(!bitset.get(size - 2));
        assert!(bitset.get(size - 1));

        bitset.clear(0);
        bitset.clear(7);
        bitset.clear(size - 1);

        assert!(!bitset.get(0));
        assert!(!bitset.get(7));
        assert!(!bitset.get(size - 1));
    }

    #[rstest]
    #[case(14)]
    #[case(512)]
    fn test_allocation_table(#[case] size: usize) {
        let mut table = PageAllocationTable::new(size);

        assert!(!table.is_allocated(PageId(0)));
        assert!(!table.is_allocated(PageId(5)));
        assert!(!table.is_allocated(PageId((size - 1) as u32)));

        table.mark_allocated(PageId(0));
        table.mark_allocated(PageId(5));
        table.mark_allocated(PageId((size - 1) as u32));

        assert!(table.is_allocated(PageId(0)));
        assert!(table.is_allocated(PageId(5)));
        assert!(table.is_allocated(PageId((size - 1) as u32)));
        
        table.mark_free(PageId(5));
        
        assert!(table.is_allocated(PageId(0)));
        assert!(!table.is_allocated(PageId(5)));
        assert!(table.is_allocated(PageId((size - 1) as u32)));
        
        assert_eq!(table.checkpoint(), 0);
        table.advance_checkpoint(5);
        assert_eq!(table.checkpoint(), 5);
    }

    #[rstest]
    #[case::encode_table_without_hmac(None)]
    #[case::encode_table_with_hmac(Some(b"test".as_ref()))]
    fn test_encoding_allocation_table(#[case] hmac_key: Option<&[u8]>) {
        let table = PageAllocationTable::new(34);
        let mut buffer = Vec::new();
        encode_allocation_table(&table, hmac_key, &mut buffer)
            .expect("allocation table should be encoded successfully");
    }

    #[rstest]
    #[case::decode_sha256(None, None, DecodeVerification::Sha256)]
    #[case::decode_hmac(Some(b"test".as_ref()), Some(b"test".as_ref()), DecodeVerification::Hmac)]
    #[case::decode_either_sha256(
        None,
        None,
        DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256
    )]
    #[case::decode_either_with_hmac_key(None, Some(b"test".as_ref()), DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256)]
    #[should_panic]
    #[case::encode_table_with_hmac_fail(Some(b"test".as_ref()), Some(b"other".as_ref()), DecodeVerification::Hmac)]
    #[should_panic]
    #[case::encode_table_with_hmac_fail(Some(b"test".as_ref()), None, DecodeVerification::Hmac)]
    #[should_panic]
    #[case::encode_table_with_hmac_fail(None, Some(b"test".as_ref()), DecodeVerification::Hmac)]
    #[should_panic]
    #[case::encode_table_with_sha_fail(Some(b"test".as_ref()), None, DecodeVerification::Sha256)]
    fn test_encode_decode_allocation_table(
        #[case] sign_hmac_key: Option<&[u8]>,
        #[case] verify_hmac_key: Option<&[u8]>,
        #[case] verification: DecodeVerification,
    ) {
        let mut table = PageAllocationTable::new(34);

        table.mark_allocated(PageId(0));
        table.mark_allocated(PageId(5));
        table.mark_allocated(PageId(33));

        let mut buffer = Vec::new();
        encode_allocation_table(&table, sign_hmac_key, &mut buffer)
            .expect("allocation table should be encoded successfully");

        let decoded_table =
            decode_allocation_table(&buffer, verify_hmac_key, verification)
                .expect("decode log entry should pass");
        assert_eq!(decoded_table, table);
    }
}
