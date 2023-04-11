use std::ops::Range;

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

// pub const BLOCK_CAPACITY: usize = 512 << 10;
// pub const COMPRESSION_LEVEL: i32 = 1;
pub type BlockId = u64;
pub type BlockLocations = Vec<(BlockId, BlockInfo)>;

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug, Clone, Eq, PartialEq)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct BlockInfo {
    /// The position of the block in the current fragment.
    pub location: Range<u32>,
    /// The CRC32 checksum of the given block.
    pub checksum: u32,
}

impl BlockInfo {
    /// The length of the block data.
    pub fn len(&self) -> u32 {
        self.location.end - self.location.start
    }

    /// Get the location of the block with a usize.
    pub fn location_usize(&self) -> Range<usize> {
        self.location.start as usize..self.location.end as usize
    }
}

// /// The document block encoder
// ///
// /// The encoder serializes documents into bytes and records
// /// the length of each document.
// ///
// /// Once the block has reached capacity or is closed the buffer
// /// is compressed.
// pub struct BlockEncoder {
//     temp_buffer: Vec<u8>,
//     lengths: SmallVec<[u32; 32]>,
// }
//
// impl BlockEncoder {
//     /// Create a new block encoder.
//     pub fn new() -> Self {
//         Self {
//             temp_buffer: Vec::with_capacity(BLOCK_CAPACITY),
//             lengths: smallvec![],
//         }
//     }
//
//     /// Resets and clears the existing encoder so that it can be reused.
//     ///
//     /// The reserved allocation capacity is kept.
//     pub fn reset(&mut self) {
//         self.temp_buffer.clear();
//         self.lengths.clear();
//     }
//
//     /// Pushes a document into the block encoder.
//     ///
//     /// Returns if the encoder is full or not.
//     pub fn push_document<'a: 'b, 'b, S: AsRef<str> + 'b>(
//         &mut self,
//         ts: u64,
//         field_lookup: &BTreeMap<String, FieldId>,
//         fields: impl IntoIterator<Item = (&'b S, &'b DocField<'a>)>,
//         num_fields: usize,
//         hash_key: Option<FieldId>,
//     ) -> bool {
//         let start = self.temp_buffer.len();
//         let key = jocky::encode_document_to(
//             &mut self.temp_buffer,
//             ts,
//             field_lookup,
//             num_fields,
//             fields,
//             hash_key,
//         );
//         let len = self.temp_buffer.len() - start;
//         self.temp_buffer.extend_from_slice(&key.to_le_bytes());
//         self.temp_buffer
//             .extend_from_slice(&(len as u32).to_le_bytes());
//
//         self.temp_buffer.len() >= BLOCK_CAPACITY
//     }
//
//     /// Compresses the temp buffer.
//     pub fn export(&self) -> io::Result<Vec<u8>> {
//         zstd::encode_all(self.temp_buffer.as_slice(), COMPRESSION_LEVEL)
//     }
// }
//
// /// Decompresses a block.
// pub fn decompress_block(bytes: &[u8]) -> io::Result<Vec<u8>> {
//     zstd::decode_all(bytes)
// }
