use std::ops::Range;

use serde_derive::{Deserialize, Serialize};

pub static FOOTER_MAGIC_BYTES: &[u8] = b"__LNX_BLOB_ENTRY__";
pub const FOOTER_MAGIC_BYTES_LEN: usize = 18;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileEntryFooter {
    /// The path of the file.
    pub file_path: String,
    /// The range offsets for the file blob.
    pub data_range: Range<u64>,
    /// The UNIX timestamp of when the file was created.
    pub created_at: u64,
    /// The transaction id attached to file.
    ///
    /// If this is Some(ID) the file is part of a bulk transaction
    /// and is only valid if a transaction commit marker exists.
    pub transaction_id: Option<ulid::Ulid>,
}

impl FileEntryFooter {
    pub const FOOTER_LENGTH_BYTES: usize = size_of::<u32>();
    pub const FOOTER_ADDITIONAL_OVERHEAD_SIZE: usize =
        FOOTER_MAGIC_BYTES_LEN + Self::FOOTER_LENGTH_BYTES;

    #[inline]
    /// Returns the size of the blob.
    pub fn blob_size(&self) -> u64 {
        self.data_range.end - self.data_range.start
    }

    /// Serializes the footer into a byte buffer.
    ///
    /// This will prefix the buffer with the [FOOTER_MAGIC_BYTES] and an u32 length
    /// of the buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(FOOTER_MAGIC_BYTES);
        output.extend_from_slice(&[0; size_of::<u32>()]);

        rmp_serde::encode::write(&mut output, self)
            .expect("Footer serializer should never fail");

        let len = (output.len() - Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE) as u32;
        output[FOOTER_MAGIC_BYTES_LEN..FOOTER_MAGIC_BYTES_LEN + size_of::<u32>()]
            .copy_from_slice(&len.to_le_bytes());

        output
    }

    /// Deserializes the footer from a byte buffer.
    ///
    /// This method expects the [FOOTER_MAGIC_BYTES] and buffer length as u32 and  to be at the start of the buffer.
    pub fn from_bytes(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE + 1 {
            return None;
        }

        let len = u32::from_le_bytes(
            buffer[FOOTER_MAGIC_BYTES_LEN..FOOTER_MAGIC_BYTES_LEN + size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let offset = Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE;

        if buffer.len() < Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE + len {
            return None;
        }

        rmp_serde::from_slice(&buffer[offset..offset + len]).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_serialize() {
        let footer = FileEntryFooter {
            file_path: "example/file/here.txt".to_string(),
            data_range: 0..123,
            created_at: 11243123,
            transaction_id: None,
        };

        let bytes = footer.to_bytes();
        assert_eq!(
            &bytes[..FOOTER_MAGIC_BYTES_LEN],
            FOOTER_MAGIC_BYTES,
            "Footer magic bytes have not been appended to buffer"
        );
    }

    #[test]
    fn test_footer_deserialize() {
        let footer_bytes: &[u8] = &[
            95, 95, 76, 78, 88, 95, 66, 76, 79, 66, 95, 69, 78, 84, 82, 89, 95, 95, 32,
            0, 0, 0, 148, 181, 101, 120, 97, 109, 112, 108, 101, 47, 102, 105, 108, 101,
            47, 104, 101, 114, 101, 46, 116, 120, 116, 146, 0, 123, 206, 0, 171, 142,
            115, 192,
        ];
        let footer = FileEntryFooter::from_bytes(footer_bytes)
            .expect("Footer should deserialize correctly");
        assert_eq!(footer.file_path, "example/file/here.txt");
        assert_eq!(footer.data_range, 0..123);
        assert_eq!(footer.created_at, 11243123);
    }

    #[test]
    fn test_footer_too_small_none() {
        let footer = FileEntryFooter::from_bytes(&[]);
        assert!(footer.is_none());
    }
}
