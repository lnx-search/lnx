use std::ops::Range;

use serde_derive::{Deserialize, Serialize};

pub static FOOTER_MAGIC_BYTES: &[u8] = b"__LNX_BLOB_ENTRY__";

#[derive(Debug, Serialize, Deserialize)]
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
    /// Serializes the footer into a byte buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf =
            rmp_serde::to_vec(self).expect("Footer serialization should never fail");
        buf.extend_from_slice(FOOTER_MAGIC_BYTES);
        buf
    }

    /// Deserializes the footer from a byte buffer.
    ///
    /// This method expects the 4 byte magic code to be at the end of the buffer.
    pub fn from_bytes(mut buffer: &[u8]) -> Option<Self> {
        assert!(buffer.len() > 5, "Buffer is too small to be a footer");
        buffer = &buffer[..buffer.len() - 2];
        rmp_serde::from_slice(buffer).ok()
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
            &bytes[bytes.len() - FOOTER_MAGIC_BYTES.len()..],
            FOOTER_MAGIC_BYTES,
            "Footer magic bytes have not been appended to buffer"
        );
    }

    #[test]
    fn test_footer_deserialize() {
        let footer_bytes: &[u8] = &[
            148, 181, 101, 120, 97, 109, 112, 108, 101, 47, 102, 105, 108, 101, 47, 104,
            101, 114, 101, 46, 116, 120, 116, 146, 0, 123, 206, 0, 171, 142, 115, 192,
            95, 95, 76, 78, 88, 95, 66, 76, 79, 66, 95, 69, 78, 84, 82, 89, 95, 95,
        ];
        let footer = FileEntryFooter::from_bytes(footer_bytes)
            .expect("Footer should deserialize correctly");
        assert_eq!(footer.file_path, "example/file/here.txt");
        assert_eq!(footer.data_range, 0..123);
        assert_eq!(footer.created_at, 11243123);
    }

    #[should_panic]
    #[test]
    fn test_footer_too_small_panic() {
        FileEntryFooter::from_bytes(&[]);
    }
}
