use std::ops::Range;
use serde_derive::{Serialize, Deserialize};

pub static FOOTER_MAGIC_BYTES: &[u8] = b"bffb";

#[derive(Debug, Serialize, Deserialize)]
pub struct FileEntryFooter {
    /// The path of the file.
    pub file_path: String,
    /// The range offsets for the file blob.
    pub data_range: Range<u64>,
    /// The UNIX timestamp of when the file was created.
    pub created_at: u64,
}

impl FileEntryFooter {
    /// Serializes the footer into a byte buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = rmp_serde::to_vec(self)
            .expect("Footer serialization should never fail");
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