use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;

use bytecheck::CheckBytes;
use datacake_crdt::HLCTimestamp;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Debug))]
/// Metadata for the given index segment.
pub struct Metadata {
    /// The name of the parent index this segment is apart of.
    pub(crate) index_name: String,

    /// The unique ID of the segment.
    pub(crate) segment_id: HLCTimestamp,

    /// The specific positions of where each 'file' lies within the blob.
    pub(crate) files: HashMap<String, Range<usize>>,
}

impl Metadata {
    /// Add a file to the metadata with a given set of positions.
    pub(crate) fn add_file(&mut self, path: &Path, offset: Range<usize>) {
        let path = path.to_string_lossy().into_owned();
        self.files.insert(path, offset);
    }

    /// Gets a specified file offset.
    ///
    /// If the file does not exist `None` is returned.
    pub(crate) fn get_file_bounds(&self, path: &Path) -> Option<Range<usize>> {
        let path = path.to_string_lossy();
        self.files.get(path.as_ref()).cloned()
    }

    /// Deserializes a metadata object from the given buffer.
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self, crate::StorageError> {
        let mut aligned_buffer = AlignedVec::new();
        aligned_buffer.extend_from_slice(data);

        rkyv::from_bytes(&aligned_buffer)
            .map_err(|e| crate::StorageError::DeserializationError(e.to_string()))
    }

    /// Serializes the metadata object to a raw buffer.
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, crate::StorageError> {
        rkyv::to_bytes::<_, 2048>(self)
            .map_err(|e| crate::StorageError::SerializationError(e.to_string()))
            .map(|buf| buf.into_vec())
    }
}
