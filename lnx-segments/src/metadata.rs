use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::{io, mem};
use std::io::ErrorKind;

use bytecheck::CheckBytes;
use datacake_crdt::HLCTimestamp;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

pub const METADATA_HEADER_SIZE: usize = mem::size_of::<u64>();

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Debug))]
/// Metadata for the given index segment.
pub struct Metadata {
    /// The name of the parent index this segment is apart of.
    index_name: String,

    /// The unique ID of the segment.
    segment_id: HLCTimestamp,

    /// The specific positions of where each 'file' lies within the blob.
    files: HashMap<String, Range<u64>>,
}

impl Metadata {
    pub fn new(index_name: String, segment_id: HLCTimestamp) -> Self {
        Self {
            index_name,
            segment_id,
            files: HashMap::default(),
        }
    }

    #[inline]
    pub fn index(&self) -> &str {
        &self.index_name
    }

    #[inline]
    pub fn segment_id(&self) -> HLCTimestamp {
        self.segment_id
    }

    #[inline]
    pub fn files(&self) -> &HashMap<String, Range<u64>> {
        &self.files
    }

    /// Add a file to the metadata with a given set of positions.
    pub(crate) fn add_file(&mut self, path: impl AsRef<Path>, offset: Range<u64>) {
        let path = path.as_ref().to_string_lossy().into_owned();
        self.files.insert(path, offset);
    }

    /// Gets a specified file offset.
    ///
    /// If the file does not exist `None` is returned.
    pub fn get_file_bounds(&self, path: &Path) -> Option<Range<usize>> {
        let path = path.to_string_lossy();
        self.files.get(path.as_ref()).map(|range| (range.start as usize)..(range.end as usize))
    }

    /// Deserializes a metadata object from the given buffer.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let mut aligned_buffer = AlignedVec::new();
        aligned_buffer.extend_from_slice(data);

        rkyv::from_bytes(&aligned_buffer)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))
    }

    /// Serializes the metadata object to a raw buffer.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        rkyv::to_bytes::<_, 2048>(self)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))
            .map(|buf| buf.into_vec())
    }
}


pub fn get_metadata_offsets(mut offset_slice: &[u8]) -> Result<(u64, u64), TryFromSliceError> {
    let start = read_be_u64(&mut offset_slice)? as u64;
    let end = read_be_u64(&mut offset_slice)? as u64;

    Ok((start, end))
}

fn read_be_u64(input: &mut &[u8]) -> Result<u64, TryFromSliceError> {
    let (int_bytes, rest) = input.split_at(mem::size_of::<u64>());
    *input = rest;

    let converted = int_bytes
        .try_into()?;

    Ok(u64::from_be_bytes(converted))
}
