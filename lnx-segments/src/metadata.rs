use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::Path;
use std::{io, mem};

use bytecheck::CheckBytes;
use datacake_crdt::HLCTimestamp;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};
use tokio::fs::File;

pub const METADATA_HEADER_SIZE: usize = mem::size_of::<u64>() * 2;

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Debug))]
/// Metadata for the given index segment.
pub struct Metadata {
    /// The name of the parent index this segment is apart of.
    index_name: String,

    /// The unique ID of the segment.
    segment_id: HLCTimestamp,

    /// A history of the segments that this segment was built from.
    derived_from: Vec<HLCTimestamp>,

    /// The specific positions of where each 'file' lies within the blob.
    files: HashMap<String, Range<u64>>,
}

impl Metadata {
    pub fn new(index_name: String, segment_id: HLCTimestamp) -> Self {
        Self::new_with_history(index_name, segment_id, vec![])
    }

    pub fn new_with_history(
        index_name: String,
        segment_id: HLCTimestamp,
        history: Vec<HLCTimestamp>,
    ) -> Self {
        Self {
            index_name,
            segment_id,
            derived_from: history,
            files: HashMap::default(),
        }
    }

    #[inline]
    pub fn index(&self) -> &str {
        &self.index_name
    }

    #[inline]
    pub fn history(&self) -> &[HLCTimestamp] {
        &self.derived_from
    }

    #[inline]
    pub fn segment_id(&self) -> HLCTimestamp {
        self.segment_id
    }

    #[inline]
    pub fn files(&self) -> &HashMap<String, Range<u64>> {
        &self.files
    }

    pub(crate) fn add_history(&mut self, segment_id: HLCTimestamp) {
        self.derived_from.push(segment_id);
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
        self.files
            .get(path.as_ref())
            .map(|range| (range.start as usize)..(range.end as usize))
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

pub fn get_metadata_offsets(
    mut offset_slice: &[u8],
) -> Result<(u64, u64), TryFromSliceError> {
    let start = read_be_u64(&mut offset_slice)? as u64;
    let len = read_be_u64(&mut offset_slice)? as u64;

    Ok((start, len))
}

pub async fn write_metadata_offsets(
    file: &mut File,
    start: u64,
    len: u64,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    file.write_u64(start).await?;
    file.write_u64(len).await?;

    Ok(())
}

#[cfg(target_os = "linux")]
pub async fn write_metadata_offsets_aio(
    file: &mut glommio::io::DmaStreamWriter,
    start: u64,
    len: u64,
) -> io::Result<()> {
    use futures_lite::AsyncWriteExt;

    let start_bytes = start.to_be_bytes();
    let len_bytes = len.to_be_bytes();

    file.write_all(&start_bytes).await?;
    file.write_all(&len_bytes).await?;

    Ok(())
}

fn read_be_u64(input: &mut &[u8]) -> Result<u64, TryFromSliceError> {
    let (int_bytes, rest) = input.split_at(mem::size_of::<u64>());
    *input = rest;

    let converted = int_bytes.try_into()?;

    Ok(u64::from_be_bytes(converted))
}
