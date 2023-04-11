use std::ops::{Deref, Range};
use std::sync::Arc;

use memmap2::Mmap;

#[derive(Clone)]
/// A cheaply clone slice.
pub struct SharedSlice {
    start: usize,
    end: usize,
    data: Arc<MmapOrBytes>,
}

impl SharedSlice {
    #[inline]
    /// Slice the bytes a given position.
    ///
    /// This is a cheap operation, it just increments a ref-count.
    pub fn slice(&self, range: Range<usize>) -> Self {
        let new_start = self.start + range.start;
        let new_end = self.start + range.end;

        debug_assert!(
            new_start < self.data.as_slice().len(),
            "Slice range out of bounds"
        );
        debug_assert!(
            new_end < self.data.as_slice().len(),
            "Slice range out of bounds"
        );

        Self {
            start: new_start,
            end: new_end,
            data: self.data.clone(),
        }
    }

    /// Copy a slice of bytes into a new buffer.
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Self::from(bytes.to_vec())
    }
}

impl AsRef<[u8]> for SharedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.data.as_slice()[self.start..self.end]
    }
}

impl Deref for SharedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data.as_slice()[self.start..self.end]
    }
}

impl From<Mmap> for SharedSlice {
    fn from(value: Mmap) -> Self {
        Self {
            start: 0,
            end: value.len(),
            data: Arc::new(MmapOrBytes::Mmap(value)),
        }
    }
}

impl From<Vec<u8>> for SharedSlice {
    fn from(value: Vec<u8>) -> Self {
        Self {
            start: 0,
            end: value.len(),
            data: Arc::new(MmapOrBytes::Bytes(value)),
        }
    }
}

impl<T> PartialEq<T> for SharedSlice
where
    T: AsRef<[u8]>,
{
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for SharedSlice {}

enum MmapOrBytes {
    /// A MMap based slice.
    Mmap(Mmap),
    /// In memory buffer.
    Bytes(Vec<u8>),
}

impl MmapOrBytes {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            MmapOrBytes::Mmap(map) => map.as_ref(),
            MmapOrBytes::Bytes(bytes) => bytes.as_slice(),
        }
    }
}
