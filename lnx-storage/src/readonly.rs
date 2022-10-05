use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::Write;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use stable_deref_trait::StableDeref;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken,
    FileHandle,
    OwnedBytes,
    TerminatingWrite,
    WatchCallback,
    WatchCallbackList,
    WatchHandle,
    WritePtr,
};
use tantivy::{Directory, HasLen};

use crate::metadata::Metadata;

#[derive(Clone)]
/// A [tantivy::Directory] implementation that maps a single index file/segment
/// and is then read like any other directory.
///
/// This directory is read only and any other write operations are implicitly
/// ignored.
pub struct ReadOnlyDirectory {
    file: Arc<memmap2::Mmap>,
    metadata: Arc<Metadata>,
    watchlist: Arc<Mutex<WatchCallbackList>>,
}

impl ReadOnlyDirectory {
    /// Opens a new index file as a tantivy directory.
    pub fn open(path: &Path) -> Result<Self, crate::StorageError> {
        let file = File::open(path)?;

        let file = unsafe { memmap2::Mmap::map(&file)? };

        let offsets_start = file.len() - (std::mem::size_of::<u32>() * 2);
        let mut offset_slice = &file[offsets_start..];

        let start = read_le_u32(&mut offset_slice)? as usize;
        let end = read_le_u32(&mut offset_slice)? as usize;

        let metadata = Metadata::from_bytes(&file[start..end])?;

        Ok(Self {
            file: Arc::new(file),
            metadata: Arc::new(metadata),
            watchlist: Default::default(),
        })
    }
}

impl Debug for ReadOnlyDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadOnlyDirectory(index={}, segment_id={})",
            self.metadata.index_name, self.metadata.segment_id
        )
    }
}

impl Directory for ReadOnlyDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Box<dyn FileHandle>, OpenReadError> {
        let bounds = self
            .metadata
            .get_file_bounds(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        let reader = FileReader {
            inner: self.file.clone(),
            path: path.to_path_buf(),
            file_bounds: bounds,
        };

        Ok(Box::new(reader))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        debug_assert!(
            path == Path::new(".tantivy-meta.lock"),
            "Expected only the metadata lock file to be attempting a delete.",
        );
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.metadata.get_file_bounds(path).is_some())
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug_assert!(
            path == Path::new(".tantivy-meta.lock"),
            "Expected only the metadata lock file to be attempting a write. This is a no-op.",
        );

        Ok(WritePtr::new(Box::new(NoOpWriter)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bounds = self
            .metadata
            .get_file_bounds(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(self.file[bounds].to_vec())
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> std::io::Result<()> {
        debug_assert!(
            path == Path::new(".tantivy-meta.lock"),
            "Expected only the metadata lock file to be attempting a write. This is a no-op.",
        );

        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watchlist.lock().subscribe(watch_callback))
    }
}

#[derive(Clone)]
/// A reader for a given file which is part of the parent segment.
///
/// This is just a wrapper around some pre-defined ranges from the parent segment.
pub struct FileReader {
    inner: Arc<memmap2::Mmap>,
    path: PathBuf,
    file_bounds: Range<usize>,
}

impl HasLen for FileReader {
    fn len(&self) -> usize {
        self.file_bounds.len()
    }
}

impl Debug for FileReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileReader(file={:?})", self.path)
    }
}

impl FileHandle for FileReader {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let start = self.file_bounds.start + range.start;
        let stop = self.file_bounds.start + range.end;

        let adjusted_range = start..stop;

        let view = FileView {
            inner: self.inner.clone(),
            range: adjusted_range,
        };

        Ok(OwnedBytes::new(view))
    }
}

#[derive(Clone)]
/// A slice view for a given file.
///
/// This just holds a reference to the parent file and the selected range.
pub struct FileView {
    inner: Arc<memmap2::Mmap>,
    range: Range<usize>,
}

impl Deref for FileView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner[self.range.clone()]
    }
}

unsafe impl StableDeref for FileView {}

/// A writer which implements [tantivy::directory::TerminatingWrite] but every operation
/// is a no-op.
pub struct NoOpWriter;

impl Write for NoOpWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for NoOpWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        Ok(())
    }
}

fn read_le_u32(input: &mut &[u8]) -> Result<u32, crate::StorageError> {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u32>());
    *input = rest;

    let converted = int_bytes
        .try_into()
        .map_err(|_| crate::StorageError::Corrupted)?;

    Ok(u32::from_le_bytes(converted))
}
