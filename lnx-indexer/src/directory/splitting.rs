//! A directory that can read from two directories and only guide writes to one specific directory.


use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::Directory;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchCallbackList, WatchHandle, WritePtr};

#[derive(Clone)]
/// A directory that can read from two directories and only guide writes to one specific directory.
///
/// This effectively gives you a copy-on-write directory with two directories.
pub struct SplittingDirectory<R, W> {
    /// The reading directory.
    reader: R,
    /// The writing directory.
    writer: W,
    /// A set of files which have been updated/written to the writer directory.
    ///
    /// If a file exists here, priority is given to the writing directory.
    written_files: Arc<RwLock<HashSet<PathBuf>>>,
    /// The callback list used to update readers.
    callbacks: Arc<WatchCallbackList>,
}

impl<R, W> SplittingDirectory<R, W> {
    /// Creates a new splitting directory.
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            written_files: Arc::new(RwLock::new(HashSet::new())),
            callbacks: Arc::new(WatchCallbackList::default()),
        }
    }
}

impl<R, W> Debug for SplittingDirectory<R, W>
where
    R: Debug,
    W: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplittingDirectory(reader={:?}, writer={:?})", self.reader, self.writer)
    }
}

impl<R, W> SplittingDirectory<R, W>
where
    R: Clone + Directory,
    W: Clone + Directory,
{
    fn has_writer_priority(&self, path: &Path) -> bool {
        let lock = self.written_files.read();
        lock.contains(path)
    }

    fn insert_written_file(&self, path: &Path) {
        let mut lock = self.written_files.write();
        lock.insert(path.to_path_buf());
    }
}

impl<R, W> Directory for SplittingDirectory<R, W>
where
    R: Clone + Directory,
    W: Clone + Directory,
{
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if !self.has_writer_priority(path) {
            match self.reader.get_file_handle(path) {
                Ok(reader) => Ok(reader),
                Err(OpenReadError::FileDoesNotExist(_)) => self.writer.get_file_handle(path),
                Err(other) => Err(other),
            }
        } else {
            self.writer.get_file_handle(path)
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.writer.delete(path)?;
        self.insert_written_file(path);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if !self.has_writer_priority(path) {
            match self.reader.exists(path) {
                Ok(true) => Ok(true),
                Ok(false) | Err(OpenReadError::FileDoesNotExist(_)) => self.writer.exists(path),
                Err(other) => Err(other),
            }
        } else {
            self.writer.exists(path)
        }
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let ptr = self.writer.open_write(path)?;
        self.insert_written_file(path);
        Ok(ptr)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if !self.has_writer_priority(path) {
            match self.reader.atomic_read(path) {
                Ok(buf) => Ok(buf),
                Err(OpenReadError::FileDoesNotExist(_)) => self.writer.atomic_read(path),
                Err(other) => Err(other),
            }
        } else {
            self.writer.atomic_read(path)
        }
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.writer.atomic_write(path, data)?;
        self.insert_written_file(path);

        if path == super::meta_filepath() {
            self.callbacks.broadcast();
        }

        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        self.writer.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.callbacks.subscribe(watch_callback))
    }
}