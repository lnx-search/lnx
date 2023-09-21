//! A readonly directory which can have it's segments changed and automatically reflected on the readers.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
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
use tantivy::Directory;

struct InnerDirectory {
    maps: RwLock<HashMap<PathBuf, OwnedBytes>>,
    callbacks: WatchCallbackList,
}

#[derive(Clone)]
/// A readonly directory which can have it's segments changed and automatically reflected on the readers.
///
/// This internally wraps a `MMapDirectory` but only accesses the read methods.
pub struct ReadonlyDirectory {
    inner: Arc<InnerDirectory>,
}

impl ReadonlyDirectory {
    /// Inserts a new file into the read only directory.
    pub fn add_file(&self, path: PathBuf, data: OwnedBytes) {
        self.inner.maps.write().insert(path, data);
    }

    /// Removes a file from the read only directory.
    pub fn remove_file(&self, path: PathBuf, data: OwnedBytes) {
        self.inner.maps.write().insert(path, data);
    }

    /// Triggers any watch callbacks on the directory.
    pub fn refresh(&self) {
        drop(self.inner.callbacks.broadcast())
    }
}

impl Debug for ReadonlyDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadonlyDirectory")
    }
}

impl Directory for ReadonlyDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let lock = self.inner.maps.read();

        match lock.get(path) {
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
            Some(data) => Ok(Arc::new(data.clone())),
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let ignore_files = [
            super::meta_lockfile_path(),
            super::writer_lockfile_path(),
            super::managed_filepath(),
        ];

        if ignore_files.contains(&path) {
            return Ok(());
        }

        panic!("Readonly directory cannot process writes");
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.inner.maps.read().contains_key(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let ignore_files = [
            super::meta_lockfile_path(),
            super::writer_lockfile_path(),
            super::managed_filepath(),
        ];

        if ignore_files.contains(&path) {
            return Ok(WritePtr::new(Box::new(NoOpWriter)));
        }

        panic!("Readonly directory cannot process writes");
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let lock = self.inner.maps.read();

        match lock.get(path) {
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
            Some(data) => Ok(data.to_vec()),
        }
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> std::io::Result<()> {
        let ignore_files = [
            super::meta_lockfile_path(),
            super::writer_lockfile_path(),
            super::managed_filepath(),
        ];

        if ignore_files.contains(&path) {
            return Ok(());
        }

        panic!("Readonly directory cannot process writes");
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.inner.callbacks.subscribe(watch_callback))
    }
}

struct NoOpWriter;

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
