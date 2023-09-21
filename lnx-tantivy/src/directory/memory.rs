//! The memory only indexing directories used to rapidly produce segments.

use std::fmt;
use std::io::{self, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::HashMap;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken,
    FileHandle,
    FileSlice,
    TerminatingWrite,
    WatchCallback,
    WatchCallbackList,
    WatchHandle,
    WritePtr,
};
use tantivy::{Directory, SegmentId};
use tracing::warn;

/// Writer associated with the [`LighweightRamDirectory`].
///
/// The Writer just writes a buffer.
struct BytesWriter {
    path: PathBuf,
    shared_directory: LighweightRamDirectory,
    data: BytesMut,
    is_flushed: bool,
}

impl BytesWriter {
    fn new(path_buf: PathBuf, shared_directory: LighweightRamDirectory) -> BytesWriter {
        BytesWriter {
            path: path_buf,
            data: BytesMut::with_capacity(4 << 10),
            shared_directory,
            is_flushed: true,
        }
    }
}

impl Drop for BytesWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            warn!(
                path = ?self.path,
                "You forgot to flush {:?} before its writer got Dropped. Do not rely on drop. This \
                 also occurs when the indexer crashed, so you may want to check the logs for the \
                 root cause.",
                self.path
            )
        }
    }
}

impl Write for BytesWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.is_flushed {
            return Ok(());
        }
        self.is_flushed = true;

        self.shared_directory
            .fs
            .write(self.path.clone(), self.data.clone().freeze());
        Ok(())
    }
}

impl TerminatingWrite for BytesWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

fn path_parts(path: &Path) -> (&str, &str) {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("File should have a name");

    let (file_name, ext) = file_name.split_once('.').unwrap_or((file_name, ""));

    (file_name, ext)
}

#[derive(Default)]
struct InnerDirectory {
    file_partitions: DashMap<String, HashMap<String, Bytes>>,
}

impl InnerDirectory {
    fn write(&self, path: PathBuf, data: Bytes) -> bool {
        let (file_name, ext) = path_parts(&path);

        self.file_partitions
            .entry(file_name.to_string())
            .or_default()
            .insert(ext.to_string(), data)
            .is_some()
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let (file_name, ext) = path_parts(path);

        self.file_partitions
            .get(file_name)
            .and_then(|files| files.get(ext).cloned())
            .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))
            .map(|b| FileSlice::from(BytesWrapper(b)))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let (file_name, ext) = path_parts(path);

        let data = self
            .file_partitions
            .get_mut(file_name)
            .and_then(|mut files| files.remove(ext));

        match data {
            Some(_) => Ok(()),
            None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        let (file_name, ext) = path_parts(path);

        self.file_partitions
            .get(file_name)
            .map(|files| files.get(ext).is_some())
            .unwrap_or(false)
    }
}

impl fmt::Debug for LighweightRamDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RamDirectory")
    }
}

/// A Directory storing everything in anonymous memory.
///
/// It is mainly meant for unit testing.
/// Writes are only made visible upon flushing.
#[derive(Clone)]
pub struct LighweightRamDirectory {
    fs: Arc<InnerDirectory>,
    watch_router: Arc<WatchCallbackList>,
}

impl LighweightRamDirectory {
    /// Creates a new [LighweightRamDirectory] with a given hook.
    pub fn new() -> Self {
        Self {
            fs: Arc::new(Default::default()),
            watch_router: Arc::new(WatchCallbackList::default()),
        }
    }

    /// Consumes the segment files and returns them.
    ///
    /// This is in the format of `vec![( file_ext, data )]`
    pub fn get_files(&self, segment_id: SegmentId) -> Vec<(String, Bytes)> {
        let id = segment_id.uuid_string();

        let data = { self.fs.file_partitions.remove(&id).map(|v| v.1) };

        data.unwrap_or_default().into_iter().collect()
    }
}

impl Directory for LighweightRamDirectory {
    #[inline]
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    #[inline]
    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.fs.open_read(path)
    }

    #[inline]
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.fs.delete(path)
    }

    #[inline]
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.fs.exists(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let path_buf = PathBuf::from(path);
        let vec_writer = BytesWriter::new(path_buf.clone(), self.clone());

        let exists = self.fs.write(path_buf.clone(), Bytes::new());

        // force the creation of the file to mimic the MMap directory.
        if exists {
            Err(OpenWriteError::FileAlreadyExists(path_buf))
        } else {
            // We write straight to memory anyway.
            Ok(BufWriter::with_capacity(0, Box::new(vec_writer)))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bytes = self.open_read(path)?.read_bytes().map_err(|io_error| {
            OpenReadError::IoError {
                io_error: Arc::new(io_error),
                filepath: path.to_path_buf(),
            }
        })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let path_buf = PathBuf::from(path);

        self.fs.write(path_buf, Bytes::from(data.to_vec()));

        if path == super::meta_filepath() {
            drop(self.watch_router.broadcast());
        }
        Ok(())
    }

    #[inline]
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watch_router.subscribe(watch_callback))
    }
}

#[derive(Debug, Clone)]
pub struct BytesWrapper(Bytes);

impl Deref for BytesWrapper {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

unsafe impl stable_deref_trait::StableDeref for BytesWrapper {}

/// Events triggered by the directory.
pub trait DirectoryHook: Send + Sync + 'static {
    /// Called when a file is finished writing.
    fn on_file_written(&self, _path: &Path, _data: Bytes) {}

    /// Called when a new file is opened.
    fn on_file_open(&self, _path: &Path) {}
}
