//! The memory only indexing directories used to rapidly produce segments.

use std::collections::HashMap;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fmt;
use std::ops::Deref;

use parking_lot::RwLock;
use bytes::{Bytes, BytesMut};
use tantivy::Directory;
use tantivy::directory::{AntiCallToken, FileHandle, FileSlice, TerminatingWrite, WatchCallback, WatchCallbackList, WatchHandle, WritePtr};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tracing::warn;

/// Writer associated with the [`RamDirectory`].
///
/// The Writer just writes a buffer.
struct VecWriter {
    path: PathBuf,
    shared_directory: RamDirectory,
    data: BytesMut,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf, shared_directory: RamDirectory) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: BytesMut::with_capacity(4 << 10),
            shared_directory,
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
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

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.is_flushed {
            return Ok(())
        }
        self.is_flushed = true;

        let mut fs = self.shared_directory.fs.write();
        fs.write(self.path.clone(), self.data.clone().freeze());
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()?;
        self.shared_directory.hook
            .on_file_written(self.path.as_path(), self.data.clone().freeze());
        Ok(())
    }
}

#[derive(Default)]
struct InnerDirectory {
    fs: HashMap<PathBuf, Bytes>,
    watch_router: WatchCallbackList,
}

impl InnerDirectory {
    fn write(&mut self, path: PathBuf, data: Bytes) -> bool {
        self.fs.insert(path, data).is_some()
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.fs
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))
            .map(|b| FileSlice::from(BytesWrapper(b.clone())))
    }

    fn delete(&mut self, path: &Path) -> Result<(), DeleteError> {
        match self.fs.remove(path) {
            Some(_) => Ok(()),
            None => Err(DeleteError::FileDoesNotExist(PathBuf::from(path))),
        }
    }

    fn exists(&self, path: &Path) -> bool {
        self.fs.contains_key(path)
    }

    fn watch(&mut self, watch_handle: WatchCallback) -> WatchHandle {
        self.watch_router.subscribe(watch_handle)
    }

    fn total_mem_usage(&self) -> usize {
        self.fs.values().map(|f| f.len()).sum()
    }
}

impl fmt::Debug for RamDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RamDirectory")
    }
}

/// A Directory storing everything in anonymous memory.
///
/// It is mainly meant for unit testing.
/// Writes are only made visible upon flushing.
#[derive(Clone)]
pub struct RamDirectory {
    fs: Arc<RwLock<InnerDirectory>>,
    hook: Arc<dyn DirectoryHook>,
}

impl RamDirectory {
    /// Creates a new [RamDirectory] with a given hook.
    pub fn new(hook: impl DirectoryHook) -> Self {
        Self {
            fs: Arc::new(Default::default()),
            hook: Arc::new(hook),
        }
    }

    /// Returns the sum of the size of the different files
    /// in the [`RamDirectory`].
    pub fn total_mem_usage(&self) -> usize {
        self.fs.read().total_mem_usage()
    }
}

impl Directory for RamDirectory {
    #[inline]
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    #[inline]
    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.fs.read().open_read(path)
    }

    #[inline]
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.fs.write().delete(path)
    }

    #[inline]
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self
            .fs
            .read()
            .exists(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let mut fs = self.fs.write();
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf.clone(), self.clone());

        let exists = fs.write(path_buf.clone(), Bytes::new());

        // force the creation of the file to mimic the MMap directory.
        if exists {
            Err(OpenWriteError::FileAlreadyExists(path_buf))
        } else {
            self.hook.on_file_open(path);
            Ok(BufWriter::new(Box::new(vec_writer)))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let path_buf = PathBuf::from(path);

        self.fs.write().write(path_buf, Bytes::from(data.to_vec()));

        if path == super::meta_filepath() {
            drop(self.fs.write().watch_router.broadcast());
        }
        Ok(())
    }

    #[inline]
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.fs.write().watch(watch_callback))
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