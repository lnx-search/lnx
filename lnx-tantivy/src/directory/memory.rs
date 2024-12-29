use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
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

use crate::directory::BytesWrapper;

type State = Arc<Mutex<BTreeMap<PathBuf, Bytes>>>;

#[derive(Clone, Default)]
/// A [tantivy::Directory] implementation that holds all data in memory.
///
/// Unlike [tantivy::directory::RamDirectory], this directory allows you to
/// retrieve the internal [Bytes] that make up the object allowing us to
/// avoid some additional copies when writing segments.
pub struct MemoryDirectory {
    inner: State,
    watch_callbacks: Arc<WatchCallbackList>,
}

impl MemoryDirectory {
    /// Gets the bytes for a file with the given path.
    pub fn get(&self, path: &Path) -> Result<Bytes, OpenReadError> {
        self.inner
            .lock()
            .get(path)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

impl Debug for MemoryDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryDirectory")
    }
}

impl tantivy::Directory for MemoryDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let data = self.get(path)?;
        let handle = OwnedBytes::new(BytesWrapper(data));
        Ok(Arc::new(handle))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.lock().remove(path);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.inner.lock().contains_key(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let writer = MemoryWriter {
            path: path.to_path_buf(),
            state: self.inner.clone(),
            inner_buffer: BytesMut::with_capacity(8 << 10),
        };
        Ok(WritePtr::with_capacity(2 << 10, Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let data = self.get(path)?;
        Ok(data.to_vec())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.inner
            .lock()
            .insert(path.to_path_buf(), Bytes::copy_from_slice(data));
        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        self.watch_callbacks.broadcast();
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watch_callbacks.subscribe(watch_callback))
    }
}

struct MemoryWriter {
    path: PathBuf,
    inner_buffer: BytesMut,
    state: State,
}

impl std::io::Write for MemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner_buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for MemoryWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        let buffer = mem::take(&mut self.inner_buffer);
        self.state.lock().insert(self.path.clone(), buffer.freeze());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tantivy::Directory;

    use super::*;

    #[test]
    fn test_directory_write_ops() {
        let dir = MemoryDirectory::default();

        let mut writer = dir
            .open_write(Path::new("written.txt"))
            .expect("Open write");
        writer.write_all(b"Hello, world!").expect("Write buffer");
        writer.flush().unwrap();
        writer.terminate().expect("Terminate write");

        let mut writer = dir
            .open_write(Path::new("written2bedeleted.txt"))
            .expect("Open write");
        writer.write_all(b"Hello, world!").expect("Write buffer");
        writer.flush().unwrap();
        writer.terminate().expect("Terminate write");

        dir.atomic_write(Path::new("example.json"), b"Hello JSON!")
            .expect("Write atomic");

        dir.delete(Path::new("written2bedeleted.txt"))
            .expect("Delete existing file");

        assert!(!dir.exists(Path::new("written2bedeleted.txt")).unwrap());
        assert!(dir.exists(Path::new("example.json")).unwrap());
        assert!(dir.exists(Path::new("written.txt")).unwrap());

        dir.delete(Path::new("doesnt-exist.json"))
            .expect("Delete no existent content");

        let content = dir
            .atomic_read(Path::new("example.json"))
            .expect("Read content");
        assert_eq!(Bytes::from(content), Bytes::from_static(b"Hello JSON!"));

        let content = dir
            .atomic_read(Path::new("written.txt"))
            .expect("Read content");
        assert_eq!(Bytes::from(content), Bytes::from_static(b"Hello, world!"));
    }
}
