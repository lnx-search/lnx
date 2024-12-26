use std::fmt::{Debug, Formatter};
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use lnx_fs::{Body, Bucket, FileMetadata, FileSystemError};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken,
    FileHandle,
    OwnedBytes,
    TerminatingWrite,
    WatchCallback,
    WatchHandle,
    WritePtr,
};
use tantivy::{Directory, HasLen};
use tracing::warn;

use crate::directory::BytesWrapper;

static SILENTLY_IGNORE_FILES: &[&str] = &[".managed.json", ".tantivy-meta.lock", ".tantivy-writer.lock"];

#[derive(Clone)]
/// A directory backed by a [Bucket].
///
/// File access is behind a base path which can be configured.
///
/// This directory only allows reads and will error on writes except
/// for meta.json file writes.
pub struct VFSDirectory {
    base_path: Arc<str>,
    bucket: Bucket,
    handle: tokio::runtime::Handle,
}

impl VFSDirectory {
    /// Creates a new [VFSDirectory].
    pub fn new(base_path_prefix: &str, bucket: Bucket) -> Self {
        let handle = tokio::runtime::Handle::current();
        Self {
            base_path: Arc::from(base_path_prefix.to_string().into_boxed_str()),
            bucket,
            handle,
        }
    }

    fn true_path(&self, suffix: &Path) -> String {
        let str_suffix = suffix.to_string_lossy();
        format!("{}/{str_suffix}", self.base_path)
    }
}

impl Debug for VFSDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "VFSDirectory(base_path={:?})", self.base_path)
    }
}

impl Directory for VFSDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let resolved_path = self.true_path(path);
        let metadata = self.bucket.metadata(&resolved_path).map_err(|e| match e {
            FileSystemError::IoError(e) => OpenReadError::IoError {
                io_error: Arc::new(e),
                filepath: path.to_path_buf(),
            },
            FileSystemError::FileNotFound(_) => {
                OpenReadError::FileDoesNotExist(path.to_path_buf())
            },
            other => OpenReadError::IoError {
                io_error: Arc::new(io::Error::new(ErrorKind::Other, other)),
                filepath: path.to_path_buf(),
            },
        })?;

        let reader = VFSFileReader {
            file_path: resolved_path,
            metadata,
            bucket: self.bucket.clone(),
            handle: self.handle.clone(),
        };

        Ok(Arc::new(reader))
    }

    #[inline]
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let name = path.file_name().unwrap().to_string_lossy();
        if SILENTLY_IGNORE_FILES.contains(&name.as_ref()) {
            return Ok(());
        }

        warn!(path = %path.display(), "Got delete when not allowed");
        let io_error = io::Error::new(
            ErrorKind::Other,
            "Writes are not supported by the VFS directory",
        );
        Err(DeleteError::IoError {
            io_error: Arc::new(io_error),
            filepath: path.to_path_buf(),
        })
    }

    #[inline]
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let path = self.true_path(path);
        Ok(self.bucket.exists(&path))
    }

    #[inline]
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let name = path.file_name().unwrap().to_string_lossy();
        if SILENTLY_IGNORE_FILES.contains(&name.as_ref()) {
            return Ok(WritePtr::new(Box::new(NoOpWriter)));
        }

        warn!(path = %path.display(), "Got write when not allowed");
        let io_error = io::Error::new(
            ErrorKind::Other,
            "Writes are not supported by the VFS directory",
        );
        Err(OpenWriteError::IoError {
            io_error: Arc::new(io_error),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let handle = self.open_read(path)?;
        handle
            .read_bytes()
            .map(|buf| buf.to_vec())
            .map_err(|e| OpenReadError::IoError {
                io_error: Arc::new(e),
                filepath: path.to_path_buf(),
            })
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        if path != Path::new("meta.json") {
            warn!(path = %path.display(), "Got atomic write when not allowed, ignoring write");
            return Ok(());
        }

        let resolved = self.true_path(path);

        let fut = async {
            self.bucket
                .write(&resolved, Body::complete(Bytes::copy_from_slice(data)))
                .await
                .map_err(|e| match e {
                    FileSystemError::IoError(e) => e,
                    other => io::Error::new(ErrorKind::Other, other),
                })
        };

        self.handle.block_on(fut)?;

        Ok(())
    }

    #[inline]
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

pub struct VFSFileReader {
    file_path: String,
    metadata: FileMetadata,
    bucket: Bucket,
    handle: tokio::runtime::Handle,
}

impl Debug for VFSFileReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VFSFileReader")
            .field("file_ath", &self.file_path)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[async_trait::async_trait]
impl FileHandle for VFSFileReader {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.handle.block_on(self.read_bytes_async(range))
    }

    async fn read_bytes_async(
        &self,
        byte_range: Range<usize>,
    ) -> io::Result<OwnedBytes> {
        let range = byte_range.start as u64..byte_range.end as u64;
        read_file_range(&self.file_path, range, &self.bucket).await
    }
}

impl HasLen for VFSFileReader {
    #[inline]
    fn len(&self) -> usize {
        self.metadata.size() as usize
    }
}

async fn read_file_range(
    path: &str,
    range: Range<u64>,
    bucket: &Bucket,
) -> io::Result<OwnedBytes> {
    let body = bucket.read_range(path, range).await.map_err(|e| match e {
        FileSystemError::IoError(e) => e,
        other => io::Error::new(ErrorKind::Other, other),
    })?;

    let full = body.collect().await?;
    Ok(OwnedBytes::new(BytesWrapper(full)))
}

struct NoOpWriter;

impl io::Write for NoOpWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for NoOpWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use lnx_fs::{Body, RuntimeOptions, VirtualFileSystem};

    use super::*;

    #[tokio::test]
    async fn test_directory_non_read_ops() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dir = tempfile::TempDir::new().unwrap();
        let vfs = VirtualFileSystem::mount(dir.path().to_path_buf(), rt_options)
            .await
            .unwrap();

        let bucket = vfs.create_bucket("test").await.expect("Create bucket");

        bucket
            .write(
                "test/sample.txt",
                Body::complete(Bytes::from_static(b"Hello, world!")),
            )
            .await
            .unwrap();

        let dir = VFSDirectory::new("test", bucket);

        let resolved = dir.true_path(Path::new("example.txt"));
        assert_eq!(resolved, "test/example.txt");

        let exists = dir.exists(Path::new("sample.txt")).unwrap();
        assert!(exists);

        dir.sync_directory().expect("no op");
        let _handle = dir.watch(WatchCallback::new(|| {})).expect("no op");

        let err = dir
            .delete(Path::new("example.txt"))
            .expect_err("System should not allow writes");
        assert!(matches!(err, DeleteError::IoError { .. }));

        let err = dir.open_write(Path::new("example.txt"));
        assert!(matches!(err, Err(OpenWriteError::IoError { .. })));

        let err = dir.open_write(Path::new("example.txt"));
        assert!(matches!(err, Err(OpenWriteError::IoError { .. })));
    }

    #[tokio::test]
    async fn test_directory_read_ops() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dir = tempfile::TempDir::new().unwrap();
        let vfs = VirtualFileSystem::mount(dir.path().to_path_buf(), rt_options)
            .await
            .unwrap();

        let bucket = vfs.create_bucket("test").await.expect("Create bucket");

        bucket
            .write(
                "test/sample.txt",
                Body::complete(Bytes::from_static(b"Hello, world!")),
            )
            .await
            .unwrap();

        let dir = VFSDirectory::new("test", bucket);

        let slice = dir
            .open_read(Path::new("sample.txt"))
            .expect("Open read correctly");
        let contents = slice.read_bytes_async().await.expect("Read all bytes");
        assert_eq!(contents.as_slice(), b"Hello, world!");

        let slice = slice.slice(..12);
        let contents = slice.read_bytes_async().await.expect("Read all bytes");
        assert_eq!(contents.as_slice(), b"Hello, world");

        let handle = dir
            .get_file_handle(Path::new("sample.txt"))
            .expect("Get file handle");
        let contents = handle.read_bytes_async(1..12).await.unwrap();
        assert_eq!(contents.as_slice(), b"ello, world");

        tokio::task::spawn_blocking(move || {
            let slice = dir
                .open_read(Path::new("sample.txt"))
                .expect("Open read correctly");
            let contents = slice.read_bytes().expect("Read all bytes");
            assert_eq!(contents.as_slice(), b"Hello, world!");

            let slice = slice.slice(..12);
            let contents = slice.read_bytes().expect("Read all bytes");
            assert_eq!(contents.as_slice(), b"Hello, world");

            let handle = dir
                .get_file_handle(Path::new("sample.txt"))
                .expect("Get file handle");
            let contents = handle.read_bytes(1..12).unwrap();
            assert_eq!(contents.as_slice(), b"ello, world");
        })
        .await
        .unwrap();
    }
}
