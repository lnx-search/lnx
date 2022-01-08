use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use bincode::Options;
use serde::Serialize;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    FileHandle,
    MmapDirectory,
    WatchCallback,
    WatchHandle,
    WritePtr,
};
use tantivy::Directory;

use crate::helpers::cr32_hash;

static WATCHED_MANAGED_FILE: &str = ".managed.json";
static WATCHED_META_FILE: &str = "meta.json";

/// The sub-directory where any lnx-metadata is stored for the index.
static METASTORE_INNER_ROOT: &str = "metadata";

/// The sub-directory where Tantivy's data is stored.
///
/// This maintains compatibility with any Tantivy directory.
static DATA_INNER_ROOT: &str = "data";

#[derive(Debug)]
pub enum OpenType {
    Dir(PathBuf),
    TempFile,
}

impl OpenType {
    pub fn exists(&self) -> bool {
        match self {
            Self::TempFile => true,
            Self::Dir(path) => path.exists(),
        }
    }
}

/// A wrapper around a MmapDirectory but using sled to provide
/// the atomic write/read interface.
///
/// The only difference is Tantivy's special `meta.json` and `managed.json` is
/// ignored and kept mount to the mmap directory.
/// This same logic is applied to watching files, only those aformentioned files
/// are watched.
#[derive(Clone)]
pub struct SledBackedDirectory {
    inner: MmapDirectory,
    conn: sled::Db,
}

impl SledBackedDirectory {
    /// Creates or opens the given path.
    ///
    /// If OpenType::Dir(p) is set the system will ensure the directories are
    /// created and ensured.
    ///
    /// If OpenType::TempFile is set the system will create a temporary structure,
    /// normally for testing.
    pub fn new_with_root(path: &OpenType) -> anyhow::Result<Self> {
        let (conn, inner) = match path {
            OpenType::Dir(path) => {
                std::fs::create_dir_all(path)?;
                std::fs::create_dir_all(path.join(DATA_INNER_ROOT))?;

                (
                    sled::Config::new()
                        .use_compression(true)
                        .mode(sled::Mode::HighThroughput)
                        .path(path.join(METASTORE_INNER_ROOT))
                        .open()?,
                    MmapDirectory::open(path.join(DATA_INNER_ROOT))?,
                )
            },
            OpenType::TempFile => (
                sled::Config::new()
                    .use_compression(true)
                    .mode(sled::Mode::HighThroughput)
                    .temporary(true)
                    .open()?,
                MmapDirectory::create_from_tempdir()?,
            ),
        };

        Ok(Self { inner, conn })
    }
}

impl Debug for SledBackedDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SledBackedDirectory")
    }
}

impl Directory for SledBackedDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> core::result::Result<Box<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> core::result::Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> core::result::Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> core::result::Result<WritePtr, OpenWriteError> {
        self.inner.open_write(path)
    }

    #[instrument(name = "directory-atomic-reader", level = "debug", skip(self))]
    fn atomic_read(&self, path: &Path) -> core::result::Result<Vec<u8>, OpenReadError> {
        // Special case handling for Tantivy's file watchlist.
        if let Some(name) = path.file_name() {
            if name == WATCHED_MANAGED_FILE || name == WATCHED_META_FILE {
                debug!("using inner atomic read due to special file {:?}", &name);
                return self.inner.atomic_read(path);
            }
        }

        debug!("using sled backed atomic read");
        let value = self.conn.get(cr32_hash(path).to_string())
            .map_err(|e| {
                match e {
                    sled::Error::CollectionNotFound(_) =>
                        OpenReadError::FileDoesNotExist(path.to_path_buf()),
                    sled::Error::Unsupported(_) =>
                        OpenReadError::IoError {
                            io_error: std::io::Error::new(
                                ErrorKind::InvalidData,
                                "Metastore has been used in a un-supported way",
                            ),
                            filepath: path.to_path_buf(),
                        },
                    sled::Error::ReportableBug(e) =>
                        panic!("Failed to perform operation due to unexpected error: {}, Please report this as a bug.", e),
                    sled::Error::Io(e) =>
                        OpenReadError::IoError { io_error: e, filepath: path.to_path_buf() },
                    sled::Error::Corruption { at, .. } =>
                        OpenReadError::IoError {
                            io_error: std::io::Error::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "Data corruption has been detected within the metastore system, Possible Info: {:?}",
                                    at,
                                )
                            ),
                            filepath: path.to_path_buf(),
                        },
                    #[allow(unreachable_patterns)]
                    _ => unreachable!(),
                }
            })?;

        value
            .map(|v| v.to_vec())
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    #[instrument(name = "directory-atomic-writer", level = "debug", skip(self, data))]
    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        // Special case handling for Tantivy's file watchlist.
        if let Some(name) = path.file_name() {
            if name == WATCHED_MANAGED_FILE || name == WATCHED_META_FILE {
                debug!("using inner atomic write due to special file {:?}", &name);
                return self.inner.atomic_write(path, data);
            }
        }

        debug!("using sled backed atomic write");
        let id = cr32_hash(path).to_string();
        self.conn.insert(id, data)?;

        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

/// A wrapper around the SledBackedDirectory providing serializer and loading
/// interfaces.
///
/// This is mostly just a sugar wrapper.
#[derive(Clone)]
pub struct StorageBackend {
    conn: SledBackedDirectory,
}

impl StorageBackend {
    pub fn using_conn(conn: SledBackedDirectory) -> Self {
        Self { conn }
    }

    pub fn store_structure<T: Serialize>(
        &self,
        keyspace: &str,
        value: &T,
    ) -> Result<()> {
        let data = bincode::options().with_big_endian().serialize(value)?;

        self.conn.atomic_write(keyspace.as_ref(), &data)?;
        Ok(())
    }

    pub fn load_structure(&self, keyspace: &str) -> Result<Option<Vec<u8>>> {
        let compressed = match self.conn.atomic_read(keyspace.as_ref()) {
            Ok(data) => data,
            Err(OpenReadError::FileDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(Error::from(e)),
        };

        Ok(Some(compressed))
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("StorageBackend")
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use bincode::Options;

    use super::*;

    #[test]
    fn test_loading_and_unloading() -> Result<()> {
        let test_structure = vec!["foo", "bar"];

        let dir = SledBackedDirectory::new_with_root(&OpenType::TempFile)?;
        let storage = StorageBackend::using_conn(dir);
        storage.store_structure("test", &test_structure)?;
        if let Some(buffer) = storage.load_structure("test")? {
            let test_res: Vec<&str> = bincode::options()
                .with_big_endian()
                .deserialize(&buffer)
                .context("failed to deserialize base type")?;

            assert_eq!(test_structure, test_res);
        };

        Ok(())
    }
}
