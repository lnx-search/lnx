use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Result};
use bincode::serialize;
use compress::lz4;
use serde::Serialize;
use sled::{DiskPtr, Mode};
use tantivy::directory::error::{DeleteError, Incompatibility, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryClone, FileHandle, MmapDirectory, RamDirectory, WatchCallback, WatchCallbackList, WatchHandle, WritePtr};
use tantivy::Directory;


static WATCHED_MANAGED_FILE: &str = "managed.json";
static WATCHED_META_FILE: &str = "meta.json";
static METASTORE_INNER_ROOT: &str = "metadata";
static DATA_INNER_ROOT: &str = "data";


pub enum OpenType<'a> {
    Dir(&'a Path),
    TempFile,
    Memory,
}


#[derive(Clone)]
pub struct SledBackedDirectory {
    inner: Arc<dyn Directory>,
    watched_files: WatchCallbackList,
    conn: sled::Db,
}

impl SledBackedDirectory {
    pub fn new_with_root(path: &OpenType) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;

        let (conn, inner) = match path {
            OpenType::Dir(path) => (
                sled::Config::new()
                    .use_compression(true)
                    .mode(Mode::HighThroughput)
                    .path(path.join(METASTORE_INNER_ROOT))
                    .open()?,
                Arc::<dyn Directory>::new(
                    MmapDirectory::open(path.join(DATA_INNER_ROOT))?,
                ),
            ),
            OpenType::TempFile => (
                sled::Config::new()
                    .use_compression(true)
                    .mode(Mode::HighThroughput)
                    .temporary(true)
                    .open()?,
                Arc::<dyn Directory>::new(
                    MmapDirectory::create_from_tempdir()?,
                ),
            ),
            OpenType::Memory => (
                sled::Config::new()
                    .use_compression(true)
                    .mode(Mode::LowSpace)
                    .temporary(true)
                    .open()?,
                Arc::<dyn Directory>::new(
                    RamDirectory::create(),
                ),
            ),
        };

        let watched_files = WatchCallbackList::default();

        Ok(Self {
            inner,
            watched_files,
            conn,
        })
    }
}

impl Debug for SledBackedDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SledBackedDirectory")
    }
}

impl DirectoryClone for SledBackedDirectory {
    fn box_clone(&self) -> Box<dyn Directory> {
        Box::new(self.clone())
    }
}

impl Directory for SledBackedDirectory {
    fn get_file_handle(&self, path: &Path) -> core::result::Result<Box<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> core::result::Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> core::result::Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) ->  core::result::Result<WritePtr, OpenWriteError> {
        self.inner.open_write(path)
    }

    fn atomic_read(&self, path: &Path) ->  core::result::Result<Vec<u8>, OpenReadError> {
        let value = self.conn.get(path.as_ref())
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
                    other =>
                        OpenReadError::IoError {
                            io_error: std::io::Error::new(
                                ErrorKind::Other,
                                format!(
                                    "Unknown error: {}",
                                    other,
                                )
                            ),
                            filepath: path.to_path_buf(),
                        },
                }
            })?;

        value
            .map(|v| v.to_vec())
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.conn.insert(path.as_ref(), data)?;

        // Special case handling for Tantivy's file watchlist.
        if let Some(name) = path.file_name() {
            if name == WATCHED_MANAGED_FILE || name == WATCHED_META_FILE {
                // For now we assume the reader and writer are both the same process
                // so we wont worry about waiting for elapsed callbacks.
                let _ = self.watched_files.broadcast();
            }
        }

        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watched_files.subscribe(watch_callback))
    }
}

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
        let data = serialize(value)?;
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
        f.write_str(&format!("StorageBackend(fp={:?})", self.fp))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loading_and_unloading() -> Result<()> {
        let test_structure = vec!["foo", "bar"];

        let storage = StorageBackend::connect(None)?;
        storage.store_structure("test", &test_structure)?;
        if let Some(buffer) = storage.load_structure("test")? {
            let test_res: Vec<&str> = bincode::deserialize(&buffer)?;
            assert_eq!(test_structure, test_res);
        };

        Ok(())
    }
}
