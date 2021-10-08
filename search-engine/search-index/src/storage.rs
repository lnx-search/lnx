use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::{Error, Result};
use bincode::serialize;
use lzzzz::lz4;
use serde::Serialize;
use tantivy::directory::{MmapDirectory, RamDirectory, error::OpenReadError};
use tantivy::Directory;

/// A wrapper around a SQLite connection that manages the index state.
#[derive(Clone)]
pub struct StorageBackend {
    fp: Option<String>,
    conn: Arc<Box<dyn Directory>>,
}

impl StorageBackend {
    /// Connects to the sqlite DB.
    pub fn connect(fp: Option<String>) -> Result<Self> {
        let conn: Box<dyn Directory>;
        if let Some(ref fp) = fp {
            std::fs::create_dir_all(fp)?;
            conn = Box::new(MmapDirectory::open(fp)?)
        } else {
            conn = Box::new(RamDirectory::create());
        }

        Ok(Self {
            fp,
            conn: Arc::new(conn),
        })
    }

    pub fn store_structure<T: Serialize>(
        &self,
        keyspace: &str,
        value: &T,
    ) -> Result<()> {
        let data = serialize(value)?;
        let mut compressed = Vec::new();
        let _ = lz4::compress_to_vec(&data, &mut compressed, lz4::ACC_LEVEL_DEFAULT)?;
        let path = format!("./{}", keyspace);
        self.conn.atomic_write(path.as_ref(), &compressed)?;
        Ok(())
    }

    pub fn load_structure(&self, keyspace: &str) -> Result<Option<Vec<u8>>> {
        let path = format!("./{}", keyspace);
        let compressed = match self.conn.atomic_read(path.as_ref()) {
            Ok(data) => data,
            Err(OpenReadError::FileDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(Error::from(e))
        };

        let mut data = Vec::new();
        let _ = lz4::decompress(&compressed, &mut data)?;
        Ok(Some(data))
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("StorageBackend(fp={:?})", self.fp))
    }
}
