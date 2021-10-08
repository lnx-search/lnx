use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::Result;
use bincode::serialize;
use lzzzz::lz4;
use serde::Serialize;
use tantivy::Directory;
use tantivy::directory::{MmapDirectory, RamDirectory};


/// A wrapper around a SQLite connection that manages the index state.
#[derive(Clone)]
pub(crate) struct StorageBackend {
    fp: Option<String>,
    conn: Arc<Box<dyn Directory>>,
}

impl StorageBackend {
    /// Connects to the sqlite DB.
    pub(crate) fn connect(fp: Option<String>) -> Result<Self> {
        let conn: Box<dyn Directory>;
        if let Some(ref fp) = fp {
            conn = Box::new(MmapDirectory::open(fp)?)
        } else {
            conn = Box::new(RamDirectory::create());
        }

        Ok(Self {
            fp,
            conn: Arc::new(conn),
        })
    }

    pub(crate) fn store_structure<T: Serialize>(&self, keyspace: &str, value: &T) -> Result<()> {
        let data = serialize(value)?;
        let mut compressed = Vec::new();
        let _ = lz4::compress_to_vec(&data, &mut compressed, lz4::ACC_LEVEL_DEFAULT)?;
        let path = format!("./{}", keyspace);
        self.conn.atomic_write(path.as_ref(), &compressed)?;
        Ok(())
    }

    pub(crate) fn load_structure(&self, keyspace: &str) -> Result<Vec<u8>> {
        let path = format!("./{}", keyspace);
        let compressed = self.conn.atomic_read(path.as_ref())?;
        let mut data = Vec::new();
        let _ = lz4::decompress(&compressed, &mut data)?;
        Ok(data)
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("StorageBackend(fp={:?})", self.fp))
    }
}
