use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::{Error, Result};
use bincode::serialize;
use compress::lz4;
use rusqlite::OpenFlags;
use serde::Serialize;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{MmapDirectory, RamDirectory};
use tantivy::Directory;


/// A wrapper around a SQLite connection that manages the index state.
#[derive(Clone)]
pub struct StorageBackend {
    fp: Option<String>,
    conn: Arc<dyn Directory>,
}

impl StorageBackend {
    /// Connects to the sqlite DB.
    pub fn connect(fp: Option<String>) -> Result<Self> {
        let conn: Arc<dyn Directory> = if let Some(ref fp) = fp {
            std::fs::create_dir_all(fp)?;
            Arc::new(MmapDirectory::open(fp)?)
        } else {
            Arc::new(RamDirectory::create())
        };

        Ok(Self { fp, conn })
    }

    pub fn store_structure<T: Serialize>(
        &self,
        keyspace: &str,
        value: &T,
    ) -> Result<()> {
        let data = serialize(value)?;
        let mut compressed = Vec::new();
        lz4::encode_block(&data, &mut compressed);

        let path = format!("./{}", keyspace);
        self.conn.atomic_write(path.as_ref(), &compressed)?;
        Ok(())
    }

    pub fn load_structure(&self, keyspace: &str) -> Result<Option<Vec<u8>>> {
        let path = format!("./{}", keyspace);
        let compressed = match self.conn.atomic_read(path.as_ref()) {
            Ok(data) => data,
            Err(OpenReadError::FileDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(Error::from(e)),
        };

        let mut data = Vec::new();
        lz4::decode_block(&compressed, &mut data);
        Ok(Some(data))
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("StorageBackend(fp={:?})", self.fp))
    }
}


fn run_sqlite_worker(db_path: Option<String>) -> anyhow::Result<()> {
    let conn = match db_path {
        None => rusqlite::Connection::open_in_memory()?,
        Some(fp) => rusqlite::Connection::open(fp)?,
    };

    let set: String = conn.query_row(
        "PRAGMA journal_mode=WAL;",
        [],
        |r| r.get(0),
    )?;

    if set == "wal" {
        info!("Persistent store using SQLite WAL mode.")
    } else {
        warn!("Failed to set store to use SQLite WAL mode, performance may be degraded.")
    }

    Ok(())
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

    #[test]
    fn test_sqlite_worker() -> Result<()> {
        run_sqlite_worker(None)
    }
}
