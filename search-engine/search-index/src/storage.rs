use std::fmt::{Debug, Formatter};

use anyhow::{Error, Result};
use rusqlite::Connection;

pub(crate) type ColumnInfo<'a> = (&'a str, &'a str);

/// A wrapper around a SQLite connection that manages the index state.
pub(crate) struct StorageBackend {
    fp: Option<String>,
    conn: Option<Connection>,
}

impl StorageBackend {
    /// Connects to the sqlite DB.
    pub(crate) fn connect(fp: Option<String>) -> Result<Self> {
        let conn;
        if let Some(ref fp) = fp {
            conn = Connection::open(fp)?
        } else {
            conn = Connection::open_in_memory()?;
        }

        Ok(Self {
            fp,
            conn: Some(conn),
        })
    }

    /// Attempts to get the connection otherwise raising an error.
    ///
    /// In normal circumstances this should always succeed, if it does error
    /// something has gone very, very wrong.
    #[inline]
    fn conn(&self) -> Result<&Connection> {
        if let Some(conn) = self.conn.as_ref() {
            return Ok(conn);
        }

        return Err(Error::msg("connection is un-initialised, this is a bug."));
    }

    /// Clears the given table of all data.
    pub(crate) fn clear_table(&self, table_name: &str) -> Result<()> {
        self.conn()?
            .execute(&format!("DELETE FROM {}", table_name), [])?;

        Ok(())
    }

    /// Creates a table with a given name and fields.
    ///
    /// The fields given are in `(name, type)` format.
    pub(crate) fn create_table(
        &self,
        table_name: &str,
        fields: Vec<ColumnInfo>,
    ) -> Result<()> {
        let columns: String = fields
            .iter()
            .map(|v| format!("{} {}", v.0, v.1))
            .collect::<Vec<String>>()
            .join(", ");

        let query_string =
            format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns);
        debug!(
            "[ META-STORAGE ] executing table creation: {}",
            &query_string
        );
        self.conn()?.execute(&query_string, [])?;

        Ok(())
    }

    /// Prepares the given SQL statement returning it ready to be executed.
    pub(crate) fn prepare(&self, stmt: &str) -> Result<rusqlite::Statement> {
        trace!("preparing sql statement {}", stmt);
        Ok(self.conn()?.prepare(stmt)?)
    }

    /// Prepares the given cached SQL statement returning it ready to be executed.
    pub(crate) fn prepare_cached(
        &self,
        stmt: &str,
    ) -> Result<rusqlite::CachedStatement> {
        trace!("preparing sql cached statement {}", stmt);
        Ok(self.conn()?.prepare_cached(stmt)?)
    }

    /// Creates a new connection to the database returning it's 'clone'
    pub(crate) fn duplicate_conn(&self) -> Result<Self> {
        trace!("duplicating connection for path {:?}", &self.fp);
        Self::connect(self.fp.clone())
    }
}

impl Drop for StorageBackend {
    fn drop(&mut self) {
        trace!("dropping storage backend");
        // This is bad, but does it matter if we're just dropping it?
        let conn = self.conn.take().expect("take connection on drop.");

        let _ = conn.close();
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("StorageBackend(fp={:?})", self.fp))
    }
}
