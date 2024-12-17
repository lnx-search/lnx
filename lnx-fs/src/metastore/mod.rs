//! The metastore manages file metadata, offsets and position within
//! the virtual file system. It behaves a bit like S3.
//!
//! Internally it is backed by an SQLite database for each bucket.

pub(crate) mod checkpoint;
mod db;
mod mutate;
pub(crate) mod recovery;

use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::instrument;

pub(crate) use self::mutate::BulkMetastoreModifyOperation;
use crate::metastore::db::MetastoreDB;

#[derive(Debug, thiserror::Error)]
/// An error that can occur when the metastore attempts
/// to track a change in the file system.
pub enum MetastoreError {
    #[error("metastore shutdown")]
    /// The metastore system has shutdown or aborted.
    Shutdown,
    #[error("metastore row corrupted")]
    /// The metastore row was corrupted.
    ///
    /// This should never occur unless manual tampering of the metastore
    /// was performed.
    Corrupted,
    #[error("file {0:?} does not exist")]
    /// The file being targeted by an operation does not exist.
    ///
    /// This is only returned on operations that cannot upsert, like renames.
    FileNotFound(String),
    #[error("SQLx Error: {0}")]
    SQLxError(#[from] sqlx::Error),
    #[error("Config Serde Error: {0}")]
    ConfigSerdeError(serde_json::Error),
    #[error("Recovery Error: {0}")]
    /// An error occurred while recovering the metastore state.
    RecoverError(io::Error),
}

#[derive(Clone)]
/// A metastore instance for a given bucket.
pub struct Metastore {
    state: Arc<RwLock<ahash::HashMap<String, MetastoreEntry>>>,
    /// THe SQLite DB wrapper for persisting file information.
    db: MetastoreDB,
}

impl Metastore {
    /// Connect to the metastore located at the given path.
    pub async fn connect(path: &str) -> Result<Self, MetastoreError> {
        let db = MetastoreDB::connect(path).await?;

        Ok(Self {
            state: Arc::default(),
            db,
        })
    }

    /// Attempt to get a file with the given path.
    ///
    /// Returns the full [FileUrl] and [FileMetadata].
    pub(crate) fn get_file(&self, path: &str) -> Option<MetastoreEntry> {
        self.state.read().get(path).map(|e| e.clone())
    }

    /// Returns if the file currently exists with the given path.
    pub(crate) fn exists(&self, path: &str) -> bool {
        self.state.read().contains_key(path)
    }

    #[instrument(skip_all)]
    /// Begins a bulk metastore modify operation which can perform multiple mutations
    /// within a state lock.
    ///
    /// NOTE: This does not isolate file operations, once a file is written it is possible
    /// for it to be recovered upon restart.
    pub(crate) fn begin_mutate(&self) -> BulkMetastoreModifyOperation {
        BulkMetastoreModifyOperation {
            metastore: self,
            mutations: Vec::with_capacity(1),
        }
    }

    /// Returns a list of all files currently within the metastore.
    pub fn list_all_files(&self) -> Vec<MetastoreEntry> {
        let lock = self.state.read();
        lock.values().cloned().collect()
    }

    #[allow(unused)]
    /// Returns a list of all tablets forming the bucket.
    pub fn list_tablets(&self) -> BTreeSet<TabletId> {
        let lock = self.state.read();

        lock.values()
            .map(|entry| entry.metadata.tablet_id)
            .collect()
    }

    #[allow(unused)] // TODO: Add GC system
    /// Returns a list of all files within the given tablet.
    pub fn list_files_in_tablet(&self, tablet_id: TabletId) -> Vec<MetastoreEntry> {
        self.list_files_with_predicate(|entry| entry.metadata.tablet_id == tablet_id)
    }

    /// Returns a list of all files which match the given predicate.
    pub fn list_files_with_predicate<F>(&self, mut pred: F) -> Vec<MetastoreEntry>
    where
        F: FnMut(&MetastoreEntry) -> bool,
    {
        let lock = self.state.read();

        lock.values()
            .filter(|entry| pred(&*entry))
            .cloned()
            .collect()
    }

    /// Attempts to retrieve a configuration value with the given key.
    pub async fn get_config_value<V>(
        &self,
        key: &str,
    ) -> Result<Option<V>, MetastoreError>
    where
        V: serde::de::DeserializeOwned,
    {
        self.db.get_config_value(key).await
    }

    /// Attempts to set a config value with the given key.
    ///
    /// This is implemented as an UPSERT.
    pub async fn set_config_value<V>(
        &self,
        key: &str,
        value: &V,
    ) -> Result<(), MetastoreError>
    where
        V: serde::Serialize + ?Sized,
    {
        self.db.set_config_value(key, value).await
    }

    /// Deletes a config value with a given key.
    pub async fn del_config_value(&self, key: &str) -> Result<(), MetastoreError> {
        self.db.del_config_value(key).await
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct MetastoreEntry {
    pub path: String,
    pub metadata: FileMetadata,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// A tablet ID is an identifier for the raw storage on the file system.
///
/// Each ID is unique and can be lexicographically sorted for timestamp ordering.
pub struct TabletId(pub(super) ulid::Ulid);

impl TabletId {
    #[allow(clippy::new_without_default)]
    /// Creates a new [TabletId] with a unique ID.
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}

impl Display for TabletId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <ulid::Ulid as Display>::fmt(&self.0, f)
    }
}

impl FromStr for TabletId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = ulid::Ulid::from_str(s)?;
        Ok(Self(inner))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FileMetadata {
    /// The tablet ID containing the file data.
    pub tablet_id: TabletId,
    /// The start and stop position of the file in the larger tablet.
    pub(crate) position: Range<u64>,
    /// The UNIX timestamp when the file was created in seconds.
    pub created_at: u64,
}

impl FileMetadata {
    #[inline]
    /// Returns the size of the file.
    pub fn size(&self) -> u64 {
        self.position.end - self.position.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileEvent;

    #[tokio::test]
    async fn test_add_and_get_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/sample.gzip".into(), 0..128),
        );
        bulk_op.commit();

        let MetastoreEntry { path, metadata } = metastore
            .get_file("foo/sample.gzip")
            .expect("File should exist");
        assert_eq!(path, "foo/sample.gzip");
        assert_eq!(metadata.tablet_id, tablet);

        let MetastoreEntry { path, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(path, "foo/bar/example.txt");
        assert_eq!(metadata.tablet_id, tablet);
        assert_eq!(metadata.position, 0..128);
    }

    #[tokio::test]
    async fn test_add_and_list_all_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/example.gzip".into(), 0..128),
        );
        bulk_op.commit();

        let files = metastore.list_all_files();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablets() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/sample.gzip".into(), 0..128),
        );
        bulk_op.commit();

        let files = metastore.list_tablets();
        assert_eq!(files.len(), 1);

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            TabletId::new(),
            FileEvent::create(None, "foo/sample2.gzip".into(), 0..128),
        );
        bulk_op.commit();

        let files = metastore.list_tablets();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablet_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example1.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example2.txt".into(), 0..128),
        );
        bulk_op.add_event(
            TabletId::new(),
            FileEvent::create(None, "foo/bar/example2.txt".into(), 0..128),
        );
        bulk_op.commit();

        let files = metastore.list_files_in_tablet(tablet);
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_add_and_list_extension_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example1.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example2.gzip".into(), 0..128),
        );
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example3.gzip".into(), 0..128),
        );
        bulk_op.commit();

        let files =
            metastore.list_files_with_predicate(|entry| entry.path.ends_with("gzip"));
        assert_eq!(files.len(), 2);
        let files =
            metastore.list_files_with_predicate(|entry| entry.path.ends_with("txt"));
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_add_and_remove_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.commit();

        let MetastoreEntry { path, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(path, "foo/bar/example.txt");
        assert_eq!(metadata.tablet_id, tablet);

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet,
            FileEvent::delete(None, "foo/bar/example.txt".into()),
        );
        bulk_op.commit();

        let maybe_file = metastore.get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should be deleted");
    }

    #[tokio::test]
    async fn test_remove_missing_file() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            TabletId::new(),
            FileEvent::delete(None, "foo/bar/example1.txt".into()),
        );
        bulk_op.commit();
    }

    #[tokio::test]
    async fn test_add_duplicate_file() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet_a = TabletId::new();
        let tablet_b = TabletId::new();

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            tablet_a,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.add_event(
            tablet_b,
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..128),
        );
        bulk_op.commit();

        let MetastoreEntry { path, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(path, "foo/bar/example.txt");
        assert_eq!(metadata.tablet_id, tablet_b);
    }

    #[tokio::test]
    async fn test_bulk_operation_abort() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            TabletId::new(),
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..123),
        );
        bulk_op.rollback();

        let maybe_file = metastore.get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should not exit");

        let mut bulk_op = metastore.begin_mutate();
        bulk_op.add_event(
            TabletId::new(),
            FileEvent::create(None, "foo/bar/example.txt".into(), 0..123),
        );
        bulk_op.commit();

        let MetastoreEntry { path, metadata, .. } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(path, "foo/bar/example.txt");
        assert_eq!(metadata.position, 0..123);
    }
}
