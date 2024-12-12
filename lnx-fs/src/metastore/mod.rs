//! The metastore manages file metadata, offsets and position within
//! the virtual file system. It behaves a bit like S3.
//!
//! Internally it is backed by an SQLite database for each bucket.

mod bulk;
mod db;

use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use parking_lot::Mutex;
use tracing::instrument;

pub(crate) use crate::metastore::bulk::BulkMetastoreModifyOperation;
use crate::metastore::db::MetastoreDB;

pub(crate) type Cache = moka::sync::Cache<String, (TabletId, FileMetadata)>;

/// The maximum amount of metadata to cache in memory in bytes.
const MAX_CACHE_CAPACITY: u64 = 8 << 10; // 4KB

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
}

#[derive(Clone)]
/// A metastore instance for a given bucket.
pub struct Metastore {
    /// The reader view of the metadata store.
    reader_state: evmap::handles::ReadHandle<String, MetastoreEntry, (), ahash::RandomState>,
    /// The metastore state writer.
    write_state: Arc<Mutex<evmap::handles::WriteHandle<String, MetastoreEntry, (), ahash::RandomState>>>,
    /// THe SQLite DB wrapper for persisting file information.
    db: MetastoreDB,
}

impl Metastore {
    /// Connect to the metastore located at the given path.
    pub async fn connect(path: &str) -> Result<Self, MetastoreError> {
        let db = MetastoreDB::connect(path).await?;
        
        // # Safety
        // The types meet the safety requirement and trait constraints for the map
        // and ahash mimics the same behaviour as the stdlib hasher in regard to consistency.
        let (wx, rx) = unsafe { evmap::with_hasher((), ahash::RandomState::new()) };

        Ok(Self {
            reader_state: rx,
            write_state: Arc::new(Mutex::new(wx)),
            db,
        })
    }

    /// Attempt to get a file with the given path.
    ///
    /// Returns the full [FileUrl] and [FileMetadata].
    pub(crate) fn get_file(
        &self,
        path: &str,
    ) -> Option<MetastoreEntry> {
        self.reader_state
            .get_one(path)
            .map(|e| e.clone())
    }

    #[instrument(skip_all)]
    /// Begins a bulk metastore modify operation which can perform multiple mutations
    /// within a single atomic transaction.
    pub(crate) fn begin_bulk(
        &self,
    ) -> BulkMetastoreModifyOperation {
        BulkMetastoreModifyOperation {
            metastore: self,
            mutations: Vec::with_capacity(1),
        }
    }

    /// Returns a list of all files currently within the metastore.
    pub fn list_all_files(
        &self,
    ) -> Vec<MetastoreEntry> {
        let guard = match self.reader_state.enter() {
            None => return Vec::new(),
            Some(guard) => guard,
        };
        
        guard.values()
            .filter_map(|values| values.get_one())
            .cloned()
            .collect()
        
    }

    /// Returns a list of all tablets forming the bucket.
    pub fn list_tablets(&self) -> BTreeSet<TabletId> {
        let guard = match self.reader_state.enter() {
            None => return BTreeSet::new(),
            Some(guard) => guard,
        };

        guard.values()
            .filter_map(|values| values.get_one())
            .map(|entry| entry.url.tablet_id)
            .collect()
    }

    #[allow(unused)] // TODO: Add GC system
    /// Returns a list of all files within the given tablet.
    pub fn list_files_in_tablet(
        &self,
        tablet_id: TabletId,
    ) -> Vec<MetastoreEntry> {
        self.list_files_with_predicate(|entry| entry.url.tablet_id == tablet_id)
    }

    /// Returns a list of all files which match the given predicate.
    pub fn list_files_with_predicate<F>(
        &self,
        mut pred: F,
    ) -> Vec<MetastoreEntry> 
    where 
        F: FnMut(&MetastoreEntry) -> bool,
    {
        let guard = match self.reader_state.enter() {
            None => return Vec::new(),
            Some(guard) => guard,
        };

        guard.values()
            .filter_map(|values| values.get_one())
            .filter(|entry | pred(&*entry))
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
pub(crate) struct MetastoreEntry {
    pub(crate) url: FileUrl,
    pub(crate) metadata: FileMetadata,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
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

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub struct FileUrl {
    pub path: String,
    tablet_id: TabletId,
}

impl FileUrl {
    /// Creates a new [FileUrl] using the given components.
    pub fn new(path: &str, tablet_id: TabletId) -> Self {
        Self {
            path: path.to_string(),
            tablet_id,
        }
    }

    #[inline]
    /// Returns the [TabletId] of where the file is stored.
    pub(crate) fn tablet_id(&self) -> TabletId {
        self.tablet_id
    }
}

impl Display for FileUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "lnx://{}/{}", self.tablet_id, self.path)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FileMetadata {
    /// The start and stop position of the file in the larger tablet.
    pub(crate) position: Range<u64>,
    /// The UNIX timestamp when the file was created in seconds.
    pub created_at: u64,
}

impl FileMetadata {
    const SIZE_IN_CACHE: usize = size_of::<Self>();

    #[inline]
    /// Returns the size of the file.
    pub fn size(&self) -> u64 {
        self.position.end - self.position.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_get_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, .. } = metastore
            .get_file("foo/sample.gzip")
            .expect("File should exist");
        assert_eq!(url.path, "foo/sample.gzip");
        assert_eq!(url.tablet_id, tablet);

        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(url.tablet_id, tablet);
        assert_eq!(metadata.position, 0..128);
        assert_eq!(metadata.created_at, 12314);
    }

    #[tokio::test]
    async fn test_add_and_list_all_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let files = metastore.list_all_files();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablets() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let files = metastore.list_tablets();
        assert_eq!(files.len(), 1);

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let files = metastore.list_tablets();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablet_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let files = metastore
            .list_files_in_tablet(tablet);
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_extension_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            );
        bulk_op.commit().await.unwrap();

        let files = metastore
            .list_files_with_predicate(|entry| entry.url.path.ends_with("gzip"));
        assert_eq!(files.len(), 2);
        let files = metastore
            .list_files_with_predicate(|entry| entry.url.path.ends_with("txt"));
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_add_and_remove_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, .. } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(url.tablet_id, tablet);

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .remove_file("foo/bar/example.txt");
        bulk_op.commit().await.unwrap();

        let maybe_file = metastore
            .get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should be deleted");
    }

    #[tokio::test]
    async fn test_remove_missing_file() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .remove_file("foo/sample.gzip");
        bulk_op.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_add_duplicate_file() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 123,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(metadata.created_at, 123);
    }

    #[tokio::test]
    async fn test_rename_file() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(metadata.created_at, 12314);

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .rename_file("foo/bar/example.txt", "foo/path2/example.bar")
            .expect("Metastore should allow duplicate path keys and update");
        bulk_op.commit().await.unwrap();

        let maybe_file = metastore
            .get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should be moved");
        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/path2/example.bar")
            .expect("File should exist");
        assert_eq!(url.path, "foo/path2/example.bar");
        assert_eq!(metadata.created_at, 12314);
    }

    #[tokio::test]
    async fn test_delete_tablet_files() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(metadata.created_at, 12314);

        let mut bulk_op = metastore.begin_bulk();
        let _files = bulk_op.delete_tablet_files(tablet);
        bulk_op.commit().await.unwrap();

        let maybe_file = metastore
            .get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should not exit");
    }

    #[tokio::test]
    async fn test_bulk_operation_abort() {
        let metastore = Metastore::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op.rollback();

        let maybe_file = metastore
            .get_file("foo/bar/example.txt");
        assert!(maybe_file.is_none(), "File should not exit");

        let mut bulk_op = metastore.begin_bulk();
        bulk_op
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            );
        bulk_op.commit().await.unwrap();

        let MetastoreEntry { url, metadata } = metastore
            .get_file("foo/bar/example.txt")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(metadata.created_at, 12314);
    }
}
