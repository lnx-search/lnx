use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bon::Builder;
use moka::policy::EvictionPolicy;
use tracing::{debug, info, instrument, trace};

use crate::io::{
    Body,
    RuntimeDispatcher,
    TabletReader,
    TabletReaderOptions,
    TabletWriter,
    TabletWriterOptions,
};
use crate::metastore::{
    BulkMetastoreModifyOperation,
    FileUrl,
    Metastore,
    MetastoreError,
    TabletId,
};
use crate::service::FileSystemError;
use crate::{BucketConfig, FileMetadata, MaybeUnset};

static TABLET_PATH: &str = "tablets";
static METASTORE_FILE: &str = "metastore.sqlite";
const DEFAULT_TTI_SECS: u64 = 60 * 60; // 1 hour.
const DEFAULT_MAX_OPEN_READERS: usize = 512; // 1 hour.

/// A bucket that can be cheaply cloned and shared
/// by being wrapped in an [Arc].
pub type SharedBucket = Arc<Bucket>;

#[derive(Debug, Builder)]
/// Options that can be configured when creating a bucket.
pub struct BucketCreateOptions {
    #[builder(into)]
    /// The name of the bucket.
    name: String,
    /// The base path for the bucket on disk.
    bucket_path: PathBuf,
}

/// Virtual File System Bucket
///
/// This is a way of organising a set of files into completely isolated partitions, similar
/// to that of S3.
///
/// Buckets are not designed to be created in the hundreds or thousands, or even tens,
/// they should be used in situations where you _must_ have the file system isolation
/// or when the system cannot keep up with a single SQLite metastore.
///
/// ### File System Structure
///
/// Buckets are laid out on disk in a consistent structure:
///
/// ```text
/// base_path/
/// ├── metastore.sqlite
/// └── tablets/
///     └── 01JCXNCND5Q2ANW5JD8F08DN3V.tablet
///     └── 01JCXNCND4PG1S3317HA4JC2B6.tablet
///     └── 01JCXNCNDRT1YGN3X459XTQSCA.tablet
/// ```
///
/// #### `metastore.sqlite`
///
/// This contains the metadata of live and active files contained within the
/// `tablets`, along with metadata about the bucket itself, e.g. name, config, etc...
///
/// #### `tablets/`
///
/// This is the main data directory where all tablet files are written to.
///
/// It is possible that some tablets exist within the directory while not being in used
/// anymore, this is because the system only periodically performs a compaction and GC
/// of the dead files.
///
pub struct Bucket {
    /// The currently active bucket config.
    config: BucketConfig,
    /// The paths within the bucket containing various parts of the bucket data.
    paths: BucketPaths,
    /// The metastore for the bucket.
    metastore: Metastore,
    /// The tablet writer for completing new write requests.
    writer: TabletWriter,
    /// An LFU cache of open tablet readers.
    ///
    /// The size of the cache can be configured to hold a variable number
    /// of open files to help minimize file descriptor errors.
    readers: moka::sync::Cache<TabletId, TabletReader, ahash::RandomState>,
    /// The IO runtime for the bucket.
    runtime: RuntimeDispatcher,
}

impl Bucket {
    /// Creates a new bucket with the given runtime.
    ///
    /// If a bucket already exist at the target path a [FileSystemError::BucketAlreadyExists]
    /// is returned.
    pub(crate) async fn create(
        options: BucketCreateOptions,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let paths = BucketPaths::from_base(options.bucket_path.clone());

        if paths.metastore_exists()? {
            return Err(FileSystemError::BucketAlreadyExists(options.name));
        }

        paths.ensure_bucket_path_exists()?;
        paths.ensure_tablets_path_exists()?;
        paths.ensure_metastore_file_exists()?;

        let metastore = Metastore::connect(&paths.metastore_sqlite_path()).await?;

        let config = BucketConfig::builder().name(options.name).build();
        config.store_in_metastore(&metastore).await?;

        Self::open_bucket_inner(config, paths, metastore, runtime).await
    }

    /// Opens an existing bucket with the given runtime.
    ///
    /// If no bucket exists in the given folder a [FileSystemError::BucketNotFound]
    /// error is returned.
    pub(crate) async fn open(
        base_path: PathBuf,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let paths = BucketPaths::from_base(base_path.clone());

        if !paths.metastore_exists()? {
            return Err(FileSystemError::BucketNotFound(paths.guess_bucket_name()));
        }

        let metastore = Metastore::connect(&paths.metastore_sqlite_path()).await?;

        let mut config = BucketConfig::default();
        config.load_from_metastore(&metastore).await?;

        Self::open_bucket_inner(config, paths, metastore, runtime).await
    }

    async fn open_bucket_inner(
        config: BucketConfig,
        paths: BucketPaths,
        metastore: Metastore,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let tablets = metastore.list_tablets().await?;

        // Ensure all the tablets exist, if some are missing, we have an issue.
        for tablet in tablets {
            if !paths.tablet_exists(tablet) {
                return Err(FileSystemError::Corrupted(format!(
                    "Bucket {:?} is missing tablet file {tablet}",
                    config.name
                )));
            }
        }

        let writer_options = TabletWriterOptions::builder()
            .base_path(paths.tablets_path.clone())
            .maybe_max_active_writers(config.max_active_writers())
            .maybe_max_tablet_size(config.max_tablet_size_bytes())
            .build();
        let writer = TabletWriter::new(writer_options, runtime.clone());

        let max_open_readers = config
            .max_open_readers()
            .unwrap_or(DEFAULT_MAX_OPEN_READERS);
        let time_to_idle = Duration::from_secs(
            config
                .readers_time_to_idle_secs()
                .unwrap_or(DEFAULT_TTI_SECS),
        );

        let readers = moka::sync::CacheBuilder::new(max_open_readers as u64)
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .time_to_idle(time_to_idle)
            .build_with_hasher(ahash::RandomState::new());

        Ok(Self {
            config,
            paths,
            metastore,
            writer,
            readers,
            runtime,
        })
    }

    /// Returns the name of the bucket.
    pub fn name(&self) -> &str {
        match &self.config.name {
            MaybeUnset::Unset => panic!("Bucket config name should always be set"),
            MaybeUnset::None => panic!("Bucket config name should always be set"),
            MaybeUnset::Some(name) => name,
        }
    }

    /// Returns a reference to the current bucket config.
    pub fn config(&self) -> &BucketConfig {
        &self.config
    }

    /// Returns the path the bucket is mounted to.
    pub fn path(&self) -> &Path {
        self.paths.base_path.as_path()
    }

    #[instrument(skip(self))]
    /// Begins a new bulk operation transaction.
    ///
    /// All operations applied via the [BulkBucketTx] will be all-or-nothing,
    /// meaning either all the operations go through or none of them do.
    ///
    /// Be aware that data is still written to disk and the atomic handling
    /// of operations is done via the metastore. Data which then gets left behind
    /// after a rollback will be cleaned up eventually by the bucket GC.
    pub async fn begin_tx(&self) -> Result<BulkBucketTx<'_>, FileSystemError> {
        let metastore = self.metastore.begin_bulk().await?;
        Ok(BulkBucketTx {
            metastore,
            bucket: self,
            num_ops_pending: 0,
        })
    }

    #[instrument(skip(self, body))]
    /// Write a blob body stream to the store with the given path.
    ///
    /// Once this call completes, the blob is safely persisted to disk.
    pub async fn write(&self, path: &str, body: Body) -> Result<(), FileSystemError> {
        assert!(!path.ends_with('/'), "Path cannot end with `/`");

        trace!("Begin writing blob");

        let response = self.writer.write(body).await?;
        trace!("Blob write complete");

        let url = FileUrl::new(path, response.tablet_id);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let metadata = FileMetadata {
            position: response.position,
            created_at: now as i64,
        };

        let mut bulk = self.metastore.begin_bulk().await?;
        bulk.add_file(url, metadata).await?;
        bulk.commit().await?;
        trace!("Metadata updated");

        Ok(())
    }

    #[instrument(skip(self))]
    /// Creates creates a new read stream for the given file if it exists.
    ///
    /// NOTE:
    /// This operation can be quite expensive if the file is not in the cache,
    /// this is because it will have to open and map the file with various buffers.
    ///
    /// To minimise this impact, it is important to have a suitably sized
    /// reader cache allowance.
    pub async fn read(&self, path: &str) -> Result<Body, FileSystemError> {
        trace!("Begin reading blob");

        let (url, metadata) = self
            .metastore
            .get_file(path)
            .await?
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))?;
        let tablet_id = url.tablet_id();

        if let Some(reader) = self.readers.get(&tablet_id) {
            return reader
                .read(metadata.position)
                .await
                .map_err(FileSystemError::from);
        }

        debug!("Reader is not cached, creating new");
        let options = TabletReaderOptions::builder()
            .base_path(self.paths.tablets_path.clone())
            .tablet_id(tablet_id)
            .maybe_sequential_read_threshold(
                self.config.sequential_read_threshold_bytes(),
            )
            .maybe_max_concurrent_reads(self.config.max_concurrent_tablet_reads())
            .build();

        let reader = TabletReader::open(options, self.runtime.clone()).await?;
        self.readers.insert(tablet_id, reader.clone());

        reader
            .read(metadata.position)
            .await
            .map_err(FileSystemError::from)
    }

    #[instrument(skip(self))]
    /// Rename a file from one name to another name.
    ///
    /// This will overwrite any existing file implicitly.
    pub async fn rename(
        &self,
        from_path: &str,
        to_path: &str,
    ) -> Result<(), FileSystemError> {
        let mut bulk = self.metastore.begin_bulk().await?;
        bulk.rename_file(from_path, to_path).await?;
        bulk.commit().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    /// Deletes a file from the system.
    ///
    /// Does nothing if the file doesn't exist.
    pub async fn delete(&self, path: &str) -> Result<(), FileSystemError> {
        let mut bulk = self.metastore.begin_bulk().await?;
        bulk.remove_file(path).await?;
        bulk.commit().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    /// Returns the file metadata associated with the given file.
    pub async fn metadata(&self, path: &str) -> Result<FileMetadata, FileSystemError> {
        trace!("Get metadata");
        let url_and_metadata = self.metastore.get_file(path).await?;
        url_and_metadata
            .map(|pair| pair.1)
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))
    }

    #[instrument(skip(self))]
    /// List all files in the bucket
    pub async fn list_all_files(
        &self,
    ) -> Result<Vec<(String, FileMetadata)>, FileSystemError> {
        let files = self.metastore.list_all_files().await?;

        Ok(files
            .into_iter()
            .map(|(url, metadata)| (url.path, metadata))
            .collect())
    }

    #[instrument(skip(self))]
    /// List all files in the bucket with a given extension
    pub async fn list_files_with_ext(
        &self,
        extension: &str,
    ) -> Result<Vec<(String, FileMetadata)>, FileSystemError> {
        let files = self.metastore.list_files_with_ext(extension).await?;

        Ok(files
            .into_iter()
            .map(|(url, metadata)| (url.path, metadata))
            .collect())
    }

    #[instrument(skip_all)]
    /// Attempts to update the currently configured config values
    /// with the new provided set of values.
    ///
    /// The system will need to restart before these changes are applied.
    pub async fn update_config(
        &self,
        config: BucketConfig,
    ) -> Result<(), MetastoreError> {
        config.store_in_metastore(&self.metastore).await
    }
}

/// A bucket operation that allows applying multiple mutations
/// as part of a single operation.
///
/// This means multiple files can be written, deleted, etc... as part of a single
/// atomic operation.
///
/// That being said, it is important to note _data is still written to disk_, it is just
/// not committed in the metastore and the now written data will eventually be cleaned
/// up by the bucket's GC system.
pub struct BulkBucketTx<'bucket> {
    metastore: BulkMetastoreModifyOperation<'bucket>,
    bucket: &'bucket Bucket,
    num_ops_pending: usize,
}

impl<'bucket> BulkBucketTx<'bucket> {
    #[instrument("bulk_write", skip(self, body))]
    /// Write a blob body stream to the store with the given path.
    ///
    /// Once this call completes, the blob is safely persisted to disk.
    pub async fn write(
        &mut self,
        path: &str,
        body: Body,
    ) -> Result<(), FileSystemError> {
        assert!(!path.ends_with('/'), "Path cannot end with `/`");

        trace!("Begin writing blob");

        let response = self.bucket.writer.write(body).await?;
        trace!("Blob write complete");

        let url = FileUrl::new(path, response.tablet_id);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let metadata = FileMetadata {
            position: response.position,
            created_at: now as i64,
        };

        self.metastore.add_file(url, metadata).await?;
        trace!("Metadata updated");

        self.num_ops_pending += 1;

        Ok(())
    }

    #[instrument("bulk_rename", skip(self))]
    /// Rename a file from one name to another name.
    ///
    /// This will overwrite any existing file implicitly.
    pub async fn rename(
        &mut self,
        from_path: &str,
        to_path: &str,
    ) -> Result<(), FileSystemError> {
        self.metastore.rename_file(from_path, to_path).await?;
        self.num_ops_pending += 1;
        Ok(())
    }

    #[instrument("bulk_delete", skip(self))]
    /// Deletes a file from the system.
    ///
    /// Does nothing if the file doesn't exist.
    pub async fn delete(&mut self, path: &str) -> Result<(), FileSystemError> {
        self.metastore.remove_file(path).await?;
        self.num_ops_pending += 1;
        Ok(())
    }

    #[instrument(skip(self))]
    /// Commits all currently pending bucket operations.
    pub async fn commit(self) -> Result<(), FileSystemError> {
        if self.num_ops_pending == 0 {
            return Ok(());
        }
        self.metastore.commit().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    /// Explicitly rollback all currently pending operations.
    ///
    /// This is implicitly ran if the operation is dropped before
    /// either `rollback` or `commit` is explicitly called.
    pub async fn rollback(self) -> Result<(), FileSystemError> {
        if self.num_ops_pending == 0 {
            return Ok(());
        }
        self.metastore.rollback().await?;
        Ok(())
    }
}

struct BucketPaths {
    metastore_path: PathBuf,
    tablets_path: PathBuf,
    base_path: PathBuf,
}

impl BucketPaths {
    fn from_base(base_path: PathBuf) -> Self {
        Self {
            metastore_path: base_path.join(METASTORE_FILE),
            tablets_path: base_path.join(TABLET_PATH),
            base_path,
        }
    }

    fn metastore_exists(&self) -> io::Result<bool> {
        self.metastore_path.try_exists()
    }

    fn metastore_sqlite_path(&self) -> String {
        format!("sqlite:{}", self.metastore_path.display())
    }

    fn guess_bucket_name(&self) -> String {
        if let Some(dir) = self.base_path.file_name() {
            dir.to_string_lossy().to_string()
        } else {
            self.base_path.display().to_string()
        }
    }

    fn ensure_tablets_path_exists(&self) -> io::Result<()> {
        if self.tablets_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.tablets_path.display(), "Create tablet path");
        std::fs::create_dir(self.tablets_path.as_path())?;

        Ok(())
    }

    fn ensure_metastore_file_exists(&self) -> io::Result<()> {
        if self.metastore_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.metastore_path.display(), "Create metastore");
        std::fs::File::create(self.metastore_path.as_path())?;

        Ok(())
    }

    fn ensure_bucket_path_exists(&self) -> io::Result<()> {
        if self.base_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.base_path.display(), "Create bucket path");
        std::fs::create_dir(self.base_path.as_path())?;

        Ok(())
    }

    fn tablet_exists(&self, tablet_id: TabletId) -> bool {
        self.tablets_path.join(tablet_id.to_string()).exists()
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use bytes::Bytes;

    use super::*;
    use crate::io::RuntimeOptions;

    #[test]
    fn test_paths_resolve_to_correct_layout() {
        let base_path = temp_dir().join("test-metastore");
        let paths = BucketPaths::from_base(base_path.clone());
        assert_eq!(paths.metastore_path, base_path.join(METASTORE_FILE));
        assert_eq!(paths.tablets_path, base_path.join(TABLET_PATH));
        assert_eq!(paths.guess_bucket_name(), "test-metastore");
    }

    #[tokio::test]
    async fn test_create_new_bucket() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name)
            .build();

        let _bucket = Bucket::create(options, dispatch)
            .await
            .expect("Create bucket");
    }

    #[tokio::test]
    async fn test_open_existing_bucket() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");
        drop(bucket);

        let expected_path = temp_dir().join(bucket_name);
        Bucket::open(expected_path, dispatch)
            .await
            .expect("Open existing bucket");
    }

    #[tokio::test]
    async fn test_update_bucket_config() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let new_config = BucketConfig::builder()
            .max_open_readers(2)
            .max_active_writers(1)
            .build();
        bucket
            .update_config(new_config)
            .await
            .expect("Update config in metastore");
        drop(bucket);

        let expected_path = temp_dir().join(bucket_name);
        let bucket = Bucket::open(expected_path, dispatch)
            .await
            .expect("Open existing bucket");
        let loaded_config = bucket.config();
        assert_eq!(loaded_config.max_active_writers(), Some(1));
        assert_eq!(loaded_config.max_open_readers(), Some(2));
    }

    #[tokio::test]
    async fn test_bucket_write_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let body = Body::complete(Bytes::from_static(b"Hello, World!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "example.txt");
    }

    #[tokio::test]
    async fn test_bucket_read_write_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let body = Body::complete(Bytes::from_static(b"Hello, World!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let body = bucket.read("example.txt").await.expect("Read file");

        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_bucket_delete_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let body = Body::complete(Bytes::from_static(b"Hello, World!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);

        bucket.delete("example.txt").await.expect("Delete file");
        let files = bucket.list_all_files().await.expect("List files");
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_bucket_rename_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let body = Body::complete(Bytes::from_static(b"Hello, World!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "example.txt");

        bucket
            .rename("example.txt", "example2.bar")
            .await
            .expect("rename file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "example2.bar");
    }

    #[tokio::test]
    async fn test_get_metadata() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let body = Body::complete(Bytes::from_static(b"Hello, World!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);

        let metadata = bucket.metadata("example.txt").await.expect("Get metadata");
        assert_eq!(metadata.position, 0..13);
    }

    #[tokio::test]
    async fn test_list_with_extensions() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        bucket
            .write(
                "example1.txt",
                Body::complete(Bytes::from_static(b"Hello, World!")),
            )
            .await
            .expect("Write file");
        bucket
            .write(
                "example2.txt",
                Body::complete(Bytes::from_static(b"Hello, World!")),
            )
            .await
            .expect("Write file");
        bucket
            .write(
                "example.bar",
                Body::complete(Bytes::from_static(b"Hello, World!")),
            )
            .await
            .expect("Write file");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 3);

        let files = bucket.list_files_with_ext("txt").await.expect("List files");
        assert_eq!(files.len(), 2);
        let files = bucket.list_files_with_ext("bar").await.expect("List files");
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_bucket_bulk_write_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let mut bulk = bucket.begin_tx().await.expect("Create bulk operation");
        bulk.write(
            "example1.txt",
            Body::complete(Bytes::from_static(b"Hello, World 1!")),
        )
        .await
        .expect("Write file");
        bulk.write(
            "example2.txt",
            Body::complete(Bytes::from_static(b"Hello, World 2!")),
        )
        .await
        .expect("Write file");
        bulk.write(
            "example3.txt",
            Body::complete(Bytes::from_static(b"Hello, World 3!")),
        )
        .await
        .expect("Write file");
        bulk.commit().await.expect("Commit operation");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].0, "example1.txt");
        assert_eq!(files[1].0, "example2.txt");
        assert_eq!(files[2].0, "example3.txt");
    }

    #[tokio::test]
    async fn test_bucket_bulk_rename_file() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let mut bulk = bucket.begin_tx().await.expect("Create bulk operation");
        bulk.write(
            "example1.txt",
            Body::complete(Bytes::from_static(b"Hello, World 1!")),
        )
        .await
        .expect("Write file");
        bulk.write(
            "example2.txt",
            Body::complete(Bytes::from_static(b"Hello, World 2!")),
        )
        .await
        .expect("Write file");
        bulk.write(
            "example3.txt",
            Body::complete(Bytes::from_static(b"Hello, World 3!")),
        )
        .await
        .expect("Write file");
        bulk.commit().await.expect("Commit operation");

        let mut bulk = bucket.begin_tx().await.expect("Create bulk operation");
        bulk.rename("example1.txt", "example4.txt")
            .await
            .expect("Rename file");
        bulk.rename("example2.txt", "example5.txt")
            .await
            .expect("Rename file");
        bulk.delete("example4.txt").await.expect("Del file");
        bulk.commit().await.expect("Commit operation");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, "example5.txt");
        assert_eq!(files[1].0, "example3.txt");
    }

    #[tokio::test]
    async fn test_bucket_transaction_isolation() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketCreateOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");

        let mut bulk = bucket.begin_tx().await.expect("Create bulk operation");
        bulk.write(
            "example1.txt",
            Body::complete(Bytes::from_static(b"Hello, World 1!")),
        )
        .await
        .expect("Write file");
        bulk.commit().await.expect("Commit operation");

        let mut bulk = bucket.begin_tx().await.expect("Create bulk operation");
        bulk.write(
            "example2.txt",
            Body::complete(Bytes::from_static(b"Hello, World 2!")),
        )
        .await
        .expect("Write file");
        bulk.delete("example1.txt").await.expect("Del file");
        bulk.rollback().await.expect("Rollback operation");

        let files = bucket.list_all_files().await.expect("List files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "example1.txt");
    }
}
