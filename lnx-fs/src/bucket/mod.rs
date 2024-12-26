mod reader;
mod setup;
pub mod statistics;
mod transaction;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bon::Builder;
use tracing::{info, instrument, trace};

pub use self::transaction::BulkBucketTx;
use crate::bucket::reader::BucketReader;
use crate::bucket::setup::BucketPaths;
use crate::bucket::statistics::{ReadStatistics, StatisticsEnabled, WriteStatistics};
use crate::config::MAX_PATH_LENGTH;
use crate::io::{
    Body,
    FlushWaker,
    RuntimeDispatcher,
    TabletWriter,
    TabletWriterOptions,
    WriterEventHook,
};
use crate::metastore::{checkpoint, Metastore, MetastoreEntry, MetastoreError};
use crate::service::FileSystemError;
use crate::{BucketConfig, FileMetadata, MaybeUnset};

/// A bucket with statistics return enabled.
pub type BucketWithStatistics = Bucket<statistics::On>;

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
/// ├── tablets/
/// │   ├── 01JCXNCND5Q2ANW5JD8F08DN3V.tablet
/// │   ├── 01JCXNCND4PG1S3317HA4JC2B6.tablet
/// │   └── 01JCXNCNDRT1YGN3X459XTQSCA.tablet
/// └── tablet_metadata/
///     ├── 01JCXNCND5Q2ANW5JD8F08DN3V.ckpt
///     ├── 01JCXNCND4PG1S3317HA4JC2B6.ckpt
///     └── 01JCXNCNDRT1YGN3X459XTQSCA.ckpt
/// ```
///
/// #### `metastore.sqlite`
///
/// This contains the metadata of the bucket itself, e.g. name, config, etc...
///
/// #### `tablets/`
///
/// This is the main data directory where all tablet files are written to.
///
/// It is possible that some tablets exist within the directory while not being in used
/// anymore, this is because the system only periodically performs a compaction and GC
/// of the dead files.
///
/// #### `tablet_metadata/`
///
/// This contains persisted metadata for the tablet with the matching name, this includes
/// a compact representation of the files stored within the tablet, the offsets for each blob
/// and the individual file metadata.
///
/// These metadata files are written asynchronously as snapshots of the memory state,
/// _IT IS OK FOR THESE FILES TO BE MISSING OR CORRUPTED_, the system will re-build the state
/// from the main `.tablet` files in this event.
///
pub struct Bucket<S = statistics::Off> {
    /// The currently active bucket config.
    config: Arc<BucketConfig>,
    /// The paths within the bucket containing various parts of the bucket data.
    paths: Arc<BucketPaths>,
    /// The metastore for the bucket.
    metastore: Metastore,
    /// The tablet writer for completing new write requests.
    writer: TabletWriter,
    /// The IO runtime for the bucket.
    runtime: RuntimeDispatcher,
    /// The bucket reader.
    reader: Arc<BucketReader>,
    /// used to signal when stats are returned from operations or not.
    _stats_enabled: PhantomData<S>,
}

impl<S> Clone for Bucket<S> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            paths: self.paths.clone(),
            metastore: self.metastore.clone(),
            writer: self.writer.clone(),
            runtime: self.runtime.clone(),
            reader: self.reader.clone(),
            _stats_enabled: PhantomData,
        }
    }
}

impl Bucket<statistics::Off> {
    #[instrument(skip(runtime))]
    /// Creates a new bucket with the given runtime.
    ///
    /// If a bucket already exist at the target path a [FileSystemError::BucketAlreadyExists]
    /// is returned.
    ///
    /// NOTE: The created bucket will have statistics disabled.
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
        paths.ensure_tablets_metadata_path_exists()?;
        paths.ensure_metastore_file_exists()?;

        let metastore = Metastore::connect(&paths.metastore_sqlite_path()).await?;

        let config = BucketConfig::builder().name(options.name).build();
        config.store_in_metastore(&metastore).await?;

        Self::open_bucket_inner(config, paths, metastore, runtime).await
    }

    #[instrument(skip(runtime))]
    /// Opens an existing bucket with the given runtime.
    ///
    /// If no bucket exists in the given folder a [FileSystemError::BucketNotFound]
    /// error is returned.
    ///
    /// NOTE: The created bucket will have statistics disabled.
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
        info!("Loading existing metastore state from checkpoints");
        crate::metastore::recovery::load_metastore_state_from_disk(
            metastore.clone(),
            &paths.tablets_path,
            &paths.tablet_metadata_path,
        )
        .await?;

        info!("Setting up tablet writer");
        let writer = setup_tablet_writer(&paths, &config, runtime.clone()).await?;

        let reader = BucketReader::new(
            config.clone(),
            paths.tablets_path.clone(),
            runtime.clone(),
            metastore.clone(),
        );

        Ok(Bucket {
            config: Arc::new(config),
            paths: Arc::new(paths),
            metastore,
            writer,
            runtime,
            reader: Arc::new(reader),
            _stats_enabled: PhantomData,
        })
    }
}

impl<S> Bucket<S>
where
    S: StatisticsEnabled,
{
    /// Enables statistics return on the current bucket object.
    pub fn enable_statistics_return(self) -> Bucket<statistics::On> {
        Bucket {
            config: self.config,
            paths: self.paths,
            metastore: self.metastore,
            writer: self.writer,
            runtime: self.runtime,
            reader: self.reader,
            _stats_enabled: PhantomData,
        }
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

    /// Returns if a file with a given path exists.
    pub fn exists(&self, path: &str) -> bool {
        self.metastore.exists(path)
    }

    /// Begins a new bulk operation transaction.
    ///
    /// All operations applied via the [BulkBucketTx] will be all-or-nothing,
    /// meaning either all the operations go through or none of them do.
    ///
    /// Be aware that data is still written to disk and the atomic handling
    /// of operations is done via the metastore. Data which then gets left behind
    /// after a rollback will be cleaned up eventually by the bucket GC.
    pub fn begin_tx(&self) -> BulkBucketTx<'_, S> {
        let metastore = self.metastore.begin_mutate();
        BulkBucketTx::new(metastore, self)
    }

    #[instrument(skip(self))]
    /// Creates a new read stream for the given file if it exists.
    ///
    /// NOTE:
    /// This operation can be quite expensive if the file is not in the cache,
    /// this is because it will have to open and map the file with various buffers.
    ///
    /// To minimise this impact, it is important to have a suitably sized
    /// reader cache allowance.
    pub async fn read(
        &self,
        path: &str,
    ) -> Result<S::Wrapped<Body, ReadStatistics>, FileSystemError> {
        self.read_range(path, ..).await
    }

    #[instrument(skip(self))]
    /// Creates a new read stream for the given file if it exists and selects
    /// only the data within the provided range.
    ///
    /// NOTE:
    /// This operation can be quite expensive if the file is not in the cache,
    /// this is because it will have to open and map the file with various buffers.
    ///
    /// To minimise this impact, it is important to have a suitably sized
    /// reader cache allowance.
    pub async fn read_range<R>(
        &self,
        path: &str,
        range: R,
    ) -> Result<S::Wrapped<Body, ReadStatistics>, FileSystemError>
    where
        R: RangeBounds<u64> + Debug,
    {
        validate_path(path)?;
        self.reader.read_range::<S, _>(path, range).await
    }

    #[instrument(skip(self, body))]
    /// Write a blob body stream to the store with the given path.
    ///
    /// Once this call completes, the blob is safely persisted to disk.
    pub async fn write(
        &self,
        path: &str,
        body: Body,
    ) -> Result<S::Wrapped<FlushWaker, WriteStatistics>, FileSystemError> {
        validate_path(path)?;

        let mut statistics = WriteStatistics::default();

        trace!("Begin writing blob");
        let metadata = crate::io::Metadata {
            path: path.to_string(),
            transaction_id: None,
        };

        let response = self.writer.write(metadata, body).await?;
        trace!("Blob write complete");

        statistics.io_bytes += response.bytes_written;

        self.reader.evict_path_from_cache(path, &mut statistics);

        let mut bulk = self.metastore.begin_mutate();
        bulk.add_event(response.tablet_id, response.event);
        bulk.commit();
        trace!("Metadata updated");

        Ok(S::wrap(response.flush_waker, statistics))
    }

    #[instrument(skip(self))]
    /// Deletes a file from the system.
    ///
    /// Does nothing if the file doesn't exist.
    pub async fn delete(
        &self,
        path: &str,
    ) -> Result<S::Wrapped<FlushWaker, WriteStatistics>, FileSystemError> {
        validate_path(path)?;

        let mut statistics = WriteStatistics::default();

        trace!("Begin delete blob");
        let metadata = crate::io::Metadata {
            path: path.to_string(),
            transaction_id: None,
        };
        let response = self.writer.delete(metadata).await?;
        trace!("Blob delete write complete");

        statistics.io_bytes += response.bytes_written;

        self.reader.evict_path_from_cache(path, &mut statistics);

        let mut bulk = self.metastore.begin_mutate();
        bulk.add_event(response.tablet_id, response.event);
        bulk.commit();

        Ok(S::wrap(response.flush_waker, statistics))
    }

    #[instrument(skip(self))]
    /// Renames a file from the provided path to a new provided path.
    ///
    /// Returns a [FileSystemError::FileNotFound] error if the file being
    /// targeted does not exist.
    pub async fn rename(
        &self,
        from_path: &str,
        to_path: &str,
    ) -> Result<S::Wrapped<FlushWaker, WriteStatistics>, FileSystemError> {
        validate_path(from_path)?;
        validate_path(to_path)?;

        if !self.exists(from_path) {
            return Err(FileSystemError::FileNotFound(from_path.to_string()));
        }

        let mut statistics = WriteStatistics::default();

        trace!("Begin delete blob");
        let metadata = crate::io::Metadata {
            path: from_path.to_string(),
            transaction_id: None,
        };
        let response = self.writer.rename(metadata, to_path.to_string()).await?;
        trace!("Blob delete write complete");

        statistics.io_bytes += response.bytes_written;

        // TODO: Maybe we can be smarter about renames?
        self.reader
            .evict_path_from_cache(from_path, &mut statistics);
        self.reader.evict_path_from_cache(to_path, &mut statistics);

        let mut bulk = self.metastore.begin_mutate();
        bulk.add_event(response.tablet_id, response.event);
        bulk.commit();

        Ok(S::wrap(response.flush_waker, statistics))
    }

    /// Returns the file metadata associated with the given file.
    pub fn metadata(&self, path: &str) -> Result<FileMetadata, FileSystemError> {
        trace!("Get metadata");
        let url_and_metadata = self.metastore.get_file(path);
        url_and_metadata
            .map(|entry| entry.metadata)
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))
    }

    /// List all files in the bucket
    pub fn list_all_files(&self) -> Vec<MetastoreEntry> {
        let mut files = self.metastore.list_all_files();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        files
    }

    /// List all files in the bucket with a given predicate match.
    pub fn list_files_with_predicate<F>(&self, mut pred: F) -> Vec<MetastoreEntry>
    where
        F: FnMut(&String, &FileMetadata) -> bool,
    {
        let mut files = self
            .metastore
            .list_files_with_predicate(|entry| pred(&entry.path, &entry.metadata));
        files.sort_by(|a, b| a.path.cmp(&b.path));
        files
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

async fn setup_tablet_writer(
    paths: &BucketPaths,
    config: &BucketConfig,
    runtime: RuntimeDispatcher,
) -> Result<TabletWriter, FileSystemError> {
    let checkpoint_options = checkpoint::CheckpointOptions::builder()
        .base_path(paths.tablet_metadata_path.as_path())
        .build();

    info!("Spawning checkpointing actor");
    let checkpoint_hook =
        checkpoint::spawn_checkpoint_actor(checkpoint_options.clone()).await?;

    let writer_options = TabletWriterOptions::builder()
        .base_path(paths.tablets_path.clone())
        .maybe_max_active_writers(config.max_active_writers())
        .maybe_max_tablet_size(config.max_tablet_size_bytes())
        .event_hooks(vec![Box::new(checkpoint_hook) as Box<dyn WriterEventHook>])
        .maybe_flush_delay_window(config.flush_delay_millis().map(Duration::from_millis))
        .build();

    let writer = TabletWriter::new(writer_options, runtime);

    Ok(writer)
}

pub(super) fn validate_path(path: &str) -> Result<(), FileSystemError> {
    if path.starts_with("__lnx_fs/") {
        Err(FileSystemError::PathInvalid(format!(
            "path {path:?} uses reserved file prefix"
        )))
    } else if path.ends_with('/') {
        Err(FileSystemError::PathInvalid(format!(
            "path {path:?} ends with `/` which is not allowed"
        )))
    } else if path.len() > MAX_PATH_LENGTH {
        Err(FileSystemError::PathTooLong(path.to_string()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use bytes::Bytes;

    use super::*;
    use crate::config::{METASTORE_FILE, TABLET_PATH};
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

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "example.txt");
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
    async fn test_bucket_read_range_file() {
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

        let body = bucket
            .read_range("example.txt", ..)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b"Hello, World!");

        let body = bucket
            .read_range("example.txt", ..5)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b"Hello");

        let body = bucket
            .read_range("example.txt", 5..)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b", World!");

        let body = bucket
            .read_range("example.txt", 5..12)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b", World");

        let err = bucket
            .read_range("example.txt", 5..9000)
            .await
            .expect_err("Read should error due to range");
        assert!(
            matches!(err, FileSystemError::ReadOutOfRange(_, _)),
            "Expected read out of range, got {err:?}"
        );
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

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 1);

        bucket.delete("example.txt").await.expect("Delete file");
        let files = bucket.list_all_files();
        assert!(files.is_empty());
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

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 1);

        let metadata = bucket.metadata("example.txt").expect("Get metadata");
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

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 3);

        let files = bucket.list_files_with_predicate(|path, _| path.ends_with(".txt"));
        assert_eq!(files.len(), 2);
        let files = bucket.list_files_with_predicate(|path, _| path.ends_with(".bar"));
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

        let mut bulk = bucket.begin_tx();
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

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].path, "example1.txt");
        assert_eq!(files[1].path, "example2.txt");
        assert_eq!(files[2].path, "example3.txt");
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

        let mut bulk = bucket.begin_tx();
        bulk.write(
            "example1.txt",
            Body::complete(Bytes::from_static(b"Hello, World 1!")),
        )
        .await
        .expect("Write file");
        bulk.commit().await.expect("Commit operation");

        let mut bulk = bucket.begin_tx();
        bulk.write(
            "example2.txt",
            Body::complete(Bytes::from_static(b"Hello, World 2!")),
        )
        .await
        .expect("Write file");
        bulk.delete("example1.txt").await.unwrap();
        bulk.rollback();

        let files = bucket.list_all_files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "example1.txt");
    }

    #[tokio::test]
    async fn test_bucket_read_range_adjust_relative_pos_to_absolute() {
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
        let body = Body::complete(Bytes::from_static(b"Hello, World other test!"));
        bucket.write("example.txt", body).await.expect("Write file");

        let body = bucket
            .read_range("example.txt", ..)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read all content");
        assert_eq!(data.as_ref(), b"Hello, World other test!");

        let body = bucket
            .read_range("example.txt", ..12)
            .await
            .expect("Read file");
        let data = body.collect().await.expect("Read content");
        assert_eq!(data.as_ref(), b"Hello, World");
    }
}
