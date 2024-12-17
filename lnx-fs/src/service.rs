use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, instrument};

use crate::bucket::{Bucket, BucketCreateOptions};
use crate::io::{RuntimeDispatcher, RuntimeOptions};
use crate::metastore::MetastoreError;

/// A [VirtualFileSystem] provides an IO abstraction layer in an LSM-like fashion
/// for efficiently writing blobs to disk.
///
/// Internally blobs are read and written using direct IO and a separate async runtime,
/// it greatly improves the performance of writing small blobs and managing files on disk.
///
/// The system is akin to that of an object storage service like S3.
///
/// ## Blob Caching
///
/// It is important to note that this system does _no caching of blobs_, this includes
/// the file system cache which is bypassed because we use direct IO.
///
/// It is up to the application using this system to cache blobs how it sees fit (for now.)
///
/// ## Buckets
///
/// Like S3, the system has the concept of buckets. This a completely separate
/// folder in the file system with a separate metastore. It is not recommended to have lots
/// of small buckets as it reduces the efficiency of the system, but a couple large
/// buckets may improve performance if there is a _lot_ of activity and SQLite starts
/// to become a limiting factor.
///
pub struct VirtualFileSystem {
    mount_point: Arc<PathBuf>,
    buckets: Arc<RwLock<ahash::HashMap<String, Bucket>>>,
    runtime: RuntimeDispatcher,
}

impl VirtualFileSystem {
    #[instrument]
    /// Mount the file system on the given directory.
    ///
    /// Internally it will create the buckets within this directory
    /// and write all its data.
    ///
    /// If any buckets exist when mounting, they will be loaded
    /// and accessible.
    pub async fn mount(
        path: PathBuf,
        runtime_options: RuntimeOptions,
    ) -> Result<Self, FileSystemError> {
        let runtime = crate::io::create_io_runtime(runtime_options)?;

        let mut buckets = ahash::HashMap::default();
        for entry in path.read_dir()? {
            let entry = entry?;

            info!(path = %entry.path().display(), "Attempting to open bucket");
            let bucket = Bucket::open(entry.path(), runtime.clone()).await?;
            buckets.insert(bucket.name().to_string(), bucket);
        }

        Ok(Self {
            mount_point: Arc::new(path),
            buckets: Arc::new(RwLock::new(buckets)),
            runtime,
        })
    }

    #[cfg(feature = "test-utils")]
    pub async fn create_for_test() -> Result<(Self, tempfile::TempDir), FileSystemError>
    {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dir = tempfile::TempDir::new()?;
        let fs = Self::mount(dir.path().to_path_buf(), rt_options).await?;
        Ok((fs, dir))
    }

    /// Returns a clone of the [Bucket] if it exists.
    pub fn bucket(&self, bucket: &str) -> Option<Bucket> {
        self.buckets.read().get(bucket).cloned()
    }

    /// Creates a new bucket with the given [BucketCreateOptions].
    ///
    /// Returns an error if the bucket already exists (on disk.)
    pub async fn create_bucket(&self, name: &str) -> Result<Bucket, FileSystemError> {
        let options = BucketCreateOptions::builder()
            .name(name)
            .bucket_path(self.mount_point.join(name))
            .build();

        let bucket = Bucket::create(options, self.runtime.clone()).await?;

        let mut lock = self.buckets.write();
        lock.insert(bucket.name().to_string(), bucket.clone());

        Ok(bucket)
    }

    /// Attempts to delete the bucket with the given name.
    ///
    /// WARNING:
    ///
    /// This does _NO_ checking if the bucket is no longer in use or not
    /// and great care should be taken using this method.
    pub async fn delete_bucket(&self, bucket: &str) -> io::Result<()> {
        let maybe_bucket = {
            let mut lock = self.buckets.write();
            lock.remove(bucket)
        };

        if let Some(bucket) = maybe_bucket {
            tokio::fs::remove_dir_all(bucket.path()).await?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that can occur from the file system service.
pub enum FileSystemError {
    #[error("IO Error: {0}")]
    /// An IO error that occurred while attempting to complete
    /// the operation.
    IoError(#[from] io::Error),
    #[error("Metastore Error: {0}")]
    /// An error that occurred when attempting to access the metastore.
    MetastoreError(#[from] MetastoreError),
    #[error("Bucket not found: {0:?}")]
    /// The target bucket does not exist.
    BucketNotFound(String),
    #[error("File not found: {0:?}")]
    /// No file exists within the bucket with the given name.
    FileNotFound(String),
    #[error("Path for file is too long: {0:?}")]
    /// The provided file path is too long.
    PathTooLong(String),
    #[error("Path invalid: {0}")]
    /// The provided file path is invalid
    PathInvalid(String),
    #[error("Bucket already exists: {0:?}")]
    /// A bucket with the provided name already exists.
    BucketAlreadyExists(String),
    #[error("Bucket Corrupted: {0}")]
    /// Some part of the bucket data is corrupted.
    Corrupted(String),
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;

    #[tokio::test]
    async fn test_delete_bucket_non_existant() {
        let mount_point = temp_dir().join(ulid::Ulid::new().to_string());
        std::fs::create_dir(&mount_point).unwrap();

        let options = RuntimeOptions::builder().num_threads(1).build();

        let service = VirtualFileSystem::mount(mount_point.clone(), options)
            .await
            .expect("Create file system");
        service.delete_bucket("doesnt-exist").await.unwrap();
    }

    #[tokio::test]
    async fn test_create_delete_bucket() {
        let mount_point = temp_dir().join(ulid::Ulid::new().to_string());
        std::fs::create_dir(&mount_point).unwrap();

        let options = RuntimeOptions::builder().num_threads(1).build();

        let service = VirtualFileSystem::mount(mount_point.clone(), options)
            .await
            .expect("Create file system");

        service
            .create_bucket("testing")
            .await
            .expect("Create new bucket");

        let bucket = service.bucket("testing");
        assert!(bucket.is_some());

        service
            .delete_bucket("testing")
            .await
            .expect("Delete bucket");
        let bucket = service.bucket("testing");
        assert!(bucket.is_none());
        assert!(!mount_point.join("testing").exists());
    }

    #[tokio::test]
    async fn test_create_load_existing_buckets() {
        let mount_point = temp_dir().join(ulid::Ulid::new().to_string());
        std::fs::create_dir(&mount_point).unwrap();

        let options = RuntimeOptions::builder().num_threads(1).build();

        let service = VirtualFileSystem::mount(mount_point.clone(), options)
            .await
            .expect("Create file system");

        service
            .create_bucket("testing1")
            .await
            .expect("Create new bucket");
        service
            .create_bucket("testing2")
            .await
            .expect("Create new bucket");
        service
            .create_bucket("testing3")
            .await
            .expect("Create new bucket");
        drop(service);

        let options = RuntimeOptions::builder().num_threads(1).build();

        let service = VirtualFileSystem::mount(mount_point, options)
            .await
            .expect("Create file system");
        assert!(service.bucket("testing1").is_some());
        assert!(service.bucket("testing2").is_some());
        assert!(service.bucket("testing3").is_some());
    }
}
