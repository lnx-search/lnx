use tracing::{instrument, trace};

use crate::bucket::Bucket;
use crate::config::COMMIT_MARKER_PREFIX;
use crate::io::BulkFlushWaker;
use crate::metastore::BulkMetastoreModifyOperation;
use crate::{Body, FileSystemError};

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
    transaction_id: ulid::Ulid,
    flush_wakers: BulkFlushWaker,
    pending_cache_evictions: Vec<String>,
}

impl<'bucket> BulkBucketTx<'bucket> {
    pub(super) fn new(
        metastore: BulkMetastoreModifyOperation<'bucket>,
        bucket: &'bucket Bucket,
    ) -> Self {
        Self {
            metastore,
            bucket,
            num_ops_pending: 0,
            transaction_id: ulid::Ulid::new(),
            flush_wakers: BulkFlushWaker::default(),
            pending_cache_evictions: Vec::new(),
        }
    }

    #[instrument("bulk_write", skip(self, body))]
    /// Write a blob body stream to the store with the given path.
    ///
    /// Once this call completes, the blob is safely persisted to disk.
    pub async fn write(
        &mut self,
        path: &str,
        body: Body,
    ) -> Result<(), FileSystemError> {
        crate::bucket::validate_path(path)?;

        trace!("Begin writing blob");

        let write_metadata = crate::io::Metadata {
            path: path.to_string(),
            transaction_id: Some(self.transaction_id),
        };

        let response = self.bucket.writer.write(write_metadata, body).await?;
        trace!("Blob write complete");

        self.pending_cache_evictions.push(path.to_string());
        self.flush_wakers.push(response.flush_waker);
        self.metastore.add_event(response.tablet_id, response.event);
        trace!("Metadata updated");

        self.num_ops_pending += 1;

        Ok(())
    }

    #[instrument("bulk_rename", skip(self))]
    /// Renames a file from the provided path to a new provided path.
    ///
    /// Returns a [FileSystemError::FileNotFound] error if the file being
    /// targeted does not exist.
    pub async fn rename(
        &mut self,
        from_path: &str,
        to_path: &str,
    ) -> Result<(), FileSystemError> {
        crate::bucket::validate_path(from_path)?;
        crate::bucket::validate_path(to_path)?;

        trace!("Begin writing blob");

        if !self.bucket.exists(from_path) {
            return Err(FileSystemError::FileNotFound(from_path.to_string()));
        }

        trace!("Begin delete blob");
        let metadata = crate::io::Metadata {
            path: from_path.to_string(),
            transaction_id: Some(self.transaction_id),
        };
        let response = self
            .bucket
            .writer
            .rename(metadata, to_path.to_string())
            .await?;
        trace!("Blob delete write complete");

        self.pending_cache_evictions.push(from_path.to_string());
        self.pending_cache_evictions.push(to_path.to_string());
        self.flush_wakers.push(response.flush_waker);
        self.metastore.add_event(response.tablet_id, response.event);

        self.num_ops_pending += 1;

        Ok(())
    }

    #[instrument("bulk_delete", skip(self))]
    /// Deletes a file from the system.
    ///
    /// Does nothing if the file doesn't exist.
    pub async fn delete(&mut self, path: &str) -> Result<(), FileSystemError> {
        crate::bucket::validate_path(path)?;

        trace!("Begin delete blob");
        let write_metadata = crate::io::Metadata {
            path: path.to_string(),
            transaction_id: Some(self.transaction_id),
        };

        let response = self.bucket.writer.delete(write_metadata).await?;
        trace!("Blob delete write complete");

        self.pending_cache_evictions.push(path.to_string());
        self.flush_wakers.push(response.flush_waker);
        self.metastore.add_event(response.tablet_id, response.event);

        self.num_ops_pending += 1;

        Ok(())
    }

    #[instrument(skip(self))]
    /// Commits all currently pending bucket operations.
    pub async fn commit(mut self) -> Result<BulkFlushWaker, FileSystemError> {
        if self.num_ops_pending == 0 {
            return Ok(self.flush_wakers);
        }

        let write_metadata = crate::io::Metadata {
            path: format!("{COMMIT_MARKER_PREFIX}/{}.commit", self.transaction_id),
            transaction_id: Some(self.transaction_id),
        };
        let response = self
            .bucket
            .writer
            .write(write_metadata, Body::empty())
            .await?;
        trace!("Blob delete write complete");

        self.flush_wakers.push(response.flush_waker);
        trace!("Writer flushes acknowledged");

        for path in self.pending_cache_evictions {
            self.bucket.evict_path_from_cache(&path);
        }

        self.metastore.commit();

        Ok(self.flush_wakers)
    }

    #[instrument(skip(self))]
    /// Explicitly rollback all currently pending operations.
    ///
    /// This is implicitly ran if the operation is dropped before
    /// either `rollback` or `commit` is explicitly called.
    pub fn rollback(self) {
        self.metastore.rollback();
    }
}
