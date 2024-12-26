use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;
use bytes::Bytes;
use lnx_fs::{Body, Bucket};
use tantivy::schema::Schema;
use tantivy::store::Compressor;
use tantivy::{IndexMeta, IndexSettings, IndexWriter, ReloadPolicy, Segment, TantivyError};
use tantivy::indexer::{DefaultMergePolicy, LogMergePolicy, SegmentEntry, Stamper};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::directory::VFSDirectory;
use crate::indexer::SegmentMemory;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Tantivy Error: {0}")]
    /// An error returned from tantivy.
    Tantivy(#[from] tantivy::TantivyError),
    #[error("VFS Error: {0}")]
    /// An error returned from tantivy.
    VFS(#[from] lnx_fs::FileSystemError),
    #[error("Serialize Error: {0}")]
    /// An error returned while attempting the serialize the state.
    Serialize(#[from] serde_json::Error),
}

#[derive(Clone)]
/// A single lnx index wrapping an internal [tantivy::Index].
///
/// This can be used to add/remove segments and get access to readers and writers
/// for the index.
pub struct LnxIndex {
    index_name: String,
    bucket: Bucket,
    index: tantivy::Index,
    reader: tantivy::IndexReader,
    stamper: Stamper,
    writer: Arc<Mutex<IndexWriter>>,
}

impl Debug for LnxIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LnxIndex")
            .field("index_name", &self.index_name)
            .field("schema", &self.index.schema())
            .finish()
    }
}

impl LnxIndex {
    /// Opens an existing [LnxIndex].
    pub async fn open(
        index_name: impl Into<String>,
        bucket: Bucket,
    ) -> Result<Self, IndexError> {
        let index_name = index_name.into();
        let base_path = format!("indexes/{index_name}");
        let dir = VFSDirectory::new(&base_path, bucket.clone());

        let (index, reader, meta, writer) = tokio::task::spawn_blocking(move || {
            let index = tantivy::Index::open(dir)?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .doc_store_cache_num_blocks(0)
                .try_into()?;

            let meta = index.load_metas()?;
            let writer = index.writer_with_num_threads(1, 15 << 20)?;

            Ok::<_, IndexError>((index, reader, meta, writer))
        })
        .await
        .expect("Join background thread")?;

        Ok(Self {
            index_name,
            bucket,
            index,
            reader,
            stamper: Stamper::new(meta.opstamp),
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Creates a new [LnxIndex] using the given tantivy schema.
    ///
    /// Errors if the index already exists.
    pub async fn create(
        index_name: impl Into<String>,
        bucket: Bucket,
        schema: Schema,
    ) -> Result<Self, IndexError> {
        let index_name = index_name.into();
        let base_path = format!("indexes/{index_name}");
        let dir = VFSDirectory::new(&base_path, bucket.clone());

        // Not used directly because of how the system writes segments.
        let settings = IndexSettings {
            docstore_compression: Compressor::None,
            docstore_compress_dedicated_thread: false,
            docstore_blocksize: 128,
        };

        let (index, reader, meta, writer) = tokio::task::spawn_blocking(move || {
            if tantivy::Index::exists(&dir).unwrap_or(false) {
                return Err(IndexError::Tantivy(TantivyError::IndexAlreadyExists));
            }

            let index = tantivy::Index::create(dir, schema, settings)?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .doc_store_cache_num_blocks(0)
                .try_into()?;

            let meta = index.load_metas()?;
            let writer = index.writer_with_num_threads(1, 15 << 20)?;

            Ok::<_, IndexError>((index, reader, meta, writer))
        })
        .await
        .expect("Join background thread")?;

        Ok(Self {
            index_name,
            bucket,
            index,
            reader,
            stamper: Stamper::new(meta.opstamp),
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Adds a new segment to the index.
    ///
    /// This will persist the segment to disk and update the metadata as part of
    /// the same transaction.
    pub async fn add_segment(&self, segment: SegmentMemory) -> Result<(), IndexError> {
        let prefix = self.prefix();

        let mut bulk = self.bucket.begin_tx();
        segment.write_to(&prefix, &mut bulk).await?;
        bulk.commit().await?;

        let mut lock = self.writer.lock().await;
        lock.add_segment(segment.segment_meta).await?;
        let prepared = lock.prepare_commit()?;
        prepared.commit_future().await?;

        Ok(())
    }

    #[inline]
    /// The name of the index.
    pub fn name(&self) -> &str {
        &self.index_name
    }

    #[inline]
    /// The [Schema] of the index.
    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    #[inline]
    /// Returns a reference to the live index reader.
    pub fn reader(&self) -> &tantivy::IndexReader {
        &self.reader
    }

    pub async fn meta(&self) -> IndexMeta {
        let index = self.index.clone();
        tokio::task::spawn_blocking(move || index.load_metas().unwrap())
            .await
            .expect("Reader reload task panicked")
    }

    /// Reload the index readers to see new segments.
    pub async fn reload_readers(&self) {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = reader.reload() {
                warn!(error = ?e, "Failed to reload reader due to error");
            }
        })
        .await
        .expect("Reader reload task panicked")
    }

    #[inline]
    /// Creates a new single segment indexer.
    pub fn new_indexer(&self, num_docs: u64) -> crate::indexer::SingleSegmentIndexer {
        let range = self.stamper.stamps(num_docs);
        crate::indexer::SingleSegmentIndexer::new(self.index.schema(), range)
    }

    fn prefix(&self) -> String {
        format!("indexes/{}", self.index_name)
    }
}



#[cfg(test)]
mod tests {
    use lnx_fs::VirtualFileSystem;
    use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};

    use super::*;

    #[tokio::test]
    async fn test_index_create() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("name", TEXT | STORED);
        schema_builder.add_u64_field("id", STORED | FAST);
        let schema = schema_builder.build();

        let (vfs, _guard) = VirtualFileSystem::create_for_test().await.unwrap();
        let bucket = vfs.create_bucket("test").await.unwrap();

        let _index = LnxIndex::create("test", bucket, schema)
            .await
            .expect("Create new index");
    }

    #[tokio::test]
    async fn test_index_create_already_exists() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("name", TEXT | STORED);
        schema_builder.add_u64_field("id", STORED | FAST);
        let schema = schema_builder.build();

        let (vfs, _guard) = VirtualFileSystem::create_for_test().await.unwrap();
        let bucket = vfs.create_bucket("test").await.unwrap();

        let index = LnxIndex::create("test", bucket.clone(), schema.clone())
            .await
            .expect("Create new index");
        drop(index);

        let error = LnxIndex::create("test", bucket.clone(), schema.clone())
            .await
            .expect_err("index already exists and should error");
        assert!(matches!(
            error,
            IndexError::Tantivy(TantivyError::IndexAlreadyExists)
        ));
    }

    #[tokio::test]
    async fn test_open_existing_index() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("name", TEXT | STORED);
        schema_builder.add_u64_field("id", STORED | FAST);
        let original_schema = schema_builder.build();

        let (vfs, _guard) = VirtualFileSystem::create_for_test().await.unwrap();
        let bucket = vfs.create_bucket("test").await.unwrap();

        let index = LnxIndex::create("test", bucket.clone(), original_schema.clone())
            .await
            .expect("Create new index");
        drop(index);

        let index = LnxIndex::open("test", bucket)
            .await
            .expect("Open existing index");
        assert_eq!(
            index.index.schema(),
            original_schema,
            "Schemas do not match"
        );
    }
}
