use std::sync::Arc;

use bytes::Bytes;
use lnx_fs::{Body, Bucket};
use tantivy::schema::Schema;
use tantivy::store::Compressor;
use tantivy::{IndexSettings, ReloadPolicy};
use tokio::sync::Mutex;
use tracing::warn;

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
    inner_state: Arc<Mutex<tantivy::IndexMeta>>,
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

        let (index, reader, meta) = tokio::task::spawn_blocking(move || {
            let index = tantivy::Index::open(dir)?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .doc_store_cache_num_blocks(0)
                .try_into()?;
            let meta = index.load_metas()?;

            Ok::<_, IndexError>((index, reader, meta))
        })
        .await
        .expect("Join background thread")?;

        Ok(Self {
            index_name,
            bucket,
            index,
            reader,
            inner_state: Arc::new(Mutex::new(meta)),
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
            docstore_blocksize: 31 << 10,
        };

        let (index, reader, meta) = tokio::task::spawn_blocking(move || {
            let index = tantivy::Index::create(dir, schema, settings)?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .doc_store_cache_num_blocks(0)
                .try_into()?;
            let meta = index.load_metas()?;

            Ok::<_, IndexError>((index, reader, meta))
        })
        .await
        .expect("Join background thread")?;

        Ok(Self {
            index_name,
            bucket,
            index,
            reader,
            inner_state: Arc::new(Mutex::new(meta)),
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

        // Important: This must be acquired before the commit stages to prevent
        //            multiple things trying to commit and update the state file at the
        //            same time.
        let mut state = self.inner_state.lock().await;

        // We create a copy so if we run into an error we don't have any partial state.
        let mut state_copy = state.clone();
        state_copy.opstamp += segment.num_docs;
        state_copy.segments.push(segment.segment_meta.clone());

        let serialized = serde_json::to_vec(&state_copy)?;
        let meta_path = self.prefix_with_name("meta.json");
        bulk.write(&meta_path, Body::complete(Bytes::from(serialized)))
            .await?;
        bulk.commit().await?;

        // Now all the fallible IO has completed we can update the memory state.
        *state = state_copy;
        drop(state);

        Ok(())
    }

    /// Returns a reference to the live index reader.
    pub fn reader(&self) -> &tantivy::IndexReader {
        &self.reader
    }

    /// Reload the index readers to see new segments.
    pub fn reload_readers(&self) {
        if let Err(e) = self.reader.reload() {
            warn!(error = ?e, "Failed to reload reader due to error");
        }
    }

    /// Creates a new single segment indexer.
    pub fn new_indexer(&self) -> crate::indexer::SingleSegmentIndexer {
        crate::indexer::SingleSegmentIndexer::new(self.index.schema())
    }

    fn prefix(&self) -> String {
        format!("indexes/{}", self.index_name)
    }

    fn prefix_with_name(&self, name: &str) -> String {
        format!("indexes/{}/{name}", self.index_name)
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
}
