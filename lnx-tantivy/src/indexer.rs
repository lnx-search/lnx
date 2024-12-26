use lnx_fs::{Body, BulkBucketTx, Bytes};
use tantivy::index::SegmentComponent;
use tantivy::indexer::operation::AddOperation;
use tantivy::indexer::SegmentWriter;
use tantivy::store::Compressor;
use tantivy::{Index, IndexSettings, Opstamp, Segment, SegmentMeta};
use tracing::{info, instrument};

use crate::directory::MemoryDirectory;

/// The memory budget isn't actually used since the directory itself is in memory,
/// but it allows tantivy to be a bit more efficient with a higher budget in the indexing memory arena.
const PSEUDO_MEMORY_BUDGET: usize = 50 << 20;

/// An indexer that produces a single tantivy segment and works entirely within memory.
///
/// This allows indexing operations to remain atomic and isolated from other operations
/// currently going on at the same time, although this does increase memory pressure it
/// should not reduce the pressure to the point where it is too much since this system
/// is designed to run with a couple of hundred MB _at most_ as part of a single operation,
/// although the most likely amount will be closer to `50MB` at most (100k * 0.5KB docs)
///
/// TODO: This currently doesn't support delete ops because we need to have a working
///     searcher first...
pub struct SingleSegmentIndexer {
    _index: Index,
    segment: Segment,
    segment_writer: SegmentWriter,
    directory: MemoryDirectory,
    opstamp: Opstamp,
}

impl SingleSegmentIndexer {
    /// Creates a new [SingleSegmentIndexer] with the given tantivy schema.
    pub(crate) fn new(schema: tantivy::schema::Schema) -> Self {
        let settings = IndexSettings {
            docstore_compression: Compressor::None, // Compression is handled externally.
            docstore_compress_dedicated_thread: false,
            // Smaller blocks because we bypass tantivy's internal cache and the lower
            // block size increases our external cache granularity and efficiency.
            docstore_blocksize: 128,
        };

        let directory = MemoryDirectory::default();
        let index = Index::create(directory.clone(), schema, settings)
            .expect("Index created with memory directory shouldn't error");

        let segment = index.new_segment();
        let segment_writer =
            SegmentWriter::for_segment(PSEUDO_MEMORY_BUDGET, segment.clone())
                .expect("Segment created with memory directory shouldn't error");

        Self {
            _index: index,
            directory,
            segment,
            segment_writer,
            opstamp: 0,
        }
    }

    /// Adds and indexes a new document for the given segment.
    pub fn add_document(
        &mut self,
        doc: tantivy::TantivyDocument,
    ) -> tantivy::Result<()> {
        self.opstamp += 1;
        let op = AddOperation {
            opstamp: self.opstamp,
            document: doc,
        };
        self.segment_writer.add_document(op)
    }

    #[instrument(skip(self), fields(segment_id = %self.segment.id()))]
    /// Completes the indexing operation and returns the memory
    /// forming the segment data.
    ///
    /// This includes:
    ///
    /// - Store
    /// - Postings
    /// - Fast fields
    /// - Field Norms
    /// - Terms
    /// - Positions
    pub fn finish(self) -> tantivy::Result<SegmentMemory> {
        info!(
            segment_memory_usage_bytes = self.segment_writer.mem_usage(),
            num_docs = self.segment_writer.num_docs(),
            "indexing segment finalising",
        );

        self.segment_writer.finalize()?;

        // Segment data should now be available to read.
        let store = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::Store))?;
        let terms = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::Terms))?;
        let postings = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::Postings))?;
        let positions = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::Positions))?;
        let field_norms = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::FieldNorms))?;
        let fast_fields = self
            .directory
            .get(&self.segment.relative_path(SegmentComponent::FastFields))?;
        let segment_meta = self.segment.meta().clone();

        Ok(SegmentMemory {
            segment_meta,
            num_docs: self.opstamp,
            store,
            terms,
            postings,
            positions,
            field_norms,
            fast_fields,
        })
    }
}

#[derive(Debug)]
/// The core data forming a single indexing segment.
pub struct SegmentMemory {
    pub(crate) segment_meta: SegmentMeta,
    pub(crate) num_docs: u64,
    store: Bytes,
    terms: Bytes,
    postings: Bytes,
    positions: Bytes,
    field_norms: Bytes,
    fast_fields: Bytes,
}

impl SegmentMemory {
    /// Writes the segment memory to the given [lnx_fs::Bucket].
    pub async fn write_to(
        &self,
        path_prefix: &str,
        tx: &mut BulkBucketTx<'_>,
    ) -> Result<(), lnx_fs::FileSystemError> {
        tx.write(
            &format!("{path_prefix}/{}.store", self.segment_meta.id()),
            Body::complete(self.store.clone()),
        )
        .await?;
        tx.write(
            &format!("{path_prefix}/{}.term", self.segment_meta.id()),
            Body::complete(self.terms.clone()),
        )
        .await?;
        tx.write(
            &format!("{path_prefix}/{}.idx", self.segment_meta.id()),
            Body::complete(self.postings.clone()),
        )
        .await?;
        tx.write(
            &format!("{path_prefix}/{}.pos", self.segment_meta.id()),
            Body::complete(self.positions.clone()),
        )
        .await?;
        tx.write(
            &format!("{path_prefix}/{}.fieldnorm", self.segment_meta.id()),
            Body::complete(self.field_norms.clone()),
        )
        .await?;
        tx.write(
            &format!("{path_prefix}/{}.fast", self.segment_meta.id()),
            Body::complete(self.fast_fields.clone()),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use lnx_fs::VirtualFileSystem;
    use tantivy::doc;
    use tantivy::schema::{
        IndexRecordOption,
        Schema,
        TextFieldIndexing,
        TextOptions,
        FAST,
        STORED,
    };

    use super::*;

    #[test]
    fn test_add_document_basic() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(
            "text_demo",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default().set_fieldnorms(false),
            ),
        );
        let schema = schema_builder.build();

        let doc = doc!(
            text_field => "Example text with the document here"
        );

        let mut indexer = SingleSegmentIndexer::new(schema);
        indexer.add_document(doc).expect("Index document");
        let _memory = indexer.finish().expect("Indexing finish");
    }

    #[test]
    fn test_add_document_with_field_norms() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(
            "text_demo",
            TextOptions::default()
                .set_stored()
                .set_indexing_options(TextFieldIndexing::default().set_fieldnorms(true)),
        );
        let schema = schema_builder.build();

        let doc = doc!(
            text_field => "Example text with the document here",
        );

        let mut indexer = SingleSegmentIndexer::new(schema);
        indexer.add_document(doc).expect("Index document");
        let _memory = indexer.finish().expect("Indexing finish");
    }

    #[test]
    fn test_add_document_with_fast_fields() {
        let mut schema_builder = Schema::builder();
        let fast_field = schema_builder.add_u64_field("fast_demo", STORED | FAST);
        let schema = schema_builder.build();

        let doc = doc!(
            fast_field => 123u64,
        );

        let mut indexer = SingleSegmentIndexer::new(schema);
        indexer.add_document(doc).expect("Index document");
        let _memory = indexer.finish().expect("Indexing finish");
    }

    #[test]
    fn test_add_document_with_positions() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(
            "text_demo",
            TextOptions::default().set_stored().set_indexing_options(
                TextFieldIndexing::default()
                    .set_fieldnorms(true)
                    .set_index_option(IndexRecordOption::Basic),
            ),
        );
        let schema = schema_builder.build();

        let doc = doc!(
            text_field => "Example text with the document here",
        );

        let mut indexer = SingleSegmentIndexer::new(schema);
        indexer.add_document(doc).expect("Index document");
        let _memory = indexer.finish().expect("Indexing finish");
    }

    #[tokio::test]
    async fn test_memory_write_to_bucket() {
        let (fs, _guard) = VirtualFileSystem::create_for_test()
            .await
            .expect("Create FS");

        let bucket = fs.create_bucket("test").await.expect("Create bucket");

        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(
            "text_demo",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default().set_fieldnorms(false),
            ),
        );
        let schema = schema_builder.build();

        let doc = doc!(
            text_field => "Example text with the document here"
        );

        let mut indexer = SingleSegmentIndexer::new(schema);
        indexer.add_document(doc).expect("Index document");
        let memory = indexer.finish().expect("Indexing finish");
        let mut tx = bucket.begin_tx();
        memory
            .write_to("test", &mut tx)
            .await
            .expect("Write segment to bucket");
        tx.commit().await.unwrap();
    }
}
