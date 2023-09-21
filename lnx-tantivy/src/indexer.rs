use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tantivy::query::Query;
use tantivy::schema::{DocumentAccess, Schema};
use tantivy::{
    AddOperation,
    Index,
    IndexSettings,
    Opstamp,
    Result,
    Segment,
    SegmentId,
    SegmentWriter,
};

use crate::directory::memory::LighweightRamDirectory;
use crate::realtime_reader::RealtimeIndexReader;

const MEMORY_ARENA_SIZE: usize = 80 << 20;






#[derive(Clone, Default)]
/// An atomic opstamper for tantivy docs which can be shared between threads.
///
/// This basically just copies what tantivy does.
pub struct Stamper {
    counter: Arc<AtomicU64>,
}

impl Stamper {
    /// Gets the next op stamp ID.
    pub fn next(&self) -> Opstamp {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Clone)]
/// A indexer factory that produces segments.
pub struct IndexerFactory {
    index: Index,
    dir: LighweightRamDirectory,
}

impl IndexerFactory {
    /// Create a new indexer factory.
    pub fn create(schema: Schema) -> Result<Self> {
        let dir = LighweightRamDirectory::new();
        let index = Index::create(dir.clone(), schema, IndexSettings::default())?;

        Ok(Self { index, dir })
    }

    #[inline]
    /// Creates a new lightweight indexer.
    pub fn new_indexer(&self) -> Result<LightweightIndexer> {
        let segment = self.index.new_segment();
        LightweightIndexer::new(segment)
    }

    #[inline]
    /// Consumes the content of the segment files.
    pub fn get_segment_files(&self, segment: &Segment) -> Vec<(String, Bytes)> {
        self.dir.get_files(segment.id())
    }
}

/// A lightweight tantivy segment writer.
///
/// A key difference with this indexer VS that of tantivy
/// is opstamps are **not** monotonic and should not be relied on.
///
/// The system has specialised handling of deletes to ensure they are applied
/// correctly, however, this is indeed an abuse of the internal logic of tantivy.
pub struct LightweightIndexer {
    num_docs: u32,
    segment: Segment,
    writer: SegmentWriter,
}

impl LightweightIndexer {
    /// Creates a new lightweight indexer.
    fn new(segment: Segment) -> Result<Self> {
        let writer = SegmentWriter::for_segment(MEMORY_ARENA_SIZE, segment.clone())?;

        Ok(Self {
            segment,
            writer,
            num_docs: 0,
        })
    }

    #[inline]
    /// Adds a new document to the segment.
    pub fn add_document<D>(&mut self, document: D) -> Result<()>
    where
        D: DocumentAccess,
    {
        self.num_docs += 1;
        self.writer.add_document(AddOperation {
            document,
            opstamp: self.num_docs as Opstamp,
        })
    }

    #[inline]
    /// Finishes building the segment.
    pub fn finish(self) -> Result<Segment> {
        self.writer.finalize()?;
        Ok(self.segment.with_max_doc(self.num_docs))
    }
}

#[derive(Clone)]
/// A type which produces delete bitsets for removing
/// entries from the current, readable index.
///
/// The process is largely copied from tantivy except we write the
/// bitset to memory, always, and don't actually mutate the readable index.
pub struct DeletesProcessor {
    reader: RealtimeIndexReader,
}

impl DeletesProcessor {
    /// Creates a new deletes processor using the given reader.
    pub fn new(reader: RealtimeIndexReader) -> Self {
        Self { reader }
    }

    #[inline]
    /// Creates a new set of delete bitsets using the given query.
    pub fn delete_by_query(&self, query: &dyn Query) -> Result<Vec<(SegmentId, Bytes)>> {
        let searcher = self.reader.searcher();
        searcher.get_delete_bitsets(query)
    }
}
