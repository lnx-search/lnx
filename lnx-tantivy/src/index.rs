use smallvec::SmallVec;
use tantivy::schema::DocumentAccess;
use crate::realtime_reader::RealtimeIndexReader;
use crate::{LightweightIndexer, TantivyIndexerPool};

pub type DocumentBatch<D> = SmallVec<[DocumentInfo<D>; 10]>;


/// A single document payload for indexing.
pub struct DocumentInfo<D: DocumentAccess> {
    /// The primary key ID of the document.
    pub doc_id: u64,
    /// The document data that is actually indexed.
    pub doc_data: D,
}


/// A single search index.
pub struct Index {
    /// The inner tantivy index.
    ///
    /// This index functions a bit differently to normal indexes
    /// as the directory does a lot of magic mutation in order to
    /// reflect live changes as they occur.
    inner: tantivy::Index,
    /// The lightweight indexer.
    ///
    /// This only handles insertions
    indexer: LightweightIndexer,
    /// The internal index reader.
    reader: RealtimeIndexReader,
    /// The indexer threadpool for submitting requests.
    threadpool: TantivyIndexerPool,
}