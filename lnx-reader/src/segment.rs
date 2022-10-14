use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use lnx_common::schema::RESERVED_DOCUMENT_ID_FIELD;
use lnx_segments::{DeleteValue, DELETES_FILE};
use lnx_storage::ReadOnlyDirectory;
use tantivy::collector::{Collector, FilterCollector};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::Field;
use tantivy::{Directory, DocAddress, Index, IndexReader, ReloadPolicy, Searcher, Term};

use crate::deletes::{DeletesCollector, DeletesFilter};
use crate::ReaderError;

/// The ratio of the number of docs to the size of the bloom filter.
const FILTER_SIZE_TO_DOC_COUNT_RATIO: f64 = 0.35;

/// A tantivy index reader for single file segments.
pub(crate) struct SegmentIndex {
    dir: ReadOnlyDirectory,
    index: Index,
    reader: IndexReader,
    doc_id_field: Field,
    deletes: Arc<ArcSwap<DeletesFilter>>,
}

impl SegmentIndex {
    /// Open a new index reader for the given segment.
    pub(crate) async fn open(path: &Path) -> Result<Self, ReaderError> {
        let dir = ReadOnlyDirectory::open(path)?;

        // TODO: Handle error better.
        let deletes_raw = dir
            .atomic_read(Path::new(DELETES_FILE))
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let deletes = lnx_segments::Deletes::from_compressed_bytes(deletes_raw).await?;

        let index = Index::open(dir.clone())?;
        let schema = index.schema();
        let doc_id_field = schema
            .get_field(RESERVED_DOCUMENT_ID_FIELD)
            .ok_or_else(|| ReaderError::Unsupported)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let searcher = reader.searcher();
        let deletes = tokio::task::spawn_blocking(move || {
            search_for_deletes(searcher, deletes, doc_id_field)
        })
        .await
        .expect("Spawn background task")?;

        Ok(Self {
            dir,
            index,
            reader,
            doc_id_field,
            deletes: Arc::new(ArcSwap::from_pointee(deletes)),
        })
    }

    /// Creates a new searcher for the given index.
    pub(crate) fn reader(&self) -> SegmentReader {
        let searcher = self.reader.searcher();
        let deletes = self.deletes.clone();
        let doc_id_field = self.doc_id_field;

        SegmentReader {
            searcher,
            doc_id_field,
            deletes,
        }
    }
}

pub struct SegmentReader {
    searcher: Searcher,
    doc_id_field: Field,
    deletes: Arc<ArcSwap<DeletesFilter>>,
}

impl SegmentReader {
    /// Get the stored document data located at the given doc address.
    ///
    /// The doc address must have been previously produced by the same searcher.
    pub fn get_doc(
        &self,
        doc_address: DocAddress,
    ) -> Result<tantivy::Document, tantivy::TantivyError> {
        self.searcher.doc(doc_address)
    }

    /// Executes a given search query.
    ///
    /// This takes into account any marked deletes within the index.
    pub fn execute<Q, C>(&self, query: &Q, collector: C) -> Result<(), ReaderError>
    where
        Q: Query,
        C: Collector,
    {
        let deletes = self.deletes.clone();
        let predicate = move |id: u64| {
            let guard = deletes.load();
            guard.is_deleted(id)
        };

        let filter = FilterCollector::new(self.doc_id_field, predicate, collector);

        Ok(())
    }
}

macro_rules! get_or_continue {
    ($v:expr) => {{
        match $v {
            None => continue,
            Some(v) => v,
        }
    }};
}

fn search_for_deletes(
    searcher: Searcher,
    deletes: lnx_segments::Deletes,
    doc_id_field: Field,
) -> Result<DeletesFilter, tantivy::TantivyError> {
    let schema = searcher.schema();

    let mut query_parts = vec![];

    for delete in deletes.0 {
        let field = get_or_continue!(schema.get_field(&delete.field));
        let term = match delete.value {
            DeleteValue::I64(val) => Term::from_field_i64(field, val),
            DeleteValue::U64(val) => Term::from_field_u64(field, val),
            DeleteValue::F64(val) => Term::from_field_f64(field, val),
            DeleteValue::String(val) => Term::from_field_text(field, &val),
            DeleteValue::Bytes(val) => Term::from_field_bytes(field, &val),
        };

        query_parts.push((
            Occur::Should,
            Box::new(TermQuery::new(
                term,
                tantivy::schema::IndexRecordOption::Basic,
            )) as Box<dyn Query>,
        ));
    }

    let query = BooleanQuery::new(query_parts);
    let collector = DeletesCollector::new(doc_id_field);

    let segment_deletes = searcher.search(&query, &collector)?;
    let num_docs: usize = segment_deletes.iter().map(|v| v.len()).sum();

    let size = calculate_bloom_capacity(num_docs);
    let mut deletes_filter = DeletesFilter::new_blank(size);

    for segment in segment_deletes {
        for doc_id in segment {
            deletes_filter.mark_deleted(doc_id);
        }
    }

    Ok(deletes_filter)
}

fn calculate_bloom_capacity(num_docs: usize) -> usize {
    // A roughly calculated threshold where the additional memory usage
    // isn't worth the potential performance trade off due to the extra
    // hashing being done.
    if num_docs <= 250_000 {
        return num_docs;
    }

    (num_docs as f64 * FILTER_SIZE_TO_DOC_COUNT_RATIO) as usize
}
