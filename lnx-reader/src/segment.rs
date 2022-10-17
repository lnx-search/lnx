use std::io::{self, ErrorKind};
use std::path::Path;

use lnx_common::schema::RESERVED_DOCUMENT_ID_FIELD;
use lnx_segments::{DeleteValue, DELETES_FILE};
use lnx_storage::ReadOnlyDirectory;
use tantivy::collector::{Collector, FilterCollector};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::Field;
use tantivy::{Directory, DocAddress, Index, IndexReader, ReloadPolicy, Searcher, Term};

use crate::deletes::{DeletesCollector, DeletesFilter};
use crate::ReaderError;

/// A tantivy index reader for single file segments.
pub struct SegmentIndex {
    dir: ReadOnlyDirectory,
    reader: IndexReader,
    doc_id_field: Field,
    deletes: DeletesFilter,
}

impl SegmentIndex {
    /// Open a new index reader for the given segment.
    pub async fn open(path: &Path) -> Result<Self, ReaderError> {
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
            .ok_or(ReaderError::Unsupported)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let searcher = reader.searcher();
        let deletes = tokio::task::spawn_blocking(move || {
            find_initial_deletes(searcher, deletes, doc_id_field)
        })
        .await
        .expect("Spawn background task")?;

        Ok(Self {
            dir,
            reader,
            doc_id_field,
            deletes,
        })
    }

    /// Reads the segment delete information.
    pub async fn read_deletes_file(&self) -> Result<lnx_segments::Deletes, ReaderError> {
        let deletes_raw = self
            .dir
            .atomic_read(Path::new(DELETES_FILE))
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let deletes = lnx_segments::Deletes::from_compressed_bytes(deletes_raw).await?;

        Ok(deletes)
    }

    /// Registers a new set of deletes fro the segment.
    ///
    /// This will perform a search to get all the documents matching the current deletes.
    pub async fn register_deletes(
        &self,
        deletes: lnx_segments::Deletes,
    ) -> Result<(), ReaderError> {
        let searcher = self.reader.searcher();
        let doc_id_field = self.doc_id_field;

        let segment_deletes = tokio::task::spawn_blocking(move || {
            search_for_deletes(searcher, deletes, doc_id_field)
        })
        .await
        .expect("Spawn background thread")?;

        let iter = segment_deletes.into_iter().flatten();
        self.deletes.mark_doc_ids_deleted(iter);

        Ok(())
    }

    /// Creates a new searcher for the given index.
    pub fn reader(&self) -> SegmentReader {
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
    deletes: DeletesFilter,
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
    pub fn execute<Q, C>(&self, _query: &Q, collector: C) -> Result<(), ReaderError>
    where
        Q: Query,
        C: Collector,
    {
        let deletes = self.deletes.clone();
        let predicate = move |id: u64| deletes.is_deleted(id);

        let _filter = FilterCollector::new(self.doc_id_field, predicate, collector);

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

fn find_initial_deletes(
    searcher: Searcher,
    deletes: lnx_segments::Deletes,
    doc_id_field: Field,
) -> Result<DeletesFilter, tantivy::TantivyError> {
    let segment_deletes = search_for_deletes(searcher, deletes, doc_id_field)?;

    let deletes_filter = DeletesFilter::new();

    let iter = segment_deletes.into_iter().flatten();
    deletes_filter.mark_doc_ids_deleted(iter);

    Ok(deletes_filter)
}

fn search_for_deletes(
    searcher: Searcher,
    deletes: lnx_segments::Deletes,
    doc_id_field: Field,
) -> Result<Vec<Vec<u64>>, tantivy::TantivyError> {
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

    Ok(segment_deletes)
}
