use std::hash::Hasher;
use std::net::Ipv6Addr;

use ahash::AHasher;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{Column, MonotonicallyMappableToU128};
use tantivy::query::Weight;
use tantivy::schema::{Field, FieldType};
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader};

use crate::collectors::ext::CollectorExt;
use crate::collectors::SeenHashIds;

/// A collector that takes the _distinct_ top K results.
///
/// Whether a document is distinct or not is determined by the specified target fields.
/// The target fields must be fast fields in order to be used.
pub struct Distinct<C> {
    select_fields: Vec<Field>,
    inner_collector: C,
}

impl<C> Distinct<C> {
    /// Creates a new distinct collector which de-duplicates value by the provided fields.
    pub fn for_fields(fields: Vec<Field>, inner: C) -> Self {
        Self {
            select_fields: fields,
            inner_collector: inner,
        }
    }
}

impl<C> Collector for Distinct<C>
where
    C: Collector + CollectorExt,
{
    type Fruit = C::Fruit;
    type Child = DistinctSegmentCollector<C::Child>;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let inner = self
            .inner_collector
            .for_segment(segment_local_id, segment)?;
        let tracker = DistinctDocsTracker::for_segment(segment, &self.select_fields)?;

        Ok(DistinctSegmentCollector { tracker, inner })
    }

    fn requires_scoring(&self) -> bool {
        self.inner_collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut seen_hash_ids = SeenHashIds::default();
        let filter_predicate = |hash_id: u64| seen_hash_ids.insert(hash_id);
        let merged_fruits = self
            .inner_collector
            .merge_fruits_with_filter(segment_fruits, filter_predicate)?;
        Ok(merged_fruits)
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        segment: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        let mut tracker =
            DistinctDocsTracker::for_segment(segment, &self.select_fields)?;
        let filter_predicate = |doc_id: DocId| tracker.insert(doc_id);
        let fruit = self.inner_collector.collect_segment_with_filter(
            weight,
            segment_ord,
            segment,
            filter_predicate,
        )?;
        Ok((fruit, tracker.seen_hash_id))
    }
}

pub struct DistinctSegmentCollector<C> {
    tracker: DistinctDocsTracker,
    inner: C,
}

impl<C> SegmentCollector for DistinctSegmentCollector<C>
where
    C: SegmentCollector,
{
    type Fruit = (C::Fruit, SeenHashIds);

    fn collect(&mut self, doc: DocId, score: Score) {
        let did_insert = self.tracker.insert(doc);
        if did_insert {
            self.inner.collect(doc, score);
        }
    }

    fn harvest(self) -> Self::Fruit {
        (self.inner.harvest(), self.tracker.into_hash_ids())
    }
}

// TODO: Include JSON fields...
struct DistinctDocsTracker {
    segment_columns: Vec<DynamicColumn>,
    seen_hash_id: SeenHashIds,
}

impl DistinctDocsTracker {
    fn for_segment(segment: &SegmentReader, fields: &[Field]) -> tantivy::Result<Self> {
        let fast_fields = segment.fast_fields();
        let schema = segment.schema();

        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            let entry = schema.get_field_entry(*field);

            if !entry.is_fast() {
                return Err(tantivy::TantivyError::InvalidArgument(format!(
                    "Field {:?} is not a columnar field",
                    entry.name()
                )));
            }

            let field_type = entry.field_type();
            let column = match field_type {
                FieldType::IpAddr(_) => {
                    let inner = fast_fields.ip_addr(entry.name())?;
                    DynamicColumn::IpColumn(inner)
                },
                _ => match fast_fields.u64_lenient_for_type(None, entry.name())? {
                    None => continue,
                    Some((inner, _)) => DynamicColumn::U64MappedColumn(inner),
                },
            };

            columns.push(column);
        }

        Ok(Self {
            segment_columns: columns,
            seen_hash_id: SeenHashIds::default(),
        })
    }

    /// Attempts to insert a document into the tracker.
    ///
    /// Returns `true` if the document has not been seen before and `false` if it has been
    /// seen already.
    fn insert(&mut self, doc: DocId) -> bool {
        let mut hasher = AHasher::default();
        for column in self.segment_columns.iter() {
            column.write_value_to_hasher(doc, &mut hasher);
        }

        let hash_id = hasher.finish();
        self.seen_hash_id.insert(hash_id)
    }

    fn into_hash_ids(self) -> SeenHashIds {
        self.seen_hash_id
    }
}

enum DynamicColumn {
    U64MappedColumn(Column),
    IpColumn(Column<Ipv6Addr>),
}

impl DynamicColumn {
    fn write_value_to_hasher(&self, doc: DocId, hasher: &mut AHasher) {
        match self {
            DynamicColumn::U64MappedColumn(inner) => {
                for value in inner.values_for_doc(doc) {
                    hasher.write_u64(value);
                }
            },
            DynamicColumn::IpColumn(inner) => {
                for value in inner.values_for_doc(doc) {
                    hasher.write_u128(value.to_u128());
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::indexer::IndexWriterOptions;
    use tantivy::query::AllQuery;
    use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
    use tantivy::{doc, Index, TantivyError};

    use super::*;
    use crate::collectors::top_docs::TopDocs;

    fn create_test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
        let title = schema_builder.add_text_field("title", TEXT | STORED | FAST);
        let description = schema_builder.add_text_field("description", STORED);
        let ip = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(0)
            .build();
        let mut writer = index.writer_with_options(options).unwrap();
        writer
            .add_document(doc!(
                id => 1u64,
                title => "The old man and the sea",
                description => "example text here today",
                ip => Ipv6Addr::LOCALHOST,
            ))
            .unwrap();
        writer
            .add_document(doc!(
                id => 2u64,
                title => "The old man and the sea",
                description => "example text here today",
                ip => Ipv6Addr::LOCALHOST,
            ))
            .unwrap();
        writer
            .add_document(doc!(
                id => 3u64,
                title => "X men",
                description => "Something something rivals",
            ))
            .unwrap();
        writer.commit().unwrap();
        index
    }

    #[test]
    fn test_distinct_without_offset() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let title_field = schema.get_field("title").unwrap();

        let collector = Distinct::for_fields(vec![title_field], TopDocs::with_limit(10));
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 2);
    }

    #[test]
    fn test_distinct_no_duplicates() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let id_field = schema.get_field("id").unwrap();

        let collector = Distinct::for_fields(vec![id_field], TopDocs::with_limit(10));
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 3);
    }

    #[test]
    fn test_distinct_zero_limit() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let title_field = schema.get_field("title").unwrap();

        let collector = Distinct::for_fields(vec![title_field], TopDocs::with_limit(0));
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 0);
    }

    #[test]
    fn test_distinct_with_offset() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let title_field = schema.get_field("title").unwrap();

        let collector = Distinct::for_fields(
            vec![title_field],
            TopDocs::with_limit(3).and_offset(1),
        );
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 1);
    }

    #[test]
    fn test_distinct_non_fast_field() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let description_field = schema.get_field("description").unwrap();

        let collector =
            Distinct::for_fields(vec![description_field], TopDocs::with_limit(3));
        let error = searcher
            .search(&AllQuery, &collector)
            .expect_err("Cannot search distinct on non-fast field");
        assert!(matches!(error, TantivyError::InvalidArgument(_)));
    }

    #[test]
    fn test_distinct_ip_field() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = index.schema();
        let ip_field = schema.get_field("ip").unwrap();

        let collector = Distinct::for_fields(vec![ip_field], TopDocs::with_limit(3));
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 2);
    }
}
