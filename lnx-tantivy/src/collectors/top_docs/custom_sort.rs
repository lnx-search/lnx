use std::cmp::{Ordering, Reverse};
use std::net::Ipv6Addr;

use smallvec::SmallVec;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::fastfield::FastFieldReaders;
use tantivy::schema::{Field, FieldType, Schema};
use tantivy::{DocAddress, DocId, Order, Score, SegmentOrdinal, SegmentReader};

use crate::collectors::ext::CollectorExt;
use crate::collectors::top_docs::{TopNComputer, TopSegmentCollector};
use crate::collectors::SeenHashIds;

/// A single key that can be used to sort/reorder the documents.
pub enum SortableKey {
    /// Sort by the match score.
    Score,
    /// Sort by a given fast field.
    Column(Field),
}

impl SortableKey {
    fn is_score(&self) -> bool {
        matches!(self, Self::Score)
    }
}

/// Select the top K documents from a search query.
///
/// This is mostly a mirror of the original [tantivy::collector::TopDocs]
/// collector but custom-made so we can use the distinct collector.
pub struct CustomSortTopDocs {
    limit: usize,
    offset: usize,
    sort_by: Vec<(SortableKey, Order)>,
}

impl CustomSortTopDocs {
    /// Creates a new [crate::collectors::TopDocs] using the given limit.
    pub fn with_limit_and_sort(
        limit: usize,
        sort_by: Vec<(SortableKey, Order)>,
    ) -> Self {
        Self {
            limit,
            offset: 0,
            sort_by,
        }
    }

    /// Adds an offset to skip `offset` number of documents
    /// before collecting results.
    pub fn and_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
}

impl Collector for CustomSortTopDocs {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = CustomScoreTopDocsSegmentCollector;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let inner = TopSegmentCollector {
            segment_ord,
            topn_computer: TopNComputer::new(self.limit + self.offset),
        };

        let stages = create_sort_for_segment(segment, &self.sort_by)?;

        let collector = CustomScoreTopDocsSegmentCollector { stages, inner };

        Ok(collector)
    }

    fn requires_scoring(&self) -> bool {
        self.sort_by.iter().any(|(key, _)| key.is_score())
    }

    fn merge_fruits(
        &self,
        children: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }

        let mut top_collector: TopNComputer<_, _> =
            TopNComputer::new(self.limit + self.offset);
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                top_collector.push(feature, doc);
            }
        }

        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .skip(self.offset)
            .map(|cdoc| (cdoc.feature.score, cdoc.doc))
            .collect())
    }
}

impl CollectorExt for CustomSortTopDocs {
    fn merge_fruits_with_filter<P>(
        &self,
        children: Vec<(<Self::Child as SegmentCollector>::Fruit, SeenHashIds)>,
        mut predicate: P,
    ) -> tantivy::Result<Self::Fruit>
    where
        P: FnMut(u64) -> bool,
    {
        if self.limit == 0 {
            return Ok(Vec::new());
        }

        let mut top_collector: TopNComputer<_, _> =
            TopNComputer::new(self.limit + self.offset);
        for (child_fruit, seen_hashes) in children {
            for ((feature, doc), hash_id) in std::iter::zip(child_fruit, seen_hashes) {
                if predicate(hash_id) {
                    top_collector.push(feature, doc);
                }
            }
        }

        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .skip(self.offset)
            .map(|cdoc| (cdoc.feature.score, cdoc.doc))
            .collect())
    }
}

type SortValues = SmallVec<[SortValue; 4]>;

/// A [SegmentCollector] that collects the top K documents
/// sorting by the document's score.
pub struct CustomScoreTopDocsSegmentCollector {
    stages: Vec<SortStage>,
    inner: TopSegmentCollector<DocScoreAndSortValues>,
}

impl SegmentCollector for CustomScoreTopDocsSegmentCollector {
    type Fruit = Vec<(DocScoreAndSortValues, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let mut sort_values = SortValues::new();
        for stage in self.stages.iter() {
            let value = stage.get_sort_value(doc, score);
            sort_values.push(value);
        }
        let sort_score = DocScoreAndSortValues::new(score, sort_values);
        self.inner.collect(doc, sort_score);
    }

    fn harvest(self) -> Self::Fruit {
        self.inner.harvest()
    }
}

fn create_sort_for_segment(
    segment: &SegmentReader,
    fields: &[(SortableKey, Order)],
) -> tantivy::Result<Vec<SortStage>> {
    let schema = segment.schema();
    let fast_fields = segment.fast_fields();

    let mut stages = Vec::with_capacity(fields.len());
    for (key, order) in fields {
        let field = match key {
            SortableKey::Column(field) => *field,
            SortableKey::Score => continue,
        };

        let column = open_dynamic_column(schema, fast_fields, field)?;
        let stage = SortStage {
            column: Some(column),
            order: order.clone(),
        };

        stages.push(stage);
    }

    Ok(stages)
}

fn open_dynamic_column(
    schema: &Schema,
    fast_fields: &FastFieldReaders,
    field: Field,
) -> tantivy::Result<DynamicColumn> {
    let entry = schema.get_field_entry(field);
    if !entry.is_fast() {
        return Err(tantivy::TantivyError::InvalidArgument(format!(
            "Field {:?} is not a columnar field",
            entry.name()
        )));
    }

    let field_type = entry.field_type();
    let column = match field_type {
        FieldType::IpAddr(_) => fast_fields
            .column_opt(entry.name())?
            .map(DynamicColumn::Ipv6)
            .unwrap_or(DynamicColumn::NullIpv6),
        _ => fast_fields
            .u64_lenient_for_type(None, entry.name())?
            .map(|(column, _)| DynamicColumn::U64Mapped(column))
            .unwrap_or(DynamicColumn::NullU64Mapped),
    };

    Ok(column)
}

#[derive(Debug, Clone)]
/// A sortable value that holds onto the original score and the values used for
/// sorting the document.
pub struct DocScoreAndSortValues {
    score: Score,
    sort_values: SortValues,
}

impl DocScoreAndSortValues {
    fn new(score: Score, sort_values: SortValues) -> Self {
        Self { score, sort_values }
    }
}

impl PartialEq for DocScoreAndSortValues {
    fn eq(&self, other: &Self) -> bool {
        self.sort_values.eq(&other.sort_values)
    }
}

impl PartialOrd for DocScoreAndSortValues {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.sort_values.partial_cmp(&other.sort_values)
    }
}

#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
enum SortValue {
    AscScore(Reverse<Score>),
    DescScore(Score),
    AscU64Ordinal(Reverse<Option<u64>>),
    DescU64Ordinal(Option<u64>),
    AscIpv6(Reverse<Option<Ipv6Addr>>),
    DescIpv6(Option<Ipv6Addr>),
}

struct SortStage {
    /// The column to read from to find the sort value.
    ///
    /// If this is `None` then this is the score.
    column: Option<DynamicColumn>,
    order: Order,
}

impl SortStage {
    fn get_sort_value(&self, doc: DocId, score: Score) -> SortValue {
        match &self.column {
            None => {
                // Score sort
                match self.order {
                    Order::Asc => SortValue::AscScore(Reverse(score)),
                    Order::Desc => SortValue::DescScore(score),
                }
            },
            Some(DynamicColumn::NullU64Mapped) => match self.order {
                Order::Asc => SortValue::AscU64Ordinal(Reverse(None)),
                Order::Desc => SortValue::DescU64Ordinal(None),
            },
            Some(DynamicColumn::NullIpv6) => match self.order {
                Order::Asc => SortValue::AscU64Ordinal(Reverse(None)),
                Order::Desc => SortValue::DescU64Ordinal(None),
            },
            Some(DynamicColumn::U64Mapped(column)) => {
                let values = column.values_for_doc(doc).max();
                match self.order {
                    Order::Asc => SortValue::AscU64Ordinal(Reverse(values)),
                    Order::Desc => SortValue::DescU64Ordinal(values),
                }
            },
            Some(DynamicColumn::Ipv6(column)) => {
                let values = column.values_for_doc(doc).max();
                match self.order {
                    Order::Asc => SortValue::AscIpv6(Reverse(values)),
                    Order::Desc => SortValue::DescIpv6(values),
                }
            },
        }
    }
}

enum DynamicColumn {
    NullU64Mapped,
    NullIpv6,
    U64Mapped(Column),
    Ipv6(Column<Ipv6Addr>),
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tantivy::indexer::IndexWriterOptions;
    use tantivy::query::{AllQuery, QueryParser};
    use tantivy::schema::{Value, FAST, INDEXED, STORED, TEXT};
    use tantivy::{doc, Index, Term};

    use super::*;
    use crate::collectors::TopDocs;

    fn create_test_index(ip_values: [Option<Ipv6Addr>; 3]) -> Index {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
        let title = schema_builder.add_text_field("title", TEXT | STORED | FAST);
        let description = schema_builder.add_text_field("description", STORED | FAST);
        let ip = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(0)
            .build();
        let mut writer = index.writer_with_options(options).unwrap();

        let mut doc = doc!(
            id => 1u64,
            title => "The old man and the sea",
            description => "example text here today",
        );
        if let Some(value) = ip_values[0] {
            doc.add_ip_addr(ip, value);
        }
        writer.add_document(doc).unwrap();

        let mut doc = doc!(
            id => 2u64,
            title => "Iron man 4",
            description => "example text here today",
        );
        if let Some(value) = ip_values[1] {
            doc.add_ip_addr(ip, value);
        }
        writer.add_document(doc).unwrap();

        let mut doc = doc!(
            id => 3u64,
            title => "X men",
            description => "Something something rivals",
        );
        if let Some(value) = ip_values[2] {
            doc.add_ip_addr(ip, value);
        }
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();

        let segment_ids = index
            .searchable_segments()
            .unwrap()
            .into_iter()
            .map(|seg| seg.id())
            .collect::<Vec<_>>();
        writer.merge(&segment_ids).wait().unwrap();

        index
    }
    #[test]
    fn test_top_k_custom_score_without_offset() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id = schema.get_field("id").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(id), Order::Desc)],
        );
        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(results.len(), 3);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();
        let doc3: tantivy::TantivyDocument = searcher.doc(results[2].1).unwrap();
        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(3));
        assert_eq!(doc2.get_first(id).unwrap().as_u64(), Some(2));
        assert_eq!(doc3.get_first(id).unwrap().as_u64(), Some(1));
    }

    #[test]
    fn test_top_k_custom_score_zero_limit() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id_field = schema.get_field("id").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            0,
            vec![(SortableKey::Column(id_field), Order::Desc)],
        );
        let all_docs = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(all_docs.len(), 0);
    }

    #[test]
    fn test_top_k_custom_score_with_offset() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id = schema.get_field("id").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(id), Order::Desc)],
        )
        .and_offset(2);
        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Complete search");
        assert_eq!(results.len(), 1);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(1));
    }

    #[test]
    fn test_top_k_custom_score_for_segment() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let title = schema.get_field("title").unwrap();
        let segments = searcher.segment_readers();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(title), Order::Desc)],
        )
        .and_offset(1);
        let mut collector = collector
            .for_segment(0, &segments[0])
            .expect("Collect for segment");
        collector.collect(1, 1.0);
        collector.collect(2, 1.0);
        let results = collector.harvest();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_top_k_custom_score_sort_multi_value() {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
        let title = schema_builder.add_text_field("title", TEXT | STORED | FAST);
        let description = schema_builder.add_text_field("description", STORED | FAST);
        let ip = schema_builder.add_ip_addr_field("ip", STORED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(0)
            .build();
        let mut writer = index.writer_with_options(options).unwrap();

        let mut doc = tantivy::TantivyDocument::new();
        doc.add_u64(id, 1);
        doc.add_text(title, "Iron man 4");
        doc.add_text(description, "Man made of iron does stuff.");
        doc.add_ip_addr(ip, Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped());
        doc.add_ip_addr(ip, Ipv4Addr::new(127, 0, 0, 3).to_ipv6_mapped());
        doc.add_ip_addr(ip, Ipv4Addr::new(127, 0, 0, 2).to_ipv6_mapped());
        writer.add_document(doc).unwrap();

        let mut doc = tantivy::TantivyDocument::new();
        doc.add_u64(id, 2);
        doc.add_text(title, "The old man and the sea");
        doc.add_text(description, "A very well known book that I do not know.");
        doc.add_ip_addr(ip, Ipv4Addr::new(192, 0, 0, 1).to_ipv6_mapped());
        doc.add_ip_addr(ip, Ipv4Addr::new(129, 0, 0, 2).to_ipv6_mapped());
        writer.add_document(doc).unwrap();

        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(ip), Order::Desc)],
        );

        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Get sorted results");
        assert_eq!(results.len(), 2);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();

        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(2));
        assert_eq!(doc2.get_first(id).unwrap().as_u64(), Some(1));
    }

    #[test]
    fn test_top_k_custom_score_sort_dense() {
        let index = create_test_index([
            Some(Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped()),
            Some(Ipv4Addr::new(82, 1, 4, 68).to_ipv6_mapped()),
            Some(Ipv4Addr::new(192, 168, 0, 2).to_ipv6_mapped()),
        ]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id = schema.get_field("id").unwrap();
        let ip = schema.get_field("ip").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(ip), Order::Desc)],
        );

        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Get sorted results");
        assert_eq!(results.len(), 3);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();
        let doc3: tantivy::TantivyDocument = searcher.doc(results[2].1).unwrap();

        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(3));
        assert_eq!(doc2.get_first(id).unwrap().as_u64(), Some(1));
        assert_eq!(doc3.get_first(id).unwrap().as_u64(), Some(2));
    }

    #[test]
    fn test_top_k_custom_score_sort_sparse() {
        let index = create_test_index([
            Some(Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped()),
            None,
            Some(Ipv4Addr::new(192, 168, 0, 2).to_ipv6_mapped()),
        ]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id = schema.get_field("id").unwrap();
        let ip = schema.get_field("ip").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(ip), Order::Asc)],
        );

        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Get sorted results");
        assert_eq!(results.len(), 3);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();
        let doc3: tantivy::TantivyDocument = searcher.doc(results[2].1).unwrap();

        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(2));
        assert_eq!(doc2.get_first(id).unwrap().as_u64(), Some(1));
        assert_eq!(doc3.get_first(id).unwrap().as_u64(), Some(3));
    }

    #[test]
    fn test_top_k_custom_score_sort_null_col() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let id = schema.get_field("id").unwrap();
        let ip = schema.get_field("ip").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![
                (SortableKey::Column(ip), Order::Desc),
                (SortableKey::Column(id), Order::Asc),
            ],
        );

        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Get sorted results");
        assert_eq!(results.len(), 3);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();
        let doc3: tantivy::TantivyDocument = searcher.doc(results[2].1).unwrap();

        assert_eq!(doc1.get_first(id).unwrap().as_u64(), Some(1));
        assert_eq!(doc2.get_first(id).unwrap().as_u64(), Some(2));
        assert_eq!(doc3.get_first(id).unwrap().as_u64(), Some(3));
    }

    #[test]
    fn test_top_k_custom_score_with_deletes() {
        let index = create_test_index([None, None, None]);
        let reader = index.reader().unwrap();
        let schema = index.schema();
        let id_field = schema.get_field("id").unwrap();

        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(1)
            .build();
        let mut writer: tantivy::IndexWriter =
            index.writer_with_options(options).unwrap();
        writer.delete_term(Term::from_field_u64(id_field, 2));
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        let title = schema.get_field("title").unwrap();

        let collector = CustomSortTopDocs::with_limit_and_sort(
            10,
            vec![(SortableKey::Column(title), Order::Asc)],
        );

        let results = searcher
            .search(&AllQuery, &collector)
            .expect("Get sorted results");
        assert_eq!(results.len(), 2);

        let doc1: tantivy::TantivyDocument = searcher.doc(results[0].1).unwrap();
        let doc2: tantivy::TantivyDocument = searcher.doc(results[1].1).unwrap();

        assert_eq!(
            doc1.get_first(title).unwrap().as_str(),
            Some("The old man and the sea")
        );
        assert_eq!(doc2.get_first(title).unwrap().as_str(), Some("X men"));
    }
}
