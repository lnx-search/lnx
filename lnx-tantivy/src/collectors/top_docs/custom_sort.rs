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
