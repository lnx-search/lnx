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
