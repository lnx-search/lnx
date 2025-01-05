//! Collect a set of distinct results with a specific set of target fields.
//!
//! All target fields must be fast fields, this is not a cheap collector to use.
//!
use tantivy::collector::{
    Collector,
    FilterCollector,
    SegmentCollector,
    TopDocs,
    TopNComputer,
};
use tantivy::query::Weight;
use tantivy::schema::Field;
use tantivy::{DocAddress, Score, SegmentOrdinal, SegmentReader};

/// A collector that takes the top K distinct results.
///
/// De-duplication is done off a first-come-first-served basis.
pub struct DistinctTopKCollector {
    distinct_fields: Vec<Field>,
    top_k_collector: TopDocs,
}

impl Collector for DistinctTopKCollector {
    type Fruit = <TopDocs as Collector>::Fruit;
    type Child = <TopDocs as Collector>::Child;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        todo!()
    }

    fn requires_scoring(&self) -> bool {
        todo!()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        todo!()
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        self.top_k_collector
            .collect_segment(weight, segment_ord, reader);
        todo!()
    }
}
