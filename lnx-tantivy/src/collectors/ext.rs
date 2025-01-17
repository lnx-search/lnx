use tantivy::collector::{Collector, SegmentCollector};
use tantivy::query::Weight;
use tantivy::{DocId, SegmentReader};

use crate::collectors::SeenHashIds;

/// A collector extension trait that extends the behaviour of the existing a new tantivy
/// collectors.
///
/// This is required for things like the distinct collector which may filter out docs
/// already selected by a segment collector.
pub trait CollectorExt
where
    Self: Collector,
{
    /// Merges a set of segment fruits using the given filter predicate.
    fn merge_fruits_with_filter<P>(
        &self,
        segment_fruits: Vec<(<Self::Child as SegmentCollector>::Fruit, SeenHashIds)>,
        predicate: P,
    ) -> tantivy::Result<Self::Fruit>
    where
        P: FnMut(u64) -> bool;

    /// Works identically to [Collector::collect_segment] except for docs are only
    /// collected if the predicate `P` returns `true`.
    fn collect_segment_with_filter<P>(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
        mut predicate: P,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit>
    where
        P: FnMut(DocId) -> bool,
    {
        let mut segment_collector = self.for_segment(segment_ord, reader)?;
        match (reader.alive_bitset(), self.requires_scoring()) {
            (Some(alive_bitset), true) => {
                weight.for_each(reader, &mut |doc, score| {
                    if alive_bitset.is_alive(doc) || predicate(doc) {
                        segment_collector.collect(doc, score);
                    }
                })?;
            },
            (Some(alive_bitset), false) => {
                weight.for_each_no_score(reader, &mut |docs| {
                    for doc in docs.iter().cloned() {
                        if alive_bitset.is_alive(doc) || predicate(doc) {
                            segment_collector.collect(doc, 0.0);
                        }
                    }
                })?;
            },
            (None, true) => {
                weight.for_each(reader, &mut |doc, score| {
                    if predicate(doc) {
                        segment_collector.collect(doc, score);
                    }
                })?;
            },
            (None, false) => {
                weight.for_each_no_score(reader, &mut |docs| {
                    for doc in docs.iter().cloned() {
                        if predicate(doc) {
                            segment_collector.collect(doc, 0.0);
                        }
                    }
                })?;
            },
        }

        Ok(segment_collector.harvest())
    }
}
