use std::sync::Arc;

use arc_swap::ArcSwap;
use bitvec::prelude::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::Column;
use tantivy::schema::Field;

/// The ratio of the number of docs to the size of the bloom filter.
const FILTER_SIZE_TO_DOC_COUNT_RATIO: f64 = 0.35;

#[derive(Clone)]
pub(crate) struct DeletesFilter {
    bloom_filter: Arc<ArcSwap<BitVec>>,
    deletes: Arc<ArcSwap<hashbrown::HashSet<u64>>>,
}

impl DeletesFilter {
    /// Creates a new set of deletes with a empty bloom filter.
    pub fn new() -> Self {
        Self {
            bloom_filter: Arc::new(ArcSwap::from_pointee(BitVec::new())),
            deletes: Arc::new(ArcSwap::from_pointee(hashbrown::HashSet::new())),
        }
    }

    /// Marks a given document as deleted.
    pub fn mark_doc_ids_deleted(&self, doc_ids: impl Iterator<Item = u64>) {
        let mut slf_clone = self.deletes.load().as_ref().clone();

        for doc_id in doc_ids {
            slf_clone.insert(doc_id);
        }

        let new_size = calculate_bloom_capacity(slf_clone.len());

        let mut bloom_filter = BitVec::with_capacity(new_size);
        bloom_filter.resize(new_size, false);

        self.bloom_filter.store(Arc::new(bloom_filter));
    }

    #[inline(always)]
    /// Checks if the given document is deleted or not.
    pub fn is_deleted(&self, doc_id: u64) -> bool {
        let filter_guard = self.bloom_filter.load();
        let position = (doc_id % filter_guard.len() as u64) as usize;

        // It's not in the filter which means that it's definitely not deleted.
        if !filter_guard[position] {
            return false;
        }

        let slf_clone = self.deletes.load();
        slf_clone.contains(&doc_id)
    }
}

/// A custom tantivy collector for collecting
/// the document ids from a fast field.
pub struct DeletesCollector {
    doc_id_field: Field,
}

impl DeletesCollector {
    pub fn new(doc_id_field: Field) -> Self {
        Self { doc_id_field }
    }
}

impl Collector for DeletesCollector {
    type Fruit = Vec<<Self::Child as SegmentCollector>::Fruit>;

    type Child = DeletesSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column = segment.fast_fields().u64(self.doc_id_field)?;

        Ok(DeletesSegmentCollector {
            column,
            values: vec![],
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits)
    }
}

pub struct DeletesSegmentCollector {
    column: Arc<dyn Column<u64>>,
    values: Vec<u64>,
}

impl SegmentCollector for DeletesSegmentCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        let sequential_id = self.column.get_val(doc as u64);
        self.values.push(sequential_id);
    }

    fn harvest(self) -> Self::Fruit {
        self.values
    }
}

pub(crate) fn calculate_bloom_capacity(num_docs: usize) -> usize {
    // A roughly calculated threshold where the additional memory usage
    // isn't worth the potential performance trade off due to the extra
    // hashing being done.
    if num_docs <= 250_000 {
        return num_docs;
    }

    (num_docs as f64 * FILTER_SIZE_TO_DOC_COUNT_RATIO) as usize
}
