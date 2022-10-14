use std::sync::Arc;

use bitvec::prelude::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::Column;
use tantivy::schema::Field;

pub(crate) struct DeletesFilter {
    bloom_filter: BitVec,
    deletes: hashbrown::HashSet<u64>,
}

impl DeletesFilter {
    /// Creates a new set of deletes with a bloom filter of a given size.
    pub fn new_blank(num_docs: usize) -> Self {
        let mut bloom_filter = BitVec::with_capacity(num_docs);
        bloom_filter.resize(num_docs, false);

        Self {
            bloom_filter,
            deletes: Default::default(),
        }
    }

    /// Marks a given document as deleted.
    pub fn mark_deleted(&mut self, doc_id: u64) {
        self.deletes.insert(doc_id);

        let position = (doc_id % self.bloom_filter.len() as u64) as usize;
        self.bloom_filter.set(position, true);
    }

    #[inline(always)]
    /// Checks if the given document is deleted or not.
    pub fn is_deleted(&self, doc_id: u64) -> bool {
        let position = (doc_id % self.bloom_filter.len() as u64) as usize;

        // It's not in the filter which means that it's definitely not deleted.
        if !self.bloom_filter[position] {
            return false;
        }

        self.deletes.contains(&doc_id)
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
        segment_local_id: tantivy::SegmentOrdinal,
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
