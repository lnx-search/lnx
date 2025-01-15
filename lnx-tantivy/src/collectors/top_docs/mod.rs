use std::fmt;

use tantivy::collector::{Collector, ComparableDoc, SegmentCollector};
use tantivy::query::Weight;
use tantivy::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

use crate::collectors::ext::CollectorExt;
use crate::collectors::SeenHashIds;

mod custom_score;
mod distinct;

/// Select the top K documents from a search query.
///
/// This is mostly a mirror of the original [tantivy::collector::TopDocs]
/// collector but custom-made so we can use the distinct collector.
pub struct TopDocs {
    limit: usize,
    offset: usize,
}

impl TopDocs {
    /// Creates a new [TopDocs] using the given limit.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit,
            offset: 0,
        }
    }
    
    /// Adds an offset to skip `offset` number of documents
    /// before collecting results.
    pub fn and_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
}

impl Collector for TopDocs {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = TopDocsSegmentCollector;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let inner = TopSegmentCollector {
            segment_ord,
            topn_computer: TopNComputer::new(self.limit + self.offset),
        };
        Ok(TopDocsSegmentCollector(inner))
    }

    fn requires_scoring(&self) -> bool {
        true
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
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
        self.collect_segment_with_filter(weight, segment_ord, reader, |_doc_id| true)
    }
}

impl CollectorExt for TopDocs {
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
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

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
        let mut top_n: TopNComputer<_, _> = TopNComputer::new(self.limit + self.offset);
        let mut threshold = Score::MIN;
        top_n.threshold = Some(threshold);
        if let Some(alive_bitset) = reader.alive_bitset() {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) || !predicate(doc) {
                    return threshold;
                }

                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if !predicate(doc) {
                    return threshold;
                }
                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        }

        let fruit = top_n
            .into_sorted_vec()
            .into_iter()
            .map(|cid| {
                (
                    cid.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: cid.doc,
                    },
                )
            })
            .collect();
        Ok(fruit)
    }
}

/// A [SegmentCollector] that collects the top K documents
/// sorting by the document's score.
pub struct TopDocsSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopDocsSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score)
    }

    fn harvest(self) -> Self::Fruit {
        self.0.harvest()
    }
}

pub(crate) struct TopSegmentCollector<T> {
    /// We reverse the order of the feature in order to
    /// have top-semantics instead of bottom semantics.
    topn_computer: TopNComputer<T, DocId>,
    segment_ord: u32,
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    fn new(segment_ord: SegmentOrdinal, limit: usize) -> TopSegmentCollector<T> {
        TopSegmentCollector {
            topn_computer: TopNComputer::new(limit),
            segment_ord,
        }
    }
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(T, DocAddress)> {
        let segment_ord = self.segment_ord;
        self.topn_computer
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: comparable_doc.doc,
                    },
                )
            })
            .collect()
    }

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    #[inline]
    pub fn collect(&mut self, doc: DocId, feature: T) {
        self.topn_computer.push(feature, doc);
    }
}

/// Copied from tantivy source so we can mirror the topk collector.
pub(crate) struct TopNComputer<Score, D, const REVERSE_ORDER: bool = true> {
    /// The buffer reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<ComparableDoc<Score, D, REVERSE_ORDER>>,
    top_n: usize,
    pub(crate) threshold: Option<Score>,
}

impl<Score: fmt::Debug, D, const REVERSE_ORDER: bool> fmt::Debug
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopNComputer")
            .field("buffer_len", &self.buffer.len())
            .field("top_n", &self.top_n)
            .field("current_threshold", &self.threshold)
            .finish()
    }
}

// Custom clone to keep capacity
impl<Score: Clone, D: Clone, const REVERSE_ORDER: bool> Clone
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn clone(&self) -> Self {
        let mut buffer_clone = Vec::with_capacity(self.buffer.capacity());
        buffer_clone.extend(self.buffer.iter().cloned());

        TopNComputer {
            buffer: buffer_clone,
            top_n: self.top_n,
            threshold: self.threshold.clone(),
        }
    }
}

impl<Score, D, const R: bool> TopNComputer<Score, D, R>
where
    Score: PartialOrd + Clone,
    D: Ord,
{
    /// Create a new `TopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new(top_n: usize) -> Self {
        let vec_cap = top_n.max(1) * 2;
        TopNComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: None,
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    #[inline]
    pub fn push(&mut self, feature: Score, doc: D) {
        if let Some(last_median) = self.threshold.clone() {
            if feature < last_median {
                return;
            }
        }
        if self.buffer.len() == self.buffer.capacity() {
            let median = self.truncate_top_n();
            self.threshold = Some(median);
        }

        // This is faster since it avoids the buffer resizing to be inlined from vec.push()
        // (this is in the hot path)
        // TODO: Replace with `push_within_capacity` when it's stabilized
        let uninit = self.buffer.spare_capacity_mut();
        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is 2.
        uninit[0].write(ComparableDoc { doc, feature });
        // This is safe because it would panic in the line above
        unsafe {
            self.buffer.set_len(self.buffer.len() + 1);
        }
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) -> Score {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable(self.top_n);

        let median_score = median_el.feature.clone();
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        median_score
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<ComparableDoc<Score, D, R>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.sort_unstable();
        self.buffer
    }
}


#[cfg(test)]
mod tests {
    use tantivy::{doc, Index, Term};
    use tantivy::collector::{Collector, SegmentCollector};
    use tantivy::indexer::IndexWriterOptions;
    use tantivy::query::{AllQuery, QueryParser};
    use tantivy::schema::{Schema, Value, FAST, INDEXED, STORED, TEXT};
    use crate::collectors::top_docs::TopDocs;

    fn create_test_index() -> Index {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
        let title = schema_builder.add_text_field("title", TEXT | STORED | FAST);
        let description = schema_builder.add_text_field("description", STORED | FAST);
        let schema = schema_builder.build();
        
        let index = Index::create_in_ram(schema);
        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(0)
            .build();
        let mut writer = index.writer_with_options(options).unwrap();
        writer.add_document(doc!(
            id => 1u64,
            title => "The old man and the sea",
            description => "example text here today",
        )).unwrap();
        writer.add_document(doc!(
            id => 2u64,
            title => "Iron man 4",
            description => "example text here today",
        )).unwrap();
        writer.add_document(doc!(
            id => 3u64,
            title => "X men",
            description => "Something something rivals",
        )).unwrap();
        writer.commit().unwrap();        
        index        
    }
    
    #[test]
    fn test_top_k_without_offset() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        
        let collector = TopDocs::with_limit(10);
        let all_docs = searcher.search(&AllQuery, &collector).expect("Complete search");        
        assert_eq!(all_docs.len(), 3);
    }

    #[test]
    fn test_top_k_zero_limit() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let collector = TopDocs::with_limit(0);
        let all_docs = searcher.search(&AllQuery, &collector).expect("Complete search");
        assert_eq!(all_docs.len(), 0);
    }
    
    #[test]
    fn test_top_k_with_offset() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let collector = TopDocs::with_limit(10)
            .and_offset(2);
        let all_docs = searcher.search(&AllQuery, &collector).expect("Complete search");
        assert_eq!(all_docs.len(), 1);
    }

    #[test]
    fn test_top_k_for_segment() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segments = searcher.segment_readers();
        
        
        let collector = TopDocs::with_limit(10)
            .and_offset(2);
        let mut collector = collector
            .for_segment(0, &segments[0])
            .expect("Collect for segment");
        collector.collect(1, 1.0);
        let results = collector.harvest();
        assert_eq!(results.len(), 1);
    }
    
    #[test]
    fn test_top_k_sorting_correct() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let schema = searcher.schema();
        let title_field = schema.get_field("title").unwrap();
        
        let parser = QueryParser::for_index(&index, vec![title_field]);
        let query = parser.parse_query("old man").unwrap();
        let docs = searcher.search(&query, &TopDocs::with_limit(10)).expect("Complete search");
        let expected_docs = searcher.search(&query, &tantivy::collector::TopDocs::with_limit(10)).expect("Complete search");
        
        let mut docs_titles = Vec::new();
        for (_score, doc) in docs {
            let doc = searcher.doc::<tantivy::TantivyDocument>(doc).unwrap();
            let title = doc.get_first(title_field).unwrap();
            docs_titles.push(title.as_str().unwrap().to_string());
        }

        let mut expected_docs_titles = Vec::new();
        for (score, doc) in expected_docs {
            let doc = searcher.doc::<tantivy::TantivyDocument>(doc).unwrap();
            let title = doc.get_first(title_field).unwrap();
            expected_docs_titles.push(title.as_str().unwrap().to_string());
        }
        
        assert_eq!(docs_titles, expected_docs_titles);
    }
    
    #[test]
    fn test_top_k_with_deletes() {
        let index = create_test_index();
        let reader = index.reader().unwrap();
        let schema = index.schema();
        let id_field = schema.get_field("id").unwrap();
        let title_field = schema.get_field("title").unwrap();
        
        let options = IndexWriterOptions::builder()
            .num_worker_threads(1)
            .num_merge_threads(1)
            .build();
        let mut writer: tantivy::IndexWriter = index.writer_with_options(options).unwrap();
        writer.delete_term(Term::from_field_u64(id_field, 2));
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        reader.reload().unwrap();
        
        let searcher = reader.searcher();
        let collector = tantivy::collector::TopDocs::with_limit(10);
        let all_docs = searcher.search(&AllQuery, &collector).expect("Complete search");
        assert_eq!(all_docs.len(), 2);
        
        let collector = TopDocs::with_limit(10);
        let all_docs = searcher.search(&AllQuery, &collector).expect("Complete search");
        assert_eq!(all_docs.len(), 2);
    }
}