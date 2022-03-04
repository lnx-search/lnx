use std::ops::Deref;
use std::sync::Arc;

use tantivy::query::{Explanation, Query};
use tantivy::schema::Field;

use crate::index::base::Index;
use crate::index::handlers::searchable::{Filter, FilterOp, Searchable, SearchError};
use crate::types::document::DocId;

pub type MicroIndexId = u64;

#[derive(Clone)]
pub struct MicroIndex(Arc<InnerMicroIndex>);

impl Deref for MicroIndex {
    type Target = InnerMicroIndex;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub struct InnerMicroIndex {
    id: MicroIndexId,
    id_field: Field,
    parent: Index,
}

impl Searchable for InnerMicroIndex {
    fn execute(
        &self,
        queries: Vec<Box<dyn Query>>,
        limit: usize,
        offset: usize,
        filters: Vec<Filter>,
    ) -> Result<Vec<DocId>, SearchError> {
        let mut micro_index_filter = Filter::new(self.id_field, FilterOp::Eq(self.id));
        micro_index_filter.and(filters);

        let filters = vec![micro_index_filter];

        self.parent.execute(queries, limit, offset, filters)
    }

    fn explain(
        &self,
        doc_id: DocId,
        query: Box<dyn Query>,
    ) -> Result<Vec<Explanation>, SearchError> {
        self.parent.explain(doc_id, query)
    }
}
