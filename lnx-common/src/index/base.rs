use std::ops::Deref;
use std::sync::Arc;
use tantivy::query::{Explanation, Query};

use crate::index::context::IndexContext;
use crate::index::handlers::searchable::{Filter, Searchable, SearchError};
use crate::types::document::DocId;


#[derive(Clone)]
pub struct Index(Arc<InnerIndex>);

impl Deref for Index {
    type Target = InnerIndex;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub struct InnerIndex {
    ctx: IndexContext,
    inner: tantivy::Index,
}

impl Searchable for InnerIndex {
    fn execute(
        &self,
        queries: Vec<Box<dyn Query>>,
        limit: usize,
        offset: usize,
        filters: Filter,
    ) -> Result<Vec<DocId>, SearchError> {
        todo!()
    }

    fn explain(
        &self,
        doc_id: DocId,
        query: Box<dyn Query>,
    ) -> Result<Vec<Explanation>, SearchError> {
        todo!()
    }
}