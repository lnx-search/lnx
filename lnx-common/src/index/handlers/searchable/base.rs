use tantivy::query::{Explanation, Query};

use crate::types::document::DocId;
use super::filter::Filter;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("Tantivy Error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Search Error: {0}")]
    Other(#[from] anyhow::Error),
}


pub trait Searchable {
    /// Executes a given query returning the set of doc ids.
    ///
    /// The order of query execution is important, a given query must
    /// take presidency over the following queries.
    fn execute(
        &self,
        queries: Vec<Box<dyn Query>>,
        limit: usize,
        offset: usize,
        filters: Filter,
    ) -> Result<Vec<DocId>, SearchError>;

    /// Produce an explanation for the given doc with a given query.
    fn explain(
        &self,
        doc_id: DocId,
        query: Box<dyn Query>,
    ) -> Result<Vec<Explanation>, SearchError>;
}
