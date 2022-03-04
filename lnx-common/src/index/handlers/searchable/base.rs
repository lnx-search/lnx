use tantivy::DocAddress;
use tantivy::query::Query;

use super::filter::Filter;

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("Tantivy Error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Search Error: {0}")]
    Other(#[from] anyhow::Error)
}


pub trait Searchable {
    fn execute(
        &self,
        query: Box<dyn Query>,
        limit: usize,
        offset: usize,
        filters: Vec<Filter>,
    ) -> Result<Vec<DocAddress>, SearchError>;
}
