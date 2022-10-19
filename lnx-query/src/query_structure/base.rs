use std::fmt::Display;

use tantivy::{schema::{Field, FieldEntry}, Term, query::Query};


#[derive(Debug, thiserror::Error)]
#[error("Invalid value for term: {0}")]
pub struct InvalidTermValue(pub String);

impl InvalidTermValue {
    pub fn from(v: impl Display) -> Self {
        Self(v.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unable to build query: {0}")]
pub struct BuildQueryError(pub String);

pub trait AsQueryTerm {
    fn as_term(&self, field: Field, entry: &FieldEntry) -> Result<Term, InvalidTermValue>;
}

pub trait AsQuery {
    fn as_query(&self, field: Field, entry: &FieldEntry) -> Result<Box<dyn Query>, InvalidTermValue>;
}
