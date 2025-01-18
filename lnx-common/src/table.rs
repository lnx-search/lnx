use std::ops::Deref;
use std::sync::Arc;

use lnx_tantivy::tantivy;

#[derive(Debug, Clone)]
/// A single table backed by a tantivy index.
///
/// This is a cheap to clone reference of the inner table object.
pub struct LnxTable(Arc<LnxTableInner>);

impl Deref for LnxTable {
    type Target = LnxTableInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
/// A single table backed by a tantivy index.
pub struct LnxTableInner {
    /// The tantivy index schema.
    tantivy_schema: tantivy::schema::Schema,
}

impl LnxTableInner {
    /// Returns a reference to the [tantivy::schema::Schema] used by the table.
    pub fn tantivy_schema(&self) -> &tantivy::schema::Schema {
        &self.tantivy_schema
    }
}
