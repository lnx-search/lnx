use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IndexContext(Arc<IndexContextInner>);

impl Deref for IndexContext {
    type Target = IndexContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct IndexContextInner {
    pub name: String,
    schema: tantivy::schema::Schema,
}

impl IndexContextInner {
    #[inline]
    pub fn tantivy_schema(&self) -> tantivy::schema::Schema {
        self.schema.clone()
    }
}

#[derive(Debug, Clone)]
pub struct AppContext(Arc<AppContextInner>);

impl Deref for AppContext {
    type Target = AppContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct AppContextInner {
    pub tmp_path: PathBuf,
}
