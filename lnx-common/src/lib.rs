#[macro_use]
extern crate tracing;

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

pub mod fast_fuzzy;
pub mod schema;
pub mod stop_words;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[derive(Debug, thiserror::Error)]
#[error("The provided data is corrupted or unable to be deserialized.")]
pub struct CorruptedData;

#[derive(Debug, Clone)]
pub struct IndexContext(Arc<IndexContextInner>);

impl From<IndexContextInner> for IndexContext {
    fn from(ctx: IndexContextInner) -> Self {
        Self(Arc::new(ctx))
    }
}

impl Deref for IndexContext {
    type Target = IndexContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct IndexContextInner {
    pub name: String,
    pub schema: tantivy::schema::Schema,
}

impl IndexContextInner {
    #[inline]
    pub fn tantivy_schema(&self) -> tantivy::schema::Schema {
        self.schema.clone()
    }
}

#[derive(Debug, Clone)]
pub struct AppContext(Arc<AppContextInner>);

impl From<AppContextInner> for AppContext {
    fn from(ctx: AppContextInner) -> Self {
        Self(Arc::new(ctx))
    }
}

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
