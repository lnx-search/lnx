use std::ops::Deref;
use std::sync::Arc;
use crate::index::context::IndexContext;


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

