use std::ops::Deref;
use lnx_storage::templates::engine_store::EngineStore;
use crate::backends::BackendSelector;

pub struct Engine {
    store: Box<dyn EngineStore>,
    config: BackendSelector,
}

impl Deref for Engine {
    type Target = Box<dyn EngineStore>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl Engine {
    #[inline]
    pub fn config(&self) -> &BackendSelector {
        &self.config
    }
}