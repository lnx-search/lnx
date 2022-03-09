use std::ops::Deref;
use std::path::{Path, PathBuf};
use lnx_storage::templates::engine_store::EngineStore;
use crate::backends::BackendSelector;

pub struct Engine {
    store: Box<dyn EngineStore>,
    config: BackendSelector,
    base_path: PathBuf,
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

    #[inline]
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}