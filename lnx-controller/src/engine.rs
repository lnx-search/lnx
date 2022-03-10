use std::ops::Deref;
use std::path::{Path, PathBuf};
use once_cell::sync::OnceCell;
use lnx_storage::templates::engine_store::EngineStore;
use crate::backends::BackendSelector;

static ENGINE: OnceCell<Engine> = OnceCell::new();

pub fn init() {
    ENGINE.get_or_init(|| todo!());
}

pub fn get() -> &'static Engine {
    ENGINE.get().expect("Engine initialised")
}

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