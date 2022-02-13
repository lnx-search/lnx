#[macro_use]
extern crate tracing;

mod impls;
mod meta_store;
mod change_log;
mod doc_store;
mod configure;
mod manager;
mod engine_store;

pub use manager::{IndexStore, StorageManager, init_with_config, engine};
pub use configure::BackendSelector;
pub use change_log::{    
    ChangeKind,
    DocId,
    ChangeLogStore,
};
pub use doc_store::DocStore;
pub use meta_store::{
    Synonyms,
    MetaStore,
};
pub use engine_store::EngineStore;
