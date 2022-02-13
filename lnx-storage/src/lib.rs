#[macro_use]
extern crate tracing;

mod impls;
mod base_stores;
mod change_log;
mod doc_store;

pub use change_log::{    
    ChangeKind,
    DocId,
    ChangeLogStore,
};
pub use doc_store::DocStore;
pub use base_stores::{
    Synonyms,
    MetaStore,
    EngineStore,
};