#[macro_use]
extern crate tracing;

mod change_log;
mod configure;
mod doc_store;
mod engine_store;
mod impls;
mod manager;
mod meta_store;

pub use change_log::{ChangeKind, ChangeLogStore, DocId};
pub use configure::BackendSelector;
pub use doc_store::DocStore;
pub use engine_store::EngineStore;
pub use manager::{engine, init_with_config, IndexStore, StorageManager};
pub use meta_store::{MetaStore, Synonyms};
