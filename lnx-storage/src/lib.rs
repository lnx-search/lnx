mod impls;
mod base;
mod error;

pub use error::ChangelogStoreError;
pub use base::{
    ChangeKind,
    DocId,
    DocStore,
    Synonyms,
    ChangeLogStore,
    MetaStore,
    EngineStore,
};