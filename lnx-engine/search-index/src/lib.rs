#[macro_use]
extern crate tracing;

use std::sync::Arc;

use aexecutor::SearcherExecutorPool;

mod corrections;
mod helpers;
mod index;
mod query;
mod reader;
mod stop_words;
mod storage;
pub mod structures;
mod writer;
mod schema;

pub use helpers::cr32_hash;
pub use index::Index;
pub use query::DocumentId;
pub use reader::{QueryPayload, QueryResults};
pub use storage::StorageBackend;

pub(crate) type ReaderExecutor = Arc<SearcherExecutorPool>;
