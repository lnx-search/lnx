#[macro_use]
extern crate log;

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

pub use index::Index;
pub use query::QueryPayload;

pub(crate) type ReaderExecutor = Arc<SearcherExecutorPool>;
