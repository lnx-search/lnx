#[macro_use]
extern crate tracing;

mod error;
mod handler;
mod helpers;
mod indexer;
mod poller;
mod doc_handler;

pub use handler::{get, start, Index, IndexerHandlerConfig};
pub use poller::start_polling_for_index;
pub use doc_handler::add_documents;
