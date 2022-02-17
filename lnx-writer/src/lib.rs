#[macro_use]
extern crate tracing;

mod doc_handler;
mod error;
mod handler;
mod helpers;
mod indexer;
mod poller;

pub use doc_handler::add_documents;
pub use handler::{get, start, Index, IndexerHandlerConfig};
pub use poller::start_polling_for_index;
