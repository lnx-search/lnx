#[macro_use]
extern crate tracing;

pub mod error;
pub mod helpers;
mod indexer;
pub mod poller;
mod handler;

pub use handler::{add_documents, remove_documents, update_documents};
