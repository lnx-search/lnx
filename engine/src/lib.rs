#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;

mod engine;
mod index;
mod storage;
mod correction;
pub mod structures;

pub use engine::{LeasedIndex, SearchEngine};
pub use tantivy;
