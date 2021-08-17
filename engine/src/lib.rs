#![deny(unused)]

#[macro_use]
extern crate log;

mod engine;
mod index;
mod storage;
pub mod structures;

pub use engine::{LeasedIndex, SearchEngine};
pub use tantivy;
