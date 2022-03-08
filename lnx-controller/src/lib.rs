#[macro_use]
extern crate tracing;

mod backends;
mod indexes;
mod engine;

pub use indexes::{get, remove};
