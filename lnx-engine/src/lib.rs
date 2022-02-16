#[macro_use]
extern crate tracing;

mod engine;
mod error;
mod index;

pub use engine::init_engine;
pub use error::InitEngineError;
pub use index::{add_index, remove_index};
