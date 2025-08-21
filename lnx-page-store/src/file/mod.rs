use std::any::Any;
use std::sync::Arc;

mod arena;
mod buffer;
mod ctx;
mod page_data;
mod page_op_log;
mod page_table;
mod scheduler;
mod stream_reader;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod utils;

pub const ALLOC_PAGE_SIZE: usize = 4096;
pub const DISK_ALIGN: usize = 4096;
pub type DynamicGuard = Arc<dyn Any + Send + Sync>;
