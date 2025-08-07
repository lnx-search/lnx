use std::any::Any;
use std::sync::Arc;

mod arena;
mod buffer;
mod ctx;
mod page_data;
mod page_metadata;
mod page_op_log;
mod scheduler;
mod utils;
mod error;

pub const ALLOC_PAGE_SIZE: usize = 4096;
pub const DISK_ALIGN: usize = 4096;
pub type DynamicGuard = Arc<dyn Any + Send + Sync>;
