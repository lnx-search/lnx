#[macro_use]
extern crate tracing;

mod pools;

pub use pools::{
    build_default_pools,
    execute_search,
    get_search_usage,
    reset_search_usage,
    schedule_search,
    spawn_blocking_task,
};
pub use tokio::task::JoinHandle;

/// Spawns a blocking task in a new thread.
///
/// Unlike the executor pools, this assumes the task
/// is going to be long running and potentially in high contention.
pub fn start_blocking_thread<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f)
}
