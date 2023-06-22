use std::any::Any;
use std::cmp;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use arc_swap::ArcSwap;
use futures::channel::oneshot;
use futures::FutureExt;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

/// The executor pool used for searches only.
///
/// This allows us to isolate load and prevent tail latencies
/// from becoming too high due to interference.
static SEARCH_POOL: OnceLock<ArcSwap<ExecutorPool>> = OnceLock::new();
/// The executor pool for any operations that are not searches.
///
/// This typically includes compression, IO, etc...
static DEFAULT_POOL: OnceLock<ArcSwap<Runtime>> = OnceLock::new();

/// The default ratio of other operations to CPU searches.
///
/// This starts off at 25% other / 75% search to prevent
/// search operations being impacted directly by things
/// like indexing.
const START_SEARCH_PCT: f32 = 75.0;

/// Creates the executor pools in their default configuration.
pub fn build_default_pools(num_threads: usize) -> anyhow::Result<()> {
    install_pools(num_threads, START_SEARCH_PCT)
}

/// Get the current search pool usage pct.
pub fn get_search_usage() -> f32 {
    SEARCH_POOL
        .get()
        .map(|v: &ArcSwap<ExecutorPool>| {
            let guard = v.load();
            guard.get_threadpool_usage_pct()
        })
        .unwrap_or_default()
}

/// Reset the search pool usage metrics.
pub fn reset_search_usage() {
    if let Some(pool) = SEARCH_POOL.get() {
        pool.load().reset_usage_metrics();
    }
}

/// Execute an operation on the search pool.
pub fn execute_search<OP, T>(op: OP) -> impl Future<Output = Option<T>>
where
    T: Send + 'static,
    OP: FnOnce() -> T + Send + 'static,
{
    SEARCH_POOL
        .get()
        .expect("Search executor pool should be initialised.")
        .load()
        .execute(op)
}

/// Execute an operation on the default pool.
pub fn spawn_blocking_task<OP, T>(op: OP) -> JoinHandle<T>
where
    T: Send + 'static,
    OP: Future<Output = T> + Send + 'static,
{
    DEFAULT_POOL
        .get()
        .expect("Default executor pool should be initialised.")
        .load()
        .spawn(op)
}

/// Spawn an operation on the search pool.
pub fn schedule_search<OP>(op: OP)
where
    OP: FnOnce() + Send + 'static,
{
    SEARCH_POOL
        .get()
        .expect("Search executor pool should be initialised.")
        .load()
        .schedule(op)
}

/// Creates the executor pools with a given search executor pct.
pub(crate) fn install_pools(num_threads: usize, search_pct: f32) -> anyhow::Result<()> {
    if search_pct <= 10.0 {
        bail!("Search executor percentage share must be above 10%");
    }

    let search_threads = get_threads(num_threads, search_pct / 100.0);
    let default_threads = get_threads(num_threads, (100.0 - search_pct) / 100.0);

    info!(
        total_thread_allocation = num_threads,
        search_threads = search_threads,
        default_threads = default_threads,
        "Creating threadpool."
    );

    let search_pool = spawn_thread_pool("search", search_threads)?;
    let default_pool = spawn_runtime_pool("default", default_threads)?;

    if let Some(existing) = SEARCH_POOL.get() {
        existing.store(Arc::new(search_pool));
    } else {
        let _ = SEARCH_POOL.set(ArcSwap::from_pointee(search_pool));
    }

    if let Some(existing) = DEFAULT_POOL.get() {
        existing.store(Arc::new(default_pool));
    } else {
        let _ = DEFAULT_POOL.set(ArcSwap::from_pointee(default_pool));
    }

    Ok(())
}

/// A wrapper around a rayon threadpool that ensures
/// all operations are completed before shutting down the pool.
///
/// This is done via attaching a reference to the pool for each task
/// it owns, meaning it's self-referential as long as tasks are still
/// pending.
pub struct ExecutorPool {
    pool: Arc<rayon::ThreadPool>,
    start_instant: parking_lot::Mutex<Instant>,
    execution_time: Arc<AtomicU64>,
}

impl From<rayon::ThreadPool> for ExecutorPool {
    fn from(value: rayon::ThreadPool) -> Self {
        Self {
            pool: Arc::new(value),
            start_instant: parking_lot::Mutex::new(Instant::now()),
            execution_time: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ExecutorPool {
    /// Get the current average usage pct.
    pub fn get_threadpool_usage_pct(&self) -> f32 {
        let total_duration = self.start_instant.lock().elapsed();

        // The total is derived from the execution time per thread.
        let total_micros = self.execution_time.load(Ordering::Relaxed)
            / self.pool.current_num_threads() as u64;

        let total_execution_time = Duration::from_micros(total_micros);

        if total_execution_time.as_secs_f32() <= 0.0
            || total_duration.as_secs_f32() <= 0.0
        {
            return 0.0;
        }

        (total_execution_time.as_secs_f32() / total_duration.as_secs_f32()) * 100.0
    }

    /// Reset the current executor usage metrics.
    pub fn reset_usage_metrics(&self) {
        (*self.start_instant.lock()) = Instant::now();
        self.execution_time.store(0, Ordering::Relaxed);
    }

    /// Executes a blocking task in the CPU executor and returns the result.
    pub fn execute<OP, T>(&self, op: OP) -> impl Future<Output = Option<T>>
    where
        T: Send + 'static,
        OP: FnOnce() -> T + Send + 'static,
    {
        trace!("Executing operation");

        let guard = self.pool.clone();
        let execution_time = self.execution_time.clone();

        let (tx, rx) = oneshot::channel();
        let callable = move || {
            let start = Instant::now();

            let result = (op)();
            if tx.send(result).is_err() {
                warn!("Executor completed task but receiver was already dropped, has the submitter crashed?");
            }

            let elapsed = start.elapsed();
            trace!(elapsed = ?elapsed, "Execute operation completed.");
            execution_time.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

            drop(guard);
        };
        self.pool.spawn(callable);

        rx.map(|v| v.ok())
    }

    /// Schedule a task to be executed some time in the near future.
    pub fn schedule<OP>(&self, op: OP)
    where
        OP: FnOnce() + Send + 'static,
    {
        trace!("Scheduling operation");

        let guard = self.pool.clone();
        let execution_time = self.execution_time.clone();

        let wrapper = move || {
            let start = Instant::now();

            (op)();

            let elapsed = start.elapsed();
            trace!(elapsed = ?elapsed, "Scheduled operation completed.");
            execution_time.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

            drop(guard);
        };
        self.pool.spawn(wrapper)
    }
}

fn spawn_thread_pool(
    nickname: &'static str,
    num_threads: usize,
) -> anyhow::Result<ExecutorPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(move |n| format!("lnx-executor-{nickname}-{n}"))
        .panic_handler(move |error: Box<dyn Any + Send>| {
            if let Some(error) = error.downcast_ref::<&str>() {
                error!(pool_name = nickname, error = %error, "Executor thread panicked while completing task.");
                return
            }

            if let Some(error) = error.downcast_ref::<String>() {
                error!(pool_name = nickname, error = %error, "Executor thread panicked while completing task.");
                return
            }

            error!(pool_name = nickname, error = "Unknown", "Executor thread panicked while completing task.");
        })
        .start_handler(move |thread_id| {
            debug!(pool_name = nickname, thread_id = thread_id, "Spawning worker thread.");
        })
        .build()
        .context("Create executor pool")
        .map(ExecutorPool::from)
}

fn spawn_runtime_pool(
    nickname: &'static str,
    num_threads: usize,
) -> anyhow::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(format!("lnx-executor-{nickname}"))
        .worker_threads(num_threads)
        .enable_all()
        .build()
        .context("Build runtime executor")
}

fn get_threads(num_total: usize, ratio: f32) -> usize {
    let num_threads = num_total as f32;
    let n_threads = (num_threads * ratio) as usize;
    cmp::max(n_threads, 1)
}
