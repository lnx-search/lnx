mod reader_executor;

use std::borrow::Borrow;

use anyhow::Result;
use tantivy::{LeasedItem, Searcher};
use tokio::sync::{oneshot, Semaphore};

use crate::reader_executor::TantivyExecutorPool;

/// A thread pool that waits for a given task to complete
/// before resolving the future.
///
/// This is mostly used to run CPU heavy tasks without blocking the
/// scheduler. Basically tokio's spawn_blocking but with a set pool.
pub struct ExecutorPool {
    limiter: Semaphore,
    thread_pool: rayon::ThreadPool,
}

impl ExecutorPool {
    /// Creates a new thread pool with a set concurrency.
    ///
    /// The set concurrency determines the number of threads spawned.
    pub async fn create(max_concurrency: usize) -> Result<Self> {
        let limiter = Semaphore::new(max_concurrency);
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|n| format!("executor-pool-worker-{}", n))
            .num_threads(max_concurrency)
            .build()?;

        Ok(Self {
            limiter,
            thread_pool,
        })
    }

    /// Spawns a new function into the pool and returns
    /// the results once complete.
    pub async fn spawn<F, T>(&self, func: F) -> Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Sync + Send + 'static,
    {
        let _permit = self.limiter.acquire().await?;
        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            let result = func();
            let _ = tx.send(result);
        });

        Ok(rx.await?)
    }
}

/// A thread pool that waits for a given task after passing
/// the given reader searcher as an arg to complete before
/// resolving the future.
///
/// This is mostly used to run CPU heavy tasks without blocking the
/// scheduler. Basically tokio's spawn_blocking but with a set pool.
pub struct SearcherExecutorPool {
    reader: tantivy::IndexReader,
    reader_executors: reader_executor::TantivyExecutorPool,
    limiter: Semaphore,
    thread_pool: rayon::ThreadPool,
}

impl SearcherExecutorPool {
    /// Creates a new thread pool with a set concurrency.
    ///
    /// The set concurrency determines the number of threads spawned.
    pub async fn create(
        reader: tantivy::IndexReader,
        threads_per_reader: usize,
        max_concurrency: usize,
    ) -> Result<Self> {
        let limiter = Semaphore::new(max_concurrency);
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|n| format!("executor-pool-worker-{}", n))
            .num_threads(max_concurrency)
            .build()?;

        let reader_executors =
            TantivyExecutorPool::create(max_concurrency, threads_per_reader).await?;

        Ok(Self {
            reader,
            reader_executors,
            limiter,
            thread_pool,
        })
    }

    /// Spawns a new function into the pool and returns
    /// the results once complete.
    pub async fn spawn<F, T>(&self, func: F) -> Result<T>
    where
        F: FnOnce(LeasedItem<Searcher>, &tantivy::Executor) -> T + Send + 'static,
        T: Sync + Send + 'static,
    {
        let _permit = self.limiter.acquire().await?;
        let executor = self.reader_executors.get().await?;
        let searcher = self.reader.searcher();
        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            let result = func(searcher, executor.borrow());
            let _ = tx.send(result);
        });

        Ok(rx.await?)
    }

    #[inline]
    pub fn searcher(&self) -> LeasedItem<Searcher> {
        self.reader.searcher()
    }
}
