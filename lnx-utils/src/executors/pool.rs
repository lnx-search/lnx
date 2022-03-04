use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};

use crate::executors::search::ExecutorError;

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("{0}")]
    ExecutorError(#[from] ExecutorError),

    #[error("Failed to receive result of the given task.")]
    JoinError,
}

#[derive(Debug, thiserror::Error)]
pub enum CreatePoolError {
    #[error("{0}")]
    ExecutorError(#[from] ExecutorError),

    #[error("Failed to create pool due to error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Failed to create pool due to error: {0}")]
    RayonError(#[from] rayon::ThreadPoolBuildError),
}

/// A thread pool that waits for a given task to complete
/// before resolving the future.
///
/// This is mostly used to run CPU heavy tasks without blocking the
/// scheduler. Basically tokio's spawn_blocking but with a set pool.
#[derive(Clone)]
pub struct ExecutorPool {
    max_concurrency: usize,
    limiter: Arc<Semaphore>,
    thread_pool: Arc<rayon::ThreadPool>,
}

impl ExecutorPool {
    #[inline]
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
    
    /// Creates a new thread pool with a set concurrency.
    ///
    /// The set concurrency determines the number of threads spawned.
    pub fn create(max_concurrency: usize) -> Result<Self, CreatePoolError> {
        let limiter = Semaphore::new(max_concurrency);
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|n| format!("executor-pool-worker-{}", n))
            .num_threads(max_concurrency)
            .build()?;

        Ok(Self {
            max_concurrency,
            limiter: Arc::new(limiter),
            thread_pool: Arc::new(thread_pool),
        })
    }
    
    

    /// Spawns a new function into the pool and returns
    /// the results once complete.
    pub async fn spawn<F, T>(&self, func: F) -> Result<T, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Sync + Send + 'static,
    {
        let _permit = self
            .limiter
            .acquire()
            .await
            .map_err(|_| SpawnError::ExecutorError(ExecutorError::ExhaustedPool));

        let (tx, rx) = oneshot::channel();
        self.thread_pool.spawn(move || {
            let result = func();
            let _ = tx.send(result);
        });

        rx.await.map_err(|_| SpawnError::JoinError)
    }
}

