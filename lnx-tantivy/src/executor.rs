use std::sync::Arc;

use tokio::sync::{oneshot, Semaphore};
use tracing::error;

#[derive(Debug, thiserror::Error)]
#[error("The task executed panicked unexpectedly")]
/// The task being executed panicked.
pub struct TaskPanicked;

#[derive(Debug, thiserror::Error)]
#[error("Failed to create pool: {0}")]
/// The pool was unable to be created due to an error.
pub struct CreatePoolError(pub String);

#[derive(Debug, Clone)]
/// A thread pool for executing computationally expensive tasks without blocking
/// the tokio runtime.
pub struct ExecutorPool {
    limiter: Arc<Semaphore>,
    pool: Arc<rayon::ThreadPool>,
}

impl ExecutorPool {
    /// Create a new [ExecutorPool] with the given name and pool size.
    pub fn create(
        name: impl Into<String>,
        pool_size: usize,
    ) -> Result<Self, CreatePoolError> {
        let name = name.into();
        let name_clone = name.clone();
        let limiter = Arc::new(Semaphore::new(pool_size * 4));

        if pool_size == 0 {
            return Err(CreatePoolError("Pool size must be at least 1".to_string()));
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(pool_size)
            .thread_name(move |idx| format!("lnx-executor-{name_clone}-{idx}"))
            .panic_handler(move |stack| {
                let msg = if let Some(msg) = stack.downcast_ref::<String>() {
                    msg.to_string()
                } else if let Some(msg) = stack.downcast_ref::<&'static str>() {
                    msg.to_string()
                } else {
                    "unknown".to_string()
                };

                error!(pool = %name, error = %msg, "Executor pool panicked");
            })
            .build()
            .map_err(|e| CreatePoolError(e.to_string()))?;

        Ok(Self {
            limiter,
            pool: Arc::new(pool),
        })
    }

    /// Runs a task in the executor pool and waits for the result.
    ///
    /// This may have to wait for capacity to be available in the pool.
    pub async fn run<CB, T>(&self, cb: CB) -> Result<T, TaskPanicked>
    where
        T: Send + 'static,
        CB: Send + 'static + FnOnce() -> T,
    {
        let _permit = self.limiter.acquire().await;

        let (tx, rx) = oneshot::channel();
        let wrapped = move || {
            let result = cb();
            let _ = tx.send(result);
        };

        self.pool.spawn(wrapped);

        rx.await.map_err(|_| TaskPanicked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_use_pool() {
        let pool = ExecutorPool::create("test_1", 1).expect("Create pool Ok");
        let result = pool
            .run(move || "hello")
            .await
            .expect("Tasks should not be cancelled.");
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn test_create_empty_pool_err() {
        let _err =
            ExecutorPool::create("test_1", 0).expect_err("Create pool should error");
    }

    #[tokio::test]
    async fn test_pool_panic() {
        let pool = ExecutorPool::create("test_1", 1).expect("Create pool Ok");
        pool.run(move || panic!("demo!"))
            .await
            .expect_err("Task panic should result in error");
    }
}
