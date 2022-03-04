use std::borrow::Borrow;

use async_channel::{Receiver, Sender};
use tantivy::Executor;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Failed to create pool due to error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("All executors have been taken at the time of acquiring, this is a bug.")]
    ExhaustedPool,
}

#[derive(Clone)]
pub struct TantivyExecutorPool {
    executors: Receiver<Executor>,
    executors_returner: Sender<Executor>,
}

impl TantivyExecutorPool {
    pub fn create(
        pool_size: usize,
        threads_per_reader: usize,
    ) -> Result<Self, ExecutorError> {
        let (tx, rx) = async_channel::bounded(pool_size);
        for _ in 0..pool_size {
            let executor = if threads_per_reader <= 1 {
                tantivy::Executor::single_thread()
            } else {
                tantivy::Executor::multi_thread(threads_per_reader, "reader-executor-")?
            };

            tx.try_send(executor).expect("fill pool");
        }

        Ok(Self {
            executors: rx,
            executors_returner: tx,
        })
    }

    pub async fn get(&self) -> Result<ExecutorHandle, ExecutorError> {
        if let Ok(exec) = self.executors.recv().await {
            Ok(ExecutorHandle::new(exec, self.executors_returner.clone()))
        } else {
            Err(ExecutorError::ExhaustedPool)
        }
    }
}

/// A wrapper around a tantivy executor.
///
/// This automatically returns the executor back to the pool
/// once dropped.
pub struct ExecutorHandle {
    inner: Option<Executor>,
    returner: Sender<Executor>,
}

impl ExecutorHandle {
    /// Creates a new handle from a given executor ah
    fn new(executor: Executor, queue: Sender<Executor>) -> Self {
        Self {
            inner: Some(executor),
            returner: queue,
        }
    }
}

impl Borrow<Executor> for ExecutorHandle {
    fn borrow(&self) -> &Executor {
        self.inner.as_ref().expect("get executor")
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if self.returner.try_send(inner).is_err() {
                panic!("failed to return executor to pool")
            }
        }
    }
}
