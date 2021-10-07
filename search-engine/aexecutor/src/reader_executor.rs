use std::borrow::Borrow;
use std::sync::Arc;

use anyhow::{Error, Result};
use crossbeam::queue::ArrayQueue;
use tantivy::Executor;

type ExecutorQueue = Arc<ArrayQueue<Executor>>;

pub(crate) struct TantivyExecutorPool {
    executors: ExecutorQueue,
}

impl TantivyExecutorPool {
    pub(crate) fn create(pool_size: usize, threads_per_reader: usize) -> Result<Self> {
        let reader_executors = ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let executor = if threads_per_reader <= 1 {
                tantivy::Executor::single_thread()
            } else {
                tantivy::Executor::multi_thread(threads_per_reader, "reader-executor-")?
            };

            let _ = reader_executors.push(executor);
        }

        Ok(Self {
            executors: Arc::new(reader_executors),
        })
    }

    pub(crate) fn get(&self) -> Result<ExecutorHandle> {
        if let Some(exec) = self.executors.pop() {
            Ok(ExecutorHandle::new(exec, self.executors.clone()))
        } else {
            Err(Error::msg("executor pool exhausted")
                .context("all executors have been taken at the time of acquiring"))
        }
    }
}

/// A wrapper around a tantivy executor.
///
/// This automatically returns the executor back to the pool
/// once dropped.
pub struct ExecutorHandle {
    inner: Option<Executor>,
    returner: ExecutorQueue,
}

impl ExecutorHandle {
    /// Creates a new handle from a given executor ah
    fn new(executor: Executor, queue: ExecutorQueue) -> Self {
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
        self.inner.take().map(|v| self.returner.push(v));
    }
}
