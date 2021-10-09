use std::borrow::Borrow;
use async_channel::{Sender, Receiver};
use anyhow::{Error, Result};
use tantivy::Executor;


pub(crate) struct TantivyExecutorPool {
    executors: Receiver<Executor>,
    executors_returner: Sender<Executor>,
}

impl TantivyExecutorPool {
    pub(crate) async fn create(pool_size: usize, threads_per_reader: usize) -> Result<Self> {
        let (tx, rx) = async_channel::bounded(pool_size);
        for _ in 0..pool_size {
            let executor = if threads_per_reader <= 1 {
                tantivy::Executor::single_thread()
            } else {
                tantivy::Executor::multi_thread(threads_per_reader, "reader-executor-")?
            };

            let _ = tx.send(executor).await;
        }

        Ok(Self {
            executors: rx,
            executors_returner: tx,
        })
    }

    pub(crate) async fn get(&self) -> Result<ExecutorHandle> {
        if let Ok(exec) = self.executors.recv().await {
            Ok(ExecutorHandle::new(exec, self.executors_returner.clone()))
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
        println!("dropping and returning executor");
        if let Some(inner) = self.inner.take() {
            if let Err(_) = self.returner.try_send(inner) {
                panic!("failed to return executor to pool")
            }
        }
    }
}
