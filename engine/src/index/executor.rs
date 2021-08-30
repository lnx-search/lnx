use anyhow::{Result, Error};
use std::sync::Arc;
use std::borrow::Borrow;
use crossbeam::queue::ArrayQueue;
use tantivy::Executor;

#[derive(Clone)]
pub(super) struct ExecutorPool {
    index_name: Arc<String>,
    executors: Arc<ArrayQueue<Executor>>,
}

impl ExecutorPool {
    pub(super) fn create(index_name: &str, pool_size: usize, threads_per_reader: usize) -> Result<Self> {
        let executors = ArrayQueue::new(pool_size);
        for i in 0..pool_size {
            let executor = if threads_per_reader > 1 {
                info!(
                    "[ READER {} @ {} ] executor startup, mode: multi-threaded, threads: {}",
                    i, index_name, threads_per_reader
                );
                Executor::multi_thread(threads_per_reader, "index-reader-")?
            } else {
                info!(
                    "[ READER {} @ {} ] executor startup, mode: single-threaded (no-op)",
                    i, index_name,
                );
                Executor::single_thread()
            };

            if let Err(_) = executors.push(executor) {
                panic!("executor pool was full yet executor was not added on creation, this is a bug.")
            };
        }

        let executors = Arc::new(executors);

        Ok(Self {
            executors,
            index_name: Arc::new(index_name.to_string())
        })
    }

    pub(super) fn shutdown(&self) {
        while let Some(exec) = self.executors.pop() {
            drop(exec);
        }
    }

    pub(super) fn acquire(&self) -> Result<ExecutorHandle> {
        debug!("[ EXECUTOR-POOL @ {} ] taking executor from pool", self.index_name.as_ref());
        if let Some(executor) = self.executors.pop() {
            Ok(ExecutorHandle {
                inner: Some(executor),
                queue: self.executors.clone(),
            })
        } else {
            Err(Error::msg("no executors were available even though the max concurrent was not reached"))
        }
    }
}

pub(super) struct ExecutorHandle {
    inner: Option<Executor>,
    queue: Arc<ArrayQueue<Executor>>,
}

impl Borrow<Executor> for ExecutorHandle {
    fn borrow(&self) -> &Executor {
        let ref_ = self.inner.as_ref();
        ref_.expect("extract executor")
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        debug!("[ EXECUTOR-POOL @ {} ] returning executor to pool", self.index_name.as_ref());
        if let Some(inner) = self.inner.take() {
            let maybe_err = self.queue.push(inner);
            if maybe_err.is_err() {
                panic!("failed to return executor to pool");
            }
        };
    }
}