use std::sync::Arc;

use tokio::sync::Semaphore;

#[derive(Clone)]
/// A custom, multi-threaded tantivy indexer.
///
/// This index atomically processes each batch of documents
/// as a single segment, allowing each request to be atomic
/// and not affected by other operations.
pub struct TantivyIndexerPool {
    /// The concurrency limiter for applying backpressure.
    limiter: Arc<Semaphore>,
    /// The core indexer threadpool.
    threadpool: Arc<rayon::ThreadPool>,
}

impl TantivyIndexerPool {
    /// Creates a new indexer pool with a given number of cpus.
    pub fn create(num_cpus: usize) -> Self {
        let limiter = Arc::new(Semaphore::new(num_cpus));
        let threadpool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus)
            .thread_name(|n| format!("lnx-indexer-{n}"))
            .build()
            .expect("Start threadpool");

        Self {
            limiter,
            threadpool: Arc::new(threadpool),
        }
    }

    /// Schedules a task on the threadpool once there is availability.
    pub async fn schedule<OP>(&self, op: OP)
    where
        OP: FnOnce() + Send + 'static
    {
        let permit = self.limiter.clone()
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed");
        self.threadpool.spawn(move || {
            let _permit = permit;
            op();
        });
    }
}
