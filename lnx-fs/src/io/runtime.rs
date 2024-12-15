use std::io;
use std::io::ErrorKind;
use std::time::Duration;

use bon::Builder;
use futures_util::future::LocalBoxFuture;
use glommio::PoolPlacement;
use tokio::sync::oneshot;
use tracing::info;

use crate::io::actors::ActorFactory;

const DEFAULT_IO_MEMORY: usize = 10 << 20;

type ActorFactoryTask = Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>;

#[derive(Debug, Builder)]
/// Configuration options for creating the file system IO pool.
pub struct RuntimeOptions {
    #[builder(default = 1)]
    /// Configure the number of threads in the pool.
    num_threads: usize,
    #[builder(default = DEFAULT_IO_MEMORY)]
    /// mount of memory to reserve for storage I/ O.
    ///
    /// This will be preallocated and registered with io_uring. It is still possible
    /// to use more than that, but it will come from the standard allocator and performance
    /// will suffer.
    ///
    /// The system will always try to allocate at least 64 kiB for I/ O memory,
    /// and the default is 10 MiB.
    io_memory: usize,
    #[builder(default = 128)]
    /// The depth of the IO rings to create. This influences the level of IO concurrency.
    ///
    /// A higher ring depth allows a shard to submit a greater number of IO requests to
    /// the kernel at once.
    ///
    /// Values above zero are valid and the default is 128.
    ring_depth: usize,
    /// Spin for duration before parking a reactor.
    spin_before_park: Option<Duration>,
    #[builder(default = Duration::from_millis(100))]
    /// How often need_preempt will return true by default.
    ///
    /// Lower values mean task queues will switch execution more often, which can help
    /// latency but harm throughput. When individual task queues are present, this value
    /// can still be dynamically lowered through the Latency setting.
    ///
    /// Default is `100ms`.
    preempt_timer: Duration,
}

/// Spawns the IO executor pool in a set of background threads.
pub fn create_io_runtime(options: RuntimeOptions) -> io::Result<RuntimeDispatcher> {
    let placement = PoolPlacement::MaxPack(options.num_threads, None);

    let mut builder = glommio::LocalExecutorPoolBuilder::new(placement)
        .name("lnx-io")
        .io_memory(options.io_memory)
        .ring_depth(options.ring_depth)
        .preempt_timer(options.preempt_timer);

    if let Some(spin) = options.spin_before_park {
        builder = builder.spin_before_park(spin);
    }

    let (tx, rx) = flume::bounded::<ActorFactoryTask>(128);
    builder.on_all_shards(|| async move {
        info!("Storage IO runtime worker is active");
        while let Ok(task) = rx.recv_async().await {
            task().await;
        }

        info!("Storage IO runtime worker shutting down");
    })?;

    Ok(RuntimeDispatcher { worker: tx })
}

#[derive(Clone)]
/// A dispatcher that balances new tasks to spawn actors.
///
/// Tasks a re-balanced in a round-robin fashion.
pub struct RuntimeDispatcher {
    worker: flume::Sender<ActorFactoryTask>,
}

impl RuntimeDispatcher {
    /// Spawns a given [ActorFactory] into an available runtime.
    pub async fn spawn<F>(&self, task: F) -> io::Result<()>
    where
        F: ActorFactory + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let task = move || {
            use futures_util::future::FutureExt;

            let fut = async move {
                let res = task.spawn_actor().await;
                let _ = tx.send(res);
            };

            fut.boxed_local()
        };

        self.worker.send_async(Box::new(task)).await.map_err(|_| {
            io::Error::new(ErrorKind::Other, "Worker IO runtime died, this is a bug")
        })?;

        rx.await.map_err(|_| {
            io::Error::new(ErrorKind::Other, "Runtime cancelled spawn task")
        })??;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::io::actors::MockActorFactory;
    use super::*;

    #[tokio::test]
    async fn test_runtime_dispatch_pass_result() {
        let options = RuntimeOptions::builder().build();

        let dispatch = create_io_runtime(options)
            .expect("Runtime should be created");
        
        let mut mock_factory = MockActorFactory::new();
        mock_factory
            .expect_spawn_actor()
            .return_once(|| {
                Ok(())
            });
        let result = dispatch.spawn(mock_factory).await;
        assert!(result.is_ok(), "Spawn actor ok");
        
        let mut mock_factory = MockActorFactory::new();
        mock_factory
            .expect_spawn_actor()
            .return_once(|| {
                Err(io::Error::new(ErrorKind::AddrInUse, "Oops!"))
            });
        let err = dispatch.spawn(mock_factory)
            .await
            .expect_err("Error should be returned from spawning.");
        assert_eq!(err.kind(), ErrorKind::AddrInUse);
    }
}
