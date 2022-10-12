use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use glommio::{LocalExecutorBuilder, LocalExecutorPoolBuilder, PoolPlacement};
use tokio::sync::oneshot;

use crate::aio::combiner::AioCombinerActorSetup;
use crate::aio::exporter::AioExporterActorSetup;

type Responder = oneshot::Sender<io::Result<()>>;

#[derive(Debug, thiserror::Error)]
#[error("The runtime is not initialised or running.")]
pub struct DeadRuntime;

/// Attempts to create a new runtime with n threads acting as the executors.
///
/// If the runtime has already been initialised it will not replace the existing
/// runtime.
pub fn create_runtime(num_threads: usize) -> glommio::Result<AioRuntime, ()> {
    let (tasks_tx, tasks_rx) = flume::unbounded();

    // Check to see if our runtime can start ok.
    let handle = LocalExecutorBuilder::default().spawn(|| async {})?;
    handle.join()?;

    LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(num_threads))
        .spin_before_park(Duration::from_millis(10))
        .on_all_shards(move || run_tasks(tasks_rx))?;

    let rt = AioRuntimeInner { tasks_tx };

    Ok(AioRuntime(Arc::new(rt)))
}

pub(super) enum AioTask {
    Combiner(AioCombinerActorSetup),
    Exporter(AioExporterActorSetup),
}

impl From<AioCombinerActorSetup> for AioTask {
    fn from(v: AioCombinerActorSetup) -> Self {
        Self::Combiner(v)
    }
}

impl From<AioExporterActorSetup> for AioTask {
    fn from(v: AioExporterActorSetup) -> Self {
        Self::Exporter(v)
    }
}

impl AioTask {
    async fn spawn_actor(self) -> io::Result<()> {
        match self {
            Self::Combiner(inner) => inner.run_actor().await,
            Self::Exporter(inner) => inner.run_actor().await,
        }
    }
}

#[derive(Clone)]
pub struct AioRuntime(Arc<AioRuntimeInner>);

impl Deref for AioRuntime {
    type Target = AioRuntimeInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct AioRuntimeInner {
    tasks_tx: flume::Sender<(AioTask, Responder)>,
}

impl AioRuntimeInner {
    pub(super) async fn spawn_actor(
        &self,
        task: AioTask,
        responder: Responder,
    ) -> Result<(), DeadRuntime> {
        self.tasks_tx
            .send_async((task, responder))
            .await
            .map_err(|_| DeadRuntime)
    }
}

async fn run_tasks(tasks: flume::Receiver<(AioTask, Responder)>) {
    while let Ok((task, responder)) = tasks.recv_async().await {
        glommio::spawn_local(async move {
            let res = task.spawn_actor().await;
            let _ = responder.send(res);
        })
        .detach();
    }
}
