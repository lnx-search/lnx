use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use glommio::{LocalExecutorPoolBuilder, PoolPlacement};

use crate::aio::combiner::AioCombinerActorSetup;
use crate::aio::exporter::AioExporterActorSetup;

#[derive(Debug, thiserror::Error)]
#[error("The runtime is not initialised or running.")]
pub struct DeadRuntime;

/// Attempts to create a new runtime with n threads acting as the executors.
///
/// If the runtime has already been initialised it will not replace the existing
/// runtime.
pub fn create_runtime(num_threads: usize) -> glommio::Result<AioRuntime, ()> {
    let (tasks_tx, tasks_rx) = flume::unbounded();

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxPack(num_threads, None))
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
    async fn run_actor(self) -> io::Result<()> {
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
    tasks_tx: flume::Sender<AioTask>,
}

impl AioRuntimeInner {
    pub(super) async fn spawn_actor(&self, task: AioTask) -> Result<(), DeadRuntime> {
        self.tasks_tx
            .send_async(task)
            .await
            .map_err(|_| DeadRuntime)
    }
}

async fn run_tasks(tasks: flume::Receiver<AioTask>) {
    while let Ok(task) = tasks.recv_async().await {
        glommio::spawn_local(task.run_actor()).detach();
    }
}
