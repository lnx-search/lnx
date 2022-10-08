use std::io;
use std::time::Duration;
use glommio::{LocalExecutorPoolBuilder, PoolPlacement, PoolThreadHandles};
use once_cell::sync::OnceCell;
use crate::aio::combiner::{AioCombinerActor, AioCombinerActorSetup};

static RUNTIME: OnceCell<AioRuntime> = OnceCell::new();

pub(super) async fn spawn_actor(task: AioTask) -> Result<(), DeadRuntime>{
    let rt = RUNTIME.get().ok_or(DeadRuntime)?;
    rt.tasks_tx.send_async(task).await.map_err(|_| DeadRuntime)
}

#[derive(Debug, thiserror::Error)]
#[error("The runtime is not initialised or running.")]
pub struct DeadRuntime;

/// Attempts to create a new runtime with n threads acting as the executors.
///
/// If the runtime has already been initialised it will not replace the existing
/// runtime.
pub fn try_init(num_threads: usize) -> glommio::Result<(), ()> {
    let (tasks_tx, tasks_rx) = flume::unbounded();

    let pool = LocalExecutorPoolBuilder::new(PoolPlacement::MaxPack(num_threads, None))
        .spin_before_park(Duration::from_millis(10))
        .on_all_shards(move || run_tasks(tasks_rx))?;

    let rt = AioRuntime {
        pool,
        tasks_tx,
    };

    let _ = RUNTIME.set(rt);

    Ok(())
}


pub(super) enum AioTask {
    Combiner(AioCombinerActorSetup),
}

impl From<AioCombinerActorSetup> for AioTask {
    fn from(v: AioCombinerActorSetup) -> Self {
        Self::Combiner(v)
    }
}

impl AioTask {
    async fn run_actor(self) -> io::Result<()> {
        match self {
            Self::Combiner(inner) => inner.run_actor().await,
        }
    }
}


struct AioRuntime {
    pool: PoolThreadHandles<()>,
    tasks_tx: flume::Sender<AioTask>,
}

async fn run_tasks(tasks: flume::Receiver<AioTask>) {
    while let Ok(task) = tasks.recv_async().await {
        glommio::spawn_local(task.run_actor()).detach();
    }
}