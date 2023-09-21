use std::future::Future;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use futures::channel::oneshot;
use glommio::Placement;

use crate::{Context, IoRuntime, IoWriteOp, IoWriterActor};

/// The runtime for running async, direct IO operations.
///
/// This system runs each actor within the [glommio] runtime.
pub struct DirectIoRuntime;

#[async_trait(?Send)]
impl IoRuntime for DirectIoRuntime {
    async fn spawn_writer<A>(state: A::State, ctx: Context<IoWriteOp>) -> Result<()>
    where
        A: IoWriterActor,
    {
        static WRITER_RUNTIME: OnceLock<Runtime> = OnceLock::new();

        let runtime = match WRITER_RUNTIME.get() {
            Some(runtime) => runtime,
            None => {
                let rt = Runtime::spawn().await?;
                WRITER_RUNTIME.get_or_init(|| rt)
            },
        };

        let task = WriterTask { state, ctx };

        runtime.submit(task).await
    }
}

/// A task ready to be scheduled on the background runtime.
trait RuntimeTask: Send + 'static {
    fn start(self, waiter_tx: oneshot::Sender<Result<()>>);
}

/// A writer actor task to be spawned.
struct WriterTask<A>
where
    A: IoWriterActor,
{
    state: A::State,
    ctx: Context<IoWriteOp>,
}

impl<A> RuntimeTask for WriterTask<A>
where
    A: IoWriterActor,
{
    /// Starts a new task within the glommio async context.
    ///
    /// This is what actually creates and runs the writer actor.
    fn start(self, tx: oneshot::Sender<Result<()>>) {
        let path = self.ctx.path.clone();
        let fut = async move {
            let actor_res = A::create(self.state, &self.ctx).await;

            let actor = match actor_res {
                Ok(actor) => actor,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                },
            };

            if tx.send(Ok(())).is_err() {
                return;
            }

            actor.consume_ops(self.ctx.path, self.ctx.ops).await;
        };

        glommio::spawn_local(wrap(fut, path)).detach();
    }
}

#[instrument("direct-io", skip_all, fields(path = %_path.display()))]
/// Wraps a given future but attaches the tracing instrumentation to it.
async fn wrap<F, T>(fut: F, _path: PathBuf) -> T
where
    F: Future<Output = T>,
{
    fut.await
}

type TaskOp = (oneshot::Sender<Result<()>>, Box<dyn RuntimeTask>);

/// A wrapper around a glommio runtime running in a background thread.
struct Runtime {
    submitter: flume::Sender<TaskOp>,
}

impl Runtime {
    /// Spawns a new [glommio] runtime in a background thread.
    async fn spawn() -> Result<Self> {
        let (tx, rx) = flume::bounded(5);
        let (waiter_tx, waiter_rx) = oneshot::channel();

        let handle = std::thread::Builder::new()
            .name(format!("direct-io-runtime"))
            .spawn(move || {
                let res = glommio::LocalExecutorBuilder::new(Placement::Unbound)
                    .spin_before_park(Duration::from_millis(250))
                    .make();

                let rt = match res {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = waiter_tx.send(Err(anyhow::Error::from(e)));
                        return;
                    },
                };

                if waiter_tx.send(Ok(())).is_err() {
                    return;
                }

                rt.run(runtime_main(rx));
            })
            .expect("Spawn thread");

        match waiter_rx.await {
            Ok(res) => res?,
            Err(_) => bail!(
                "Failed to spawn background runtime task due to error: {:?}",
                handle.join()
            ),
        }

        Ok(Self { submitter: tx })
    }

    /// Submits a new task to the runtime.
    async fn submit<T>(&self, task: T) -> Result<()>
    where
        T: RuntimeTask,
    {
        let (tx, rx) = oneshot::channel();

        self.submitter
            .send_async((tx, Box::new(task)))
            .await
            .map_err(|_| anyhow!("Failed to submit task to background runtime"))?;

        rx.await
            .map_err(|_| anyhow!("Runtime task died unexpectedly"))?
    }
}

async fn runtime_main(tasks: flume::Receiver<TaskOp>) {
    while let Ok(task) = tasks.recv_async().await {
        let (tx, task) = task;
        task.start(tx);
    }
}
