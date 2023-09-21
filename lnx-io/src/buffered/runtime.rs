use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::channel::oneshot;
use futures::executor::block_on;

use crate::{Context, IoRuntime, IoWriteOp, IoWriterActor};

/// The runtime for running blocking, buffered IO operations.
///
/// This system runs each actor in it's own independent thread.
pub struct BufferedIoRuntime;

#[async_trait]
impl IoRuntime for BufferedIoRuntime {
    async fn spawn_writer<A>(state: A::State, ctx: Context<IoWriteOp>) -> Result<()>
    where
        A: IoWriterActor,
    {
        let (tx, rx) = oneshot::channel();

        std::thread::Builder::new()
            .name(format!("io-writer-{}", ctx.path.display()))
            .spawn(move || run_actor::<A>(state, ctx, tx))
            .expect("Spawn thread");

        rx.await.map_err(|_| anyhow!("Failed to spawn writer"))?
    }
}

#[instrument("buffered-io", skip_all, fields(path = %ctx.path.display()))]
fn run_actor<A>(
    state: A::State,
    ctx: Context<IoWriteOp>,
    tx: oneshot::Sender<Result<()>>,
) where
    A: IoWriterActor,
{
    let actor_res = block_on(A::create(state, &ctx));

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

    block_on(actor.consume_ops(ctx.path, ctx.ops));
}
