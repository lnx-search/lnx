#[macro_use]
extern crate tracing;

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::oneshot;
use futures::TryFutureExt;

mod utils;

mod buffered;
pub use self::buffered::BufferedIoWriter;

#[cfg(feature = "directio")]
mod direct;
#[cfg(feature = "directio")]
pub use self::direct::DirectIoWriter;

/// Spawns a new IO writer actor.
///
/// State can be passed to the actor if more information is required
/// outside of the core [Context] data.
pub async fn spawn_writer<A>(
    path: &Path,
    state: A::State,
) -> Result<IoActorMailbox<IoWriteOp>>
where
    A: IoWriterActor,
{
    let (tx, rx) = flume::bounded(5);

    let ctx = Context {
        path: path.to_path_buf(),
        ops: rx,
    };

    <A::Runtime as IoRuntime>::spawn_writer::<A>(state, ctx).await?;

    Ok(IoActorMailbox::new(tx))
}

/// Spawns a new IO writer actor.
///
/// Unlike [spawn_writer] this method will automatically try to select the
/// most performant IO option.
///
/// This means it will attempt to use the direct IO backend if it is enabled.
pub async fn spawn_writer_auto(path: &Path) -> Result<IoActorMailbox<IoWriteOp>> {
    #[cfg(feature = "directio")]
    {
        match spawn_writer::<DirectIoWriter>(path, ()).await {
            Ok(mailbox) => return Ok(mailbox),
            Err(e) => {
                error!(path = %path.display(), error = ?e, "Failed to create direct IO writer, falling back to buffered IO");
            },
        };
    }

    // The buffered IO approach is always the fallback.
    // On non-linux systems this is also the default.
    spawn_writer::<BufferedIoWriter>(path, ()).await
}

/// The actor mailbox for sending events to the IO actor.
pub struct IoActorMailbox<O>(flume::Sender<O>);

impl<O> IoActorMailbox<O> {
    /// Creates a new mailbox using the given event sender.
    pub(crate) fn new(tx: flume::Sender<O>) -> Self {
        Self(tx)
    }

    /// Sends an IO operation to the actor.
    fn send_op(&self, op: O) -> impl Future<Output = Result<()>> + '_ {
        self.0
            .send_async(op)
            .map_err(|_| anyhow!("Writer has died unexpectedly"))
    }
}

impl IoActorMailbox<IoWriteOp> {
    #[inline]
    /// Writes all of the buffer to the actor.
    ///
    /// This will return the starting position of the blob
    /// on successful write.
    pub fn write_all(&self, buffer: Bytes) -> impl Future<Output = Result<u64>> + '_ {
        let (tx, rx) = oneshot::channel();
        let op = IoWriteOp::WriteAll { tx, buffer };

        self.send_op(op)
            .and_then(|_| rx.map_err(|_| anyhow!("Writer has died unexpectedly")))
            .and_then(|res| async { res })
    }

    #[inline]
    /// Flushes any memory buffered to disk.
    ///
    /// This operation should complete when all data is
    /// safely persisted to disk and durable.
    pub fn flush(&self) -> impl Future<Output = Result<()>> + '_ {
        let (tx, rx) = oneshot::channel();
        let op = IoWriteOp::Flush { tx };

        self.send_op(op)
            .and_then(|_| rx.map_err(|_| anyhow!("Writer has died unexpectedly")))
            .and_then(|res| async { res })
    }
}

#[async_trait(?Send)]
pub trait IoWriterActor: Sized + 'static {
    /// The runtime the actor should be spawned on.
    type Runtime: IoRuntime;
    /// The state to pass to the actor when it's created.
    type State: Send + 'static;

    /// Attempts to create a new actor.
    ///
    /// This may assume it is within the given runtime context already.
    async fn create(state: Self::State, ctx: &Context<IoWriteOp>) -> Result<Self>;

    /// Writes all of the buffer to the actor.
    ///
    /// Returns the start position of the buffer on success.
    async fn write_all(&mut self, buffer: Bytes) -> Result<u64>;

    /// Flushes any memory buffered to disk.
    ///
    /// This operation should complete when all data is
    /// safely persisted to disk and durable.
    async fn flush(&mut self) -> Result<()>;

    /// Consumes a stream of IO operations.
    async fn consume_ops(mut self, path: PathBuf, ops: flume::Receiver<IoWriteOp>) {
        let mut op_counter = 0;

        while let Ok(op) = ops.recv_async().await {
            self.handle_op(op).await;
            op_counter += 1;
        }

        debug!(path = %path.display(), num_ops_handled = op_counter, "Io writer is shutting down");
    }

    /// Handles a new IO write op.
    async fn handle_op(&mut self, op: IoWriteOp) {
        match op {
            IoWriteOp::WriteAll { tx, buffer } => {
                let res = self.write_all(buffer).await;
                let _ = tx.send(res);
            },
            IoWriteOp::Flush { tx } => {
                let res = self.flush().await;
                let _ = tx.send(res);
            },
        }
    }
}

/// A since write IO op.
pub enum IoWriteOp {
    /// Write the entire buffer to the writer.
    WriteAll {
        tx: oneshot::Sender<Result<u64>>,
        buffer: Bytes,
    },
    /// Flushes any memory buffered to disk.
    ///
    /// This operation should complete when all data is
    /// safely persisted to disk and durable.
    Flush { tx: oneshot::Sender<Result<()>> },
}

/// The core context for the IO actors.
///
/// This primarily includes:
///
/// - The file path
/// - The IO event stream
pub struct Context<O> {
    /// The path of the file.
    pub path: PathBuf,
    /// The IO op event stream.
    pub ops: flume::Receiver<O>,
}

#[async_trait]
/// A runtime for spawning future onto it.
///
/// This allows for abstraction of the direct IO runtimes and threaded runtimes.
pub trait IoRuntime {
    /// Spawn a future onto the runtime.
    ///
    /// It is the runtimes responsibility to load balance across threads if it
    /// is using multiple threads.
    async fn spawn_writer<A>(state: A::State, ctx: Context<IoWriteOp>) -> Result<()>
    where
        A: IoWriterActor;
}
