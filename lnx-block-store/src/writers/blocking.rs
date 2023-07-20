use std::borrow::Cow;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufWriter, ErrorKind, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::oneshot;
use lnx_tools::files::SyncOnFlushFile;
use lnx_tools::supervisor;
use lnx_tools::supervisor::RecreateCallback;
use rkyv::AlignedVec;

use crate::writers::SegmentWriter;

/// The number of ops that can be enqueued before some the sender starts to block while waiting.
const CHANNEL_CAPACITY: usize = 5;

#[derive(Clone)]
/// A segment writer that uses traditional buffered IO and blocks the thread
/// it's in when writing.
pub struct BlockingIoWriter(flume::Sender<Op>);

#[async_trait]
impl SegmentWriter for BlockingIoWriter {
    /// Opens a new blocking writer.
    ///
    /// Any blocking operations are wrapped in a thread pool.
    async fn open(path: PathBuf) -> io::Result<Self> {
        let state = lnx_executor::spawn_blocking_task(async move {
            BlockingIoWriterState::open(path)
        })
        .await
        .map_err(|e| {
            io::Error::new(ErrorKind::Other, format!("Task panicked: {e}"))
        })??;

        let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);
        run_actor(state, rx).await;

        Ok(Self(tx))
    }

    /// Writes some bytes to the file.
    ///
    /// Any blocking operations are wrapped in a thread pool.
    async fn write_all(&mut self, buffer: Arc<AlignedVec>) -> io::Result<u64> {
        // TODO: Ensure this is actually safe
        let (tx, rx) = oneshot::channel();

        self.0
            .send_async(Op::WriteAll { buffer, tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with actor"))?;

        rx.await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with actor"))?
    }

    /// Ensures any data written to the writer is persisted on disk.
    ///
    /// Any blocking operations are wrapped in a thread pool.
    async fn flush(&mut self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();

        self.0
            .send_async(Op::Flush { tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with actor"))?;

        rx.await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with actor"))?
    }
}

enum Op {
    WriteAll {
        buffer: Arc<AlignedVec>,
        tx: oneshot::Sender<io::Result<u64>>,
    },
    Flush {
        tx: oneshot::Sender<io::Result<()>>,
    },
}

/// Spawns the initial actor and hands it off the supervisor.
async fn run_actor(state: BlockingIoWriterState, rx: flume::Receiver<Op>) {
    let supervisor_state = ActorSupervisionState {
        path: state.path.clone(),
        rx: rx.clone(),
    };

    let task = lnx_executor::spawn_blocking_task(actor_task(state, rx));
    supervisor::supervise(task, supervisor_state);
}

async fn actor_task(mut state: BlockingIoWriterState, rx: flume::Receiver<Op>) {
    while let Ok(op) = rx.recv_async().await {
        match op {
            Op::WriteAll { buffer, tx } => {
                let _ = tx.send(state.write_all(&buffer));
            },
            Op::Flush { tx } => {
                let _ = tx.send(state.flush());
            },
        }
    }
}

/// The state required for re-creating the actor in the event
/// the task fails.
struct ActorSupervisionState {
    path: PathBuf,
    rx: flume::Receiver<Op>,
}

#[async_trait]
impl supervisor::SupervisedState for ActorSupervisionState {
    fn name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("blocking-segment-writer-{}", self.path.display()))
    }

    async fn recreate<'a>(&self, watcher: RecreateCallback<'a>) -> anyhow::Result<()> {
        let state = BlockingIoWriterState::open(self.path.clone())?;

        watcher.submit(lnx_executor::spawn_blocking_task(actor_task(
            state,
            self.rx.clone(),
        )));

        Ok(())
    }
}

struct BlockingIoWriterState {
    path: PathBuf,
    inner: BufWriter<SyncOnFlushFile>,
    cursor: u64,
}

impl BlockingIoWriterState {
    fn open(path: PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let cursor = file.metadata()?.len();

        if cursor != 0 {
            file.seek(SeekFrom::Start(cursor))?;
        }

        let inner = BufWriter::new(SyncOnFlushFile::from(file));

        Ok(Self {
            path,
            inner,
            cursor,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let start = self.cursor;
        self.inner.write_all(bytes)?;
        self.cursor += bytes.len() as u64;
        Ok(start)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
