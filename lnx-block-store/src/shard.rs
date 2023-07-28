use std::borrow::Cow;
use std::io;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use futures::channel::oneshot;
use lnx_tools::supervisor::RecreateCallback;
use rkyv::AlignedVec;
use tracing::{debug, info, instrument};

use crate::writers::BlockStoreWriter;
use crate::FileKey;

/// Starts a new block writer shard with a given shard ID.
///
/// A new file will be created in the given base_path in the format of `<timestamp>-<shard_id>.blocks`.
///
/// The shard is a supervised task, which means it will be automatically restarted in the event of
/// a panic, even though this task should never panic in theory.
pub async fn start_shard(
    shard_id: usize,
    base_path: PathBuf,
) -> Result<StorageShardMailbox> {
    let (file_key, path) = crate::get_new_segment(&base_path, shard_id);
    let writer = BlockStoreWriter::open(&path).await?;

    info!(shard_id = shard_id, path = %path.display(), "Created new storage shard");

    let actor = StorageShardActor {
        shard_id,
        base_path: base_path.clone(),
        file_key,
        writer,
        flush_delta: 0,
    };

    let (tx, rx) = flume::bounded(5);

    let state = SupervisorState {
        base_path,
        shard_id,
        rx: rx.clone(),
    };

    let task = tokio::spawn(actor.run_actor(rx));
    lnx_tools::supervisor::supervise(task, state);

    Ok(StorageShardMailbox { shard_id, tx })
}

#[derive(Debug, Clone)]
/// A cheap to clone access to the given storage shard.
pub struct StorageShardMailbox {
    shard_id: usize,
    tx: flume::Sender<Op>,
}

impl StorageShardMailbox {
    #[inline]
    /// Returns the shard ID of this mailbox.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    /// Writes a given block of data to the shard.
    ///
    /// This returns the location of where the data was written.
    pub async fn write_all(&self, data: Arc<AlignedVec>) -> io::Result<WriteLocation> {
        debug!(
            shard_id = self.shard_id,
            num_bytes = data.len(),
            "Submitting bytes to file"
        );

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::WriteAll { tx, data })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with shard"))?;

        rx.await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with shard"))?
    }

    /// Flushes the writer ensuring all data is safely persisted to disk.
    pub async fn flush(&self) -> io::Result<()> {
        debug!(shard_id = self.shard_id, "Submitting flush to writer");

        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Op::Flush { tx })
            .await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with shard"))?;

        rx.await
            .map_err(|_| io::Error::new(ErrorKind::Other, "Lost contact with shard"))?
    }
}

struct SupervisorState {
    base_path: PathBuf,
    shard_id: usize,
    rx: flume::Receiver<Op>,
}

#[async_trait]
impl lnx_tools::supervisor::SupervisedState for SupervisorState {
    fn name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("storage-shard-{}", self.shard_id))
    }

    async fn recreate<'a>(&self, watcher: RecreateCallback<'a>) -> Result<()> {
        let (file_key, path) = crate::get_new_segment(&self.base_path, self.shard_id);
        let writer = BlockStoreWriter::open(&path).await?;

        let actor = StorageShardActor {
            shard_id: self.shard_id,
            base_path: self.base_path.clone(),
            file_key,
            writer,
            flush_delta: 0,
        };

        let task = tokio::spawn(actor.run_actor(self.rx.clone()));
        watcher.submit(task);

        Ok(())
    }
}

/// A core storage shard actor which manages block store files.
struct StorageShardActor {
    shard_id: usize,
    base_path: PathBuf,
    file_key: FileKey,
    writer: BlockStoreWriter,
    flush_delta: usize,
}

impl StorageShardActor {
    async fn run_actor(mut self, rx: flume::Receiver<Op>) {
        while let Ok(op) = rx.recv_async().await {
            self.handle_op(op).await;
        }
    }

    #[instrument(name = "storage-shard", skip_all, fields(shard_id = self.shard_id))]
    async fn handle_op(&mut self, op: Op) {
        // TODO: Handle flush ops more efficiently by allowing them to be buffered.
        //       this should improve performance when handling high-concurrency.
        match op {
            Op::WriteAll { tx, data } => {
                let len = data.len();
                let start = Instant::now();

                let res = self.writer.write_all(data).await.map(|pos| WriteLocation {
                    file_key: self.file_key,
                    pos,
                });
                let _ = tx.send(res);

                self.flush_delta += len;
                debug!(
                    elapsed = ?start.elapsed(),
                    num_bytes = len,
                    "Wrote bytes to disk",
                );
            },
            Op::Flush { tx } => {
                let start = Instant::now();

                let res = self.writer.flush().await;

                if res.is_err() {
                    let _ = tx.send(res);
                    return;
                }

                // The writer is full, let's create a new file and swap the file.
                if self.writer.is_full() {
                    let res = self.create_new_writer().await;
                    let _ = tx.send(res);
                }

                debug!(
                    elapsed = ?start.elapsed(),
                    flushed_bytes = self.flush_delta,
                    "Wrote bytes to disk",
                );

                self.flush_delta = 0;
            },
        }
    }

    /// Replaces the existing writer with a new blank writer.
    async fn create_new_writer(&mut self) -> io::Result<()> {
        info!(
            file_size = self.writer.file_size(),
            "Writer is now full, beginning file rollover..."
        );

        let (file_key, path) =
            crate::get_new_segment(&self.base_path, self.shard_id);
        let new_writer = BlockStoreWriter::open(&path).await?;

        self.file_key = file_key;
        self.writer = new_writer;

        Ok(())
    }
}

enum Op {
    WriteAll {
        tx: oneshot::Sender<io::Result<WriteLocation>>,
        data: Arc<AlignedVec>,
    },
    Flush {
        tx: oneshot::Sender<io::Result<()>>,
    },
}

#[derive(Debug, Copy, Clone)]
/// The location in which the given block of data was written.
pub struct WriteLocation {
    pub file_key: FileKey,
    pub pos: u64,
}
