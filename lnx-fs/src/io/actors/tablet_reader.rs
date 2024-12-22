use std::fmt::Debug;
use std::io::{ErrorKind, Result};
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::{cmp, io};

use async_trait::async_trait;
use bon::Builder;
use bytes::Bytes;
use futures_util::StreamExt;
use glommio::io::{DmaFile, MergedBufferLimit, OpenOptions, ReadAmplificationLimit};
use glommio::sync::Semaphore;
use smallvec::SmallVec;
use tracing::{debug, error, info, instrument, warn};

use crate::config::READ_SPLIT_SIZE;
use crate::io::actors::ActorFactory;
use crate::io::runtime::RuntimeDispatcher;
use crate::io::{Body, BodySender};
use crate::metastore::TabletId;

const BUFFER_MERGE_SIZE: usize = 32 << 10;
const READ_MEMORY_LIMIT_BYTES: usize = 5 << 20;
pub type Positions = SmallVec<[Range<u64>; 4]>;

#[derive(Debug, Builder)]
pub struct TabletReaderOptions {
    /// The base path to store tablet files.
    base_path: PathBuf,
    /// The [TabletId] to read.
    tablet_id: TabletId,
    #[builder(default = 10)]
    /// The maximum number of concurrent reads allowed on
    /// the single file.
    max_concurrent_reads: usize,
}

#[derive(Clone)]
/// A [TabletReader] allows both random and sequential read access to tablet files.
///
/// Unlike the [TabletWriter](super::tablet_writer::TabletWriter), the reader requires
/// to specify what tablet and position you want to read, and reads are executed
/// concurrently rather than 1 at a time in the file.
pub struct TabletReader {
    /// Not used but kept to ensure the runtime doesn't shut down before all users
    /// finish.
    _runtime: RuntimeDispatcher,
    events_tx: flume::Sender<ReadEvent>,
}

impl TabletReader {
    /// Open a new [TabletReader] using the given options and runtime.
    pub async fn open(
        options: TabletReaderOptions,
        runtime: RuntimeDispatcher,
    ) -> Result<Self> {
        let (events_tx, events_rx) = flume::bounded(options.max_concurrent_reads);

        let file_path =
            crate::io::get_tablet_file_path(&options.base_path, options.tablet_id);

        let factory = TabletReaderActorFactory {
            tablet_id: options.tablet_id,
            file_path,
            max_concurrent_reads: options.max_concurrent_reads,
            events: events_rx,
        };

        runtime.spawn(factory).await?;

        Ok(Self {
            _runtime: runtime,
            events_tx,
        })
    }

    #[instrument(skip(self))]
    /// Performs a read for a blob at the given position.
    ///
    /// The system performs this operation as a single read request to the kernel
    /// so large reads need to be broken up into smaller reads. This can be
    /// done with the `read_many` handler instead.
    pub async fn read(&self, position: Range<u64>) -> Result<Body> {
        let len = position.end - position.start;
        if len > READ_SPLIT_SIZE {
            let positions = split_read_position(position);
            return self.read_many(positions).await;
        }

        let (ack, body) = Body::channel();

        let event = ReadEvent::ReadAt(ReadAtEvent { position, ack });

        self.events_tx.send_async(event).await.map_err(|_| {
            warn!("Tablet reader actor aborted unexpectedly");
            io::Error::new(ErrorKind::Other, "Reader closed")
        })?;

        Ok(body)
    }

    #[instrument(skip(self))]
    /// Performs a bulk read operation.
    ///
    /// The system may execute this read a random read or sequential read
    /// depending on the size of the blob.
    pub async fn read_many<P>(&self, positions: P) -> Result<Body>
    where
        P: Into<Positions> + Debug,
    {
        let positions = positions.into();
        let (ack, body) = Body::channel();

        let event = ReadEvent::BulkReadAt(BulkReadAtEvent { positions, ack });

        self.events_tx.send_async(event).await.map_err(|_| {
            warn!("Tablet reader actor aborted unexpectedly");
            io::Error::new(ErrorKind::Other, "Reader closed")
        })?;

        Ok(body)
    }
}

struct TabletReaderActorFactory {
    tablet_id: TabletId,
    file_path: PathBuf,
    max_concurrent_reads: usize,
    events: flume::Receiver<ReadEvent>,
}

#[async_trait(?Send)]
impl ActorFactory for TabletReaderActorFactory {
    async fn spawn_actor(self) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .dma_open(self.file_path.as_path())
            .await?;

        let file = Rc::new(file);

        let actor = TabletReaderActor {
            tablet_id: self.tablet_id,
            file,
            max_concurrent_reads: self.max_concurrent_reads,
            read_limiter: Rc::new(Semaphore::new(self.max_concurrent_reads as u64)),
            events: self.events.clone(),
        };

        glommio::spawn_local(actor.run()).detach();

        Ok(())
    }
}

struct TabletReaderActor {
    /// The tablet ID of this actor reads data from.
    tablet_id: TabletId,
    /// The open file of the tablet.
    file: Rc<DmaFile>,
    /// The maximum number of reads concurrently accessing the same file.
    max_concurrent_reads: usize,
    /// The limiter that controls the maximum concurrency
    /// of active reads on the file.
    read_limiter: Rc<Semaphore>,
    /// Incoming [ReadEvent]s to be processed.
    events: flume::Receiver<ReadEvent>,
}

impl TabletReaderActor {
    #[instrument("tablet-reader", skip(self), fields(tablet_id = %self.tablet_id))]
    async fn run(mut self) {
        info!("Tablet reader is active");
        while let Ok(event) = self.events.recv_async().await {
            self.handle_event(event).await;
        }

        info!("Reader is closing, waiting for existing reads to finish");
        let _ = self
            .read_limiter
            .acquire(self.max_concurrent_reads as u64)
            .await;

        debug!("Closing file");
        if let Err(e) = self.file.close_rc().await {
            error!(error = ?e, "Failed to close file");
        }
    }

    async fn handle_event(&mut self, event: ReadEvent) {
        match event {
            ReadEvent::ReadAt(event) => self.spawn_random_read_op(event).await,
            ReadEvent::BulkReadAt(event) => self.spawn_random_bulk_read_op(event).await,
        }
    }

    async fn spawn_random_read_op(&self, event: ReadAtEvent) {
        let permit = self
            .read_limiter
            .acquire_static_permit(1)
            .await
            .expect("Semaphore should never be closed");

        let file = self.file.clone();

        glommio::spawn_local(async move {
            let _permit = permit;
            random_read(file, event).await;
        })
        .detach();
    }

    async fn spawn_random_bulk_read_op(&self, event: BulkReadAtEvent) {
        let permit = self
            .read_limiter
            .acquire_static_permit(1)
            .await
            .expect("Semaphore should never be closed");

        let file = self.file.clone();

        glommio::spawn_local(async move {
            let _permit = permit;
            random_bulk_read(file, event.positions, event.ack).await;
        })
        .detach();
    }
}

async fn random_read(file: Rc<DmaFile>, event: ReadAtEvent) {
    debug!(position = ?event.position, "Random read");

    let len = (event.position.end - event.position.start) as usize;
    match file.read_at(event.position.start, len).await {
        Ok(buffer) => {
            let chunk = Bytes::copy_from_slice(&buffer);
            event.ack.send(chunk).await;
            event.ack.finish().await;
        },
        Err(e) => {
            event.ack.error(e.into()).await;
        },
    }
}

async fn random_bulk_read(file: Rc<DmaFile>, positions: Positions, ack: BodySender) {
    use futures_util::stream;

    debug!(positions = ?positions, "Bulk random read");

    let positions = positions.iter().cloned().map(|pos| {
        let len = (pos.end - pos.start) as usize;
        (pos.start, len)
    });

    let mut stream = file
        .read_many(
            stream::iter(positions),
            MergedBufferLimit::DeviceMaxSingleRequest,
            ReadAmplificationLimit::Custom(BUFFER_MERGE_SIZE),
        )
        .with_memory_limit(Some(READ_MEMORY_LIMIT_BYTES));

    while let Some(result) = stream.next().await {
        match result {
            Err(e) => {
                let error: io::Error = e.into();
                ack.error(io::Error::new(error.kind(), error.to_string()))
                    .await;
                return;
            },
            Ok((_, buf)) => {
                let chunk = Bytes::copy_from_slice(&buf);
                let did_send = ack.send(chunk).await;
                if !did_send {
                    return;
                }
            },
        }
    }

    ack.finish().await;
}

enum ReadEvent {
    ReadAt(ReadAtEvent),
    BulkReadAt(BulkReadAtEvent),
}

struct ReadAtEvent {
    position: Range<u64>,
    ack: BodySender,
}

struct BulkReadAtEvent {
    positions: Positions,
    ack: BodySender,
}

fn split_read_position(position: Range<u64>) -> Positions {
    let read_len = position.end - position.start;
    let mut num_positions = read_len / READ_SPLIT_SIZE;
    if read_len % READ_SPLIT_SIZE != 0 {
        num_positions += 1;
    }

    let mut positions = Positions::with_capacity(num_positions as usize);
    let mut offset = 0;
    for _ in 0..num_positions {
        let end = cmp::min(offset + READ_SPLIT_SIZE, read_len);
        positions.push(offset..end);
        offset += READ_SPLIT_SIZE;
    }

    positions
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;

    use tempfile::TempPath;

    use super::*;
    use crate::io::runtime;
    use crate::io::runtime::RuntimeOptions;

    #[allow(clippy::single_range_in_vec_init)]
    #[allow(clippy::identity_op)]
    #[test]
    fn test_split_read_position() {
        let positions = split_read_position(0..READ_SPLIT_SIZE);
        assert_eq!(positions.as_slice(), &[0..READ_SPLIT_SIZE]);

        let positions = split_read_position(0..READ_SPLIT_SIZE * 4);
        assert_eq!(
            positions.as_slice(),
            &[
                0..READ_SPLIT_SIZE,
                READ_SPLIT_SIZE * 1..READ_SPLIT_SIZE * 2,
                READ_SPLIT_SIZE * 2..READ_SPLIT_SIZE * 3,
                READ_SPLIT_SIZE * 3..READ_SPLIT_SIZE * 4,
            ],
        );

        let positions = split_read_position(0..(READ_SPLIT_SIZE * 2) + 14);
        assert_eq!(
            positions.as_slice(),
            &[
                0..READ_SPLIT_SIZE,
                READ_SPLIT_SIZE * 1..READ_SPLIT_SIZE * 2,
                READ_SPLIT_SIZE * 2..READ_SPLIT_SIZE * 2 + 14
            ],
        );
    }

    fn create_test_tablet(tablet_id: TabletId, size: usize) -> tempfile::NamedTempFile {
        let path = crate::io::get_tablet_file_path(&temp_dir(), tablet_id);
        let tmp_path = TempPath::from_path(&path);

        let file = File::create(&path).unwrap();
        let mut temp_file = tempfile::NamedTempFile::from_parts(file, tmp_path);

        let mut written = 0;
        while written < size {
            temp_file.write_all(b"Hello, World!").unwrap();
            written += 13;
        }

        temp_file
    }

    #[tokio::test]
    async fn test_single_small_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();
        let _file_guard = create_test_tablet(tablet_id, 13);

        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let read_options = TabletReaderOptions::builder()
            .base_path(temp_dir())
            .tablet_id(tablet_id)
            .max_concurrent_reads(3)
            .build();

        let reader = TabletReader::open(read_options, dispatch)
            .await
            .expect("Open reader");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let incoming = reader.read(0..13).await.expect("submit read ok");

        let body = incoming.collect().await.expect("Read body");
        assert_eq!(body.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_bulk_small_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();

        let path = crate::io::get_tablet_file_path(&temp_dir(), tablet_id);
        let tmp_path = TempPath::from_path(&path);

        let file = File::create(&path).unwrap();
        let mut temp_file = tempfile::NamedTempFile::from_parts(file, tmp_path);
        temp_file.write_all(b"foo").unwrap();
        temp_file.write_all(b"bar").unwrap();
        temp_file.write_all(b"baz").unwrap();

        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let read_options = TabletReaderOptions::builder()
            .base_path(temp_dir())
            .tablet_id(tablet_id)
            .max_concurrent_reads(3)
            .build();

        let reader = TabletReader::open(read_options, dispatch)
            .await
            .expect("Open reader");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let positions = vec![0..3, 3..6, 6..9];
        let incoming = reader
            .read_many(positions.clone())
            .await
            .expect("submit read ok");

        let chunk1 = incoming.next().await.expect("Read body");
        assert_eq!(chunk1, Some(Bytes::from_static(b"foo")));

        let chunk2 = incoming.next().await.expect("Read body");
        assert_eq!(chunk2, Some(Bytes::from_static(b"bar")));

        let chunk3 = incoming.next().await.expect("Read body");
        assert_eq!(chunk3, Some(Bytes::from_static(b"baz")));
    }

    #[tokio::test]
    async fn test_bulk_big_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();

        let path = crate::io::get_tablet_file_path(&temp_dir(), tablet_id);
        let tmp_path = TempPath::from_path(&path);

        let file = File::create(&path).unwrap();
        let mut temp_file = tempfile::NamedTempFile::from_parts(file, tmp_path);

        let mut buffer1 = Vec::new();
        for _ in 0..5000 {
            buffer1.extend_from_slice(b"foo");
        }
        temp_file.write_all(&buffer1).unwrap();

        let mut buffer2 = Vec::new();
        for _ in 0..5000 {
            buffer2.extend_from_slice(b"bar");
        }
        temp_file.write_all(&buffer2).unwrap();

        let mut buffer3 = Vec::new();
        for _ in 0..5000 {
            buffer3.extend_from_slice(b"baz");
        }
        temp_file.write_all(&buffer3).unwrap();

        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let read_options = TabletReaderOptions::builder()
            .base_path(temp_dir())
            .tablet_id(tablet_id)
            .max_concurrent_reads(3)
            .build();

        let reader = TabletReader::open(read_options, dispatch)
            .await
            .expect("Open reader");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let positions = vec![
            buffer1.len() as u64..(buffer1.len() + buffer2.len()) as u64,
            0..buffer1.len() as u64,
            (buffer1.len() + buffer2.len()) as u64
                ..(buffer1.len() + buffer2.len() + buffer3.len()) as u64,
        ];
        let incoming = reader
            .read_many(positions.clone())
            .await
            .expect("submit read ok");

        let chunk2 = incoming.next().await.expect("Read body");
        assert_eq!(chunk2, Some(Bytes::from(buffer2)));

        let chunk1 = incoming.next().await.expect("Read body");
        assert_eq!(chunk1, Some(Bytes::from(buffer1)));

        let chunk3 = incoming.next().await.expect("Read body");
        assert_eq!(chunk3, Some(Bytes::from(buffer3)));
    }
}
