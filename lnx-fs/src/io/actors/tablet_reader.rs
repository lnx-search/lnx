use std::io::{ErrorKind, Result};
use std::iter::zip;
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::{cmp, io, mem};

use async_trait::async_trait;
use bon::Builder;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use glommio::io::{
    DmaFile,
    DmaStreamReaderBuilder,
    MergedBufferLimit,
    OpenOptions,
    ReadAmplificationLimit,
};
use glommio::sync::Semaphore;
use tracing::{debug, error, info, instrument, warn};

use crate::io::actors::ActorFactory;
use crate::io::runtime::RuntimeDispatcher;
use crate::io::{Body, BodySender};
use crate::metastore::TabletId;

const BUFFER_MERGE_SIZE: usize = 32 << 10;

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
    #[builder(default = 1 << 20)]
    /// The minimum read size required for the system to switch
    /// from a single random read op to a sequential scan.
    ///
    /// The size is in bytes.
    sequential_read_threshold: usize,
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
            crate::io::utils::get_tablet_file_path(&options.base_path, options.tablet_id);

        let factory = TabletReaderActorFactory {
            tablet_id: options.tablet_id,
            file_path,
            max_concurrent_reads: options.max_concurrent_reads,
            sequential_read_threshold: options.sequential_read_threshold,
            events: events_rx,
        };

        runtime.spawn(factory).await?;

        Ok(Self {
            _runtime: runtime,
            events_tx,
        })
    }

    /// Performs a read for a blob at the given position.
    ///
    /// The system may execute this read a random read or sequential read
    /// depending on the size of the blob.
    pub async fn read(&self, position: Range<u64>) -> Result<Body> {
        let (ack, body) = Body::channel();

        let event = ReadEvent::ReadAt(ReadAtEvent { position, ack });

        self.events_tx.send_async(event).await.map_err(|_| {
            warn!("Tablet reader actor aborted unexpectedly");
            io::Error::new(ErrorKind::Other, "Reader closed")
        })?;

        Ok(body)
    }

    #[allow(unused)] // TODO: Implement bulk reads API.
    /// Performs a bulk read operation.
    ///
    /// The system may execute this read a random read or sequential read
    /// depending on the size of the blob.
    pub async fn read_many(&self, positions: Vec<Range<u64>>) -> Result<Vec<Body>> {
        let mut acks = Vec::with_capacity(positions.len());
        let mut bodies = Vec::with_capacity(positions.len());
        for _ in 0..positions.len() {
            let (ack, body) = Body::channel();
            acks.push(ack);
            bodies.push(body);
        }

        let event = ReadEvent::BulkReadAt(BulkReadAtEvent {
            positions,
            ack: acks,
        });

        self.events_tx.send_async(event).await.map_err(|_| {
            warn!("Tablet reader actor aborted unexpectedly");
            io::Error::new(ErrorKind::Other, "Reader closed")
        })?;

        Ok(bodies)
    }
}

struct TabletReaderActorFactory {
    tablet_id: TabletId,
    file_path: PathBuf,
    max_concurrent_reads: usize,
    sequential_read_threshold: usize,
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
            sequential_read_threshold: self.sequential_read_threshold,
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
    /// The threshold in bytes from when the system will perform
    /// a single read OP vs a sequential scan.
    sequential_read_threshold: usize,
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

        let len = (event.position.end - event.position.start) as usize;
        let use_sequential_scan = len >= self.sequential_read_threshold;

        let file = self.file.clone();

        glommio::spawn_local(async move {
            let _permit = permit;
            if use_sequential_scan {
                sequential_read(file, event.position, event.ack).await;
            } else {
                random_read(file, event).await;
            }
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

        let mut large_reads_pos = Vec::new();
        let mut large_reads_ack = Vec::new();
        let mut small_reads_pos = Vec::new();
        let mut small_reads_ack = Vec::new();

        for (pos, ack) in zip(event.positions, event.ack) {
            let len = (pos.end - pos.start) as usize;
            if len >= self.sequential_read_threshold {
                large_reads_ack.push(ack);
                large_reads_pos.push(pos);
            } else {
                small_reads_ack.push(ack);
                small_reads_pos.push(pos);
            }
        }

        glommio::spawn_local(async move {
            let _permit = permit;

            for (pos, ack) in zip(large_reads_pos, large_reads_ack) {
                sequential_read(file.clone(), pos, ack).await;
            }

            random_bulk_read(file, small_reads_pos, small_reads_ack).await;
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

async fn random_bulk_read(
    file: Rc<DmaFile>,
    positions: Vec<Range<u64>>,
    ack: Vec<BodySender>,
) {
    use futures_util::stream;

    debug!(positions = ?positions, "Bulk random read");

    let mut mapped_bodies = fnv::FnvHashMap::default();
    for (pos, sender) in zip(positions.iter(), ack) {
        mapped_bodies.insert(pos.start, sender);
    }

    let positions = positions.iter().cloned().map(|pos| {
        let len = (pos.end - pos.start) as usize;
        (pos.start, len)
    });

    let mut stream = file.read_many(
        stream::iter(positions),
        MergedBufferLimit::DeviceMaxSingleRequest,
        ReadAmplificationLimit::Custom(BUFFER_MERGE_SIZE),
    );

    while let Some(result) = stream.next().await {
        match result {
            Err(e) => {
                let error: io::Error = e.into();
                // Propagate error to all senders.
                for body in mapped_bodies.values() {
                    body.error(io::Error::new(error.kind(), error.to_string()))
                        .await;
                }
                return;
            },
            Ok(((start, _), buf)) => {
                let chunk = Bytes::copy_from_slice(&buf);
                let sender =
                    mapped_bodies.get(&start).expect("Body should always exist");
                sender.send(chunk).await;
                sender.finish().await;
            },
        }
    }
}

async fn sequential_read(file: Rc<DmaFile>, position: Range<u64>, ack: BodySender) {
    use futures_util::AsyncReadExt;

    debug!(position = ?position, "Sequential read");

    let len = (position.end - position.start) as usize;
    let mut reader = DmaStreamReaderBuilder::from_rc(file)
        .with_start_pos(position.start)
        .with_end_pos(position.end)
        .with_read_ahead(4)
        .with_buffer_size(512 << 10)
        .build();

    let capacity = cmp::min(len, 32 << 10);
    let mut buffer = BytesMut::with_capacity(capacity);

    loop {
        // NOTE: These are uninitialized bytes.
        let slice =
            unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr(), capacity) };
        let n = match reader.read(slice).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                ack.error(e).await;
                return;
            },
        };
        // Advance the internal buffer cursor with the now initialized bytes.
        unsafe { buffer.advance_mut(n) };

        let chunk = mem::replace(&mut buffer, BytesMut::with_capacity(capacity));
        let is_active = ack.send(chunk.freeze()).await;
        if !is_active {
            // The reader is no longer active, we have no reason to keep reading.
            return;
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
    positions: Vec<Range<u64>>,
    ack: Vec<BodySender>,
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

    fn create_test_tablet(tablet_id: TabletId, size: usize) -> tempfile::NamedTempFile {
        let path = crate::io::utils::get_tablet_file_path(&temp_dir(), tablet_id);
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
    // This test is identical to the single read test
    // except for we know it runs the sequential branch after manually checking.
    async fn test_single_sequential_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();
        let _file_guard = create_test_tablet(tablet_id, 100);

        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let read_options = TabletReaderOptions::builder()
            .base_path(temp_dir())
            .tablet_id(tablet_id)
            .max_concurrent_reads(3)
            .sequential_read_threshold(100)
            .build();

        let reader = TabletReader::open(read_options, dispatch)
            .await
            .expect("Open reader");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let incoming = reader.read(0..100).await.expect("submit read ok");

        let body = incoming.collect().await.expect("Read body");
        assert_eq!(body.len(), 100);
    }

    #[tokio::test]
    async fn test_bulk_small_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();
        let _file_guard = create_test_tablet(tablet_id, 256);

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

        let positions = vec![0..13, 48..52, 75..100];
        let incoming = reader
            .read_many(positions.clone())
            .await
            .expect("submit read ok");
        assert_eq!(incoming.len(), positions.len());

        for (pos, body) in zip(positions, incoming) {
            let len = (pos.end - pos.start) as usize;
            let body = body.collect().await.expect("Collect body");
            assert_eq!(body.len(), len);
        }
    }

    #[tokio::test]
    // This test is identical to the bulk read test
    // except for we know it runs the sequential branch after manually checking.
    async fn test_bulk_separate_small_and_large_read() {
        let _ = tracing_subscriber::fmt::try_init();

        let tablet_id = TabletId::new();
        let _file_guard = create_test_tablet(tablet_id, 256);

        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let read_options = TabletReaderOptions::builder()
            .base_path(temp_dir())
            .tablet_id(tablet_id)
            .max_concurrent_reads(3)
            .sequential_read_threshold(100)
            .build();

        let reader = TabletReader::open(read_options, dispatch)
            .await
            .expect("Open reader");

        tokio::time::sleep(Duration::from_millis(100)).await;

        let positions = vec![0..13, 48..52, 75..100, 100..256];
        let incoming = reader
            .read_many(positions.clone())
            .await
            .expect("submit read ok");
        assert_eq!(incoming.len(), positions.len());

        for (pos, body) in zip(positions, incoming) {
            let len = (pos.end - pos.start) as usize;
            let body = body.collect().await.expect("Collect body");
            assert_eq!(body.len(), len);
        }
    }
}
