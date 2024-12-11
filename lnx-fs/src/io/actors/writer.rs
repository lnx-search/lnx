use std::io;
use std::io::{ErrorKind, Result};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bon::Builder;
use futures_util::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, instrument};

use crate::io::actors::ActorFactory;
use crate::io::actors::header::FileEntryFooter;
use crate::io::body::Body;
use crate::io::runtime::RuntimeDispatcher;
use crate::metastore::TabletId;


#[derive(Debug, Builder)]
pub struct TabletWriterOptions {
    /// The base path to store tablet files.
    base_path: PathBuf,
    #[builder(default = 25 << 30)]
    /// The maximum size of a single tablet.
    max_tablet_size: u64,
    #[builder(default = 8)]
    /// The maximum number of active writers.
    max_active_writers: usize,
}

#[derive(Clone)]
/// The handle for writing new tablet files.
///
/// This handle can be cloned cheaply.
pub struct TabletWriter {
    tx: flume::Sender<WriterEvent>,
    controller: Arc<TabletWriterController>,
}

impl TabletWriter {
    /// Creates a new [TabletWriter] with the given options and [RuntimeDispatcher].
    ///
    /// This will internally spawn upto `N` active writers and create new writers
    /// as files will up and reach the `max_tablet_size`.
    ///
    /// This will create no active writers initially.
    pub fn new(options: TabletWriterOptions, runtime: RuntimeDispatcher) -> Self {
        let (tx, rx) = flume::bounded(options.max_active_writers);

        let controller = TabletWriterController {
            events_rx: rx,
            alive_writer_semaphore: Arc::new(Semaphore::new(options.max_active_writers)),
            active_writer_semaphore: Arc::new(Semaphore::new(
                options.max_active_writers,
            )),
            options,
            runtime,
        };

        Self {
            tx,
            controller: Arc::new(controller),
        }
    }

    /// Submits a body to be written to _a_ tablet and waits
    /// for the operation to be completed.
    ///
    /// If the operation is successful a [WriteResponse]
    /// is returned which contains the tablet that wrote the blob and the blob's
    /// position within the tablet file.
    pub async fn write(&self, metadata: BlobMetadata, body: Body) -> Result<WriteResponse> {
        let (ack, rx) = oneshot::channel();
        let event = WriterEvent::Write(WriteEvent { metadata, body, ack });

        self.send_to_writer(event).await?;

        rx.await.map_err(|_| {
            io::Error::new(
                ErrorKind::Interrupted,
                "Writer actor panicked and aborted prematurely",
            )
        })?
    }
        
    async fn send_to_writer(&self, event: WriterEvent) -> Result<()> {
        self.controller.maybe_spawn_writer().await?;

        self.tx.send_async(event).await.map_err(|_| {
            error!(
                "LIKELY BUG DETECTED: Controller checked to create writers but writer \
                    channel still closed, system cannot progress"
            );
            io::Error::new(ErrorKind::Other, "Writers failed to start, this is a bug")
        })?;
        
        Ok(())
    }
    
}

struct TabletWriterController {
    events_rx: flume::Receiver<WriterEvent>,
    alive_writer_semaphore: Arc<Semaphore>,
    active_writer_semaphore: Arc<Semaphore>,
    options: TabletWriterOptions,
    runtime: RuntimeDispatcher,
}

impl TabletWriterController {
    async fn maybe_spawn_writer(&self) -> Result<()> {
        let active = self.num_active_writers();
        let alive = self.num_alive_writers();

        if alive == 0 {
            return self.spawn_writer().await;
        }

        // Some writers are immediately available to pickup work.
        if active < alive {
            return Ok(());
        }

        // We can spawn a new writer without exceeding
        // the max active writers limit.
        if alive < self.options.max_active_writers {
            return self.spawn_writer().await;
        }

        Ok(())
    }

    async fn spawn_writer(&self) -> Result<()> {
        let tablet_id = TabletId::new();
        let file_path = super::get_tablet_file_path(&self.options.base_path, tablet_id);

        let alive_guard = self
            .alive_writer_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should never be closed");

        let factory = TabletWriterActorFactory {
            tablet_id,
            file_path,
            events: self.events_rx.clone(),
            alive_guard,
            active_writer_semaphore: self.active_writer_semaphore.clone(),
            max_tablet_size: self.options.max_tablet_size,
        };

        self.runtime.spawn(factory).await?;

        Ok(())
    }

    /// Returns the number of writers that are currently _actively writing to disk_.
    fn num_active_writers(&self) -> usize {
        self.options.max_active_writers
            - self.active_writer_semaphore.available_permits()
    }

    /// Returns the number of writers that are currently alive.
    ///
    /// The writer may be alive but _IDLE_ and available to pick up a task immediately.
    fn num_alive_writers(&self) -> usize {
        self.options.max_active_writers - self.alive_writer_semaphore.available_permits()
    }
}

/// A factory that creates a new [TabletWriterActor] for a given file path.
struct TabletWriterActorFactory {
    tablet_id: TabletId,
    file_path: PathBuf,
    events: flume::Receiver<WriterEvent>,
    alive_guard: OwnedSemaphorePermit,
    active_writer_semaphore: Arc<Semaphore>,
    max_tablet_size: u64,
}

#[async_trait(?Send)]
impl ActorFactory for TabletWriterActorFactory {
    async fn spawn_actor(self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .dma_open(self.file_path.as_path())
            .await?;

        info!("Tablet file created, syncing directory");
        if let Some(parent) = self.file_path.parent() {
            let dir = glommio::io::BufferedFile::open(parent).await?;
            dir.fdatasync().await?;
            dir.close().await?;
        }

        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(256 << 10)
            .build();

        let actor = TabletWriterActor {
            tablet_id: self.tablet_id,
            _alive_guard: self.alive_guard,
            active_writer_semaphore: self.active_writer_semaphore.clone(),
            events: self.events.clone(),
            writer,
            max_size: self.max_tablet_size,
        };

        glommio::spawn_local(actor.run()).detach();

        Ok(())
    }
}

/// An actor that processes disk-IO operations and executes them.
///
/// This is primarily designed to run with [glommio] and perform
/// asynchronous, Direct IO disk access.
///
/// The actor is only designed to live for as long as the tablet it
/// is writing to, once the tablet has reached its maximum size it
/// will flush all buffer and exit.
pub struct TabletWriterActor {
    /// The unique ID of the tablet being written.
    ///
    /// The ID is used as the file name on disk.
    tablet_id: TabletId,
    /// A guard that allows the [TabletWriterController]
    /// to track how many writer actors are currently
    /// alive.
    _alive_guard: OwnedSemaphorePermit,
    /// A semaphore used for tracking the number of writers
    /// currently in use so the [TabletWriterController]
    /// can determine if more writers should be spawned.
    active_writer_semaphore: Arc<Semaphore>,
    /// Incoming write events.
    ///
    /// A single write event represents a single contiguous blob
    /// and has no requirement on previous or future events being
    /// in the right order.
    events: flume::Receiver<WriterEvent>,
    writer: DmaStreamWriter,
    max_size: u64,
}

impl TabletWriterActor {
    #[instrument("tablet-writer", skip(self), fields(tablet_id = %self.tablet_id))]
    async fn run(mut self) {
        info!("Writer is ready to process events");
        while let Ok(event) = self.events.recv_async().await {
            debug!("Handling IO event");

            // Used to track if the writer is in use or not.
            let permit = self.active_writer_semaphore.clone().acquire_owned().await;

            self.handle_event(event).await;
            drop(permit);

            if self.is_full() {
                break;
            }
        }

        info!("Writer is flushing and closing file");
        if let Err(e) = self.flush_and_close().await {
            error!(error = ?e, "Failed to flush and sync file data");
        }
        debug!("Writer has exited");
    }

    async fn flush_and_close(&mut self) -> Result<()> {
        self.writer.sync().await?;
        self.writer.close().await?;
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.writer.current_pos() >= self.max_size
    }

    #[instrument(skip_all)]
    async fn handle_event(&mut self, event: WriterEvent) {
        match event {
            WriterEvent::Write(event) => {
                self.handle_write_event(event).await;
            },
        }        
    }

    #[instrument(skip_all)]
    async fn handle_write_event(&mut self, event: WriteEvent) {
        let WriteEvent { metadata, body, ack } = event;

        let start = self.writer.current_pos();
        let result = self.write_file_and_flush(metadata, body).await;

        match result {
            Ok(n_written) => {
                let response = WriteResponse {
                    position: start..start + n_written as u64,
                    tablet_id: self.tablet_id,
                };
                let _ = ack.send(Ok(response));
            },
            Err(e) => {
                let _ = ack.send(Err(e));
            },
        }
    }

    #[instrument(skip(self, body))]
    async fn write_file_and_flush(&mut self, metadata: BlobMetadata, body: Body) -> Result<usize> {
        let n_written = self.copy_data_from_event(metadata, body).await?;        
        
        if n_written > 0 {
            debug!("Flush internal buffers");
            self.writer.sync().await?;
        }

        Ok(n_written)
    }

    #[instrument(skip_all)]
    async fn copy_data_from_event(&mut self, metadata: BlobMetadata, body: Body) -> Result<usize> {
        let start_pos = self.writer.current_pos();
        
        let mut n_written = 0;
        loop {
            let Some(chunk) = body.next().await? else {
                break;
            };
            self.writer.write_all(&chunk).await?;
            n_written += chunk.len();
        }
        
        let end_pos = self.writer.current_pos();
        let footer = FileEntryFooter {
            file_path: metadata.path,
            data_range: start_pos..end_pos,
            created_at: metadata.created_at,
        };
        
        // Used in recovery of a tablet.
        let buffer = footer.to_bytes();
        self.writer.write_all(&buffer).await?;
        
        Ok(n_written)
    }
}

#[derive(Debug)]
/// Metadata relating to the blob being written.
pub struct BlobMetadata {
    /// The file path for the blob.
    pub path: String,
    /// When the file was created.
    pub created_at: u64,
}

enum WriterEvent {
    Write(WriteEvent),
}

struct WriteEvent {
    /// The file blob metadata.
    metadata: BlobMetadata,
    /// The incoming body to write to the file.
    body: Body,
    /// The channel sender for acknowledging the write op
    /// that has been completed.
    ///
    /// This is only triggered once the body is written and flushed,
    /// or there is an error.
    ack: oneshot::Sender<Result<WriteResponse>>,
}

#[derive(Debug)]
/// The response of a successful write op.
///
/// This contains basic metadata about where the blob was written
/// and in what tablet.
pub struct WriteResponse {
    /// The position of the blob with a given start and stop.
    pub position: Range<u64>,
    /// The ID of the tablet that completed this write.
    pub tablet_id: TabletId,
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::io::runtime;
    use crate::io::runtime::RuntimeOptions;

    fn create_test_writer(max_writers: usize) -> TabletWriter {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();

        let options = TabletWriterOptions::builder()
            .max_active_writers(max_writers)
            .max_tablet_size(2 << 10)
            .base_path(temp_dir())
            .build();
        TabletWriter::new(options, dispatch)
    }

    #[tokio::test]
    async fn test_controller_spawns_new_writer_when_empty() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(1);
        writer
            .controller
            .maybe_spawn_writer()
            .await
            .expect("System should create writer");

        let alive_writers = writer.controller.num_alive_writers();
        assert_eq!(alive_writers, 1); 
        let active_writers = writer.controller.num_active_writers(); 
        assert_eq!(active_writers, 0); 
    } 
 
    #[tokio::test] 
    async fn test_controller_spawns_new_writer_when_all_active() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(2);

        writer
            .controller
            .maybe_spawn_writer()
            .await
            .expect("System should create writer");

        let (tx, body) = Body::channel();
        let handle = tokio::spawn({
            let writer = writer.clone();
            async move { writer.write(body).await }
        });

        // Let system yield and start task.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let alive_writers = writer.controller.num_alive_writers();
        assert_eq!(alive_writers, 1);
        let active_writers = writer.controller.num_active_writers();
        assert_eq!(active_writers, 1);

        writer
            .controller
            .maybe_spawn_writer()
            .await
            .expect("System should create writer");
        let alive_writers = writer.controller.num_alive_writers();
        assert_eq!(alive_writers, 2);
        let active_writers = writer.controller.num_active_writers();
        assert_eq!(active_writers, 1);

        tx.finish().await;
        let _ = handle.await;

        // Let system yield and allow cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_controller_respects_limit() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(1);

        writer
            .controller
            .maybe_spawn_writer()
            .await
            .expect("System should create writer");

        let (tx, body) = Body::channel();
        let handle = tokio::spawn({
            let writer = writer.clone();
            async move { writer.write(body).await }
        });

        // Let system yield and start task.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let alive_writers = writer.controller.num_alive_writers();
        assert_eq!(alive_writers, 1);
        let active_writers = writer.controller.num_active_writers();
        assert_eq!(active_writers, 1);

        // Even though we call this, it shouldn't spawn a new writer
        // because we're already at the max.
        writer
            .controller
            .maybe_spawn_writer()
            .await
            .expect("System should check writer");
        let alive_writers = writer.controller.num_alive_writers();
        assert_eq!(alive_writers, 1);
        let active_writers = writer.controller.num_active_writers();
        assert_eq!(active_writers, 1);

        tx.finish().await;
        let _ = handle.await;

        // Let system yield and allow cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_write_buffer() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(1);

        let body = Body::complete(Bytes::from_static(b"Hello, world!"));
        let response = writer.write(body).await.expect("Write & flush body");
        assert_eq!(response.position, 0..13);
    }

    #[tokio::test]
    async fn test_write_empty_buffer() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(1);

        let body = Body::empty();
        let response = writer.write(body).await.expect("Write & flush body");
        assert_eq!(response.position, 0..0);
    }

    #[tokio::test]
    async fn test_actor_closes_once_full() {
        const NUM_ITERS: usize = 100;

        let _ = tracing_subscriber::fmt::try_init();

        let buffer = Bytes::from_static(
            b"Hello, world! This is an example of writing some data to the file!\n",
        );
        let num_bytes = (buffer.len() * NUM_ITERS) as u64;

        let writer = create_test_writer(1);

        let (tx, body) = Body::channel();
        tokio::spawn(async move {
            for _ in 0..NUM_ITERS {
                tx.send(buffer.clone()).await;
            }
            tx.finish().await;
        });

        let response = writer.write(body).await.expect("Write & flush body");
        assert_eq!(response.position, 0..num_bytes);
    }
}
