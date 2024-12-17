use std::fmt::Debug;
use std::future::Future;
use std::io::Result;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bon::Builder;
use futures_util::{AsyncWriteExt, FutureExt};
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder};
use glommio::GlommioError;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::io::actors::ActorFactory;
use crate::io::body::Body;
use crate::io::event::FileEvent;
use crate::io::flush_waker::{FlushWaker, FlushWakerController};
use crate::io::runtime::RuntimeDispatcher;
use crate::io::Metadata;
use crate::metastore::TabletId;

type EventHooks = Arc<Vec<Box<dyn WriterEventHook>>>;

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
    #[builder(default, into)]
    /// A set of event hooks to trigger during writer life cycle events.
    event_hooks: EventHooks,
    #[builder(default, into)]
    /// The duration between flushes that can occur after a write op.
    flush_delay_window: Duration,
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

    #[instrument(skip(self, body))]
    /// Submits a body to be written to _a_ tablet and waits
    /// for the operation to be completed.
    ///
    /// If the operation is successful a [WriterResponse]
    /// is returned which contains the tablet that wrote the blob and the blob's
    /// position within the tablet file.
    pub async fn write(&self, metadata: Metadata, body: Body) -> Result<WriterResponse> {
        let event = EventKind::Write(WriteEvent { metadata, body });
        self.send_to_writer(event).await
    }

    #[instrument(skip(self))]
    /// Submits a delete marker to be written to _a_ tablet and waits
    /// for the operation to be completed.
    ///
    /// If the operation is successful a [WriterResponse]
    /// is returned which contains the tablet that wrote the marker.
    ///
    /// NOTE:
    /// This does not delete the data itself, it simply puts a marker in the file
    /// so the system knows it can ignore the blob when it next runs a compaction cycle.
    pub async fn delete(&self, metadata: Metadata) -> Result<WriterResponse> {
        let event = EventKind::Delete(DeleteEvent { metadata });
        self.send_to_writer(event).await
    }

    #[instrument(skip(self))]
    /// Submits a rename marker to be written to _a_ tablet and waits
    /// for the operation to be completed.
    ///
    /// If the operation is successful a [WriterResponse]
    /// is returned which contains the tablet that wrote the marker.
    ///
    /// NOTE:
    /// This does not rename the file itself, it simply puts a marker in the file
    /// so the system knows it needs to adjust the paths when loading metadata to memory.
    pub async fn rename(
        &self,
        metadata: Metadata,
        new_path: String,
    ) -> Result<WriterResponse> {
        let event = EventKind::Rename(RenameEvent { metadata, new_path });
        self.send_to_writer(event).await
    }

    async fn send_to_writer(&self, event: EventKind) -> Result<WriterResponse> {
        let (ack, rx) = oneshot::channel();

        let wrapped = WriterEvent { ack, kind: event };

        self.controller.maybe_spawn_writer().await?;

        self.tx
            .send_async(wrapped)
            .await
            .map_err(super::writer_controller_bug_log)?;

        rx.await.map_err(super::writer_closed)?
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
        let file_path =
            crate::io::get_tablet_file_path(&self.options.base_path, tablet_id);

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
            event_hooks: self.options.event_hooks.clone(),
            flush_delay_window: self.options.flush_delay_window,
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
    event_hooks: EventHooks,
    flush_delay_window: Duration,
}

#[async_trait(?Send)]
impl ActorFactory for TabletWriterActorFactory {
    async fn spawn_actor(self) -> Result<()> {
        let file = crate::io::utils::create_rw_file_glommio(&self.file_path).await?;

        info!("Tablet file created, syncing directory");
        crate::io::utils::sync_directory_glommio(&self.file_path).await?;

        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(256 << 10)
            .build();

        let actor = TabletWriterActor::new(
            self.tablet_id,
            writer,
            self.alive_guard,
            self.active_writer_semaphore.clone(),
            self.events.clone(),
            self.max_tablet_size,
            self.event_hooks.clone(),
            self.flush_delay_window,
        );

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
    /// The inner file writer.
    writer: DmaStreamWriter,
    /// The soft-maximum size of the tablet.
    max_size: u64,
    /// Callback triggers to invoke on certain writer life cycle events.
    event_hooks: EventHooks,
    /// The flush waker controller used for informing
    /// clients about flushes.
    waker_controller: FlushWakerController,
    /// The position of the writer where data is persisted to disk.
    persisted_pos: u64,
    /// The point in time when first write occurred after the last flush.
    ///
    /// If this is `None` then no write has occurred since the last flush.
    first_write_after_flush: Option<Instant>,
    /// The deadline a flush to disk must have occurred by.
    flush_delay_window: Duration,
}

impl TabletWriterActor {
    fn new(
        tablet_id: TabletId,
        writer: DmaStreamWriter,
        alive_guard: OwnedSemaphorePermit,
        active_writer_semaphore: Arc<Semaphore>,
        events: flume::Receiver<WriterEvent>,
        max_size: u64,
        event_hooks: EventHooks,
        flush_delay_window: Duration,
    ) -> Self {
        Self {
            tablet_id,
            _alive_guard: alive_guard,
            active_writer_semaphore,
            events,
            writer,
            max_size,
            event_hooks,
            waker_controller: FlushWakerController::new(tablet_id),
            persisted_pos: 0,
            first_write_after_flush: None,
            flush_delay_window,
        }
    }

    #[instrument("tablet-writer", skip(self), fields(tablet_id = %self.tablet_id))]
    async fn run(mut self) {
        for hook in self.event_hooks.iter() {
            hook.on_writer_start(self.tablet_id);
        }

        info!("Writer is ready to process events");
        loop {
            let wrapped = self
                .wrap_with_flush_timeout(self.events.recv_async())
                .await
                .transpose();

            let event = match wrapped {
                Err(_) => break,
                Ok(Some(event)) => event,
                Ok(None) => {
                    self.flush_if_required().await;
                    continue;
                },
            };

            debug!("Handling IO event");

            // Used to track if the writer is in use or not.
            let permit = self.active_writer_semaphore.clone().acquire_owned().await;

            self.handle_event(event).await;
            drop(permit);

            if self.first_write_after_flush.is_none() {
                self.first_write_after_flush = Some(Instant::now());
            }
            self.flush_if_required().await;

            if self.is_full() {
                break;
            }
        }

        info!("Writer is flushing and closing file");
        let last_pos = self.writer.current_pos();
        if let Err(e) = self.flush_and_close().await {
            error!(error = ?e, "Failed to flush and sync file data");
        }
        self.waker_controller.wake(last_pos);

        trace!("Running event hooks for shutdown");
        for hook in self.event_hooks.iter() {
            hook.on_writer_close(self.tablet_id);
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
        let WriterEvent { ack, kind } = event;

        let result = match kind {
            EventKind::Write(WriteEvent { metadata, body }) => {
                self.write_create_blob(metadata, body).await
            },
            EventKind::Delete(DeleteEvent { metadata }) => {
                self.write_delete_blob(metadata).await
            },
            EventKind::Rename(RenameEvent { metadata, new_path }) => {
                self.write_rename_blob(metadata, new_path).await
            },
        };

        let waker = self
            .waker_controller
            .create_waker(self.writer.current_pos());

        match result {
            Ok(event) => {
                let response = WriterResponse {
                    tablet_id: self.tablet_id,
                    writer_position: self.writer.current_pos(),
                    event,
                    flush_waker: waker,
                };

                for hook in self.event_hooks.iter() {
                    hook.on_writer_response(response.clone());
                }

                let _ = ack.send(Ok(response));
            },
            Err(e) => {
                let _ = ack.send(Err(e));
            },
        }
    }

    #[instrument(skip_all)]
    async fn write_create_blob(
        &mut self,
        metadata: Metadata,
        body: Body,
    ) -> Result<FileEvent> {
        let start_pos = self.writer.current_pos();

        loop {
            let result = self
                .wrap_with_flush_timeout(body.next())
                .await
                .transpose()?;

            match result {
                None => {
                    // Timeout for flush.
                    self.flush_if_required().await;
                },
                Some(None) => break, // No more data
                Some(Some(chunk)) => {
                    // Chunk received
                    self.writer.write_all(&chunk).await?;
                },
            }
        }

        let end_pos = self.writer.current_pos();
        let footer = FileEvent::create(
            metadata.transaction_id,
            metadata.path,
            start_pos..end_pos,
        );

        self.write_footer(&footer).await?;

        Ok(footer)
    }

    #[instrument(skip_all)]
    async fn write_delete_blob(&mut self, metadata: Metadata) -> Result<FileEvent> {
        let footer = FileEvent::delete(metadata.transaction_id, metadata.path);

        self.write_footer(&footer).await?;

        Ok(footer)
    }

    #[instrument(skip_all)]
    async fn write_rename_blob(
        &mut self,
        metadata: Metadata,
        new_path: String,
    ) -> Result<FileEvent> {
        let footer = FileEvent::rename(metadata.transaction_id, metadata.path, new_path);

        self.write_footer(&footer).await?;

        Ok(footer)
    }

    async fn write_footer(&mut self, footer: &FileEvent) -> Result<()> {
        // Used in recovery of a tablet.
        let buffer = footer.to_bytes();
        self.writer.write_all(&buffer).await?;
        Ok(())
    }

    /// Wraps the provided future with a timeout that
    /// is aware of when the system next needs to flush.
    async fn wrap_with_flush_timeout<F, T>(&self, fut: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        if let Some(then) = self.first_write_after_flush {
            let remaining = self.flush_delay_window - then.elapsed();
            let wrapped = fut.map(Ok);

            match glommio::timer::timeout(remaining, wrapped).await {
                Err(GlommioError::TimedOut(_)) => None,
                Err(error) => {
                    warn!(error = ?error, "Runtime encountered an error while waiting for a timeout");
                    None // Should never happen.
                },
                Ok(res) => Some(res),
            }
        } else {
            let result = fut.await;
            Some(result)
        }
    }

    async fn flush_if_required(&mut self) {
        let Some(then) = self.first_write_after_flush else {
            return;
        };

        // Deadline has not elapsed yet. We can skip the flush step.
        if then.elapsed() < self.flush_delay_window {
            return;
        }

        let position = match self.writer.sync().await {
            Err(e) => {
                error!(error = ?e, "Failed to flush buffers due to error");
                return;
            },
            Ok(pos) => pos,
        };

        self.waker_controller.wake(position);
        self.first_write_after_flush = None;
    }
}

struct WriterEvent {
    ack: oneshot::Sender<Result<WriterResponse>>,
    kind: EventKind,
}

enum EventKind {
    Write(WriteEvent),
    Delete(DeleteEvent),
    Rename(RenameEvent),
}

struct WriteEvent {
    metadata: Metadata,
    body: Body,
}

struct DeleteEvent {
    metadata: Metadata,
}

struct RenameEvent {
    metadata: Metadata,
    new_path: String,
}

#[derive(Debug, Clone)]
/// The response of a successful writer op.
///
/// This contains the tablet ID that completed the operation
/// and the full [FileEvent] metadata.
pub struct WriterResponse {
    /// The ID of the tablet that completed this write.
    pub tablet_id: TabletId,
    /// The writer's current position in the file.
    pub writer_position: u64,
    /// The completed file event and associated metadata.
    pub event: FileEvent,
    /// The [FlushWaker] allows operations to wait until the
    /// operation is guarenteed to be persisted to disk.
    pub flush_waker: FlushWaker,
}

#[cfg_attr(test, mockall::automock)]
/// A set of event hooks triggered by the writer during various life cycle triggers.
pub trait WriterEventHook: Debug + Send + Sync {
    /// Triggered when the writer first starts.
    fn on_writer_start(&self, tablet_id: TabletId);

    /// Triggered when a new file event is completed.
    fn on_writer_response(&self, response: WriterResponse);

    /// Triggered when the writer closes.
    fn on_writer_close(&self, tablet_id: TabletId);
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::io::event::EventData;
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
            let metadata = Metadata {
                path: "example.txt".to_string(),
                transaction_id: None,
            };
            async move { writer.write(metadata, body).await }
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
            let metadata = Metadata {
                path: "example.txt".to_string(),
                transaction_id: None,
            };
            async move { writer.write(metadata, body).await }
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
        let metadata = Metadata {
            path: "example.txt".to_string(),
            transaction_id: None,
        };
        let response = writer
            .write(metadata, body)
            .await
            .expect("Write & flush body");
        match response.event.data {
            EventData::Create {
                file_path,
                data_range,
            } => {
                assert_eq!(data_range, 0..13);
                assert_eq!(file_path, "example.txt");
            },
            other => panic!("Expected create event got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_empty_buffer() {
        let _ = tracing_subscriber::fmt::try_init();

        let writer = create_test_writer(1);

        let body = Body::empty();
        let metadata = Metadata {
            path: "example.txt".to_string(),
            transaction_id: None,
        };
        let response = writer
            .write(metadata, body)
            .await
            .expect("Write & flush body");

        match response.event.data {
            EventData::Create {
                file_path,
                data_range,
            } => {
                assert_eq!(data_range, 0..0);
                assert_eq!(file_path, "example.txt");
            },
            other => panic!("Expected create event got {other:?}"),
        }
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

        let metadata = Metadata {
            path: "example.txt".to_string(),
            transaction_id: None,
        };
        let response = writer
            .write(metadata, body)
            .await
            .expect("Write & flush body");
        match response.event.data {
            EventData::Create {
                file_path,
                data_range,
            } => {
                assert_eq!(data_range, 0..num_bytes);
                assert_eq!(file_path, "example.txt");
            },
            other => panic!("Expected create event got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_actor_event_hooks() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();

        let mut mock_hook = MockWriterEventHook::new();
        mock_hook
            .expect_on_writer_start()
            .return_once(|_tablet_id| ());
        mock_hook
            .expect_on_writer_close()
            .return_once(|_tablet_id| ());
        mock_hook
            .expect_on_writer_response()
            .return_once(|_tablet_id| ());

        let options = TabletWriterOptions::builder()
            .max_active_writers(1)
            .max_tablet_size(2 << 10)
            .event_hooks(vec![Box::new(mock_hook) as Box<dyn WriterEventHook>])
            .base_path(temp_dir())
            .build();

        let writer = TabletWriter::new(options, dispatch);

        let body = Body::empty();
        let metadata = Metadata {
            path: "example.txt".to_string(),
            transaction_id: None,
        };
        let _response = writer
            .write(metadata, body)
            .await
            .expect("Write & flush body");

        drop(writer);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
