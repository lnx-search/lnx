use std::io;
use std::io::{ErrorKind, Result};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, instrument};

use crate::io::actors::tablet_writer::WriteResponse;
use crate::io::actors::ActorFactory;
use crate::io::{Metadata, RuntimeDispatcher, TabletWriterOptions};
use crate::metastore::TabletId;
use crate::Body;

#[derive(Clone)]
/// The handle for writing new files in the background without
/// waiting for writes to complete.
///
/// This handle can be cloned cheaply.
pub struct BasicWriter {
    tx: flume::Sender<WriterEvent>,
    _runtime: RuntimeDispatcher, // Used to keep the runtime alive while active.
}

impl BasicWriter {
    #[instrument(skip(runtime))]
    /// Creates a new [BasicWriter] with the given file path and [RuntimeDispatcher].
    pub async fn create(file_path: PathBuf, runtime: RuntimeDispatcher) -> Result<Self> {
        info!("Creating metadata background writer");
        let (tx, rx) = flume::bounded(256);

        let factory = BasicWriterActorFactory {
            file_path,
            events: rx,
        };

        runtime.spawn(factory).await?;

        Ok(Self {
            tx,
            _runtime: runtime,
        })
    }

    /// Submits a buffer to be written to the writer.
    ///
    /// This will not wait for to write to complete and flush.
    pub async fn write(&self, buffer: Vec<u8>) -> Result<()> {
        let event = WriterEvent::Write { buffer };

        self.tx
            .send_async(event)
            .await
            .map_err(super::writer_failed_to_start)?;

        Ok(())
    }

    /// Flushes the internal memory buffers to disk.
    pub async fn flush(&self) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        let event = WriterEvent::Flush { ack };

        self.tx
            .send_async(event)
            .await
            .map_err(super::writer_failed_to_start)?;

        rx.await.map_err(super::writer_closed)?
    }
}

/// A factory that creates a new [TabletWriterActor] for a given file path.
struct BasicWriterActorFactory {
    file_path: PathBuf,
    events: flume::Receiver<WriterEvent>,
}

#[async_trait(?Send)]
impl ActorFactory for BasicWriterActorFactory {
    async fn spawn_actor(self) -> Result<()> {
        let file = crate::io::utils::create_rw_file_glommio(&self.file_path).await?;

        info!("Tablet metadata file created, syncing directory");
        crate::io::utils::sync_directory_glommio(&self.file_path).await?;

        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(8)
            .with_buffer_size(1 << 10)
            .build();

        let actor = BasicWriterActor {
            file_path: self.file_path.clone(),
            events: self.events.clone(),
            writer,
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
/// The actor is only designed to live for as long as channel sender,
/// once this sender is dropped it will sync all buffers to disk
/// and exit gracefully.
pub struct BasicWriterActor {
    /// The file path of the file the actor is writing to.
    file_path: PathBuf,
    /// Incoming write events.
    ///
    /// A single write event represents a single contiguous blob
    /// and has no requirement on previous or future events being
    /// in the right order.
    events: flume::Receiver<WriterEvent>,
    writer: DmaStreamWriter,
}

impl BasicWriterActor {
    #[instrument("basic-writer", skip(self), fields(file_path = %self.file_path.display()))]
    async fn run(mut self) {
        info!("Writer is ready to process events");
        while let Ok(event) = self.events.recv_async().await {
            debug!("Handling IO event");
            self.handle_event(event).await;
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

    #[instrument(skip_all)]
    async fn handle_event(&mut self, event: WriterEvent) {
        match event {
            WriterEvent::Write { buffer } => {
                if let Err(e) = self.write_prefixed_buffer(buffer).await {
                    error!(error = ?e, "Failed to write buffer");
                }
            },
            WriterEvent::Flush { ack } => {
                let res = self
                    .writer
                    .sync()
                    .await
                    .map(|_| ())
                    .map_err(io::Error::from);
                let _ = ack.send(res);
            },
        }
    }

    /// Writes a buffer to the file with a prefix of the buffer length and crc32 checksum.
    async fn write_prefixed_buffer(&mut self, buffer: Vec<u8>) -> Result<()> {
        let buffer_len = buffer.len() as u32;
        let checksum = crc32fast::hash(&buffer);

        self.writer.write_all(&buffer_len.to_le_bytes()).await?;
        self.writer.write_all(&checksum.to_le_bytes()).await?;
        self.writer.write_all(&buffer).await?;

        Ok(())
    }
}

enum WriterEvent {
    Write { buffer: Vec<u8> },
    Flush { ack: oneshot::Sender<Result<()>> },
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};
    use std::time::Duration;

    use super::*;
    use crate::io::runtime;
    use crate::io::runtime::RuntimeOptions;

    async fn create_test_writer() -> (BasicWriter, tempfile::NamedTempFile) {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = runtime::create_io_runtime(rt_options).unwrap();
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let writer = BasicWriter::create(tmp_file.path().to_path_buf(), dispatch)
            .await
            .unwrap();
        (writer, tmp_file)
    }

    #[tokio::test]
    async fn test_write_buffer() {
        let _ = tracing_subscriber::fmt::try_init();

        let (writer, mut file) = create_test_writer().await;

        static CONTENT: &[u8] = b"Hello, world!";

        writer.write(CONTENT.to_vec()).await.expect("Write body");
        writer.flush().await.expect("Flush body");
        drop(writer);

        // Allow the actor to shut down and close the file.
        tokio::time::sleep(Duration::from_secs(1)).await;

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        assert_eq!(
            data.len(),
            CONTENT.len() + 4 + 4,
            "Data should have buffer + len and checksum"
        );
        assert_eq!(
            u32::from_le_bytes(data[0..4].try_into().unwrap()),
            CONTENT.len() as u32,
            "Written buffer length does not match"
        );
        assert_eq!(
            u32::from_le_bytes(data[4..8].try_into().unwrap()),
            crc32fast::hash(CONTENT),
            "Checksums should match"
        );
    }

    #[tokio::test]
    async fn test_write_empty_buffer() {
        let _ = tracing_subscriber::fmt::try_init();

        let (writer, mut file) = create_test_writer().await;

        writer.write(Vec::new()).await.expect("write body");
        writer.flush().await.expect("Flush body");
        drop(writer);

        tokio::time::sleep(Duration::from_secs(1)).await;

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        assert_eq!(
            data.len(),
            4 + 4,
            "Data should have buffer + len and checksum"
        );
        assert_eq!(
            u32::from_le_bytes(data[0..4].try_into().unwrap()),
            0,
            "Written buffer length does not match"
        );
        assert_eq!(
            u32::from_le_bytes(data[4..8].try_into().unwrap()),
            crc32fast::hash(&[]),
            "Checksums should match"
        );
    }
}
