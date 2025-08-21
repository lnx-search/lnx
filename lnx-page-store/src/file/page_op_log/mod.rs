//! The operations log tracks changes to pages in the data files.

#[cfg(all(test, not(feature = "test-miri"), feature = "bench-lib-unstable"))]
mod benches;
mod reader;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;
mod writer;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::{io, mem};

use parking_lot::Mutex;

pub use self::reader::{LogDecodeError, LogFileReader};
use crate::PageId;
use crate::file::{ctx, scheduler, utils};
use crate::layout::file_metadata::{self, Encryption};
use crate::layout::log::LogEntry;
use crate::layout::page_metadata::PageMetadata;
// TODO: Move writer bit into the writer file, probably cleaner

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The file metadata header used to identify the file and the type.
pub struct MetadataHeader {
    /// The unique ID of the log file.
    ///
    /// NOTE: This ID changes every time the WAL is flushed, although the disk
    /// allocation stays the same, the file itself is seen as 'new'.
    pub log_file_id: u32,
    /// Signals if the data in the log is encrypted or not.
    pub encryption: Encryption,
}

#[derive(Debug, Clone)]
/// Configuration options for the [OpLogWriter].
pub struct OpLogWriterOptions {
    /// The (soft) maximum size of a log file in bytes.
    pub max_file_size: u64,
    /// The maximum number of log files to keep open and reuse.
    pub max_reusable_log_files: usize,
    /// The directory to store log files.
    pub log_file_directory: PathBuf,
}

impl OpLogWriterOptions {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        let temp_dir = std::env::temp_dir().join(ulid::Ulid::new().to_string());
        Self {
            max_file_size: 512 << 10,
            max_reusable_log_files: 1,
            log_file_directory: temp_dir,
        }
    }
}

// Hacky but useful solution for automatic cleanup
#[cfg(test)]
impl Drop for OpLogWriterOptions {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.log_file_directory);
    }
}

/// The op log writer acts as a WAL for page metadata changes.
///
/// The log must only be written to once the page data itself is confirmed
/// to be persisted safely on disk (if the operation changes the data.)
///
/// The writer holds onto multiple log files which are rotated during the operation
/// of the system, once a log file is full or a given timeout has elapsed, the
/// log will be processed and the modified page files will have their metadata
/// entries updated. Once this is complete, the log will be marked as empty
/// and put back into rotation.
///
/// If the system cannot rollup the log in the background, it will force a writer
/// to rollup the log before being able to proceed.
pub struct OpLogWriter {
    /// The file context shared by all the writers.
    ctx: Arc<ctx::FileContext>,
    /// The IO scheduler to execute file IO on.
    io_scheduler: scheduler::IoScheduler,
    /// The active log writer file.
    active_writer: tokio::sync::Mutex<writer::LogFileWriter>,
    /// Open log files in rotation available for writing.
    files: Mutex<VecDeque<writer::LogFileWriter>>,
    /// Writer configuration.
    config: OpLogWriterOptions,
}

impl OpLogWriter {
    /// Write a set of log entries and ensure they are durable.
    pub async fn write_log_entries<I>(&self, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (LogEntry, Option<PageMetadata>)>,
    {
        let mut writer = self.active_writer.lock().await;

        if writer.position() >= self.config.max_file_size {
            let new_writer = self.rotate_log_writer().await?;
            tracing::info!(new_writer_id = new_writer.id(), "rotating log file writer");
            let completed_writer_file =
                mem::replace(&mut *writer, new_writer).into_ring_file();
        }

        for (entry, metadata) in entries {
            // NOTE: The writer automatically resets if an error occurs before the flush.
            writer.write_log(entry, metadata).await?;
        }
        writer.sync().await?;

        Ok(())
    }

    /// Close any open log writers.
    pub async fn close(&self) {
        let mut writer = self.active_writer.lock().await;
        if let Err(err) = writer.close().await {
            tracing::error!(error = ?err, "failed to close writer");
        }

        let files = self.files.lock().drain(..).collect::<Vec<_>>();

        for mut writer in files {
            if let Err(err) = writer.close().await {
                tracing::error!(error = ?err, "failed to close spare writer");
            }
        }
    }

    /// Gets the next spare [writer::LogFileWriter] if there is one available
    /// or create a new writer.
    async fn rotate_log_writer(&self) -> io::Result<writer::LogFileWriter> {
        {
            let mut free_writer_queue = self.files.lock();
            if let Some(writer) = free_writer_queue.pop_front() {
                return Ok(writer);
            }
        }

        self.create_new_log_writer().await
    }

    /// Creates a new [writer::LogFileWriter] that is ready to process entries.
    async fn create_new_log_writer(&self) -> io::Result<writer::LogFileWriter> {
        let mut ring_file = self.open_new_file().await?;

        let header = MetadataHeader {
            log_file_id: ring_file.id(),
            encryption: self.ctx.get_encryption_status(),
        };
        initialise_log_file(&self.ctx, &mut ring_file, &header).await?;

        let writer = writer::LogFileWriter::new(
            self.ctx.clone(),
            ring_file,
            file_metadata::HEADER_SIZE as u64,
        );

        Ok(writer)
    }

    /// Creates a new file with a randomly assigned file ID.
    ///
    /// The file ID will be regenerated if a file already exists with
    /// the generated ID.
    async fn open_new_file(&self) -> io::Result<scheduler::RingFile> {
        loop {
            let file_id = fastrand::u32(1000..);
            let file_path = self.config.log_file_directory.join(log_file_name(file_id));

            let result = tokio::task::spawn_blocking(move || {
                utils::create_file(&file_path, false)
            })
            .await
            .expect("failed to spawn background thread");

            match result {
                Ok(file) => {
                    return self.io_scheduler.make_ring_file(file_id, file).await;
                },
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }
    }
}

/// Writes the log file header and ensure it is persisted to disk.
async fn initialise_log_file(
    ctx: &ctx::FileContext,
    ring_file: &mut scheduler::RingFile,
    header: &MetadataHeader,
) -> io::Result<()> {
    let mut header_bytes = vec![0; file_metadata::HEADER_SIZE];
    file_metadata::encode_metadata(
        ctx.cipher(),
        &op_log_associated_data(ring_file.id(), PageId(0), 0),
        &header,
        &mut header_bytes,
    )
    .map_err(io::Error::other)?;

    ring_file.write_buffer(header_bytes, 0).await?;
    ring_file.fdatasync().await?;

    Ok(())
}

fn log_file_name(file_id: u32) -> String {
    format!("{file_id:010}.log.lnx")
}

/// Computes the associated data to tag file data with.
///
/// This method is used on all files and is used to prevent replay attacks
/// and a bad actor gaining information about the system by taking and swapping
/// around data in the files.
pub fn op_log_associated_data(
    file_id: u32,
    last_page_id: PageId,
    start_pos: u64,
) -> [u8; 16] {
    let mut buffer = [0; 16];
    buffer[0..4].copy_from_slice(&file_id.to_le_bytes());
    buffer[4..8].copy_from_slice(&last_page_id.0.to_be_bytes());
    buffer[8..16].copy_from_slice(&start_pos.to_le_bytes());
    buffer
}

#[cfg(all(test, not(feature = "test-miri")))]
mod test_misc {
    use super::*;

    #[test]
    fn test_associated_data() {
        let data = op_log_associated_data(1, PageId(0), 4);
        assert_eq!(data, [1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[rstest::rstest]
    #[case(1, "0000000001.log.lnx")]
    #[case(8765, "0000008765.log.lnx")]
    #[case(123456789, "0123456789.log.lnx")]
    fn test_log_file_name(#[case] file_id: u32, #[case] expected_str: &str) {
        let name = log_file_name(file_id);
        assert_eq!(name, expected_str);
    }
}
