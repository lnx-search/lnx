use std::io::ErrorKind;
use std::sync::Arc;
use std::{io, mem};

use crate::file::buffer::DmaBuffer;
use crate::file::ctx::associated_data;
use crate::file::utils::{align_down, align_up};
use crate::file::{DISK_ALIGN, DynamicGuard, ctx, scheduler};
use crate::layout::log;
use crate::layout::log::LogEntry;
use crate::layout::page_metadata::PageMetadata;

const BUFFER_SIZE: usize = 128 << 10;
const SEQUENCE_ID_START: u32 = 1;

/// The [LogFileWriter] acts like a WAL for operations occurring on the page store,
/// it only logs the metadata operations however, so any data writes should be safely
/// persisted before writing to this log.
///
/// The writer will internally buffer logs into blocks forming 512b chunks, which are then
/// buffered in memory before being flushed to disk.  The data is written in a way that
/// prevents torn-writes.
///
/// The file has a close-on-error semantic, meaning when an error occurs the writer
/// will be closed and no new operations will be available.
/// This is done in order to prevent accidental corruption of phantom data.
pub struct LogFileWriter {
    ctx: Arc<ctx::FileContext>,
    file: scheduler::RingFile,

    log_offset: u64,
    current_pos: u64,

    /// The log block that is currently being filled with [LogEntry]s.
    /// When this is filled it produces a 512 byte block which is then
    /// written to the `block_buffer`.
    wip_block: log::LogBlock,
    /// The in-memory buffer of log blocks before they are written to disk.
    /// This is used to optimise the number of IOPs submitted to the IO scheduler.
    block_buffer: DmaBuffer,
    /// The offset that points to the end of the end of the initialised buffer,
    /// aka the end of where log blocks have been written to.
    block_offset: usize,
    /// The position in the buffer that has been submitted for writing to disk.
    block_buffer_write_pos: usize,

    /// The unique monotonic ID assigned to each log entry.
    next_sequence_id: u32,
    /// The sequence ID of the entry flushed to disk but not guaranteed to be
    /// durable.
    flushed_sequence_id: u32,
    /// The sequence ID of the last successful durability flush.
    durable_sequence_id: u32,

    inflight_iop: Option<InflightIop>,
}

impl LogFileWriter {
    /// Create a new [LogFileWriter] using the provided file context, file and offset.
    pub fn new(
        ctx: Arc<ctx::FileContext>,
        file: scheduler::RingFile,
        log_offset: u64,
    ) -> Self {
        assert_eq!(
            log_offset as usize % DISK_ALIGN,
            0,
            "log offset must be a multiple of the disk alignment"
        );

        let buffer = ctx.alloc::<BUFFER_SIZE>();

        Self {
            ctx,
            file,

            log_offset,
            current_pos: 0,

            wip_block: log::LogBlock::default(),
            block_buffer: buffer,
            block_offset: log::LOG_BLOCK_SIZE,
            block_buffer_write_pos: 0,

            next_sequence_id: SEQUENCE_ID_START,
            flushed_sequence_id: 0,
            durable_sequence_id: 0,

            inflight_iop: None,
        }
    }

    #[inline]
    /// Returns writer file ID.
    pub fn id(&self) -> u64 {
        self.file.id()
    }

    #[inline]
    /// Returns whether the file is closed.
    pub fn is_closed(&self) -> bool {
        self.file.is_closed()
    }

    #[inline]
    /// Returns whether the file is locked out due to a prior error.
    pub fn is_locked_out(&self) -> bool {
        self.file.is_locked_out()
    }

    #[inline]
    /// Returns the sequence ID the writer is sitting at.
    pub fn current_sequence_id(&self) -> u32 {
        self.next_sequence_id - 1
    }

    #[inline]
    /// Returns the last sequence ID that was written to disk.
    pub fn flushed_sequence_id(&self) -> u32 {
        self.flushed_sequence_id
    }

    #[inline]
    /// Returns the last sequence ID that was flushed
    /// to disk and made durable.
    pub fn durable_sequence_id(&self) -> u32 {
        self.durable_sequence_id
    }

    #[inline]
    /// Returns the position of the writer cursor.
    ///
    /// NOTE: This is not strictly tied to the file cursor, instead it is
    ///       the absolute position of the next block as if it is about to be written.
    pub fn position(&self) -> u64 {
        self.get_absolute_block_position()
    }

    /// Consume the writer and return the inner ring file.
    pub fn into_ring_file(self) -> scheduler::RingFile {
        self.file
    }

    /// Write a set of blocks to the log file at the current position.
    ///
    /// The `sequence_id` and `last_flush_sequence_id` fields will be overwritten.
    ///
    /// WARNING: This does not strictly flush data to disk! You must call `sync()` separately
    /// to persist the data safely.
    pub async fn write_log(
        &mut self,
        entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        self.ensure_file_writeable()?;
        let result = self.write_log_inner(entry, metadata).await;
        if result.is_err() {
            self.reset_to_last_flush();
        }
        result
    }

    /// Flush the buffered log data to disk and ensure it is safely persisted.
    ///
    /// Returns the position the file is flushed up to.
    pub async fn sync(&mut self) -> io::Result<()> {
        self.ensure_file_writeable()?;
        let result = self.sync_inner().await;
        if result.is_err() {
            self.reset_to_last_flush();
        }
        result
    }

    /// Close the writer file.
    pub async fn close(&mut self) -> io::Result<()> {
        self.file.close().await
    }

    pub(self) async fn write_log_inner(
        &mut self,
        mut entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        self.assign_writer_context(&mut entry);

        let (entry, metadata) = match self.wip_block.push_entry(entry, metadata) {
            Ok(()) => return Ok(()),
            Err(pair) => pair,
        };

        self.flush_log_block_to_mem()?;

        // When the block is full, we can reset it as the alignment will be maintained.
        self.wip_block.reset();
        self.block_offset += log::LOG_BLOCK_SIZE;

        let result = self.wip_block.push_entry(entry, metadata);
        assert!(result.is_ok(), "block should never be full after reset");

        if self.block_offset >= self.block_buffer.len() {
            tracing::trace!("memory buffer capacity reached, flushing...");
            self.write_buffer().await?;
        }

        Ok(())
    }

    pub(self) async fn sync_inner(&mut self) -> io::Result<()> {
        // Flush any intermediate buffers.
        self.flush_log_block_to_mem()?;
        self.write_buffer().await?;

        if let Some(iop) = self.inflight_iop.take() {
            tracing::trace!("waiting for inflight IOP to complete");
            complete_iop(iop).await?;
        }
        self.file.fdatasync().await?;

        // Update the currently flushed sequence ID.
        self.durable_sequence_id = self.next_sequence_id - 1;

        Ok(())
    }

    fn flush_log_block_to_mem(&mut self) -> io::Result<()> {
        let absolute_position_on_disk = self.get_absolute_block_position();

        let buffer_start = self.block_offset - log::LOG_BLOCK_SIZE;
        let buffer = &mut self.block_buffer[buffer_start..][..log::LOG_BLOCK_SIZE];
        log::encode_log_block(
            self.ctx.cipher(),
            &associated_data(self.file.id(), absolute_position_on_disk),
            &self.wip_block,
            buffer,
        )
        .map_err(io::Error::other)?;

        Ok(())
    }

    /// Works out the absolute position of the block at it will be in the file.
    fn get_absolute_block_position(&self) -> u64 {
        self.log_offset
            + self.current_pos
            + (self.block_offset - log::LOG_BLOCK_SIZE) as u64
    }

    /// Submit the current memory buffer to the IO scheduler for writing
    /// and wait on the last submitted iop if applicable.
    async fn write_buffer(&mut self) -> io::Result<()> {
        let delta_len = self.block_offset - self.block_buffer_write_pos;
        let aligned_len = align_up(delta_len, DISK_ALIGN);
        let buffer = &self.block_buffer[self.block_buffer_write_pos..][..aligned_len];
        let expected_write_size = buffer.len();
        let write_offset = self.log_offset + self.current_pos;

        let buffer_ptr = buffer.as_ptr();
        let buffer_len = buffer.len();

        tracing::debug!(
            offset = write_offset,
            len = buffer_len,
            "flushing memory buffer to disk"
        );

        // We advance the write pos cursor while still maintaining alignment.
        // We can do this because future writes will replay the unaligned chunk
        // of the buffer until it is long enough to be aligned.
        self.block_buffer_write_pos += align_down(delta_len, DISK_ALIGN);

        // Advance the file cursor, for the same reason as the block buffer pos
        // we only advance the cursor by aligned steps.
        self.current_pos += align_down(delta_len, DISK_ALIGN) as u64;

        let guard = if self.block_offset >= self.block_buffer.len() {
            let buffer = self.take_memory_buffer();
            Arc::new(buffer) as DynamicGuard
        } else {
            self.block_buffer.share_guard() as DynamicGuard
        };

        // SAFETY: our op is safe to send across the thread boundaries and the buffer
        //         is guaranteed to live at least as long as the ring requires as it
        //         is passed to our ring guard.
        let reply = unsafe {
            self.file
                .submit_write(buffer_ptr, buffer_len, write_offset, Some(guard))
                .await?
        };

        let iop = InflightIop {
            reply,
            expected_write_size,
        };

        // We don't immediately wait for the reply as we don't actually care if it completes
        // until we flush. However, if reply is already set, we will attempt to get
        // the result.
        if let Some(iop) = self.inflight_iop.replace(iop) {
            complete_iop(iop).await?;
        }

        self.flushed_sequence_id = self.next_sequence_id - 1;

        Ok(())
    }

    fn assign_writer_context(&mut self, entry: &mut LogEntry) {
        entry.sequence_id = self.next_sequence_id;
        entry.last_flush_sequence_id = self.durable_sequence_id;
        self.next_sequence_id += 1;
    }

    fn ensure_file_writeable(&mut self) -> io::Result<()> {
        self.file.ensure_safe_state()
    }

    fn take_memory_buffer(&mut self) -> DmaBuffer {
        let new_buffer = self.ctx.alloc::<BUFFER_SIZE>();
        let block_buffer = mem::replace(&mut self.block_buffer, new_buffer);
        self.block_buffer_write_pos = 0;
        self.block_offset = log::LOG_BLOCK_SIZE;
        block_buffer
    }

    /// Reset the current log block, memory buffer and cursors
    /// to start from the last successful flush position
    fn reset_to_last_flush(&mut self) {
        tracing::info!("resetting log writer to last flush checkpoint");
        self.wip_block.reset();
        let _ = self.take_memory_buffer();
        self.durable_sequence_id = self.flushed_sequence_id;
        self.next_sequence_id = self.flushed_sequence_id + 1;
    }
}

async fn complete_iop(iop: InflightIop) -> io::Result<()> {
    let InflightIop {
        reply,
        expected_write_size,
    } = iop;

    let result = if let Ok(result) = reply.try_get_result() {
        result
    } else {
        reply
            .await
            .map_err(|_| io::Error::other("io scheduler panicked"))?
    };

    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else if result as usize != expected_write_size {
        Err(io::Error::new(
            ErrorKind::StorageFull,
            "storage failed to allocate",
        ))
    } else {
        Ok(())
    }
}

struct InflightIop {
    reply: i2o2::ReplyReceiver,
    expected_write_size: usize,
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::PageFileId;
    use crate::layout::log::LogOp;

    #[tokio::test]
    async fn test_writer_sequence_id() {
        let ctx = Arc::new(ctx::FileContext::for_test(false));
        let scheduler = scheduler::IoScheduler::for_test();
        let tmp_file = tempfile::tempfile().unwrap();

        let file = scheduler
            .make_ring_file(1, tmp_file)
            .await
            .expect("Failed to make ring file");

        let mut writer = LogFileWriter::new(ctx, file, 0);

        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };

        writer.write_log(entry, None).await.expect("write log");
        assert_eq!(writer.next_sequence_id, 2);
        assert_eq!(writer.durable_sequence_id, 0);

        let entries = writer.wip_block.entries();
        assert_eq!(entries.len(), 1);
        let entry = entries[0].log;
        assert_eq!(entry.sequence_id, 1);
        assert_eq!(entry.last_flush_sequence_id, 0);

        writer.sync().await.expect("sync log");
        assert_eq!(writer.durable_sequence_id, 1);

        writer.write_log(entry, None).await.expect("write log");
        assert_eq!(writer.next_sequence_id, 3);
        assert_eq!(writer.durable_sequence_id, 1);

        let entries = writer.wip_block.entries();
        assert_eq!(entries.len(), 2);
        let entry = entries[1].log;
        assert_eq!(entry.sequence_id, 2);
        assert_eq!(entry.last_flush_sequence_id, 1);
    }

    #[tokio::test]
    async fn test_writer_reset_on_error() {
        let ctx = Arc::new(ctx::FileContext::for_test(false));
        let scheduler = scheduler::IoScheduler::for_test();
        let tmp_file = tempfile::tempfile().unwrap();

        let file = scheduler
            .make_ring_file(1, tmp_file)
            .await
            .expect("Failed to make ring file");

        let mut writer = LogFileWriter::new(ctx, file, 0);

        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.expect("write log");
        writer.sync().await.expect("sync log");
        assert_eq!(writer.next_sequence_id, 2);
        assert_eq!(writer.flushed_sequence_id, 1);
        assert_eq!(writer.durable_sequence_id, 1);

        writer.write_log(entry, None).await.expect("write log");
        assert_eq!(writer.next_sequence_id, 3);

        let scenario = fail::FailScenario::setup();
        fail::cfg("ringfile::submit_write", "return(-4)").unwrap();
        tokio::task::yield_now().await;

        writer
            .write_log(entry, None)
            .await
            .expect("write log should not error because it is memory buffered");
        writer.sync().await.expect_err("sync log should error");
        assert_eq!(writer.next_sequence_id, 2);
        assert_eq!(writer.flushed_sequence_id, 1);
        assert_eq!(writer.durable_sequence_id, 1);

        scenario.teardown();
    }
}
