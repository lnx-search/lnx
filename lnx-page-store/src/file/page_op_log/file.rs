use std::io::ErrorKind;
use std::sync::Arc;
use std::{io, mem};

use i2o2::opcode::FSyncMode;

use crate::file::buffer::DmaBuffer;
use crate::file::ctx::associated_date;
use crate::file::utils::{align_down, align_up};
use crate::file::{DISK_ALIGN, DynamicGuard, ctx};
use crate::layout::log;
use crate::layout::log::LogEntry;
use crate::layout::page_metadata::PageMetadata;

const BUFFER_SIZE: usize = 128 << 10;

pub struct LogFileWriter {
    ctx: Arc<ctx::FileContext>,
    scheduler_handle: i2o2::I2o2Handle<DynamicGuard>,

    file_id: u64,
    file_ring_id: u32,
    file: std::fs::File,
    closed: bool,

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

    inflight_iop: Option<InflightIop>,
}

impl LogFileWriter {
    /// Write a set of blocks to the log file at the current position.
    ///
    /// WARNING: This does not strictly flush data to disk! You must call `sync()` separately
    /// to persist the data safely.
    pub async fn write_log(
        &mut self,
        entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        let result = self.write_log_inner(entry, metadata).await;
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    pub(self) async fn write_log_inner(
        &mut self,
        entry: LogEntry,
        metadata: Option<PageMetadata>,
    ) -> io::Result<()> {
        self.ensure_not_closed()?;

        let (entry, metadata) = match self.wip_block.push_entry(entry, metadata) {
            Ok(()) => return Ok(()),
            Err(pair) => pair,
        };

        self.flush_log_block_to_mem()?;

        let result = self.wip_block.push_entry(entry, metadata);
        assert!(result.is_ok(), "block should never be full after reset");

        // We advance the offset by the full
        self.block_offset += log::LOG_BLOCK_SIZE;
        if self.block_offset >= self.block_buffer.len() {
            self.write_buffer().await?;
        }

        Ok(())
    }

    /// Flush the buffered log data to disk and ensure it is safely persisted.
    ///
    /// Returns the position the file is flushed up to.
    pub async fn sync(&mut self) -> io::Result<u64> {
        let result = self.sync_inner().await;
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    pub(self) async fn sync_inner(&mut self) -> io::Result<u64> {
        self.ensure_not_closed()?;

        // Flush any intermediate buffers.
        self.flush_log_block_to_mem()?;
        self.write_buffer().await?;

        if let Some(iop) = self.inflight_iop.take() {
            complete_iop(iop).await?;
        }

        let op = i2o2::opcode::Fsync::new(
            i2o2::types::Fixed(self.file_ring_id),
            FSyncMode::Data,
        );

        let reply = unsafe {
            self.scheduler_handle
                .submit_async(op, None)
                .await
                .map_err(io::Error::other)?
        };

        let result = reply
            .await
            .map_err(|_| io::Error::other("io scheduler panicked"))?;

        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }

        Ok(self.current_pos)
    }

    fn flush_log_block_to_mem(&mut self) -> io::Result<()> {
        let buffer = &mut self.block_buffer[self.block_offset..][..log::LOG_BLOCK_SIZE];
        log::encode_log_block(
            self.ctx.cipher(),
            &associated_date(self.file_id, self.current_pos),
            &self.wip_block,
            buffer,
        )
        .map_err(io::Error::other)?;
        self.wip_block.reset();
        Ok(())
    }

    /// Submit the current memory buffer to the IO scheduler for writing
    /// and wait on the last submitted iop if applicable.
    async fn write_buffer(&mut self) -> io::Result<()> {
        let delta_len = self.block_offset - self.block_buffer_write_pos;
        let aligned_len = align_up(delta_len, DISK_ALIGN);
        let buffer = &self.block_buffer[self.block_buffer_write_pos..][..aligned_len];
        let expected_write_size = buffer.len();

        // We advance the write pos cursor while still maintaining alignment.
        // We can do this because future writes will replay the unaligned chunk
        // of the buffer until it is long enough to be aligned.
        self.block_buffer_write_pos += align_down(delta_len, DISK_ALIGN);

        let op = i2o2::opcode::Write::new(
            i2o2::types::Fixed(self.file_ring_id),
            buffer.as_ptr(),
            buffer.len(),
            self.current_pos,
        );
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
            self.scheduler_handle
                .submit_async(op, Some(guard))
                .await
                .map_err(io::Error::other)?
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

        Ok(())
    }

    fn ensure_not_closed(&mut self) -> io::Result<()> {
        if self.closed {
            Err(io::Error::other("log file writer closed"))
        } else {
            Ok(())
        }
    }

    fn take_memory_buffer(&mut self) -> DmaBuffer {
        let new_buffer = self.ctx.alloc::<BUFFER_SIZE>();
        let block_buffer = mem::replace(&mut self.block_buffer, new_buffer);
        self.block_buffer_write_pos = 0;
        self.block_offset = 0;
        block_buffer
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
