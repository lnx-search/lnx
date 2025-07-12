use std::io::ErrorKind;
use std::sync::Arc;
use std::{io, mem};

use i2o2::opcode::FSyncMode;

use crate::file::buffer::DmaBuffer;
use crate::file::ctx::associated_date;
use crate::file::{DynamicGuard, ctx};
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

    log_offset: u64,
    current_pos: u64,

    wip_block: log::LogBlock,
    block_buffer: DmaBuffer,
    block_offset: usize,

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
        let (entry, metadata) = match self.wip_block.push_entry(entry, metadata) {
            Ok(()) => return Ok(()),
            Err(pair) => pair,
        };

        let buffer = &mut self.block_buffer
            [self.block_offset..self.block_offset + log::LOG_BLOCK_SIZE];
        log::encode_log_block(
            self.ctx.cipher(),
            &associated_date(self.file_id, self.current_pos),
            &self.wip_block,
            buffer,
        )
        .map_err(io::Error::other)?;

        self.wip_block.reset();
        let result = self.wip_block.push_entry(entry, metadata);
        assert!(result.is_ok(), "block should never be full after reset");

        // We advance the offset by the full
        self.block_offset += log::LOG_BLOCK_SIZE;
        if self.block_offset >= self.block_buffer.len() {
            self.write_buffer().await?;
        }

        Ok(())
    }

    pub async fn flush_block_buffer(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Flush the buffered log data to disk and ensure it is safely persisted.
    ///
    /// Returns the position the file is flushed up to.
    pub async fn sync(&mut self) -> io::Result<u64> {
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

    /// Submit the current memory buffer to the IO scheduler for writing
    /// and wait on the last submitted iop if applicable.
    ///
    /// This does _not_ advance the file cursor.
    async fn write_buffer(&mut self) -> io::Result<()> {
        let mut block_buffer =
            mem::replace(&mut self.block_buffer, self.ctx.alloc::<BUFFER_SIZE>());
        let buffer_len = block_buffer.len();
        self.block_offset = 0;

        let op = i2o2::opcode::Write::new(
            i2o2::types::Fixed(self.file_ring_id),
            block_buffer.as_mut_ptr(),
            buffer_len,
            self.current_pos,
        );

        // SAFETY: our op is safe to send across the thread boundaries and the buffer
        //         is guaranteed to live at least as long as the ring requires as it
        //         is passed to our ring guard.
        let reply = unsafe {
            self.scheduler_handle
                .submit_async(op, Some(Arc::new(block_buffer)))
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
