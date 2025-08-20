use std::io::ErrorKind;
use std::sync::Arc;
use std::{cmp, io};

use crate::file::buffer::DmaBuffer;
use crate::file::ctx::associated_data;
use crate::file::utils::{align_down, align_up};
use crate::file::{DISK_ALIGN, ctx, scheduler};
use crate::layout::log;

const BUFFER_SIZE: usize = 128 << 10;

#[derive(Debug, thiserror::Error)]
/// An error that prevented the reader from decoding a log block.
pub enum LogDecodeError {
    #[error(transparent)]
    /// An IO error occurred.
    IO(#[from] io::Error),
    #[error(transparent)]
    /// The decoder could not process the buffer.
    Decode(log::DecodeLogBlockError),
}

/// The [LogFileReader] decodes
pub struct LogFileReader {
    ctx: Arc<ctx::FileContext>,
    file: scheduler::RingFile,

    read_buffer: DmaBuffer,
    buffer_size: usize,
    buffer_cursor: usize,

    current_absolute_pos: u64,
    file_cursor: u64,
    file_len: u64,
    eof: bool,
}

impl LogFileReader {
    /// Create a new [LogFileReader] using the provided file context, file and offset.
    ///
    /// NOTE: This does _not_ validate the header of the file, it assumes the file
    /// is already valid
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

        let read_buffer = ctx.alloc::<BUFFER_SIZE>();

        Self {
            ctx,
            file,
            buffer_size: 0,
            buffer_cursor: read_buffer.len(),
            read_buffer,
            current_absolute_pos: log_offset,
            file_cursor: log_offset,
            file_len: 0,
            eof: false,
        }
    }

    /// Retrieve the next log block in the file.
    ///
    /// Returns `Ok(None)` once at EOF.
    pub async fn next_block(&mut self) -> Result<Option<log::LogBlock>, LogDecodeError> {
        if self.file_len == 0 {
            self.file_len = self.file.get_len().await?;
        }

        if self.buffer_cursor >= self.buffer_size {
            if self.file_cursor >= self.file_len || self.eof {
                return Ok(None);
            }
            self.read_buffer().await?;
        }

        let block_buffer =
            &mut self.read_buffer[self.buffer_cursor..][..log::LOG_BLOCK_SIZE];

        let block = log::decode_log_block(
            self.ctx.cipher(),
            &associated_data(self.file.id(), self.current_absolute_pos),
            block_buffer,
        )
        .map_err(LogDecodeError::Decode)?;

        self.buffer_cursor += log::LOG_BLOCK_SIZE;
        self.current_absolute_pos += log::LOG_BLOCK_SIZE as u64;

        Ok(Some(block))
    }

    async fn read_buffer(&mut self) -> io::Result<()> {
        let bytes_remaining_on_file = self.file_len - self.file_cursor;
        let read_len =
            cmp::min(self.read_buffer.len(), bytes_remaining_on_file as usize);

        // This should basically never iterate more than once, and should maintain
        // alignment, but we handle this situation anyway.
        let mut bytes_read = 0;
        while bytes_read < read_len {
            let guard = self.read_buffer.share_guard();
            let read_ptr = self.read_buffer.as_mut_ptr();
            let read_n = align_up(read_len - bytes_read, DISK_ALIGN);

            let reply = unsafe {
                self.file
                    .submit_read(read_ptr, read_n, self.file_cursor, Some(guard))
                    .await?
            };

            let result = reply
                .await
                .map_err(|_| io::Error::other("io scheduler panicked"))?;

            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }

            bytes_read += result as usize;

            let advance_by = align_down(result as usize, DISK_ALIGN) as u64;
            self.file_cursor += advance_by;
            self.eof = advance_by == 0;
        }

        self.buffer_cursor = 0;
        self.buffer_size = bytes_read;

        Ok(())
    }
}
