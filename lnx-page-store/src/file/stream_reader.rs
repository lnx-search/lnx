use std::io::ErrorKind;
use std::sync::Arc;
use std::{cmp, io};

use crate::file::buffer::DmaBuffer;
use crate::file::{DISK_ALIGN, ctx, scheduler};

pub const READ_BUFFER_SIZE: usize = 32 << 10;
const MAX_READ_RETRY: usize = 2;

/// A builder for creating new [StreamReader]s.
pub struct StreamReaderBuilder {
    ctx: Arc<ctx::FileContext>,
    file: scheduler::RingFile,
    offset: u64,
    read_buffer_size: usize,
}

impl StreamReaderBuilder {
    /// Create a new [StreamReaderBuilder] using the target file.
    pub fn new(ctx: Arc<ctx::FileContext>, file: scheduler::RingFile) -> Self {
        Self {
            ctx,
            file,
            offset: 0,
            read_buffer_size: READ_BUFFER_SIZE,
        }
    }

    /// Start the reader from a target offset.
    ///
    /// This must be a multiple of [DISK_ALIGN].
    pub fn with_offset(mut self, offset: u64) -> Self {
        assert_eq!(
            offset % DISK_ALIGN as u64,
            0,
            "offset must be a multiple of DISK_ALIGN"
        );
        self.offset = offset;
        self
    }

    /// Set the buffer size used for each read IOP.
    ///
    /// This must be a multiple of [DISK_ALIGN].
    ///
    /// Defaults to 32KB.
    pub fn with_buffer_size(mut self, read_buffer_size: usize) -> Self {
        assert_ne!(read_buffer_size, 0, "buffer size must not be zero");
        assert_eq!(
            read_buffer_size % DISK_ALIGN,
            0,
            "buffer size must a multiple of DISK_ALIGN"
        );
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Build a new reader using the configured options.
    pub fn build(self) -> StreamReader {
        let read_buffer = self.ctx.alloc_pages(self.read_buffer_size / DISK_ALIGN);

        StreamReader {
            file: self.file,

            file_len: 0,
            file_len_init: false,
            file_cursor: self.offset,

            read_buffer,
            read_buffer_cursor: 0,
            read_buffer_init_end: 0,
        }
    }
}

/// A reader for scanning through a [scheduler::RingFile].
pub struct StreamReader {
    file: scheduler::RingFile,

    file_len: u64,
    file_len_init: bool,
    file_cursor: u64,

    read_buffer: DmaBuffer,
    read_buffer_cursor: usize,
    read_buffer_init_end: usize,
}

impl StreamReader {
    #[inline]
    /// Returns a reference to the inner file.
    pub fn file(&self) -> &scheduler::RingFile {
        &self.file
    }

    #[inline]
    /// Returns the unique ID assigned to the file.
    pub fn file_id(&self) -> scheduler::FileId {
        self.file.id()
    }

    #[inline]
    /// Returns the position the reader is at in the file.
    pub fn position(&self) -> u64 {
        self.file_cursor + self.read_buffer_cursor as u64
    }

    /// Read bytes from the reader into the buffer or return [ErrorKind::UnexpectedEof]
    /// if the buffer can not be filled entirely.
    pub async fn read_exact(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        let n = self.read(buffer).await?;
        if n != buffer.len() {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "could not fill buffer completely",
            ))
        } else {
            Ok(())
        }
    }

    /// Read N bytes from the reader and copy the data into the buffer.
    ///
    /// This returns the number of bytes read.
    pub async fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if !self.file_len_init {
            self.file_len = self.file.get_len().await?;
            self.file_len_init = true;
        }

        let mut read_n = output.len();
        let mut output_cursor = 0;
        loop {
            let n = self.fill_using_last_read(&mut output[output_cursor..]);
            read_n -= n;
            output_cursor += n;

            if read_n == 0 {
                return Ok(output.len());
            }

            // EOF
            if self.file_cursor >= self.file_len {
                return Ok(output.len() - read_n);
            }

            self.fill_buffer().await?;
        }
    }

    async fn fill_buffer(&mut self) -> io::Result<()> {
        let expected_len = cmp::min(
            (self.file_len - self.file_cursor) as usize,
            self.read_buffer.len(),
        );
        let mut result = self.try_fill_read_buffer().await?;

        // Retry the rare but possible short reads.
        let mut attempt = 0;
        while result != expected_len {
            attempt += 1;

            // It is incredibly rare for a read to return a size smaller
            // than request outside EOF, but we should retry if we get
            // one at least once, and then error if it happens again.
            if attempt > MAX_READ_RETRY {
                return Err(io::Error::new(
                    ErrorKind::BrokenPipe,
                    "kernel read returned small buffer after retries",
                ));
            }

            result = self.try_fill_read_buffer().await?;
        }

        self.file_cursor += expected_len as u64;
        self.read_buffer_cursor = 0;
        self.read_buffer_init_end = expected_len;

        Ok(())
    }

    async fn try_fill_read_buffer(&mut self) -> io::Result<usize> {
        let read_ptr = self.read_buffer.as_mut_ptr();
        let read_len = self.read_buffer.len();
        let read_guard = self.read_buffer.share_guard();

        // SAFETY: Read protected by the buffer guard
        let reply = unsafe {
            self.file
                .submit_read(read_ptr, read_len, self.file_cursor, Some(read_guard))
                .await?
        };

        let result = scheduler::get_reply_or_err(reply).await?;
        Ok(result as usize)
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.read_buffer[self.read_buffer_cursor..self.read_buffer_init_end]
    }

    fn fill_using_last_read(&mut self, output: &mut [u8]) -> usize {
        let buffered_data = self.remaining_buffer();
        let take_n = cmp::min(output.len(), buffered_data.len());
        output[..take_n].copy_from_slice(&buffered_data[..take_n]);
        self.read_buffer_cursor += take_n;
        take_n
    }
}

struct ScheduledRead {
    reply: i2o2::ReplyReceiver,
    buffer_start_pos: usize,
    buffer_end_pos: usize,
    offset: u64,
}
