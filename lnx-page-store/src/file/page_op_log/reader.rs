use std::io;
use std::sync::Arc;

use crate::file::ctx::associated_data;
use crate::file::stream_reader::{StreamReader, StreamReaderBuilder};
use crate::file::{ctx, scheduler};
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
    reader: StreamReader,
    scratch_space: [u8; log::LOG_BLOCK_SIZE],
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
        let reader = StreamReaderBuilder::new(ctx.clone(), file)
            .with_offset(log_offset)
            .with_buffer_size(BUFFER_SIZE)
            .build();
        Self {
            ctx,
            reader,
            scratch_space: [0; log::LOG_BLOCK_SIZE],
        }
    }

    /// Retrieve the next log block in the file.
    ///
    /// Returns `Ok(None)` once at EOF.
    pub async fn next_block(&mut self) -> Result<Option<log::LogBlock>, LogDecodeError> {
        let position = self.reader.position();

        let result = self.reader.read_exact(&mut self.scratch_space).await;

        match result {
            Ok(()) => {},
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            },
            Err(err) => return Err(err.into()),
        }

        let block = log::decode_log_block(
            self.ctx.cipher(),
            &associated_data(self.reader.file_id(), position),
            &mut self.scratch_space,
        )
        .map_err(LogDecodeError::Decode)?;

        Ok(Some(block))
    }
}
