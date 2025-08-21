use std::io;
use std::sync::Arc;

use crate::PageId;
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
    next_page_id_to_decrypt: PageId,
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
            next_page_id_to_decrypt: PageId(0),
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
            &super::op_log_associated_data(
                self.reader.file_id(),
                self.next_page_id_to_decrypt,
                position,
            ),
            &mut self.scratch_space,
        )
        .map_err(LogDecodeError::Decode)?;

        if let Some(page_id) = block.last_page_id() {
            self.next_page_id_to_decrypt = page_id;
        }

        Ok(Some(block))
    }
}
