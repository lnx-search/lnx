use std::fs;
use std::io::{BufWriter, Write};

use async_trait::async_trait;
use bytes::Bytes;

use super::runtime::BufferedIoRuntime;
use crate::{Context, IoWriteOp, IoWriterActor};

/// A buffered IO writer.
///
/// This wraps the std lib's file but ensures the flush operation
/// actually syncs the data to disk.
pub struct BufferedIoWriter {
    writer: BufWriter<SyncOnFlushFile>,
    pending_bytes: usize,
}

#[async_trait(?Send)]
impl IoWriterActor for BufferedIoWriter {
    type Runtime = BufferedIoRuntime;
    type State = ();

    #[instrument("create", skip_all)]
    async fn create(
        _state: Self::State,
        ctx: &Context<IoWriteOp>,
    ) -> anyhow::Result<Self> {
        let file = fs::File::create(ctx.path.as_path())?;
        let writer = BufWriter::new(SyncOnFlushFile::new(file));

        crate::utils::sync_directory(&ctx.path).await?;

        debug!("Created new file");

        Ok(Self {
            writer,
            pending_bytes: 0,
        })
    }

    #[instrument("write", skip_all)]
    async fn write_all(&mut self, buffer: Bytes) -> anyhow::Result<u64> {
        let pos = self.writer.get_ref().pos;
        self.writer.write_all(&buffer)?;
        self.pending_bytes += buffer.len();

        debug!(bytes_written = buffer.len(), "Write buffer");

        Ok(pos)
    }

    #[instrument("flush", skip_all)]
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()?;

        debug!(bytes_flushed = self.pending_bytes, "Flush buffer");
        self.pending_bytes = 0;

        Ok(())
    }
}

pub struct SyncOnFlushFile {
    inner: fs::File,
    pos: u64,
}

impl SyncOnFlushFile {
    fn new(file: fs::File) -> Self {
        Self {
            inner: file,
            pos: 0,
        }
    }
}

impl Write for SyncOnFlushFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let num_bytes = self.inner.write(buf)?;
        self.pos += num_bytes as u64;
        Ok(num_bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
}
