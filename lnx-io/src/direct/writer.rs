use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};

use super::runtime::DirectIoRuntime;
use crate::{Context, IoWriteOp, IoWriterActor};

pub struct DirectIoWriter {
    writer: DmaStreamWriter,
    pending_bytes: usize,
}

#[async_trait(?Send)]
impl IoWriterActor for DirectIoWriter {
    type Runtime = DirectIoRuntime;
    type State = ();

    #[instrument("create", skip_all)]
    async fn create(
        _state: Self::State,
        ctx: &Context<IoWriteOp>,
    ) -> anyhow::Result<Self> {
        let file = DmaFile::create(&ctx.path).await?;
        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(128 << 10)
            .build();

        crate::io::utils::sync_directory(&ctx.path).await?;

        debug!("Created new file");

        Ok(Self {
            writer,
            pending_bytes: 0,
        })
    }

    #[instrument("write", skip_all)]
    async fn write_all(&mut self, buffer: Bytes) -> anyhow::Result<u64> {
        let pos = self.writer.current_pos();
        self.writer.write_all(&buffer).await?;
        self.pending_bytes += buffer.len();

        debug!(bytes_written = buffer.len(), "Write buffer");

        Ok(pos)
    }

    #[instrument("flush", skip_all)]
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.sync().await?;

        debug!(bytes_flushed = self.pending_bytes, "Flush buffer");
        self.pending_bytes = 0;

        Ok(())
    }
}
