mod runtime;
mod combiner;
mod utils;

use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use datacake_crdt::HLCTimestamp;
use futures_lite::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder};
pub use runtime::try_init;
use crate::Metadata;
use crate::metadata::write_metadata_offsets_aio;

pub(crate) const BUFFER_SIZE: usize = 512 << 10;

pub struct AioWriter {
    inner: DmaStreamWriter,
    metadata: Metadata,
    path: PathBuf,
}

impl Deref for AioWriter {
    type Target = Metadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for AioWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl AioWriter {
    /// Create a new aio writer.
    pub async fn create(
        path: &Path,
        size_hint: usize,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let file = glommio::io::OpenOptions::new()
            .create(true)
            .write(true)
            .dma_open(path)
            .await?;

        let allocate = (size_hint % 1024) * 1024;
        if allocate != 0 {
            file.pre_allocate(allocate as u64).await?;
        }

        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(512 << 10)
            .build();

        let metadata = Metadata::new(index, segment_id);

        Ok(Self {
            inner: writer,
            metadata,
            path: path.to_path_buf(),
        })
    }

    #[inline]
    /// The current position of the writer.
    pub fn current_pos(&self) -> u64 {
        self.inner.current_pos()
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf).await?;
        Ok(())
    }

    pub async fn finalise(mut self) -> io::Result<PathBuf> {
        // Write the header to the end of the file buffer.
        let raw = self.metadata.to_bytes()?;
        let start = self.inner.current_pos();

        self.inner.write_all(&raw).await?;

        write_metadata_offsets_aio(&mut self.inner, start, raw.len() as u64)
            .await?;

        Ok(self.path)
    }

    pub async fn abort(self) -> io::Result<()> {
        drop(self.inner);

        // TODO: Change to use async variant.
        let path = self.path;
        glommio::io::remove(path).await?;

        Ok(())
    }
}



