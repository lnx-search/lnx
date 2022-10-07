use std::io;
use std::io::SeekFrom;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use datacake_crdt::HLCTimestamp;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::metadata::write_metadata_offsets;
use crate::{Metadata, METADATA_HEADER_SIZE};

pub mod combiner;
pub mod exporter;
mod utils;

pub(crate) struct BlockingWriter {
    inner: BufWriter<File>,
    num_bytes_written: u64,
    metadata: Metadata,
    path: PathBuf,
}

impl Deref for BlockingWriter {
    type Target = Metadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for BlockingWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl BlockingWriter {
    /// Create a new blocking writer.
    pub async fn create(
        path: &Path,
        size_hint: usize,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await?;

        file.set_len(size_hint as u64).await?;

        let writer = BufWriter::new(file);
        let metadata = Metadata::new(index, segment_id);

        Ok(Self {
            inner: writer,
            metadata,
            num_bytes_written: 0,
            path: path.to_path_buf(),
        })
    }

    #[inline]
    /// The current position of the writer.
    pub fn current_pos(&self) -> u64 {
        self.num_bytes_written
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf).await?;
        self.num_bytes_written += buf.len() as u64;

        Ok(())
    }

    pub async fn finalise(mut self) -> io::Result<PathBuf> {
        // Write the header to the end of the file buffer.
        let raw = self.metadata.to_bytes()?;
        self.inner.write_all(&raw).await?;

        self.inner.flush().await?;

        let mut file = self.inner.into_inner();

        write_metadata_offsets(&mut file, self.num_bytes_written, raw.len() as u64)
            .await?;

        // Advance the cursor now the header is written.
        self.num_bytes_written += raw.len() as u64;

        file.set_len(self.num_bytes_written + METADATA_HEADER_SIZE as u64).await?;
        file.sync_all().await?;

        Ok(self.path)
    }

    pub async fn abort(self) -> io::Result<()> {
        drop(self.inner);

        let path = self.path;
        fs::remove_file(path).await?;

        Ok(())
    }
}
