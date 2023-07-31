mod blocking;

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use rkyv::AlignedVec;

use crate::writers::blocking::BlockingIoWriter;

/// The soft limit on file size that a shard can reach before
/// being sealed.
const MAX_STORAGE_CAPACITY: u64 = 4 << 30;

/// The writer responsible for safely persisting doc blocks to disk
/// doubling as a WAL for the tantivy index as well.
pub struct BlockStoreWriter {
    segment_writer: AutoWriter,
    file_size: u64,
}

impl BlockStoreWriter {
    /// Creates a new file or opens an existing file for
    /// the doc store writer.
    pub async fn open(file_path: &Path) -> io::Result<Self> {
        let file_size = file_path.metadata()?.len();
        let segment_writer = AutoWriter::open(file_path.to_path_buf()).await?;

        Ok(Self {
            segment_writer,
            file_size,
        })
    }

    #[inline]
    /// Returns if the shard is at max capacity or not.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    #[inline]
    /// Returns if the shard is at max capacity or not.
    pub fn is_full(&self) -> bool {
        self.file_size >= MAX_STORAGE_CAPACITY
    }

    #[inline]
    /// Writes a block of data into the doc store.
    ///
    /// Returns the position the bytes were written at.
    pub async fn write_all(&mut self, bytes: Arc<AlignedVec>) -> io::Result<u64> {
        let n = self.segment_writer.write_all(bytes).await?;
        self.file_size += n;
        Ok(n)
    }

    #[inline]
    /// Ensures any pending data written to the doc store is safely persisted to disk.
    ///
    /// This internally advances the checkpoint cursor that will be stored
    /// when a index is committed.
    pub fn flush(&mut self) -> impl Future<Output = io::Result<()>> + '_ {
        self.segment_writer.flush()
    }
}

#[async_trait]
pub trait SegmentWriter: Sized {
    /// Opens a segment for writing.
    ///
    /// If the file already exists the cursor should start writing from the end of the file.
    async fn open(path: PathBuf) -> io::Result<Self>;

    /// Writes a chunk of data to the store.
    ///
    /// The writer should return the byte offset of the start of the block.
    async fn write_all(&mut self, bytes: Arc<AlignedVec>) -> io::Result<u64>;

    /// Ensures all data written to the store is safely persisted on disk.
    ///
    /// Returns the current position of the writer which is safely persisted.
    async fn flush(&mut self) -> io::Result<()>;
}

/// A writer that automatically selects the most appropriate writer
/// for the given OS and configuration.
pub enum AutoWriter {
    /// The blocking IO writer backed by a threadpool.
    Blocking(BlockingIoWriter),
}

#[async_trait]
impl SegmentWriter for AutoWriter {
    async fn open(path: PathBuf) -> io::Result<Self> {
        let writer = BlockingIoWriter::open(path).await?;
        Ok(Self::Blocking(writer))
    }

    async fn write_all(&mut self, bytes: Arc<AlignedVec>) -> io::Result<u64> {
        match self {
            Self::Blocking(writer) => writer.write_all(bytes).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Blocking(writer) => writer.flush().await,
        }
    }
}
