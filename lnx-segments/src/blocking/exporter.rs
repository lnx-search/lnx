use std::io;
use std::path::Path;
use datacake_crdt::HLCTimestamp;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use crate::blocking::BlockingWriter;

use crate::metadata::Metadata;

/// A directory exporter built around traditional blocking IO wrapped by tokio.
///
/// This is technically still async friendly because tokio runs all of the equivalent
/// operations on a background thread.
pub struct BlockingExporter {
    writer: BlockingWriter,
}

impl BlockingExporter {
    /// Create a new blocking exporter.
    pub async fn create(
        path: &Path,
        size_hint: usize,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let writer = BlockingWriter::create(path, size_hint, index, segment_id).await?;

        Ok(Self {
            writer,
        })
    }

    /// Write a new file to the exporter. The file path will be read and streamed from into
    /// the new segment.
    pub async fn write_file(&mut self, path: &Path) -> io::Result<()> {
        let file = File::open(path).await?;
        let mut reader = BufReader::new(file);

        let start = self.writer.current_pos() as u64;
        let mut block = [0; 64 << 10];

        loop {
            let n = reader.read(&mut block[..]).await?;

            if n == 0 {
                break;
            }

            self.writer.write_all(&block[..n]).await?;
        }

        let end = self.writer.current_pos();
        self.writer.add_file(path, start..end);

        Ok(())
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<File> {
        self.writer.finalise().await
    }
}



