use std::io;
use std::path::Path;

use datacake_crdt::HLCTimestamp;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

use crate::blocking::BlockingWriter;

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

        Ok(Self { writer })
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

    /// Write a new file to the exporter.
    ///
    /// Unlike the `write_file` method, this method takes a raw buffer which gets
    /// written out instead.
    pub async fn write_raw(&mut self, path: &Path, buf: &[u8]) -> io::Result<()> {
        let start = self.writer.current_pos();
        self.writer.write_all(buf).await?;
        let end = self.writer.current_pos();

        self.writer.add_file(path, start..end);

        Ok(())
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<File> {
        self.writer.finalise().await
    }

    /// Abort the segment creation.
    pub async fn abort(self) -> io::Result<()> {
        self.writer.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::io::SeekFrom;

    use datacake_crdt::get_unix_timestamp_ms;
    use tokio::fs;
    use tokio::io::AsyncSeekExt;

    use super::*;
    use crate::blocking::utils::read_metadata;
    use crate::METADATA_HEADER_SIZE;

    #[tokio::test]
    async fn test_exporter_create_and_finalise() -> io::Result<()> {
        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = temp_dir().join("exported-file-finalise.segment");

        let exporter =
            BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                .await?;
        let mut file = exporter.finalise().await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to exporter."
        );
        assert!(
            metadata.files().is_empty(),
            "Expected metadata files index to be empty."
        );
        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exporter_create_and_abort() -> io::Result<()> {
        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = temp_dir().join("exported-file-abort.segment");

        let exporter =
            BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                .await?;
        exporter.abort().await?;

        assert!(
            !path.exists(),
            "Expected segment to no longer exist after abort."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exporter() -> io::Result<()> {
        let sample_file = temp_dir().join("sample.txt");
        fs::write(&sample_file, b"Hello, world!").await?;

        let segment_id = HLCTimestamp::new(get_unix_timestamp_ms(), 0, 0);
        let path = temp_dir().join("exported-file-test.segment");

        let mut exporter =
            BlockingExporter::create(&path, 0, "test-index".to_string(), segment_id)
                .await?;
        exporter.write_file(&sample_file).await?;
        let mut file = exporter.finalise().await?;

        // Read it like a new file.
        file.seek(SeekFrom::Start(0)).await?;
        let metadata = read_metadata(&mut file).await?;

        assert_eq!(
            metadata.index(),
            "test-index",
            "Expected metadata index to be the same as what is provided to exporter."
        );
        assert_eq!(
            metadata.files().get(sample_file.to_string_lossy().as_ref()),
            Some(&(METADATA_HEADER_SIZE as u64..METADATA_HEADER_SIZE as u64 + 13)),
            "Expected metadata files index to be empty.",
        );
        assert_eq!(
            metadata.segment_id(),
            segment_id,
            "Expected segment id to match provided id."
        );

        Ok(())
    }
}
