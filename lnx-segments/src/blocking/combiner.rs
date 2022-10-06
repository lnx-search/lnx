use std::io;
use std::io::{ErrorKind, SeekFrom};
use std::path::Path;
use datacake_crdt::HLCTimestamp;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};
use crate::blocking::BlockingWriter;
use crate::blocking::utils::read_metadata;

pub struct BlockingCombiner {
    writer: BlockingWriter,
}

impl BlockingCombiner {
    /// Create a new [BlockingCombiner] instance writing to the provided path.
    pub async fn create(
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let writer = BlockingWriter::create(path, 0, index, segment_id).await?;

        Ok(Self { writer })
    }

    /// Combines a segment into the new segment.
    ///
    /// This can be called multiple times, combining several smaller segments into a single,
    /// larger segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let mut segment = File::open(segment_file).await?;
        let metadata = read_metadata(&mut segment).await?;

        // Set the seek position back to the end of the metadata.
        segment.seek(SeekFrom::Start(METADATA_HEADER_SIZE as u64)).await?;


        let mut files = metadata.files()
            .iter()
            .map(|v| (v.1.clone(), v.0.clone()))
            .collect::<Vec<_>>();

        // We want to sort them by starting index so we minimise seeking overhead.
        files.sort_by_key(|v| v.0.start);

        for (range, path) in files {
            let start = self.writer.current_pos();

            if range.start != start {
                segment.seek(SeekFrom::Start(range.start)).await?;
            }

            let mut buffer = [0; 64 << 10];
            loop {
                let n = segment.read(&mut buffer[..]).await?;

                if n == 0 {
                    break;
                }

                self.writer.write_all(&buffer[..n]).await?;
            }

            let end = self.writer.current_pos();
            self.writer.add_file(&path, start..end);
        }

        Ok(())
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(self) -> io::Result<File> {
        self.writer.finalise().await
    }

    pub async fn abort(self) -> io::Result<()> {
        self.writer.abort().await
    }
}
