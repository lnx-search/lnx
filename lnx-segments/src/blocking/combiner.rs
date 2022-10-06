use std::env::temp_dir;
use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{cmp, io};

use datacake_crdt::HLCTimestamp;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

use crate::blocking::utils::read_metadata;
use crate::blocking::BlockingWriter;
use crate::meta_merger::{ManagedMeta, MetaFile};
use crate::{MANAGED_FILE, METADATA_HEADER_SIZE, META_FILE, SPECIAL_FILES};

const BUFFER_SIZE: usize = 64 << 10;

pub struct BlockingCombiner {
    writer: BlockingWriter,
    deletes_tmp_file: File,
    deletes_tmp_path: PathBuf,

    meta_file: MetaFile,
    managed_file: ManagedMeta,
}

impl BlockingCombiner {
    /// Create a new [BlockingCombiner] instance writing to the provided path.
    pub async fn create(
        path: &Path,
        index: String,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let writer = BlockingWriter::create(path, 0, index, segment_id).await?;

        let deletes_tmp_path = temp_dir().join(Uuid::new_v4().to_string());
        let deletes_tmp_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&deletes_tmp_path)
            .await?;

        Ok(Self {
            writer,
            deletes_tmp_file,
            deletes_tmp_path,
            meta_file: MetaFile::default(),
            managed_file: ManagedMeta::default(),
        })
    }

    /// Combines a segment into the new segment.
    ///
    /// This can be called multiple times, combining several smaller segments into a single,
    /// larger segment.
    pub async fn combine_segment(&mut self, segment_file: &Path) -> io::Result<()> {
        let mut segment = File::open(segment_file).await?;
        let metadata = read_metadata(&mut segment).await?;

        // Set the seek position back to the end of the metadata.
        segment
            .seek(SeekFrom::Start(METADATA_HEADER_SIZE as u64))
            .await?;

        let mut files = metadata
            .files()
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

            if SPECIAL_FILES.contains(&path.as_str()) {
                self.merge_special_files(&mut segment, range, &path).await?;
                continue;
            }

            self.write_file(&mut segment, range).await?;

            let end = self.writer.current_pos();
            self.writer.add_file(&path, start..end);
        }

        Ok(())
    }

    /// Finalises any remaining buffers so that file is safely persisted to disk.
    pub async fn finalise(mut self) -> io::Result<File> {
        let meta = self
            .meta_file
            .to_json()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let managed = self
            .managed_file
            .to_json()
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let meta_start = self.writer.current_pos();
        self.writer.write_all(&meta).await?;
        let meta_range = meta_start..self.writer.current_pos();

        let managed_start = self.writer.current_pos();
        self.writer.write_all(&managed).await?;
        let managed_range = managed_start..self.writer.current_pos();

        self.writer.add_file("meta.json", meta_range);
        self.writer.add_file(".managed.json", managed_range);

        self.deletes_tmp_file.seek(SeekFrom::Start(0)).await?;
        copy_data(&mut self.writer.inner, &mut self.deletes_tmp_file).await?;

        self.writer.finalise().await
    }

    pub async fn abort(self) -> io::Result<()> {
        drop(self.deletes_tmp_file);

        let _ = fs::remove_file(&self.deletes_tmp_path).await;
        self.writer.abort().await
    }

    async fn write_file(
        &mut self,
        reader: &mut File,
        range: Range<u64>,
    ) -> io::Result<()> {
        let len = range.end - range.start;
        let mut bytes_written = 0;
        let mut buffer = [0; BUFFER_SIZE];
        while bytes_written < len {
            let n = reader.read(&mut buffer[..]).await?;

            if n == 0 {
                break;
            }

            let n = cmp::min(len - bytes_written, n as u64) as usize;
            self.writer.write_all(&buffer[..n]).await?;

            bytes_written += n as u64;
        }

        // Reset the cursor incase we read over the number of bytes we wanted.
        reader.seek(SeekFrom::Start(range.end)).await?;

        Ok(())
    }

    async fn merge_special_files(
        &mut self,
        reader: &mut File,
        range: Range<u64>,
        path: &str,
    ) -> io::Result<()> {
        match path {
            p if p == META_FILE => {
                let data = read_range(reader, range.clone()).await?;

                let meta = MetaFile::from_json(&data)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.meta_file.merge(meta);
            },
            p if p == MANAGED_FILE => {
                let data = read_range(reader, range.clone()).await?;

                let meta = ManagedMeta::from_json(&data)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                self.managed_file.merge(meta);
            },
            _ => return Ok(()),
        };

        // Reset the cursor incase we read over the number of bytes we wanted.
        reader.seek(SeekFrom::Start(range.end)).await?;

        Ok(())
    }
}

async fn read_range(reader: &mut File, range: Range<u64>) -> io::Result<Vec<u8>> {
    let mut data = vec![];

    let len = range.end - range.start;
    let mut bytes_written = 0;
    let mut buffer = [0; BUFFER_SIZE];
    while bytes_written < len {
        let n = reader.read(&mut buffer[..]).await?;

        if n == 0 {
            break;
        }

        let n = cmp::min(len - bytes_written, n as u64) as usize;
        data.extend_from_slice(&buffer[..n]);

        bytes_written += n as u64;
    }

    Ok(data)
}

async fn copy_data<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    reader: &mut File,
) -> io::Result<()> {
    let mut buffer = [0; BUFFER_SIZE];

    loop {
        let n = reader.read(&mut buffer[..]).await?;

        if n == 0 {
            break;
        }

        writer.write_all(&buffer).await?;
    }

    Ok(())
}
