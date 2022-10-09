use std::io;
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use datacake_crdt::HLCTimestamp;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::metadata::write_metadata_offsets;
use crate::{Deletes, DELETES_FILE, MANAGED_FILE, ManagedMeta, META_FILE, Metadata, METADATA_HEADER_SIZE, MetaFile, SpecialFile};

pub mod combiner;
pub mod exporter;
pub(crate) mod utils;

pub(crate) struct BlockingWriter {
    inner: BufWriter<File>,
    num_bytes_written: u64,
    metadata: Metadata,
    path: PathBuf,

    deletes_file: Deletes,
    meta_file: MetaFile,
    managed_file: ManagedMeta,
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
        let file = fs::OpenOptions::new()
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
            deletes_file: Default::default(),
            meta_file: Default::default(),
            managed_file: Default::default(),
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

    /// Merges a special file into the current segment state.
    pub fn write_special_file(&mut self, special: SpecialFile) {
        match special {
            SpecialFile::Deletes(deletes) => {
                self.deletes_file.merge(deletes);
            },
            SpecialFile::Managed(managed) => {
                self.managed_file.merge(managed);
            },
            SpecialFile::Meta(meta) => {
                self.meta_file.merge(meta);
            },
        };
    }

    pub async fn finalise(mut self) -> io::Result<PathBuf> {
        if !self.meta_file.is_empty() {
            let meta = self
                .meta_file
                .to_json()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            let meta_start = self.current_pos();
            self.write_all(&meta).await?;
            let meta_range = meta_start..self.current_pos();

            self.add_file(META_FILE, meta_range);
        }

        if !self.managed_file.is_empty() {
            let managed = self
                .managed_file
                .to_json()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            let managed_start = self.current_pos();
            self.write_all(&managed).await?;
            let managed_range = managed_start..self.current_pos();

            self.add_file(MANAGED_FILE, managed_range);
        }

        if !self.deletes_file.is_empty() {
            let deletes = self
                .deletes_file
                .to_compressed_bytes()
                .await?;

            let deletes_start = self.current_pos();
            self.write_all(&deletes).await?;
            let deletes_range = deletes_start..self.current_pos();

            self.add_file(DELETES_FILE, deletes_range);
        }

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
