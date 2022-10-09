pub mod combiner;
pub mod exporter;
mod runtime;
mod utils;
pub mod selector;

use std::io;
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use datacake_crdt::HLCTimestamp;
use futures_lite::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, DmaStreamWriterBuilder};
pub use runtime::create_runtime;

use crate::metadata::write_metadata_offsets_aio;
use crate::{
    Deletes,
    ManagedMeta,
    MetaFile,
    Metadata,
    SpecialFile,
    DELETES_FILE,
    MANAGED_FILE,
    META_FILE,
};

pub struct AioWriter {
    inner: DmaStreamWriter,
    metadata: Metadata,
    path: PathBuf,

    deletes_file: Deletes,
    meta_file: MetaFile,
    managed_file: ManagedMeta,
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
            deletes_file: Default::default(),
            meta_file: Default::default(),
            managed_file: Default::default(),
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
            let deletes = self.deletes_file.to_compressed_bytes().await?;

            let deletes_start = self.current_pos();
            self.write_all(&deletes).await?;
            let deletes_range = deletes_start..self.current_pos();

            self.add_file(DELETES_FILE, deletes_range);
        }

        // Write the header to the end of the file buffer.
        let raw = self.metadata.to_bytes()?;
        let start = self.inner.current_pos();

        self.inner.write_all(&raw).await?;

        dbg!(start, raw.len(), self.current_pos());
        write_metadata_offsets_aio(&mut self.inner, start, raw.len() as u64).await?;

        self.inner.flush().await?;
        Ok(self.path)
    }

    pub async fn abort(self) -> io::Result<()> {
        drop(self.inner);

        let path = self.path;
        glommio::io::remove(path).await?;

        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(test, macro_export)]
macro_rules! run_aio {
    ($fut:expr, $name:expr) => {{
        glommio::LocalExecutorBuilder::default()
            .name($name)
            .spawn($fut)
            .expect("failed to spawn local executor")
            .join()
            .expect("join runtime.")
    }};
}
