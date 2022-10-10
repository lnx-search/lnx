use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::directory::error::{
    DeleteError,
    OpenDirectoryError,
    OpenReadError,
    OpenWriteError,
};
use tantivy::directory::{
    AntiCallToken,
    FileHandle,
    MmapDirectory,
    TerminatingWrite,
    WatchCallback,
    WatchHandle,
    WritePtr,
};
use tantivy::Directory;

type BytesCounter = Arc<AtomicUsize>;

#[derive(Clone)]
/// A wrapper around a temporary [MmapDirectory] which as a
/// buffer zone for any changes to the index.
///
/// When a commit occurs the directory can then be copied over
/// to it's single file form and merged with any other segments.
pub struct DirectoryWriter {
    /// The inner directory which is mounted to the temp file system.
    inner: MmapDirectory,

    /// The estimated disk usage taken up by the directory.
    estimated_disk_usage: BytesCounter,

    /// The current existing files written to the directory and each file's
    /// estimated size.
    live_files: Arc<RwLock<HashMap<PathBuf, BytesCounter>>>,

    /// The path where all the files are currently located.
    base_path: PathBuf,
}

impl DirectoryWriter {
    /// Creates a new writer directory within the temp file system.
    pub fn create(base_path: &Path) -> Result<Self, OpenDirectoryError> {
        fs::create_dir_all(base_path).map_err(|e| {
            OpenDirectoryError::wrap_io_error(e, base_path.to_path_buf())
        })?;
        let dir = MmapDirectory::open(base_path)?;

        Ok(Self {
            inner: dir,
            estimated_disk_usage: Default::default(),
            live_files: Default::default(),
            base_path: base_path.to_path_buf(),
        })
    }

    /// The estimated disk space used by the directory so far.
    pub fn estimated_disk_usage(&self) -> usize {
        self.estimated_disk_usage.load(Ordering::Relaxed)
    }

    /// The estimated disk space used by a single specified file.
    pub fn get_file_usage(&self, path: &Path) -> Option<usize> {
        self.live_files
            .read()
            .get(path)
            .map(|v| v.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn exported_files(&self) -> &Path {
        &self.base_path
    }
}

impl Debug for DirectoryWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DirectoryWriter(disk_usage={}B, inner={:?})",
            self.estimated_disk_usage(),
            self.inner
        )
    }
}

impl Directory for DirectoryWriter {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Box<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)?;

        if let Some(usage) = self.live_files.write().remove(path) {
            let usage = usage.load(Ordering::Relaxed);
            self.estimated_disk_usage
                .fetch_sub(usage, Ordering::Relaxed);
        }

        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let ptr = self.inner.open_write(path)?;
        let inner_writer = ptr.into_inner().map_err(|e| {
            OpenWriteError::wrap_io_error(e.into_error(), path.to_path_buf())
        })?;

        let file_usage = self
            .live_files
            .write()
            .entry(path.to_path_buf())
            .or_default()
            .clone();

        let writer = ByteCountingWriter {
            inner: inner_writer,
            file_disk_usage: file_usage,
            directory_disk_usage: self.estimated_disk_usage.clone(),
        };

        Ok(WritePtr::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.inner.atomic_write(path, data)?;

        self.estimated_disk_usage
            .fetch_add(data.len(), Ordering::Relaxed);
        self.live_files
            .write()
            .entry(path.to_path_buf())
            .or_default()
            .store(data.len(), Ordering::Relaxed);

        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        self.inner.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

pub struct ByteCountingWriter {
    inner: Box<dyn TerminatingWrite>,
    directory_disk_usage: BytesCounter,
    file_disk_usage: BytesCounter,
}

impl Write for ByteCountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.directory_disk_usage.fetch_add(n, Ordering::Relaxed);
        self.file_disk_usage.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for ByteCountingWriter {
    fn terminate_ref(&mut self, token: AntiCallToken) -> std::io::Result<()> {
        self.inner.terminate_ref(token)
    }
}


#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use tantivy::{doc, Index, IndexSettings};
    use super::*;

    use tantivy::schema::{Schema, STORED, TEXT};

    #[test]
    fn test_writer_directory() {
        let mut schema_builder = Schema::builder();

        let title = schema_builder.add_text_field("title", TEXT | STORED);
        let body = schema_builder.add_text_field("body", TEXT);

        let schema = schema_builder.build();
        let dir = DirectoryWriter::create(&temp_dir().join("writer-test"))
            .expect("Create new directory.");

        let index = Index::create(dir, schema, IndexSettings::default())
            .expect("Create index.");

        let mut index_writer = index.writer(50_000_000)
            .expect("Create index writer.");

        index_writer.add_document(doc!(
            title => "Of Mice and Men",
            body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                    bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                    over the yellow sands in the sunlight before reaching the narrow pool. On one \
                    side of the river the golden foothill slopes curve up to the strong and rocky \
                    Gabilan Mountains, but on the valley side the water is lined with trees—willows \
                    fresh and green with every spring, carrying in their lower leaf junctures the \
                    debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                    limbs and branches that arch over the pool"
        )).expect("Add document.");


        index_writer.add_document(doc!(
            title => "Frankenstein",
            title => "The Modern Prometheus",
            body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
                     enterprise which you have regarded with such evil forebodings.  I arrived here \
                     yesterday, and my first task is to assure my dear sister of my welfare and \
                     increasing confidence in the success of my undertaking."
        )).expect("Add document.");

        index_writer.commit().expect("Commit documents.");
    }
}