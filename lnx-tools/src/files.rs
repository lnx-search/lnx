use std::fs::File;
use std::io;
use std::io::SeekFrom;

/// A wrapping file type that calls `fdatasync` when `flush` is called.
///
/// This remove the foot gun behaviour of the `File`'s `flush` method being a no-op.
pub struct SyncOnFlushFile {
    inner: File,
}

impl From<File> for SyncOnFlushFile {
    fn from(inner: File) -> Self {
        Self { inner }
    }
}

impl io::Write for SyncOnFlushFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        self.inner.sync_data()?;
        Ok(())
    }
}

impl io::Seek for SyncOnFlushFile {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl io::Read for SyncOnFlushFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}
