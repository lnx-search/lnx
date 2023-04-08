use std::io::{Read, Seek, SeekFrom, Write};
use std::{fs, io};

/// A wrapper type that calls `File::sync_data()` when `flush` is called.
///
/// This effectively patches the fact that `flush` on the standard `File`
/// is a no-op.
pub struct SyncOnFlushFile(fs::File);

impl From<fs::File> for SyncOnFlushFile {
    fn from(value: fs::File) -> Self {
        Self(value)
    }
}

impl Write for SyncOnFlushFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl Read for SyncOnFlushFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Seek for SyncOnFlushFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
