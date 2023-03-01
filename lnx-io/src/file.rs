use std::io::Write;

/// A wrapper type that calls `File::sync_data()` when `flush` is called.
///
/// This effectively patches the fact that `flush` on the standard `File`
/// is a no-op.
pub struct SyncOnFlushFile(std::fs::File);

impl Write for SyncOnFlushFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.sync_data()
    }
}
