use std::{cmp, io};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use glommio::io::{DmaFile, OpenOptions};
use crate::metastore::TabletId;

pub(super) fn get_tablet_file_path(base: &Path, tablet_id: TabletId) -> PathBuf {
    base.join(tablet_id.to_string()).with_extension("tablet")
}

pub(super) fn get_tablet_metadata_file_path(
    base: &Path,
    tablet_id: TabletId,
) -> PathBuf {
    base.join(tablet_id.to_string())
        .with_extension("tablet.meta")
}


/// Syncs the given file's parent directory.
pub(super) async fn sync_directory_glommio(file_path: &Path) -> io::Result<()> {
    if let Some(parent) = file_path.parent() {
        let dir = glommio::io::BufferedFile::open(parent).await?;
        dir.fdatasync().await?;
        dir.close().await?;
    }
    Ok(())
}

/// Creates a new DMA file that can be read and written to.
pub(super) async fn create_rw_file_glommio(file_path: &Path) -> io::Result<DmaFile> {
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .dma_open(file_path)
        .await?;
    Ok(file)
}


pub(super) fn read_n_bytes<R>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    length: usize,
) -> io::Result<()>
where
    R: Read,
{
    let mut buf = [0; 1024];
    while buffer.len() < length {
        let remaining = cmp::min(length - buffer.len(), buf.len());
        let n = reader.read(&mut buf[..remaining])?;
        if n == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof))
        }

        buffer.extend_from_slice(&buf[..n]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, ErrorKind};
    use super::*;

    #[test]
    fn test_read_n_bytes() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());

        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 5).unwrap();
        assert_eq!(selected, b"Hello");

        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 2).unwrap();
        assert_eq!(selected, b", ");

        let mut selected = Vec::new();
        read_n_bytes(&mut sample, &mut selected, 6).unwrap();
        assert_eq!(selected, b"world!");
    }

    #[test]
    fn test_read_n_bytes_eof() {
        let mut sample = Cursor::new(b"Hello, world!".to_vec());

        let mut selected = Vec::new();
        let err = read_n_bytes(&mut sample, &mut selected, 16)
            .expect_err("System should return IO error");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }    
}