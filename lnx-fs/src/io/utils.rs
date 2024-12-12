use std::io;
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
