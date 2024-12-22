use std::io;
use std::path::PathBuf;

use tracing::info;

use crate::config::{METASTORE_FILE, TABLET_METADATA_PATH, TABLET_PATH};

pub(super) struct BucketPaths {
    pub(super) metastore_path: PathBuf,
    pub(super) tablets_path: PathBuf,
    pub(super) tablet_metadata_path: PathBuf,
    pub(super) base_path: PathBuf,
}

impl BucketPaths {
    pub(super) fn from_base(base_path: PathBuf) -> Self {
        Self {
            metastore_path: base_path.join(METASTORE_FILE),
            tablets_path: base_path.join(TABLET_PATH),
            tablet_metadata_path: base_path.join(TABLET_METADATA_PATH),
            base_path,
        }
    }

    pub(super) fn metastore_exists(&self) -> io::Result<bool> {
        self.metastore_path.try_exists()
    }

    pub(super) fn metastore_sqlite_path(&self) -> String {
        format!("sqlite:{}", self.metastore_path.display())
    }

    pub(super) fn guess_bucket_name(&self) -> String {
        if let Some(dir) = self.base_path.file_name() {
            dir.to_string_lossy().to_string()
        } else {
            self.base_path.display().to_string()
        }
    }

    pub(super) fn ensure_tablets_path_exists(&self) -> io::Result<()> {
        if self.tablets_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.tablets_path.display(), "Create tablet path");
        std::fs::create_dir(self.tablets_path.as_path())?;

        Ok(())
    }

    pub(super) fn ensure_tablets_metadata_path_exists(&self) -> io::Result<()> {
        if self.tablet_metadata_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.tablet_metadata_path.display(), "Create tablet metadata path");
        std::fs::create_dir(self.tablet_metadata_path.as_path())?;

        Ok(())
    }

    pub(super) fn ensure_metastore_file_exists(&self) -> io::Result<()> {
        if self.metastore_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.metastore_path.display(), "Create metastore");
        std::fs::File::create(self.metastore_path.as_path())?;

        Ok(())
    }

    pub(super) fn ensure_bucket_path_exists(&self) -> io::Result<()> {
        if self.base_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.base_path.display(), "Create bucket path");
        std::fs::create_dir(self.base_path.as_path())?;

        Ok(())
    }
}
