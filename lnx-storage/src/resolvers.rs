use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

pub static BLOCK_LOCATIONS_PATH: &str = "lnx/internal/fragment-blocks";
pub static FRAGMENT_INFO_PATH: &str = "lnx/internal/info";

/// Get the path of the metastore database
pub fn metastore_folder(root: &Path) -> PathBuf {
    root.join("lnx-metadata")
}

/// Get the location of a given index fragment.
pub fn get_fragment_location(root: &Path, fragment_id: u64) -> PathBuf {
    fragments_folder(root)
        .join(fragment_id.to_string())
        .with_extension("index")
}

/// The folder storing all fragments.
pub fn fragments_folder(root: &Path) -> PathBuf {
    root.join("fragments")
}

/// Ensures all folders and directories are setup
pub fn init_folders(root: &Path) -> io::Result<()> {
    skip_err_if_exists(std::fs::create_dir_all(root))?;
    skip_err_if_exists(std::fs::create_dir(fragments_folder(root)))?;
    skip_err_if_exists(std::fs::create_dir(metastore_folder(root)))?;

    Ok(())
}

fn skip_err_if_exists(res: io::Result<()>) -> io::Result<()> {
    match res {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        other => other,
    }
}
