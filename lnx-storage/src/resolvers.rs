use std::env;
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;

static ROOT_PATH: Lazy<PathBuf> = Lazy::new(|| {
    let root_path =
        env::var("LNX_DATA_PATH").unwrap_or_else(|_| "/var/lib/lnx/data".to_string());
    PathBuf::from(root_path)
});
pub static BLOCK_LOCATIONS_PATH: &str = "lnx/internal/fragment-blocks";

/// The root path used within storage.
pub fn root() -> &'static Path {
    ROOT_PATH.as_path()
}

/// Get the path of the metastore database
pub fn metastore_path() -> PathBuf {
    ROOT_PATH.join("lnx-metadata")
}

/// Get the location of a given index fragment.
pub fn get_fragment_location(fragment_id: u64) -> PathBuf {
    fragments_folder()
        .join(fragment_id.to_string())
        .with_extension("index")
}

/// The folder storing all fragments.
pub fn fragments_folder() -> PathBuf {
    ROOT_PATH.join("fragments")
}
