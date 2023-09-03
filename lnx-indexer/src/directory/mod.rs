use std::path::Path;
use std::sync::OnceLock;

pub mod memory;
pub mod readonly;
pub mod splitting;

static META_FILEPATH: OnceLock<&'static Path> = OnceLock::new();
static MANAGED_FILEPATH: OnceLock<&'static Path> = OnceLock::new();
static META_LOCK_FILEPATH: OnceLock<&'static Path> = OnceLock::new();
static WRITER_LOCK_FILEPATH: OnceLock<&'static Path> = OnceLock::new();

#[inline]
/// The metadata file path used by tantivy.
pub fn meta_filepath() -> &'static Path {
    META_FILEPATH.get_or_init(|| Path::new("meta.json"))
}

#[inline]
/// The managed JSON file used by tantivy.
pub fn managed_filepath() -> &'static Path {
    MANAGED_FILEPATH.get_or_init(|| Path::new(".managed.json"))
}

#[inline]
/// The metadata lockfile used by tantivy.
pub fn meta_lockfile_path() -> &'static Path {
    META_LOCK_FILEPATH.get_or_init(|| Path::new(".tantivy-meta.lock"))
}

#[inline]
/// The writer lockfile used by tantivy.
pub fn writer_lockfile_path() -> &'static Path {
    WRITER_LOCK_FILEPATH.get_or_init(|| Path::new(".tantivy-writer.lock"))
}