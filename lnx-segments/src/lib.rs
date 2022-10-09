#[cfg(target_os = "linux")]
mod aio;

pub mod blocking;

mod deletes;
mod meta_merger;
mod metadata;

pub(crate) use metadata::{get_metadata_offsets, METADATA_HEADER_SIZE};
pub use metadata::Metadata;

use crate::deletes::Deletes;
use crate::meta_merger::{ManagedMeta, MetaFile};

#[cfg(not(target_os = "linux"))]
pub type Exporter = blocking::exporter::BlockingExporter;
#[cfg(not(target_os = "linux"))]
pub type Combiner = blocking::combiner::BlockingCombiner;

#[cfg(target_os = "linux")]
pub type Exporter = aio::selector::AutoExporter;
#[cfg(target_os = "linux")]
pub type Combiner = aio::selector::AutoCombiner;

pub static IGNORED_PREFIX: &str = ".tmp";
pub static IGNORED_FILES: &[&str] = &[".tantivy-meta.lock", ".tantivy-write.lock"];
pub static META_FILE: &str = "meta.json";
pub static MANAGED_FILE: &str = ".managed.json";
pub static DELETES_FILE: &str = ".lnx-deletes";
pub static SPECIAL_FILES: &[&str] = &[META_FILE, MANAGED_FILE, DELETES_FILE];

pub enum SpecialFile {
    Meta(MetaFile),
    Managed(ManagedMeta),
    Deletes(Deletes),
}

pub(crate) const BUFFER_SIZE: usize = 512 << 10;

pub(crate) fn new_buffer() -> Box<[u8]> {
    let mut buf = vec![];
    buf.resize(BUFFER_SIZE, 0);
    buf.into_boxed_slice()
}

#[cfg(test)]
pub(crate) fn get_random_tmp_file() -> std::path::PathBuf {
    std::env::temp_dir().join(uuid::Uuid::new_v4().to_string())
}
