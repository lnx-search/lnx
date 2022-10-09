#[cfg(target_os = "linux")]
pub mod aio;

mod blocking;

mod deletes;
mod meta_merger;
mod metadata;
mod selector;

pub use metadata::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};
use crate::deletes::Deletes;
use crate::meta_merger::{ManagedMeta, MetaFile};

#[cfg(not(target_os = "linux"))]
pub type Exporter = blocking::exporter::BlockingExporter;
#[cfg(not(target_os = "linux"))]
pub type Combiner = blocking::combiner::BlockingCombiner;

#[cfg(target_os = "linux")]
pub type Exporter = aio::exporter::AioExporter;
#[cfg(target_os = "linux")]
pub type Combiner = aio::combiner::AioCombiner;

pub static IGNORED_PREFIX: &str = ".tmp";
pub static IGNORED_FILES: &[&str] = &[".tantivy-meta.lock", ".tantivy-write.lock"];
pub static META_FILE: &str = "meta.json";
pub static MANAGED_FILE: &str = ".managed.json";
pub static DELETES_FILE: &str = ".lnx-deletes";
pub static SPECIAL_FILES: &[&str] = &[META_FILE, MANAGED_FILE, DELETES_FILE];
