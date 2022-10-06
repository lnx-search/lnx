mod aio;
mod blocking;

mod meta_merger;
mod metadata;
mod deletes;

pub use metadata::{get_metadata_offsets, Metadata, METADATA_HEADER_SIZE};


pub type Exporter = blocking::exporter::BlockingExporter;
pub type Combiner = blocking::combiner::BlockingCombiner;

pub static IGNORED_PREFIX: &str = ".tmp";
pub static IGNORED_FILES: &[&str] = &[".tantivy-meta.lock", ".tantivy-write.lock"];
pub static META_FILE: &str = "meta.json";
pub static MANAGED_FILE: &str = ".managed.json";
pub static DELETES_FILE: &str = ".lnx-deletes";
pub static SPECIAL_FILES: &[&str] = &[META_FILE, MANAGED_FILE, DELETES_FILE];
