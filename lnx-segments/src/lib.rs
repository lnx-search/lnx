#[cfg(target_os = "linux")]
mod aio;

pub mod blocking;

mod deletes;
mod meta_merger;
mod metadata;

use std::io;
use std::io::ErrorKind;
use std::path::Path;
pub use metadata::{get_metadata_offsets, METADATA_HEADER_SIZE};
pub use metadata::Metadata;
pub use deletes::Deletes;
pub use meta_merger::{ManagedMeta, MetaFile};


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

pub(crate) async fn deserialize_special_file(
    data: Vec<u8>,
    fp: &Path,
) -> io::Result<SpecialFile> {
    let path = fp.to_string_lossy();

    let file = match path.as_ref() {
        p if p == META_FILE => {
            let meta = MetaFile::from_json(&data)
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            SpecialFile::Meta(meta)
        },
        p if p == MANAGED_FILE => {
            let managed = ManagedMeta::from_json(&data)
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            SpecialFile::Managed(managed)
        },
        p if p == DELETES_FILE => {
            let deletes = Deletes::from_compressed_bytes(data).await?;
            SpecialFile::Deletes(deletes)
        },
        _ => return Err(io::Error::new(ErrorKind::Unsupported, format!("Special file {:?} unknown.", fp))),
    };

    Ok(file)
}