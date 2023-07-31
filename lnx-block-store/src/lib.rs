mod reader;
mod service;
mod shard;
mod writers;
mod metastore;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use lnx_tools::hashers::NoOpRandomState;

pub use reader::{BlockReadError, BlockStoreReader};
pub use service::{BlockStoreService, ServiceConfig};
pub use shard::{StorageShardMailbox, WriteLocation};

/// The default file extension produced by the block store.
pub static DEFAULT_FILE_EXT: &str = "v1blocks";
pub(crate) type Readers = HashMap<FileKey, BlockStoreReader, NoOpRandomState>;


/// Generates a new path for a new block store segment.
pub(crate) fn get_new_segment(base_path: &Path, shard_id: usize) -> (FileKey, PathBuf) {
    let file_key_raw = format!("{}-{shard_id}", timestamp());
    let file_key_hash = lnx_tools::consistent_hash(file_key_raw);
    let key = FileKey(file_key_hash);

    (key, base_path.join(format!("{}.{DEFAULT_FILE_EXT}", key)))
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The unique file ID for the doc store.
///
/// The file key is suitable to use as a hash as it is
/// derived using `lnx_tools::consistent_hash`.
pub struct FileKey(u64);

impl FileKey {
    /// Get the file key as a u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Display for FileKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl lnx_metastore::Key for FileKey {
    fn to_hash(&self) -> u64 {
        let mut buffer = [0u8; 12];
        buffer[..8].copy_from_slice(&self.0.to_le_bytes());
        buffer.as_ref().to_hash()
    }
}

impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0)
    }
}

/// Gets the current unix timestamp in seconds.
fn timestamp() -> u64 {
    SystemTime::now().elapsed().unwrap().as_secs()
}
