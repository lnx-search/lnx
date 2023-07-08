mod reader;
mod service;
mod shard;
mod writers;

use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub use reader::{BlockReadError, BlockStoreReader};
pub use service::{BlockStoreService, ServiceConfig};
pub use shard::{StorageShardMailbox, WriteLocation};

/// Generates a new path for a new block store segment.
pub(crate) fn get_new_segment(base_path: &Path, shard_id: usize) -> (FileKey, PathBuf) {
    let key = FileKey {
        timestamp: timestamp(),
        shard_id,
    };

    (key, base_path.join(format!("{}.blocks", key)))
}

#[derive(Debug, Copy, Clone)]
pub struct FileKey {
    pub timestamp: u64,
    pub shard_id: usize,
}

impl Display for FileKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.shard_id)
    }
}

impl lnx_metastore::Key for FileKey {
    fn to_hash(&self) -> u64 {
        let mut buffer = [0u8; 12];

        buffer[..8].copy_from_slice(&self.timestamp.to_be_bytes());
        buffer[8..].copy_from_slice(&(self.shard_id as u32).to_be_bytes());

        buffer.as_ref().to_hash()
    }
}

/// Gets the current unix timestamp in seconds.
fn timestamp() -> u64 {
    SystemTime::now().elapsed().unwrap().as_secs()
}
