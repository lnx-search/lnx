use std::time::Duration;

use lnx_metastore::Metastore;
use tracing::error;
use yorick::{
    BlobId,
    BlobInfo,
    CompactionConfig,
    CompactionPolicy,
    FileKey,
    ReadBuffer,
};

static SAFE_COMPACT_CHECKPOINT_KEY: &str = "lnx_storage__safe_checkpoint";

/// A compaction policy which reads the `lnx_storage__safe_checkpoint` key from the metastore.
pub struct CommitAwareCompactionPolicy {
    metastore: Metastore,
}

impl CommitAwareCompactionPolicy {
    pub(crate) fn new(metastore: Metastore) -> Self {
        Self { metastore }
    }
}

impl CompactionPolicy for CommitAwareCompactionPolicy {
    fn get_config(&self) -> CompactionConfig {
        CompactionConfig {
            scan_interval: Duration::from_secs(60),
            ..Default::default()
        }
    }

    fn get_safe_compact_checkpoint(&self) -> Option<FileKey> {
        match self.metastore.get(SAFE_COMPACT_CHECKPOINT_KEY) {
            Err(e) => {
                error!(error = ?e, "Failed to retrieve safe compaction checkpoint from metastore");
                None
            },
            Ok(res) => res,
        }
    }

    fn can_delete(
        &self,
        _blob_id: BlobId,
        info: BlobInfo,
        _data: Option<&ReadBuffer>,
    ) -> bool {
        info.is_empty()
    }
}
