use std::num::NonZeroUsize;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::helpers::serde::{BufferSize, NumThreads};

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexerHandlerConfig {
    /// The maximum number of indexers that can run concurrency.
    ///
    /// Node that this does not control how many threads each indexer uses.
    #[serde(default = "IndexerHandlerConfig::default_indexer_concurrency")]
    pub max_indexer_concurrency: NonZeroUsize,
}

impl IndexerHandlerConfig {
    fn default_indexer_concurrency() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Decode, Encode)]
pub struct IndexerConfig {
    #[serde(default)]
    /// The number of threads to use on the index writer.
    pub num_threads: NumThreads,

    #[serde(default)]
    /// The per-thread buffer size.
    pub buffer_size: BufferSize,
}
