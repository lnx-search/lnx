use std::num::NonZeroUsize;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
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
    num_threads: NumThreads,

    #[serde(default)]
    /// The per-thread buffer size.
    buffer_size: BufferSize,
}