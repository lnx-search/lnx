use std::borrow::Cow;

use lnx_tools::supervisor;
use lnx_tools::supervisor::RecreateCallback;
use tantivy::{Index, IndexWriter};

struct IndexingActorState {
    index_id: u64,
    shard_id: u64,
    index: Index,
}

impl supervisor::SupervisedState for IndexingActorState {
    fn name(&self) -> Cow<'static, str> {
        Cow::Owned(format!("index-{}-indexer-{}", self.index_id, self.shard_id))
    }

    fn recreate(&self, watcher: RecreateCallback) -> anyhow::Result<()> {
        todo!()
    }
}

struct IndexingActor {
    writer: IndexWriter,
    rx: flume::Receiver<()>,
}

impl IndexingActor {}
