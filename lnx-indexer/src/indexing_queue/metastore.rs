use anyhow::Result;
use lnx_metastore::heed::byteorder::LE;
use lnx_metastore::heed::types::U64;
use lnx_metastore::{CustomMetastore, Metastore};

static DATABASE_NAME: &str = "lnx_metastore__indexing_queue";

/// The metastore for the indexing service.
///
/// This internally is basically just a indexing queue.
pub struct IndexingMetastore {
    /// A FIFO queue of blocks needing to be indexed.
    indexing_queue: CustomMetastore<U64<LE>, U64<LE>>,
}

impl IndexingMetastore {
    /// Creates a new [IndexingMetastore] from the main [Metastore].
    pub fn create_from_metastore(metastore: &Metastore) -> Result<Self> {
        let indexing_queue = metastore.open_custom_database(DATABASE_NAME)?;
        Ok(Self { indexing_queue })
    }

    /// Inserts a set of new blocks into the indexing queue with their ID and given index ID.
    pub fn push_blocks(&self, blocks_and_size: &[(u64, u64)]) -> Result<()> {
        let mut txn = self.indexing_queue.write_txn()?;

        for (block_id, index_id) in blocks_and_size {
            self.indexing_queue.db().put(&mut txn, block_id, index_id)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Removes a set of blocks from the indexing queue.
    pub fn remove_blocks(&self, block_ids: impl IntoIterator<Item = u64>) -> Result<()> {
        let mut txn = self.indexing_queue.write_txn()?;

        for block_id in block_ids {
            self.indexing_queue.db()
                .delete(&mut txn, &block_id)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Reads all the blocks for a given index.
    pub fn read_blocks_for_index(&self, index_id: u64) -> Result<Vec<u64>> {
        let mut blocks = Vec::new();

        let txn = self.indexing_queue.read_txn()?;

        let iter = self.indexing_queue.db().iter(&txn)?;

        for row in iter {
            let (block_id, block_index_id) = row?;

            if block_index_id == index_id {
                blocks.push(block_id);
            }
        }

        Ok(blocks)
    }
}
