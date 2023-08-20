use anyhow::{Context, Result};
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

    /// Inserts a set of new blocks into the indexing queue with their ID and given size.
    pub fn push_blocks(&self, blocks_and_size: &[(u64, u64)]) -> Result<()> {
        let mut txn = self.indexing_queue.write_txn()?;

        for (block_id, size) in blocks_and_size {
            self.indexing_queue.db().put(&mut txn, block_id, size)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Reads the next `n` blocks that add up to `allowance` bytes on disk.
    ///
    /// The system might go over the allowance.
    pub fn read_next_blocks(&self, mut allowance: usize) -> Result<Vec<u64>> {
        let txn = self.indexing_queue.read_txn()?;

        let mut blocks = Vec::with_capacity(2);

        let row_iterator = self.indexing_queue.db().iter(&txn)?;

        for row in row_iterator {
            let (block_id, size) =
                row.context("Failed to read row in indexing queue")?;
            blocks.push(block_id);

            allowance = allowance.saturating_sub(size as usize);
            if allowance == 0 {
                break;
            }
        }

        Ok(blocks)
    }

    /// Clears a chunk of the indexing queue.
    ///
    /// This method assumes block_ids are sortable.
    ///
    /// This must be called in order to advance the queue as `read_next_blocks` reads in
    /// lexicographical order.
    pub fn clear_range(&self, block_ids_from: u64, block_ids_to: u64) -> Result<()> {
        let mut txn = self.indexing_queue.write_txn()?;

        self.indexing_queue
            .db()
            .delete_range(&mut txn, &(block_ids_from..=block_ids_to))?;

        txn.commit()?;

        Ok(())
    }
}
