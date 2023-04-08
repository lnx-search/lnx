use std::mem::size_of;
use std::sync::Arc;

use datacake_lmdb::heed::byteorder::LittleEndian;
use datacake_lmdb::heed::types::{ByteSlice, U64, U8};
use datacake_lmdb::heed::{Database, Env};
use datacake_lmdb::Error;
use parking_lot::Mutex;

use crate::fragments::BlockId;

/// Fragment state flags.
pub mod flags {
    /// The fragment is created but not sealed.
    ///
    /// This means it needs the block state to be recovered on
    /// startup.
    pub const CREATED: u8 = 0;
    /// The fragment is sealed and is now immutable.
    ///
    /// This means a fragment reader can open this without issue.
    pub const SEALED: u8 = 1;
}

#[derive(Clone)]
/// A wrapper around the LMDB metastore for handling metadata
/// operations.
pub struct Metastore {
    env: Arc<Mutex<Env>>,
    block_locations: Database<U64<LittleEndian>, ByteSlice>,
    fragments_info: Database<U64<LittleEndian>, U8>,
}

impl Metastore {
    /// Open the metastore for a given LMDB environment.
    pub fn from_env(env: Env) -> Result<Self, Error> {
        let mut txn = env.write_txn()?;
        let block_locations = env.create_database(&mut txn, Some("datacake-blocks"))?;
        let fragments_info =
            env.create_database(&mut txn, Some("datacake-fragments-info"))?;
        txn.commit()?;

        Ok(Self {
            env: Arc::new(Mutex::new(env)),
            block_locations,
            fragments_info,
        })
    }

    /// Insert block metadata into the store.
    ///
    /// This information is used for recovery after a restart.
    pub fn insert_block(
        &self,
        id: BlockId,
        metadata: BlockMetadata,
    ) -> Result<(), Error> {
        let lock = self.env.lock();
        let mut txn = lock.write_txn()?;
        self.block_locations
            .put(&mut txn, &id, &metadata.as_bytes())?;
        txn.commit()?;

        Ok(())
    }

    /// Remove blocks from the metastore.
    ///
    /// This is normally because the fragment has been sealed.
    pub fn remove_blocks(
        &self,
        ids: impl Iterator<Item = BlockId>,
    ) -> Result<(), Error> {
        let lock = self.env.lock();
        let mut txn = lock.write_txn()?;
        for id in ids {
            self.block_locations.delete(&mut txn, &id)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Get the list of pending blocks which need to be recovered.
    pub fn get_blocks(&self) -> Result<Vec<(BlockId, BlockMetadata)>, Error> {
        let mut blocks = Vec::new();

        let lock = self.env.lock();
        let txn = lock.read_txn()?;

        for row in self.block_locations.iter(&txn)? {
            let (id, metadata_bytes) = row?;
            let metadata = BlockMetadata::from_bytes(
                metadata_bytes
                    .try_into()
                    .expect("Corrupted metadata, this is a bug"),
            );
            blocks.push((id, metadata));
        }

        Ok(blocks)
    }

    /// Register a newly created fragment for writing.
    pub fn create_new_fragment(&self, id: u64) -> Result<(), Error> {
        let lock = self.env.lock();
        let mut txn = lock.write_txn()?;
        self.fragments_info.put(&mut txn, &id, &flags::CREATED)?;
        txn.commit()?;
        Ok(())
    }

    /// Mark the given fragment as sealed.
    pub fn seal_fragment(&self, id: u64) -> Result<(), Error> {
        let lock = self.env.lock();
        let mut txn = lock.write_txn()?;
        self.fragments_info.put(&mut txn, &id, &flags::SEALED)?;
        txn.commit()?;
        Ok(())
    }

    /// Removes a fragment from the metastore.
    pub fn remove_fragment(&self, id: u64) -> Result<(), Error> {
        let lock = self.env.lock();
        let mut txn = lock.write_txn()?;
        self.fragments_info.delete(&mut txn, &id)?;
        txn.commit()?;
        Ok(())
    }

    /// Get fragments which are sealed.
    pub fn get_sealed_fragments(&self) -> Result<Vec<u64>, Error> {
        let mut fragment_ids = Vec::new();
        let lock = self.env.lock();
        let txn = lock.read_txn()?;
        for fragment in self.fragments_info.iter(&txn)? {
            let (fragment_id, flags) = fragment?;

            if flags == flags::SEALED {
                fragment_ids.push(fragment_id);
            }
        }
        Ok(fragment_ids)
    }

    /// Get fragments which are unsealed.
    pub fn get_unsealed_fragments(&self) -> Result<Vec<u64>, Error> {
        let mut fragment_ids = Vec::new();
        let lock = self.env.lock();
        let txn = lock.read_txn()?;
        for fragment in self.fragments_info.iter(&txn)? {
            let (fragment_id, flags) = fragment?;

            if flags == flags::CREATED {
                fragment_ids.push(fragment_id);
            }
        }
        Ok(fragment_ids)
    }
}

#[derive(Copy, Clone)]
pub struct BlockMetadata {
    /// The ID of the fragment the block belongs to.
    pub fragment_id: u64,
    /// The start position of the block.
    pub start: u32,
    /// The end position of the block.
    pub end: u32,
    /// The CRC32 checksum of the block to ensure data is
    /// not corrupted.
    pub checksum: u32,
}

impl BlockMetadata {
    /// Serialize the metadata as bytes.
    pub fn as_bytes(&self) -> [u8; size_of::<Self>()] {
        let mut slice = [0u8; size_of::<Self>()];
        slice[0..8].copy_from_slice(&self.fragment_id.to_le_bytes());
        slice[8..12].copy_from_slice(&self.start.to_le_bytes());
        slice[12..16].copy_from_slice(&self.end.to_le_bytes());
        slice[16..20].copy_from_slice(&self.checksum.to_le_bytes());
        slice
    }

    /// Deserialize the metadata from bytes.
    pub fn from_bytes(slice: [u8; size_of::<Self>()]) -> Self {
        let fragment_id = u64::from_le_bytes(slice[0..8].try_into().unwrap());
        let start = u32::from_le_bytes(slice[8..12].try_into().unwrap());
        let end = u32::from_le_bytes(slice[12..16].try_into().unwrap());
        let checksum = u32::from_le_bytes(slice[16..20].try_into().unwrap());

        Self {
            fragment_id,
            start,
            end,
            checksum,
        }
    }
}
