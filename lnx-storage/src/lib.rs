#[macro_use]
extern crate tracing;

use std::future::Future;
use std::io;
use std::io::ErrorKind;

use datacake::eventual_consistency::{
    EventuallyConsistentStore,
    EventuallyConsistentStoreExtension,
    StoreError,
};
use datacake::node::{ClusterExtension, Consistency, ConsistencyError, DatacakeNode};
use datacake_lmdb::{heed, LmdbStorage};
use ownedbytes::OwnedBytes;
use tokio::time::Instant;

use crate::distributor::TaskDistributor;
use crate::fragments::{
    BlockId,
    FragmentInfo,
    FragmentReader,
    IndexFragmentsReaders,
    IndexFragmentsWriters,
};
use crate::metastore::Metastore;
use crate::rpc::StorageService;
use crate::store::{LnxStorage, StorageError, INDEX_FRAGMENTS};

mod distributor;
mod fragments;
pub mod listeners;
mod loader;
mod metastore;
mod models;
pub mod resolvers;
mod rpc;
mod store;

#[derive(Debug, thiserror::Error)]
pub enum CreateStorageError {
    #[error("Failed to open storage service: {0}")]
    OpenStore(#[from] heed::Error),
    #[error("Failed to initialise storage service: {0}")]
    ExtensionInitError(#[from] StoreError<StorageError>),
    #[error("Failed to load already existing state: {0}")]
    LoadState(io::Error),
}

pub struct LnxStorageExtension;

#[datacake::rpc::async_trait]
impl ClusterExtension for LnxStorageExtension {
    type Output = LnxStorageHandle;
    type Error = CreateStorageError;

    async fn init_extension(
        self,
        node: &DatacakeNode,
    ) -> Result<Self::Output, Self::Error> {
        let lmdb_store = LmdbStorage::open(&resolvers::metastore_path()).await?;
        let metastore = Metastore::from_env(lmdb_store.handle().env().clone())?;

        let readers = loader::load_readers(&metastore)
            .await
            .map_err(CreateStorageError::LoadState)?;
        let writers = loader::load_partial_writers(&metastore)
            .await
            .map_err(CreateStorageError::LoadState)?;

        let store = LnxStorage::new(lmdb_store, metastore.clone(), writers.clone());
        node.add_rpc_service(StorageService::new(writers.clone(), readers.clone()));

        let replication = EventuallyConsistentStoreExtension::new(store);
        let replication_handle = node.add_extension(replication).await?;
        let distributor = TaskDistributor::create(node.handle()).await;

        Ok(LnxStorageHandle {
            replication_handle,
            writers,
            readers,
            distributor,
        })
    }
}

pub struct LnxStorageHandle {
    replication_handle: EventuallyConsistentStore<LnxStorage>,
    writers: IndexFragmentsWriters,
    readers: IndexFragmentsReaders,
    distributor: TaskDistributor,
}

impl LnxStorageHandle {
    /// Get a given fragment reader.
    pub fn get_reader(&self, fragment_id: u64) -> Option<FragmentReader> {
        self.readers.get_reader(fragment_id)
    }

    /// Add a new block to the given fragment.
    pub fn add_block<D>(
        &self,
        fragment_id: u64,
        block_id: BlockId,
        data: D,
        checksum: u32,
    ) -> impl Future<Output = Result<(), ConsistencyError>> + '_
    where
        D: Into<Vec<u8>>,
    {
        self.distributor
            .send_block(fragment_id, block_id, data.into(), checksum)
    }

    /// Add a many new blocks to the given fragment.
    pub fn add_many_blocks<'a, D>(
        &'a self,
        fragment_id: u64,
        blocks: impl IntoIterator<Item = (BlockId, D, u32)> + 'a,
    ) -> impl Future<Output = Result<(), ConsistencyError>> + 'a
    where
        D: Into<Vec<u8>>,
    {
        let iter = blocks
            .into_iter()
            .map(|(block_id, data, checksum)| (block_id, data.into(), checksum));
        self.distributor.send_many_blocks(fragment_id, iter)
    }

    /// Write a file to a given fragment.
    ///
    /// This does not get replicated across the cluster until
    /// a fragment is written.
    pub async fn add_file(
        &self,
        fragment_id: u64,
        file_path: impl Into<String>,
        bytes: OwnedBytes,
    ) -> io::Result<()> {
        self.writers
            .write_file(fragment_id, file_path.into(), bytes)
            .await
    }

    #[instrument("commit-fragment", skip(self))]
    /// Seal a fragment and begin replicating it out to nodes.
    pub async fn commit_fragment(
        &self,
        fragment_id: u64,
        info: &FragmentInfo,
    ) -> Result<(), StoreError<StorageError>> {
        let start = Instant::now();
        self.writers
            .seal(fragment_id)
            .await
            .map_err(|e| StoreError::StorageError(StorageError::IO(e)))?;

        let data = rkyv::to_bytes::<_, 1024>(info)
            .map_err(|_| {
                io::Error::new(ErrorKind::Other, "Failed to serialize fragment info")
            })
            .map_err(|e| StoreError::StorageError(StorageError::IO(e)))?;

        // TODO: Currently this will take a *long* time for large fragments
        //       because we do not respond to the request until the entire fragment
        //       is downloaded... Should we change this?
        self.replication_handle
            .handle()
            .put(INDEX_FRAGMENTS, fragment_id, data, Consistency::All)
            .await?;

        info!(elapsed = ?start.elapsed(), "Fragment replicated across all live nodes");

        Ok(())
    }

    #[instrument("delete-fragment", skip(self))]
    /// Delete a fragment from the system.
    pub async fn delete_fragment(
        &self,
        fragment_id: u64,
    ) -> Result<(), StoreError<StorageError>> {
        let start = Instant::now();
        self.replication_handle
            .handle()
            .del(INDEX_FRAGMENTS, fragment_id, Consistency::All)
            .await?;

        info!(elapsed = ?start.elapsed(), "Fragment deleted across all live nodes");

        Ok(())
    }
}
