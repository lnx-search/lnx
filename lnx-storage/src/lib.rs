#[macro_use]
extern crate tracing;

use std::io;
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use datacake::eventual_consistency::{
    Document,
    EventuallyConsistentStore,
    EventuallyConsistentStoreExtension,
    ReplicatedStoreHandle,
    StoreError,
};
use datacake::node::{
    ClusterExtension,
    Consistency,
    ConsistencyError,
    DatacakeHandle,
    DatacakeNode,
};
use datacake_lmdb::{heed, LmdbStorage};
use tokio::time::Instant;

pub use self::distributor::HEARTBEAT;
pub use self::fragments::{BlockId, FragmentInfo, BLOCK_HEADER_SIZE};
use crate::distributor::TaskDistributor;
use crate::fragments::{
    FragmentReader,
    IndexFragmentsReaders,
    IndexFragmentsWriters,
    WriteDocBlock,
};
use crate::listeners::ListenerManager;
use crate::metastore::Metastore;
use crate::rpc::StorageService;
use crate::store::{LnxStorage, StorageError, INDEX_FRAGMENTS};

mod bytes;
mod distributor;
mod fragments;
pub mod listeners;
mod loader;
mod metastore;
pub mod resolvers;
mod rpc;
mod store;
#[cfg(test)]
mod tests;

pub use self::bytes::SharedSlice;

#[derive(Debug, thiserror::Error)]
pub enum CreateStorageError {
    #[error("Failed to open storage service: {0}")]
    OpenStore(#[from] heed::Error),
    #[error("Failed to initialise storage service: {0}")]
    ExtensionInitError(#[from] StoreError<StorageError>),
    #[error("Failed to load already existing state: {0}")]
    LoadState(io::Error),
}

/// The wrapping cluster extension built upon datacake to replicate
/// all lnx operations.
pub struct LnxStorageExtension {
    env: EnvCtx,
}

impl LnxStorageExtension {
    /// Create a new storage extension
    pub fn new(env: EnvCtx) -> Self {
        Self { env }
    }

    #[cfg(test)]
    /// Create a new storage extension for a unit test.
    pub fn for_test() -> Self {
        Self {
            env: EnvCtx::for_test(),
        }
    }
}

#[datacake::rpc::async_trait]
impl ClusterExtension for LnxStorageExtension {
    type Output = (StorageGuard, LnxStorageHandle);
    type Error = CreateStorageError;

    async fn init_extension(
        self,
        node: &DatacakeNode,
    ) -> Result<Self::Output, Self::Error> {
        info!("Setting up storage replication");

        let lmdb_store =
            LmdbStorage::open(&resolvers::metastore_folder(&self.env.root_path)).await?;
        let metastore = Metastore::from_env(lmdb_store.handle().env().clone())?;
        let listeners = ListenerManager::default();

        info!("Loading existing fragment readers");
        let readers =
            loader::load_readers(self.env.clone(), &metastore, listeners.clone())
                .await
                .map_err(CreateStorageError::LoadState)?;
        info!("Loading partial fragment writers");
        let writers = loader::load_partial_writers(
            self.env.clone(),
            &metastore,
            listeners.clone(),
        )
        .await
        .map_err(CreateStorageError::LoadState)?;

        let store = LnxStorage::new(
            lmdb_store,
            metastore.clone(),
            writers.clone(),
            readers.clone(),
            listeners.clone(),
        );
        node.add_rpc_service(StorageService::new(writers.clone(), readers.clone()));

        let replication = EventuallyConsistentStoreExtension::new(store);
        let replication_handle = node.add_extension(replication).await?;
        let distributor = TaskDistributor::create(node.handle()).await;

        let guard = StorageGuard {
            handle: replication_handle,
        };

        let handle = LnxStorageHandle {
            node: node.handle(),
            store_handle: guard.handle.handle(),
            writers,
            readers,
            distributor,
            listeners,
        };

        Ok((guard, handle))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AddBlockError {
    #[error("Consistency Error: {0}")]
    ConsistencyError(#[from] ConsistencyError),
    #[error("Local IO Error: {0}")]
    LocalWriteError(#[from] io::Error),
}

pub struct StorageGuard {
    handle: EventuallyConsistentStore<LnxStorage>,
}

#[derive(Clone)]
/// The controller handle to lnx's storage system.
///
/// This manages all of the replication and distribution
/// behind the scenes.
pub struct LnxStorageHandle {
    node: DatacakeHandle,
    store_handle: ReplicatedStoreHandle<LnxStorage>,
    writers: IndexFragmentsWriters,
    readers: IndexFragmentsReaders,
    distributor: TaskDistributor,
    listeners: ListenerManager,
}

impl Deref for LnxStorageHandle {
    type Target = ReplicatedStoreHandle<LnxStorage>;

    fn deref(&self) -> &Self::Target {
        &self.store_handle
    }
}

impl LnxStorageHandle {
    /// Get access to the listeners manager.
    pub fn listeners(&self) -> &ListenerManager {
        &self.listeners
    }

    /// Get a given fragment reader.
    pub fn get_reader(&self, fragment_id: u64) -> Option<FragmentReader> {
        self.readers.get_reader(fragment_id)
    }

    /// Add a new block to the given fragment.
    pub async fn add_block<D>(
        &self,
        fragment_id: u64,
        block_id: BlockId,
        data: D,
        checksum: u32,
    ) -> Result<(), AddBlockError>
    where
        D: Into<Vec<u8>>,
    {
        let ts = self.node.clock().get_time().await;
        let msg = WriteDocBlock {
            block: Document::new(block_id, ts, data),
            checksum,
        };

        self.writers.write_block(fragment_id, msg.clone()).await?;

        self.distributor.send_block(fragment_id, msg).await?;

        Ok(())
    }

    /// Add a many new blocks to the given fragment.
    pub async fn add_many_blocks<D>(
        &self,
        fragment_id: u64,
        blocks_iter: impl IntoIterator<Item = (BlockId, D, u32)>,
    ) -> Result<(), AddBlockError>
    where
        D: Into<Vec<u8>>,
    {
        let ts = self.node.clock().get_time().await;

        let mut blocks = Vec::new();
        for (block_id, data, checksum) in blocks_iter {
            blocks.push(WriteDocBlock {
                block: Document::new(block_id, ts, data),
                checksum,
            });
        }

        self.writers.write_many_blocks(fragment_id, &blocks).await?;

        self.distributor
            .send_many_blocks(fragment_id, blocks.into_iter())
            .await?;

        Ok(())
    }

    /// Write a file to a given fragment.
    ///
    /// This does not get replicated across the cluster until
    /// a fragment is written.
    pub async fn add_file(
        &self,
        fragment_id: u64,
        file_path: impl Into<String>,
        bytes: SharedSlice,
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
        info: FragmentInfo,
    ) -> Result<(), StoreError<StorageError>> {
        let start = Instant::now();
        let data = rkyv::to_bytes::<_, 1024>(&info)
            .map_err(|_| {
                io::Error::new(ErrorKind::Other, "Failed to serialize fragment info")
            })
            .map_err(|e| StoreError::StorageError(StorageError::IO(e)))?;

        self.writers
            .seal(fragment_id, info)
            .await
            .map_err(|e| StoreError::StorageError(StorageError::IO(e)))?;

        info!("Seal completed");

        self.readers
            .open_new_reader(fragment_id)
            .await
            .map_err(StorageError::IO)?;

        info!("Reader open!");

        // TODO: Currently this will take a *long* time for large fragments
        //       because we do not respond to the request until the entire fragment
        //       is downloaded... Should we change this?
        self.store_handle
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
        self.store_handle
            .del(INDEX_FRAGMENTS, fragment_id, Consistency::All)
            .await?;

        info!(elapsed = ?start.elapsed(), "Fragment deleted across all live nodes");

        Ok(())
    }
}

#[derive(Clone)]
/// The environment configuration for the storage system.
pub struct EnvCtx(Arc<EnvCtxInner>);

impl EnvCtx {
    #[cfg(test)]
    /// Create a environment context for testing.
    pub fn for_test() -> Self {
        let inner = EnvCtxInner {
            root_path: std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()),
        };

        Self(Arc::new(inner))
    }
}

impl Deref for EnvCtx {
    type Target = EnvCtxInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EnvCtxInner {
    pub root_path: PathBuf,
}
