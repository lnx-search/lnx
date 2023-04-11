use std::io;
use std::time::Duration;

use datacake::crdt::{HLCTimestamp, Key};
use datacake::eventual_consistency::{
    BulkMutationError,
    Document,
    DocumentMetadata,
    PutContext,
    Storage,
};
use datacake::rpc::{async_trait, RpcClient, Status};
use datacake_lmdb::LmdbStorage;
use rkyv::AlignedVec;

use crate::fragments::{FragmentInfo, IndexFragmentsWriters, StreamError};
use crate::listeners::ListenerManager;
use crate::rpc::GetFragment;
pub use crate::rpc::StorageService;
use crate::{IndexFragmentsReaders, Metastore};

pub static INDEX_FRAGMENTS: &str = "lnx-fragments";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("LMDB Error: {0}")]
    Lmdb(<LmdbStorage as Storage>::Error),
    #[error("Failed to deserialize payload")]
    Deserialize,
    #[error("Failed to perform RPC operation: {0}")]
    Rpc(Status),
    #[error("IO Error: {0}")]
    IO(io::Error),
}

pub struct LnxStorage {
    lmdb_store: LmdbStorage,
    metastore: Metastore,
    fragment_writers: IndexFragmentsWriters,
    fragment_readers: IndexFragmentsReaders,
    listeners: ListenerManager,
}

impl LnxStorage {
    /// Create the core storage manager.
    pub fn new(
        lmdb_store: LmdbStorage,
        metastore: Metastore,
        fragment_writers: IndexFragmentsWriters,
        fragment_readers: IndexFragmentsReaders,
        listeners: ListenerManager,
    ) -> Self {
        Self {
            lmdb_store,
            metastore,
            fragment_writers,
            fragment_readers,
            listeners,
        }
    }
}

#[async_trait]
impl Storage for LnxStorage {
    type Error = StorageError;
    type DocsIter = <LmdbStorage as Storage>::DocsIter;
    type MetadataIter = <LmdbStorage as Storage>::MetadataIter;

    #[instrument(name = "get-keyspace-list", skip_all)]
    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        info!("Retrieving last known keyspace state...");
        self.lmdb_store
            .get_keyspace_list()
            .await
            .map_err(StorageError::Lmdb)
    }

    #[instrument(name = "get-keyspace-metadata", skip(self))]
    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        info!("Retrieving keyspace metadata");
        self.lmdb_store
            .iter_metadata(keyspace)
            .await
            .map_err(StorageError::Lmdb)
    }

    #[instrument(name = "remove-tombstones", skip(self, keys))]
    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let (min, max) = keys.size_hint();
        if let Err(e) = self.lmdb_store.remove_tombstones(keyspace, keys).await {
            let keys = e.successful_doc_ids().to_vec();
            warn!(size_hint = keys.len(), error = ?e, "Purged tombstones with partial failure");
            Err(BulkMutationError::new(
                StorageError::Lmdb(e.into_inner()),
                keys,
            ))
        } else {
            info!(size_hint = max.unwrap_or(min), "Purged tombstones");
            Ok(())
        }
    }

    #[instrument("kv-put-with-ctx", skip(self, document, ctx))]
    /// Handles UPSERT events for the LNX storage system.
    ///
    /// If the keyspace matches the [INDEX_FRAGMENTS] keyspace,
    /// the system will begin to download the fragment and process it,
    /// if it does not match, it is passed to the [Storage::put] method.
    ///
    /// It's important to note that during this process fragment keyspace
    /// changes **do not** trigger storage listener events.
    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        ctx: Option<&PutContext>,
    ) -> Result<(), Self::Error> {
        if keyspace != INDEX_FRAGMENTS {
            return self.put(keyspace, document).await;
        }

        let ctx = match ctx {
            Some(ctx) => ctx,
            // This means we've just added it locally, so we'll
            // add this to our local store and that's all we need
            // to do.
            None => {
                self.lmdb_store
                    .put_with_ctx(keyspace, document, None)
                    .await
                    .map_err(StorageError::Lmdb)?;
                return Ok(());
            },
        };

        let mut aligned = AlignedVec::with_capacity(document.data().len());
        aligned.extend_from_slice(document.data());
        let info = rkyv::from_bytes::<FragmentInfo>(&aligned)
            .map_err(|_| StorageError::Deserialize)?;

        info!(
            remote_addr = %ctx.remote_addr(),
            fragment_id = info.fragment_id,
            num_docs = info.num_docs,
            num_bytes = info.num_bytes_total,
            child_of_fragments = ?info.child_of_fragments,
            "System is attempting to download fragment",
        );

        // See if we already have some data downloaded from the fragment.
        // If the fragment is an orphan then this will likely be empty
        // as we do not re-replicate the blocks while re-indexing.
        let state = self
            .fragment_writers
            .get_current_writer_state(info.fragment_id)
            .await
            .unwrap_or_default();

        let channel = ctx.remote_channel();
        let mut rpc_client = RpcClient::<StorageService>::new(channel.clone());
        rpc_client.set_timeout(Duration::from_secs(5));

        let resp = rpc_client
            .send_owned(GetFragment {
                fragment_id: info.fragment_id,
                blocks: state
                    .existing_blocks
                    .into_iter()
                    .map(|(block_id, _)| block_id)
                    .collect(),
            })
            .await
            .map_err(StorageError::Rpc)?;

        self.fragment_writers
            .write_stream(info.fragment_id, resp)
            .await
            .map_err(|e| match e {
                StreamError::Hyper(e) => StorageError::Rpc(Status::internal(e)),
                StreamError::Io(e) => StorageError::IO(e),
            })?;

        let fragment_id = info.fragment_id;
        self.fragment_writers
            .seal(fragment_id, info)
            .await
            .map_err(StorageError::IO)?;

        self.fragment_readers
            .open_new_reader(fragment_id)
            .await
            .map_err(StorageError::IO)?;

        self.lmdb_store
            .put_with_ctx(keyspace, document, None)
            .await
            .map_err(StorageError::Lmdb)?;

        Ok(())
    }

    #[instrument("kv-put", skip(self, document))]
    /// Handles arbitrary key value storage for the storage system.
    ///
    /// Any changes made to keyspace will trigger the storage listener events.
    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        self.lmdb_store
            .put(keyspace, document.clone())
            .await
            .map_err(StorageError::Lmdb)?;

        self.listeners.trigger_on_put(keyspace, document);

        Ok(())
    }

    #[instrument("kv-put-many-with-ctx", skip(self, documents, ctx))]
    /// Handles UPSERT events for the LNX storage system.
    ///
    /// If the keyspace matches the [INDEX_FRAGMENTS] keyspace,
    /// the system will begin to download the fragment and process it,
    /// if it does not match, it is passed to the [Storage::multi_put] method.
    ///
    /// It's important to note that during this process fragment keyspace
    /// changes **do not** trigger storage listener events.
    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        ctx: Option<&PutContext>,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if keyspace != INDEX_FRAGMENTS {
            return self.multi_put(keyspace, documents).await;
        }

        info!("System is attempting to download fragments!");
        let mut successful_ids = Vec::new();
        let mut successful_docs = Vec::new();
        let mut error = None;
        for doc in documents {
            let id = doc.id();
            match self.put_with_ctx(keyspace, doc.clone(), ctx).await {
                Ok(()) => {
                    successful_ids.push(id);
                    successful_docs.push(doc);
                },
                Err(e) => {
                    error = Some(e);
                },
            }
        }

        if let Some(error) = error {
            return Err(BulkMutationError::new(error, successful_ids));
        }

        Ok(())
    }

    #[instrument("kv-put-many", skip(self, documents))]
    /// Handles arbitrary key value storage for the storage system.
    ///
    /// Any changes made to keyspace will trigger the storage listener events.
    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut successful_docs = Vec::new();
        let documents = documents.map(|doc| {
            successful_docs.push(doc.clone());
            doc
        });

        // We cheat here when trigger the listeners as we know
        // LMDB will do all-or-nothing in the transaction.
        if let Err(e) = self.lmdb_store.multi_put(keyspace, documents).await {
            let keys = e.successful_doc_ids().to_vec();
            Err(BulkMutationError::new(
                StorageError::Lmdb(e.into_inner()),
                keys,
            ))
        } else {
            for doc in successful_docs {
                self.listeners.trigger_on_put(keyspace, doc);
            }
            Ok(())
        }
    }

    #[instrument("kv-delete", skip(self, doc_id, timestamp))]
    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.lmdb_store
            .mark_as_tombstone(keyspace, doc_id, timestamp)
            .await
            .map_err(StorageError::Lmdb)?;

        if keyspace != INDEX_FRAGMENTS {
            self.listeners.trigger_on_del(keyspace, doc_id);
            return Ok(());
        }

        let metastore = self.metastore.clone();
        lnx_executor::spawn_task(async move {
            metastore
                .remove_fragment(doc_id)
                .map_err(StorageError::Lmdb)
        })
        .await
        .expect("Join task")?;

        self.listeners.trigger_fragment_delete(doc_id);

        Ok(())
    }

    #[instrument("kv-delete-many", skip(self, documents))]
    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut doc_ids = Vec::new();
        let documents = documents.map(|doc| {
            doc_ids.push(doc.id);
            doc
        });

        if keyspace != INDEX_FRAGMENTS {
            // We cheat here when trigger the listeners as we know
            // LMDB will do all-or-nothing in the transaction.
            return if let Err(e) = self
                .lmdb_store
                .mark_many_as_tombstone(keyspace, documents)
                .await
            {
                let keys = e.successful_doc_ids().to_vec();
                Err(BulkMutationError::new(
                    StorageError::Lmdb(e.into_inner()),
                    keys,
                ))
            } else {
                for doc_id in doc_ids {
                    self.listeners.trigger_on_del(keyspace, doc_id);
                }
                Ok(())
            };
        }

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        self.lmdb_store
            .get(keyspace, doc_id)
            .await
            .map_err(StorageError::Lmdb)
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        self.lmdb_store
            .multi_get(keyspace, doc_ids)
            .await
            .map_err(StorageError::Lmdb)
    }
}
