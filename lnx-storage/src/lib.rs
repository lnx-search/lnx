mod fragments;
mod models;
use datacake::crdt::{HLCTimestamp, Key};
use datacake::eventual_consistency::{
    BulkMutationError,
    Document,
    DocumentMetadata,
    PutContext,
    Storage,
};
use datacake::rpc::async_trait;
use datacake::sqlite::SqliteStorage;

pub static DOC_BLOCK_KEYSPACE_SUFFIX: &str = "_doc_blocks";
pub static INDEX_FRAGMENTS_SUFFIX: &str = "_fragments";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Sqlite Error: {0}")]
    Sqlite(<SqliteStorage as Storage>::Error),
}

pub struct LnxStorage {
    sqlite_store: SqliteStorage,
}

#[async_trait]
impl Storage for LnxStorage {
    type Error = StorageError;
    type DocsIter = ();
    type MetadataIter = <SqliteStorage as Storage>::MetadataIter;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        self.sqlite_store
            .get_keyspace_list()
            .await
            .map_err(StorageError::Sqlite)
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        self.sqlite_store
            .iter_metadata(keyspace)
            .await
            .map_err(StorageError::Sqlite)
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Err(e) = self.sqlite_store.remove_tombstones(keyspace, keys).await {
            let keys = e.successful_doc_ids().to_vec();
            Err(BulkMutationError::new(
                StorageError::Sqlite(e.into_inner()),
                keys,
            ))
        } else {
            Ok(())
        }
    }

    async fn put_with_ctx(
        &self,
        keyspace: &str,
        document: Document,
        _ctx: Option<&PutContext>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn multi_put_with_ctx(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
        _ctx: Option<&PutContext>,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.sqlite_store
            .mark_as_tombstone(keyspace, doc_id, timestamp)
            .await
            .map_err(StorageError::Sqlite)?;

        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        if let Err(e) = self
            .sqlite_store
            .mark_many_as_tombstone(keyspace, documents)
            .await
        {
            let keys = e.successful_doc_ids().to_vec();
            return Err(BulkMutationError::new(
                StorageError::Sqlite(e.into_inner()),
                keys,
            ));
        }

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<Document>, Self::Error> {
        todo!()
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        todo!()
    }
}
