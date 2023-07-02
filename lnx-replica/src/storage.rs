use datacake::crdt::{HLCTimestamp, Key};
use datacake::eventual_consistency::{
    BulkMutationError,
    Document,
    DocumentMetadata,
    Storage,
};
use datacake::rpc::async_trait;

use crate::doc_blocks::DocBlockService;
use crate::metadata::MetadataStorage;

pub struct StorageService {
    block_storage: DocBlockService,
    metadata_storage: MetadataStorage,
}

#[async_trait]
impl Storage for StorageService {
    type Error = ();
    type DocsIter = ();
    type MetadataIter = ();

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        todo!()
    }

    async fn iter_metadata(
        &self,
        keyspace: &str,
    ) -> Result<Self::MetadataIter, Self::Error> {
        todo!()
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn put(&self, keyspace: &str, document: Document) -> Result<(), Self::Error> {
        todo!()
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        todo!()
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
