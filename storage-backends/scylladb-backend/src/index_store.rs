use hashbrown::HashSet;
use std::path::Path;

use anyhow::Result;
use lnx_common::index::context::IndexContext;
use lnx_common::types::document::{DocId, TypeSafeDocument};
use lnx_storage::async_trait;
use lnx_storage::templates::change_log::{
    ChangeLogEntry,
    ChangeLogIterator,
    ChangeLogStore,
};
use lnx_storage::templates::doc_store::{DocStore, DocumentIterator, DocumentUpdate};
use lnx_storage::templates::meta_store::{MetaStore, Synonyms};
use lnx_storage::types::{SegmentId, Timestamp};

use crate::ReplicationInfo;

pub struct ScyllaIndexStore {
    ctx: IndexContext,
}

impl ScyllaIndexStore {
    pub async fn setup(
        ctx: IndexContext,
        default_replication: ReplicationInfo,
    ) -> Result<Self> {
        let replication_info: ReplicationInfo = match ctx.storage_config().cloned() {
            None => default_replication,
            Some(cfg) => serde_json::from_value(cfg).unwrap_or(default_replication),
        };

        replication_info.build_index_keyspace(&ctx).await?;

        Ok(Self { ctx })
    }
}

#[async_trait]
impl DocStore for ScyllaIndexStore {
    async fn add_documents(
        &self,
        docs: &[(DocId, TypeSafeDocument)],
    ) -> Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn update_documents(&self, docs: &[DocumentUpdate]) -> Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn remove_documents(&self, docs: &[DocId]) -> Result<HashSet<SegmentId>> {
        todo!()
    }

    async fn clear_documents(&self) -> Result<()> {
        todo!()
    }

    async fn fetch_document(
        &self,
        fields: Option<Vec<String>>,
        docs: DocId,
    ) -> Result<Option<(DocId, SegmentId, TypeSafeDocument)>> {
        todo!()
    }

    async fn iter_documents(
        &self,
        fields: Option<Vec<String>>,
        chunk_size: usize,
        segment_id: Option<SegmentId>,
    ) -> Result<DocumentIterator> {
        todo!()
    }
}

#[async_trait]
impl ChangeLogStore for ScyllaIndexStore {
    async fn append_changes(&self, logs: ChangeLogEntry) -> Result<()> {
        todo!()
    }

    async fn get_pending_changes(&self, from: Timestamp) -> Result<ChangeLogIterator> {
        todo!()
    }

    async fn count_pending_changes(&self, from: Timestamp) -> Result<usize> {
        todo!()
    }
}

#[async_trait]
impl MetaStore for ScyllaIndexStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn fetch_stopwords(&self) -> Result<Vec<String>> {
        todo!()
    }

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<()> {
        todo!()
    }

    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()> {
        todo!()
    }

    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>> {
        todo!()
    }

    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()> {
        todo!()
    }

    async fn get_last_update_timestamp(&self) -> Result<Option<Timestamp>> {
        todo!()
    }

    async fn load_index_from_peer(&self, out_dir: &Path) -> Result<()> {
        todo!()
    }

    async fn update_settings(&self, key: &str, data: Vec<u8>) -> Result<()> {
        todo!()
    }

    async fn remove_settings(&self, key: &str) -> Result<()> {
        todo!()
    }

    async fn load_settings(&self, key: &str) -> Result<Option<Vec<u8>>> {
        todo!()
    }
}
