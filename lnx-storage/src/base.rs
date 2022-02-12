use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use lnx_common::types::document::Document;

use super::error::{
    ChangelogStoreError,
    DocStoreError,
    MetaStoreError,
};

pub type Timestamp = DateTime<Utc>;
pub type DocId = Uuid;

#[derive(Debug, Copy, Clone)]
pub enum ChangeKind {
    Delete,
    Update,
    Append,
}

pub struct ChangeLog {
    pub kind: ChangeKind,
    pub affected_docs: Vec<DocId>,
    pub timestamp: Timestamp,
}

#[async_trait]
pub trait ChangeLogStore {
    async fn append_changes(&self, logs: ChangeLog) -> Result<(), ChangelogStoreError>;

    async fn get_pending_changes(&self) -> Result<Vec<ChangeLog>, ChangelogStoreError>;

    async fn set_current_timestamp(&self, timestamp: Timestamp) -> Result<(), ChangelogStoreError>;
}

#[async_trait]
pub trait DocStore: ChangeLogStore {
    async fn add_documents<DOCS>(&self, docs: DOCS) -> Result<(), DocStoreError>
    where
        DOCS: Iterator<Item = (DocId, Document)>;

    async fn remove_documents(&self, docs: Vec<DocId>) -> Result<(), DocStoreError>;

    async fn fetch_documents(
        &self,
        fields: &[impl AsRef<&str>],
        docs: Vec<DocId>,
    ) -> Result<Vec<Document>, DocStoreError>;
}

pub struct Synonyms {
    pub word: String,
    pub synonyms: Vec<String>,
}

#[async_trait]
pub trait MetaStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<(), MetaStoreError>;
    async fn remove_stopwords(&self, words: Vec<String>) -> Result<(), MetaStoreError>;
    async fn fetch_stopwords(&self) -> Result<Vec<String>, MetaStoreError>;

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<(), MetaStoreError>;
    async fn remove_synonyms(&self, words: Vec<String>) -> Result<(), MetaStoreError>;
    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>, MetaStoreError>;
}

#[async_trait]
pub trait EngineStore {
}
