use async_trait::async_trait;
use anyhow::Result;

use lnx_common::types::document::Document;

use super::change_log::{ChangeLogStore, DocId};

#[async_trait]
pub trait DocStore: ChangeLogStore {
    async fn add_documents(&self, docs: Vec<(DocId, Document)>) -> Result<()>;

    async fn remove_documents(&self, docs: Vec<DocId>) -> Result<()>;

    async fn fetch_documents(
        &self,
        fields: Option<Vec<String>>,
        docs: Vec<DocId>,
    ) -> Result<Vec<(DocId, Document)>>;
}