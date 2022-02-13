use async_trait::async_trait;
use anyhow::Result;
use hashbrown::HashMap;
use serde::{Serialize, Deserialize};

use lnx_common::schema::{FieldName, Schema};

use crate::impls::scylla_backed::ReplicationInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexData {
    pub index_name: FieldName,
    pub schema: Schema,
    pub replication: ReplicationInfo,
    pub additional_settings: HashMap<String, Vec<u8>>,
}


#[async_trait]
pub trait EngineStore: Send + Sync + 'static {
    async fn fetch_indexes(&self) -> Result<Vec<IndexData>>;

    async fn store_index(&self, index: IndexData) -> Result<()>;

    async fn remove_index(&self, index: IndexData) -> Result<()>;

    async fn update_settings(&self, index_name: &FieldName, settings: HashMap<String, Vec<u8>>) -> Result<()>;

    async fn fetch_latest_settings(&self, index_name: &FieldName) -> Result<HashMap<String, Vec<u8>>>;
}
