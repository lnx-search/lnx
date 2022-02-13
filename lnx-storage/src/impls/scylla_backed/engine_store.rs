use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use lnx_common::schema::{FieldName, Schema};
use lnx_utils::{FromBytes, ToBytes};
use scylla::IntoTypedRows;

use super::connection::keyspace;
use crate::engine_store::IndexData;
use crate::impls::scylla_backed::connection::session;
use crate::impls::scylla_backed::ReplicationInfo;
use crate::EngineStore;

pub static ENGINE_STORE_KS_SUFFIX: &str = "engine";
pub static INDEXES_TABLE: &str = "indexes";

pub struct ScyllaEngineStore {
    keyspace: String,
}

impl ScyllaEngineStore {
    pub fn new() -> Self {
        Self {
            keyspace: keyspace(ENGINE_STORE_KS_SUFFIX),
        }
    }
}

#[async_trait]
impl EngineStore for ScyllaEngineStore {
    async fn fetch_indexes(&self) -> Result<Vec<IndexData>> {
        let query = format!(
            "SELECT name, schema, replication, settings FROM {ks}.{table};",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        let results = session()
            .query_prepared(&query, &[])
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(String, Vec<u8>, Vec<u8>, HashMap<String, Vec<u8>>)>();

        let mut indexes = vec![];
        for index in results {
            let (name, schema, replication, settings) = index?;

            let schema = Schema::from_bytes(&schema)?;
            let replication = ReplicationInfo::from_bytes(&replication)?;

            indexes.push(IndexData {
                index_name: FieldName(name),
                schema,
                replication,
                additional_settings: settings,
            })
        }

        Ok(indexes)
    }

    async fn store_index(&self, index: IndexData) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} (name, schema, replication, settings) VALUES (?, ?, ?, ?);",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        session()
            .query_prepared(
                &query,
                (
                    index.index_name.0,
                    index.schema.to_bytes()?,
                    index.replication.to_bytes()?,
                    index.additional_settings,
                ),
            )
            .await?;

        Ok(())
    }

    async fn remove_index(&self, index: IndexData) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE name = ?",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        session()
            .query_prepared(&query, (index.index_name.0,))
            .await?;

        Ok(())
    }

    async fn update_settings(
        &self,
        index_name: &FieldName,
        settings: HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        let query = format!(
            "UPDATE {ks}.{table} SET settings = ? WHERE name = ?;",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        session()
            .query_prepared(&query, (index_name.as_str(), settings))
            .await?;

        Ok(())
    }

    async fn fetch_latest_settings(
        &self,
        index_name: &FieldName,
    ) -> Result<HashMap<String, Vec<u8>>> {
        let query = format!(
            "SELECT settings FROM {ks}.{table} WHERE name = ?;",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        let result = session()
            .query_prepared(&query, (index_name.as_str(),))
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(HashMap<String, Vec<u8>>,)>()
            .next()
            .map(|v| v.map(|v| v.0).unwrap_or_default())
            .unwrap_or_default();

        Ok(result)
    }
}
