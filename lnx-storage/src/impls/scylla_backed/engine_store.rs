use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use lnx_common::schema::{FieldName, Schema};
use lnx_utils::{FromJSON, ToJSON};
use scylla::IntoTypedRows;

use crate::engine_store::{IndexData, PollingMode};
use crate::impls::scylla_backed::connection::{keyspace, session};
use crate::impls::scylla_backed::ReplicationInfo;
use crate::EngineStore;

pub static KEYSPACE: &str = "lnx_search_engine";
pub static INDEXES_TABLE: &str = "indexes";

pub struct ScyllaEngineStore {
    keyspace: String,
}

impl ScyllaEngineStore {
    pub fn new() -> Self {
        Self {
            keyspace: KEYSPACE.to_string(),
        }
    }
}

#[async_trait]
impl EngineStore for ScyllaEngineStore {
    async fn setup(&self) -> Result<()> {
        super::tables::create_indexes_table().await?;
        Ok(())
    }

    async fn fetch_indexes(&self) -> Result<Vec<IndexData>> {
        let query = format!(
            "SELECT name, index_schema, polling_mode, replication, settings FROM {ks}.{table};",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        let results = session()
            .query_prepared(&query, &[])
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(
                String,
                Vec<u8>,
                Vec<u8>,
                Vec<u8>,
                Option<HashMap<String, Vec<u8>>>,
            )>();

        let mut indexes = vec![];
        for index in results {
            let (name, schema, plolling_mode, replication, settings) = index?;

            let schema = Schema::from_json(&schema)?;
            let replication = ReplicationInfo::from_json(&replication)?;

            indexes.push(IndexData {
                index_name: FieldName(name),
                schema,
                replication,
                polling_mode: PollingMode::from_json(&plolling_mode)?,
                additional_settings: settings.unwrap_or_default(),
            })
        }

        Ok(indexes)
    }

    async fn store_index(&self, index: IndexData) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {ks}.{table} (
                name,
                index_schema,
                polling_mode,
                replication,
                settings
            ) VALUES (?, ?, ?, ?, ?);
            "#,
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        session()
            .query_prepared(
                &query,
                (
                    index.index_name.0,
                    index.schema.to_json()?,
                    index.polling_mode.to_json()?,
                    index.replication.to_json()?,
                    index.additional_settings,
                ),
            )
            .await?;

        Ok(())
    }

    async fn remove_index(&self, index: &str) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE name = ?;",
            ks = self.keyspace,
            table = INDEXES_TABLE,
        );

        session().query_prepared(&query, (index,)).await?;

        let query = format!("DROP KEYSPACE IF EXISTS {ks};", ks = keyspace(index),);

        session().query_prepared(&query, &[]).await?;

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
