use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use lnx_utils::{FromBytes, ToBytes};
use scylla::IntoTypedRows;
use uuid::Uuid;

use super::connection::keyspace;
use crate::change_log::Timestamp;
use crate::impls::scylla_backed::connection::session;
use crate::meta_store::MetaStore;
use crate::Synonyms;

static STOPWORDS_TABLE: &str = "index_stopwords";
static SYNONYMS_TABLE: &str = "index_synonyms";
static NODES_INFO_TABLE: &str = "index_nodes";

static NODE_ID_KEY: &[u8] = b"NODE_ID";
static LAST_UPDATED_TIMESTAMP_KEY: &[u8] = b"LAST_UPDATED";

pub struct ScyllaMetaStore {
    keyspace: String,
    node_id: Uuid,
    local_store: sled::Tree,
}

impl ScyllaMetaStore {
    pub fn load_from_local(index_name: &str, db: sled::Tree) -> Result<Self> {
        let id = match db.get(NODE_ID_KEY)? {
            Some(id) => FromBytes::from_bytes(&id)?,
            None => Uuid::new_v4(),
        };

        Ok(Self {
            keyspace: keyspace(index_name),
            node_id: id,
            local_store: db,
        })
    }
}

#[async_trait]
impl MetaStore for ScyllaMetaStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} (word) VALUES (?);",
            ks = self.keyspace,
            table = STOPWORDS_TABLE,
        );

        for word in words {
            session().query_prepared(&query, (word,)).await?;
        }

        Ok(())
    }

    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE word = ?;",
            ks = self.keyspace,
            table = STOPWORDS_TABLE,
        );

        session().query_prepared(&query, (words,)).await?;

        Ok(())
    }

    async fn fetch_stopwords(&self) -> Result<Vec<String>> {
        let query = format!(
            "SELECT word FROM {ks}.{table};",
            ks = self.keyspace,
            table = STOPWORDS_TABLE,
        );

        let mut iter = session()
            .query_iter(&query, &[])
            .await?
            .into_typed::<(String,)>();

        let mut words = vec![];
        while let Some(row) = iter.next().await {
            let (word,) = row?;
            words.push(word);
        }

        Ok(words)
    }

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} (word, synonyms) VALUES (?, ?);",
            ks = self.keyspace,
            table = SYNONYMS_TABLE,
        );

        for group in words {
            session()
                .query_prepared(&query, (group.word, group.synonyms))
                .await?;
        }

        Ok(())
    }

    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "DELETE FROM {ks}.{table} WHERE word = ?;",
            ks = self.keyspace,
            table = SYNONYMS_TABLE,
        );

        session().query_prepared(&query, (words,)).await?;

        Ok(())
    }

    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>> {
        let query = format!(
            "SELECT word, synonyms FROM {ks}.{table};",
            ks = self.keyspace,
            table = SYNONYMS_TABLE,
        );

        let mut iter = session()
            .query_iter(&query, &[])
            .await?
            .into_typed::<(String, Vec<String>)>();

        let mut groups = vec![];
        while let Some(row) = iter.next().await {
            let (word, synonyms) = row?;
            groups.push(Synonyms { word, synonyms });
        }

        Ok(groups)
    }

    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()> {
        self.local_store
            .insert(LAST_UPDATED_TIMESTAMP_KEY, timestamp.to_bytes()?)?;

        let query = format!(
            "INSERT INTO {ks}.{table} (node_id, last_updated, last_heartbeat) VALUES (?, ?, toTimeStamp(now()));",
            ks = self.keyspace,
            table = NODES_INFO_TABLE,
        );

        session()
            .query_prepared(&query, (self.node_id, timestamp))
            .await?;

        Ok(())
    }

    async fn get_last_update_timestamp(&self) -> Result<Option<Timestamp>> {
        let ts = match self.local_store.get(LAST_UPDATED_TIMESTAMP_KEY)? {
            Some(data) => Some(i64::from_bytes(&data)?),
            None => None,
        };

        Ok(ts)
    }

    async fn get_earliest_aligned_timestamp(&self) -> Result<Option<Timestamp>> {
        let query = format!(
            "SELECT MIN (last_updated) FROM {ks}.{table};",
            ks = self.keyspace,
            table = NODES_INFO_TABLE,
        );

        let result = session()
            .query_prepared(&query, &[])
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(Timestamp,)>()
            .next();

        if let Some(Ok(v)) = result {
            Ok(Some(v.0))
        } else {
            Ok(None)
        }
    }

    async fn load_index_from_peer(&self, _out_dir: PathBuf) -> anyhow::Result<()> {
        Err(anyhow!("Loading from a index is not supported"))
    }

    async fn heartbeat(&self, purge_delta: chrono::Duration) -> Result<()> {
        let query = format!(
            "INSERT INTO {ks}.{table} (node_id, last_heartbeat) VALUES (?, toTimeStamp(now()));",
            ks = self.keyspace,
            table = NODES_INFO_TABLE,
        );

        session().query_prepared(&query, (self.node_id,)).await?;

        let query = format!(
            "SELECT node_id, last_heartbeat FROM {ks}.{table};",
            ks = self.keyspace,
            table = NODES_INFO_TABLE,
        );

        let mut iter = session()
            .query_prepared(&query, (self.node_id,))
            .await?
            .rows
            .unwrap_or_default()
            .into_typed::<(Uuid, chrono::Duration)>();

        let mut purge_nodes = vec![];
        let now = chrono::Utc::now().timestamp();
        while let Some(Ok((node_id, last_heartbeat))) = iter.next() {
            if (now - last_heartbeat.num_seconds()) > purge_delta.num_seconds() {
                purge_nodes.push(node_id);
            }
        }

        Ok(())
    }
}
