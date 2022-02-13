use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use scylla::IntoTypedRows;
use uuid::Uuid;
use lnx_utils::{FromBytes, ToBytes};

use super::connection::Session;
use crate::base_stores::MetaStore;
use crate::change_log::Timestamp;
use crate::Synonyms;


static STOPWORDS_TABLE: &str = "index_stopwords";
static SYNONYMS_TABLE: &str = "index_synonyms";
static NODES_INFO_TABLE: &str = "index_nodes";

static LAST_UPDATED_TIMESTAMP_KEY: &[u8] = b"LAST_UPDATED";


pub struct ScyllaMetaStore {
    node_id: Uuid,
    local_store: sled::Tree,
    session: Arc<Session>
}

#[async_trait]
impl MetaStore for ScyllaMetaStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "INSERT INTO {table} (word) VALUES (?);",
            table = STOPWORDS_TABLE,
        );

        for word in words {
            self.session.query_prepared(&query, (word,)).await?;
        }

        Ok(())
    }

    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "DELETE FROM {table} WHERE word = ?;",
            table = STOPWORDS_TABLE,
        );

        self.session.query_prepared(&query, (words,)).await?;

        Ok(())
    }

    async fn fetch_stopwords(&self) -> Result<Vec<String>> {
        let query = format!(
            "SELECT word FROM {table};",
            table = STOPWORDS_TABLE,
        );

        let mut iter = self.session
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
            "INSERT INTO {table} (word, synonyms) VALUES (?, ?);",
            table = SYNONYMS_TABLE,
        );

        for group in words {
            self.session.query_prepared(&query, (group.word, group.synonyms)).await?;
        }

        Ok(())
    }

    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()> {
        let query = format!(
            "DELETE FROM {table} WHERE word = ?;",
            table = SYNONYMS_TABLE,
        );

        self.session.query_prepared(&query, (words,)).await?;

        Ok(())
    }

    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>> {
        let query = format!(
            "SELECT word, synonyms FROM {table};",
            table = SYNONYMS_TABLE,
        );

        let mut iter = self.session
            .query_iter(&query, &[])
            .await?
            .into_typed::<(String, Vec<String>)>();

        let mut groups = vec![];
        while let Some(row) = iter.next().await {
            let (word, synonyms) = row?;
            groups.push(Synonyms {
                word,
                synonyms,
            });
        }

        Ok(groups)
    }

    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()> {
        self.local_store.insert(LAST_UPDATED_TIMESTAMP_KEY, timestamp.to_bytes()?)?;

        let query = format!(
            "INSERT INTO {table} (node_id, timestamp) VALUES (?, ?);",
            table = NODES_INFO_TABLE,
        );

        self.session.query_prepared(&query, (self.node_id, timestamp)).await?;

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
            "SELECT MIN (timestamp) FROM {table};",
            table = NODES_INFO_TABLE,
        );

        let result = self.session
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
}