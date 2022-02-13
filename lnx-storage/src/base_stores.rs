use std::net::SocketAddr;
use std::path::PathBuf;

use async_trait::async_trait;
use anyhow::Result;

use crate::change_log::Timestamp;

/// A synonym mapping which maps a given word to a set of synonyms.
pub struct Synonyms {
    pub word: String,
    pub synonyms: Vec<String>,
}

#[async_trait]
pub trait MetaStore {
    /// Add a set of stop words to the existing set of words.
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()>;

    /// Remove a set of stop words from the existing set of words.
    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()>;

    /// Fetch all stop words.
    async fn fetch_stopwords(&self) -> Result<Vec<String>>;

    /// Add a set of synonyms to the existing set.
    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<()>;

    /// Remove a set of synonyms from the existing set.
    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()>;

    /// Fetch all stop synonyms.
    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>>;

    /// Set the node's last updated timestamp.
    ///
    /// This should be a persistent operation as it is required for the node
    /// to understand where it currently stand in the index history.
    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()>;

    /// Get node's the last updated timestamp.
    ///
    /// Used for determining the document changed need to be applied.
    ///
    /// If None is required it is assumed that the index completely blank and
    /// has no existing history.
    async fn get_last_update_timestamp(&self) -> Result<Option<Timestamp>>;

    /// Get the earliest timestamp where all nodes are up to date.
    ///
    /// This should be the timestamp where all node's last updated timestamps, are
    /// older than the ts.
    ///
    /// E.g.
    /// If we have 3 nodes with the following states respectively:
    ///     `124`, `125`, `110`
    /// Then our earliest aligned timestamp is `110`.
    async fn get_earliest_aligned_timestamp(&self) -> Result<Option<Timestamp>>;

    /// Load the existing index from the given peer and download it with
    /// the given output directory.
    async fn load_index_from_peer(&self, address: SocketAddr, out_dir: PathBuf) -> Result<()>;
}

#[async_trait]
pub trait EngineStore {
}
