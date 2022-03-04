use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;
use lnx_metrics::Stats;

/// A synonym mapping which maps a given word to a set of synonyms.
pub struct Synonyms {
    pub word: String,
    pub synonyms: Vec<String>,
}

#[async_trait]
pub trait MetaStore: Stats + Send + Sync + 'static {
    async fn setup(&self) -> Result<()>;

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
    async fn set_update_timestamp(&self, timestamp: chrono::Duration) -> Result<()>;

    /// Get node's the last updated timestamp.
    ///
    /// Used for determining the document changed need to be applied.
    ///
    /// If None is required it is assumed that the index completely blank and
    /// has no existing history.
    async fn get_last_update_timestamp(&self) -> Result<Option<chrono::Duration>>;

    /// Load the existing index from the most up to date peer and download it with
    /// the given output directory.
    async fn load_index_from_peer(&self, out_dir: &Path) -> Result<()>;

    /// Called in order to mark the node as active still.
    async fn heartbeat(&self, purge_delta: chrono::Duration) -> Result<()>;
}
