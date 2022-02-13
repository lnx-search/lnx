use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use anyhow::Result;

use crate::change_log::Timestamp;
use super::change_log::DocId;

pub struct Synonyms {
    pub word: String,
    pub synonyms: Vec<String>,
}

#[async_trait]
pub trait MetaStore {
    async fn add_stopwords(&self, words: Vec<String>) -> Result<()>;
    async fn remove_stopwords(&self, words: Vec<String>) -> Result<()>;
    async fn fetch_stopwords(&self) -> Result<Vec<String>>;

    async fn add_synonyms(&self, words: Vec<Synonyms>) -> Result<()>;
    async fn remove_synonyms(&self, words: Vec<String>) -> Result<()>;
    async fn fetch_synonyms(&self) -> Result<Vec<Synonyms>>;

    async fn set_update_timestamp(&self, timestamp: Timestamp) -> Result<()>;
    async fn get_last_update_timestamp(&self) -> Result<Option<Timestamp>>;
    async fn load_index_from_peer(&self, address: SocketAddr, out_dir: PathBuf) -> Result<()>;
}

#[async_trait]
pub trait EngineStore {
}
