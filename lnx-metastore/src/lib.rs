mod backends;

use async_trait::async_trait;
use anyhow::Result;
use time::OffsetDateTime;

/// The core metadata storage trait.
///
/// This is just a super trait for the separately defined metastore components.
pub trait Metastore:
    IndexesMetastore
    + DfsMetadata
    + NodesMetadata {}

impl<T> Metastore for T
where
    T: IndexesMetastore + DfsMetadata + NodesMetadata {}


#[async_trait]
/// The metastore operations for the indexes system.
pub trait IndexesMetastore {
    /// Create an index.
    async fn create_index(&self, info: IndexInfo) -> Result<()>;

    /// Remove an index.
    async fn remove_index(&self, index_id: String) -> Result<()>;

    /// Get all active indexes.
    async fn get_indexes(&self) -> Result<Vec<IndexInfo>>;
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Index metadata information.
pub struct IndexInfo {
    /// The unique ID of the index.
    pub index_id: String,
    /// The fields information.
    pub field_mapping: Versioned<serde_json::Value>,
    /// The search specific settings.
    pub search_settings: Versioned<IndexSearchSettings>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
/// A versioning enum for things likely to change.
pub enum Versioned<T> {
    /// Version 1
    V1(T)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Index specific search settings.
///
/// These are settings that override the user-provided settings.
pub struct IndexSearchSettings {
    /// The maximum number of search requests than can be
    /// processed concurrently.
    pub max_search_concurrency: usize,
    /// The maximum number of threads used for a single search request.
    pub max_search_threads: usize,
}


#[async_trait]
/// The metastore operations for the distributed file system.
pub trait DfsMetadata {
    /// Adds a new file to the system.
    async fn add_file(&self, file_name: String, seed_nodes: Vec<String>) -> Result<()>;

    /// Deletes a file from the system.
    async fn del_file(&self, file_name: String) -> Result<()>;

    /// Get all files created since the given timestamp.
    async fn get_files(&self, since: Option<time::OffsetDateTime>) -> Result<()>;
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Information of a given file.
pub struct FileInfo {
    /// The unique ID of the file.
    pub file_id: u64,
    /// The name of the file (including path.)
    pub file_name: String,
    /// The nodes which start with this file.
    ///
    /// This information may become stale, if no node in this list has the file
    /// the system must ask all nodes.
    pub seed_nodes: Vec<String>,
    /// The timestamp in which the file was created.
    pub created_at: OffsetDateTime,
}


#[async_trait]
/// The metastore operations for the node cluster.
pub trait NodesMetadata {
    /// Marks the node as joining the cluster.
    async fn join_cluster(&self, info: NodeInformation) -> Result<()>;

    /// Attempts to update the node's generation signalling that it is still alive.
    async fn update_node_readiness(&self, node_id: String) -> Result<()>;

    /// Leaves the cluster.
    ///
    /// This does not need to necessarily be explicitly called, but it can be useful
    /// to more quickly reflect when a node has shutdown rather than crashed.
    async fn leave_cluster(&self, node_id: String) -> Result<()>;

    /// Fetches the information relating to the cluster.
    async fn get_nodes(&self) -> Result<NodeInformation>;
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Information about the node.
pub struct NodeInformation {
    /// The unique ID of the node.
    pub node_id: String,
    /// The node generation.
    ///
    /// This is used to signal liveness.
    pub generation: u64,
    /// The information of system running the node.
    pub system_info: SystemInfo,
    /// The address which other nodes can reach this node on.
    pub broadcast_address: String,
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Information around the node system info.
pub struct SystemInfo {
    /// The number of CPU cores available to lnx.
    pub num_cpus: u32,
    /// The amount of memory available to lnx overall.
    pub num_memory_bytes: u64,
    /// The amount of indexing-memory available to lnx.
    pub max_indexing_memory_bytes: u64,
}