use scylladb_backend::{ConnectionConfig, ReplicationInfo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackendSelector {
    Scylla(ConnectionConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexStorageConfig {
    Scylla(serde_json::Value),
}
