use scylladb_backend::ConnectionConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackendSelector {
    Scylla(ConnectionConfig),
}
