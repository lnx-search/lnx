use serde::{Serialize, Deserialize};

use crate::impls::scylla_backed;


#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum BackendSelector {
    /// Store system backed by
    Scylla(scylla_backed::connection::ConnectionInfo),
}

