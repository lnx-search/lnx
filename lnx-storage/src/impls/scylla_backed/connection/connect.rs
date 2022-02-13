use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use hashbrown::HashMap;
use scylla::batch::Consistency;
use scylla::transport::Compression;

use super::session::Session;
use super::error::ConnectionError;


static KEYSPACE_PREFIX: &str = "lnx_search";


#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]  // We don't massively care about it's flaws here.
pub enum ConnectionInfo {
    /// Marks the keyspace connection as a `SimpleStrategy` used for
    /// development purposes.
    ///
    /// To try and enforce this idea that it's only for development we
    /// limit the amount of nodes to 1.
    Simple(SocketAddr),

    /// The network topology strategy. This is our production setup.
    ///
    /// The replication factor must be provided if no datacenters are provided
    /// with their own relevant node setup.
    NetworkTopology {
        nodes: Vec<SocketAddr>,

        replication_factor: Option<usize>,

        #[serde(flatten, default)]
        datacenters: HashMap<String, usize>,
    }
}

impl ConnectionInfo {
    pub async fn connect(self, index_name: &str) -> Result<Session, ConnectionError> {
        let builder = scylla::SessionBuilder::new()
            .compression(Some(Compression::Lz4));

        let session = match self {
            Self::Simple(addr) => {
                let session = builder
                    .known_node_addr(addr)
                    .default_consistency(Consistency::Any)
                    .build()
                    .await?;

                let keyspace_query = format!(
                    r#"
                    CREATE KEYSPACE {prefix}_{index}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                    "#,
                    prefix = KEYSPACE_PREFIX,
                    index = index_name,
                );

                session.query(keyspace_query.as_str(), &[]).await?;

                session
            },
            Self::NetworkTopology {
                nodes,
                replication_factor,
                datacenters,
            } => {
                let session = builder
                    .known_nodes_addr(&nodes)
                    .default_consistency(Consistency::Quorum)
                    .build()
                    .await?;

                let mut parts = vec![];
                if let Some(factor) = replication_factor {
                    parts.push(format!("'replication_factor': {}", factor));
                }

                for (dc, factor) in datacenters {
                    parts.push(format!("'{}': {}", dc, factor));
                }

                let keyspace_query = format!(
                    r#"
                    CREATE KEYSPACE {prefix}_{index}
                    WITH replication = {{'class': 'NetworkTopologyStrategy', {config}}}
                    "#,
                    prefix = KEYSPACE_PREFIX,
                    index = index_name,
                    config = parts.join(", "),
                );

                session.query(keyspace_query.as_str(), &[]).await?;

                session
            }
        };

        Ok(Session::from(session))
    }
}

