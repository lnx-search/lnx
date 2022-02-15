use hashbrown::HashMap;
use once_cell::sync::OnceCell;
use scylla::batch::Consistency;
use scylla::transport::errors::{DbError, QueryError};
use scylla::transport::Compression;
use serde::{Deserialize, Serialize};
use lnx_utils::index_id;

use super::error::ConnectionError;
use super::session::Session;

static CONNECTION: OnceCell<Session> = OnceCell::new();
static KEYSPACE_PREFIX: &str = "lnx_search";

pub(crate) fn keyspace(index_name: &str) -> String {
    format!(
        "{prefix}_{index}",
        prefix = KEYSPACE_PREFIX,
        index = index_id(index_name)
    )
}

pub async fn connect(
    nodes: &[impl AsRef<str>],
    user: &Option<String>,
    password: &Option<String>,
) -> Result<(), ConnectionError> {
    let mut builder = scylla::SessionBuilder::new().compression(Some(Compression::Lz4));

    if let (Some(user), Some(pass)) = (user, password) {
        builder = builder.user(user, pass);
    }

    let session = if (nodes.len() % 2) == 0 {
        builder
            .known_nodes(nodes)
            .default_consistency(Consistency::Any)
            .build()
            .await
    } else {
        builder
            .known_nodes(nodes)
            .default_consistency(Consistency::Quorum)
            .build()
            .await
    }?;

    let _ = CONNECTION.set(Session::from(session));

    Ok(())
}

pub fn session() -> &'static Session {
    CONNECTION.get().unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)] // We don't massively care about it's flaws here.
pub enum ReplicationInfo {
    /// Marks the keyspace connection as a `SimpleStrategy` used for
    /// development purposes.
    ///
    /// To try and enforce this idea that it's only for development we
    /// limit the amount of nodes to 1.
    Simple,

    /// The network topology strategy. This is our production setup.
    ///
    /// The replication factor must be provided if no datacenters are provided
    /// with their own relevant node setup.
    NetworkTopology {
        replication_factor: Option<usize>,

        #[serde(flatten, default)]
        datacenters: HashMap<String, usize>,
    },
}

impl ReplicationInfo {
    pub async fn build_keyspace(self, index_name: &str) -> Result<(), ConnectionError> {
        match self {
            Self::Simple => {
                let keyspace_query = format!(
                    r#"
                    CREATE KEYSPACE {ks}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                    "#,
                    ks = keyspace(index_name),
                );

                let res = session().query(keyspace_query.as_str(), &[]).await;

                match res {
                    Ok(_) => Ok(()),
                    Err(QueryError::DbError(DbError::AlreadyExists { .. }, ..)) => {
                        Ok(())
                    },
                    Err(e) => Err(e.into()),
                }
            },
            Self::NetworkTopology {
                replication_factor,
                datacenters,
            } => {
                let mut parts = vec![];
                if let Some(factor) = replication_factor {
                    parts.push(format!("'replication_factor': {}", factor));
                }

                for (dc, factor) in datacenters {
                    parts.push(format!("'{}': {}", dc, factor));
                }

                let keyspace_query = format!(
                    r#"
                    CREATE KEYSPACE {ks}
                    WITH replication = {{'class': 'NetworkTopologyStrategy', {config}}}
                    "#,
                    ks = keyspace(index_name),
                    config = parts.join(", "),
                );

                let res = session().query(keyspace_query.as_str(), &[]).await;

                match res {
                    Ok(_) => Ok(()),
                    Err(QueryError::DbError(DbError::AlreadyExists { .. }, ..)) => {
                        Ok(())
                    },
                    Err(e) => Err(e.into()),
                }
            },
        }
    }
}
