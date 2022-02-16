use hashbrown::HashMap;
use lnx_utils::index_id;
use once_cell::sync::OnceCell;
use scylla::batch::Consistency;
use scylla::transport::errors::{DbError, QueryError};
use scylla::transport::Compression;
use serde::{Deserialize, Serialize};

use super::error::ConnectionError;
use super::session::Session;
use crate::impls::scylla_backed::engine_store;

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
    replication_info: &ReplicationInfo,
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

    let _ = CONNECTION.set( Session::from(session));

    replication_info.build_keyspace(engine_store::KEYSPACE).await?;

    Ok(())
}

pub fn session() -> &'static Session {
    CONNECTION.get().unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn format_keyspace(&self, ks: &str) -> String {
        match self {
            Self::Simple => {
                format!(
                    r#"
                    CREATE KEYSPACE {ks}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                    "#,
                    ks = ks,
                )
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

                format!(
                    r#"
                    CREATE KEYSPACE {ks}
                    WITH replication = {{'class': 'NetworkTopologyStrategy', {config}}}
                    "#,
                    ks = ks,
                    config = parts.join(", "),
                )
            },
        }
    }

    pub async fn build_index_keyspace(&self, index_name: &str) -> Result<(), ConnectionError> {
        self.build_keyspace(&keyspace(index_name)).await
    }

    #[instrument(name = "keyspace-replication", skip(self))]
    pub async fn build_keyspace(&self, ks: &str) -> Result<(), ConnectionError> {
        info!("Ensuring keyspace exists...");
        let query = self.format_keyspace(ks);
        let res = session()
            .query(&query, &[])
            .await;

        match res {
            Ok(_) => {},
            Err(QueryError::DbError(DbError::AlreadyExists { .. }, ..)) => {},
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}
