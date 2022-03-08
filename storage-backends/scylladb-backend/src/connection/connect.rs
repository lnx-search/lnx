use std::time::Duration;

use hashbrown::HashMap;
use lnx_common::configuration::SEARCH_ENGINE_CONFIGURATION_KEYSPACE;
use lnx_common::index::context::IndexContext;
use once_cell::sync::OnceCell;
use scylla::transport::errors::{DbError, QueryError};
use scylla::transport::Compression;
use serde::{Deserialize, Serialize};

use super::error::ConnectionError;
use super::session::Session;

static CONNECTION: OnceCell<Session> = OnceCell::new();

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
}

impl Into<scylla::frame::types::Consistency> for Consistency {
    fn into(self) -> scylla::frame::types::Consistency {
        use scylla::frame::types::Consistency;

        match self {
            Self::Any => Consistency::Any,
            Self::One => Consistency::One,
            Self::Two => Consistency::Two,
            Self::Three => Consistency::Three,
            Self::Quorum => Consistency::Quorum,
            Self::All => Consistency::All,
            Self::LocalQuorum => Consistency::LocalQuorum,
            Self::EachQuorum => Consistency::EachQuorum,
            Self::LocalOne => Consistency::LocalOne,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    nodes: Vec<String>,
    user: Option<String>,
    password: Option<String>,
    pub engine_replication: ReplicationInfo,
    connection_timeout: Option<u64>,
    consistency: Consistency,
}

pub async fn connect(config: ConnectionConfig) -> Result<(), ConnectionError> {
    let mut builder = scylla::SessionBuilder::new()
        .compression(Some(Compression::Lz4))
        .connection_timeout(Duration::from_secs(
            config.connection_timeout.unwrap_or(30),
        ));

    if let (Some(user), Some(pass)) = (config.user, config.password) {
        builder = builder.user(user, pass);
    }

    let session = if (config.nodes.len() == 1) || (config.nodes.len() % 2) == 0 {
        builder
            .known_nodes(&config.nodes)
            .default_consistency(config.consistency.into())
            .build()
            .await
    } else {
        builder
            .known_nodes(&config.nodes)
            .default_consistency(config.consistency.into())
            .build()
            .await
    }?;

    let _ = CONNECTION.set(Session::from(session));

    config
        .engine_replication
        .build_keyspace(SEARCH_ENGINE_CONFIGURATION_KEYSPACE)
        .await?;

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

    pub async fn build_index_keyspace(
        &self,
        ctx: &IndexContext,
    ) -> Result<(), ConnectionError> {
        self.build_keyspace(&ctx.keyspace()).await
    }

    #[instrument(name = "keyspace-replication", skip(self))]
    pub async fn build_keyspace(&self, ks: &str) -> Result<(), ConnectionError> {
        info!("Ensuring keyspace exists...");
        let query = self.format_keyspace(ks);
        let res = session().query(&query, &[]).await;

        match res {
            Ok(_) => {},
            Err(QueryError::DbError(DbError::AlreadyExists { .. }, ..)) => {},
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}
