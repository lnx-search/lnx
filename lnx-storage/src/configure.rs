use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use lnx_common::schema::Schema;
use serde::{Deserialize, Serialize};

use crate::impls::scylla_backed;
use crate::impls::scylla_backed::{
    ScyllaEngineStore,
    ScyllaMetaStore,
    ScyllaPrimaryDataStore,
};
use crate::{DocStore, EngineStore, MetaStore};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub backend: BackendSelector,
    pub storage_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum BackendSelector {
    /// Store system backed by
    Scylla {
        nodes: Vec<String>,
        user: Option<String>,
        password: Option<String>,
    },
}

impl BackendSelector {
    pub async fn setup(&self) -> Result<()> {
        match self {
            Self::Scylla {
                nodes,
                user,
                password,
            } => {
                scylla_backed::connect(nodes, user, password).await?;
            },
        }

        Ok(())
    }

    pub fn get_doc_store(&self, index_name: &str, schema: &Schema) -> Arc<dyn DocStore> {
        match self {
            Self::Scylla { .. } => {
                let fields = schema.fields().keys().map(|v| v.to_string()).collect();

                let backend = ScyllaPrimaryDataStore::with_fields(index_name, fields);
                Arc::new(backend)
            },
        }
    }

    pub fn get_meta_store(
        &self,
        index_name: &str,
        local_data: &sled::Db,
    ) -> Result<Arc<dyn MetaStore>> {
        let backend: Arc<dyn MetaStore> = match self {
            Self::Scylla { .. } => {
                let tree = local_data.open_tree(format!("{}_meta_data", index_name))?;
                let backend = ScyllaMetaStore::load_from_local(index_name, tree)?;
                Arc::new(backend)
            },
        };

        Ok(backend)
    }

    pub fn get_engine_store(&self) -> Box<dyn EngineStore> {
        let backend: Box<dyn EngineStore> = match self {
            Self::Scylla { .. } => {
                let backend = ScyllaEngineStore::new();
                Box::new(backend)
            },
        };

        backend
    }
}
