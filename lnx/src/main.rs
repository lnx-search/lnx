use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use anyhow::Result;
use tantivy::directory::MmapDirectory;

use lnx_storage::{BackendSelector, PollingMode, ReplicationInfo};
use lnx_utils::Validator;


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = lnx_storage::Config {
        backend: BackendSelector::Scylla {
            nodes: vec!["127.0.0.1:9042".to_string()],
            user: None,
            password: None,
            engine_replication: ReplicationInfo::Simple
        },
        storage_path: PathBuf::from("./tests")
    };

    let indexer_cfg = lnx_writer::IndexerHandlerConfig {
        max_indexer_concurrency: NonZeroUsize::new(1).unwrap(),
    };

    lnx_engine::init_engine(cfg, indexer_cfg).await?;

    let mut schema: lnx_common::schema::Schema = serde_json::from_value(serde_json::json!({
        "fields": {
            "test": {
                "type": "text",
            },
            "name": {
                "type": "string",
            }
        },
    }))?;

    schema.validate()?;

    let path = Path::new("./tests/index");
    std::fs::create_dir_all(path)?;

    let dir = MmapDirectory::open(path)?;
    let does_exist = tantivy::Index::exists(&dir)?;

    let index = if does_exist {
        tantivy::Index::open(dir)
    } else {
        tantivy::Index::open_or_create(dir, schema.as_tantivy_schema())
    }?;

    let ref_schema = index.schema();
    schema.validate_with_tantivy_schema(&ref_schema)?;

    // lnx_engine::add_index(
    //     "test",
    //     schema,
    //     ReplicationInfo::Simple,
    //     PollingMode::Dynamic,
    //     HashMap::new(),
    //     index,
    // ).await?;

    tokio::time::sleep(Duration::from_secs(120)).await;

    Ok(())
}
