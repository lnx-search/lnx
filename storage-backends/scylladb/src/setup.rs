use serde_json::Value;

use lnx_storage::templates::setup::SetupOnce;
use lnx_storage::async_trait;

use crate::connection::{ConnectionConfig, connect};

pub struct ScyllaCluster;

#[async_trait]
impl SetupOnce for ScyllaCluster {
    async fn setup_once(config: Value) -> anyhow::Result<()> {
        let config: ConnectionConfig = serde_json::from_value(config)?;


        Ok(())
    }
}
