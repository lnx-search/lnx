use lnx_storage::async_trait;
use lnx_storage::templates::setup::SetupOnce;
use serde_json::Value;

use crate::connection::{connect, ConnectionConfig};

pub struct ScyllaCluster;

#[async_trait]
impl SetupOnce for ScyllaCluster {
    async fn setup_once(config: Value) -> anyhow::Result<()> {
        let config: ConnectionConfig = serde_json::from_value(config)?;

        connect(config).await?;

        Ok(())
    }
}
