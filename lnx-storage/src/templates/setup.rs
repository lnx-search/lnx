use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait SetupOnce {
    async fn setup_once(config: Value) -> Result<()>;
}
