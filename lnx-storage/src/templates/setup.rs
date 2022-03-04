use async_trait::async_trait;
use anyhow::Result;
use serde_json::Value;


#[async_trait]
pub trait SetupOnce {
    async fn setup_once(config: Value) -> Result<()>;
}

#[async_trait]
pub trait SetupForIndex {
    async fn setup(index_name: config: Value) -> Result<()>;    
}