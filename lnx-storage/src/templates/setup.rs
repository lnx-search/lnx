use anyhow::Result;
use async_trait::async_trait;
use lnx_common::index::context::IndexContext;
use serde_json::Value;

#[async_trait]
pub trait SetupOnce {
    async fn setup_once(config: Value) -> Result<()>;
}

#[async_trait]
pub trait SetupForIndex: Sized {
    async fn setup(ctx: IndexContext, config: Value) -> Result<Self>;
}
