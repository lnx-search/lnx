use async_trait::async_trait;
use lnx_common::index::context::IndexContext;

#[async_trait]
pub trait EngineStore: Sync + Send + 'static {
    async fn fetch_indexes(&self) -> anyhow::Result<Vec<(String, IndexContext)>>;

    async fn remove_index(&self, name: &str) -> anyhow::Result<()>;

    async fn add_index(&self, ctx: IndexContext) -> anyhow::Result<()>;
}
