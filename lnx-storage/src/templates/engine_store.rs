use async_trait::async_trait;

#[async_trait]
pub trait EngineStore {
    async fn fetch_indexes(&self) -> Vec<(String, serde_json::Value)>;
}
