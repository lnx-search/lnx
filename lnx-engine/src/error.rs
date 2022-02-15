use thiserror::Error;


#[derive(Debug, Error)]
pub enum EngineStartError {
    #[error("failed to start engine due to a storage error: {0}")]
    StorageError(anyhow::Error),

    #[error("failed to start engine due to a writer error: {0}")]
    IndexerError(anyhow::Error),
}