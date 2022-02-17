use thiserror::Error;

#[derive(Debug, Error)]
pub enum DocumentError {
    #[error("Unknown index {0:?}")]
    UnknownIndex(String),

    #[error("An error occurred while trying to join pending tasks: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("An error occurred while trying to complete this operation: {0}")]
    Error(#[from] anyhow::Error),
}
