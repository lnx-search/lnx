use thiserror::Error;

#[derive(Debug, Error)]
pub enum InitEngineError {
    #[error("failed to start engine due to a storage error: {0}")]
    StorageError(anyhow::Error),

    #[error("failed to start engine due to a tantivy error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("failed to start engine due to and error when attempting to open a directory: {0}")]
    OpenDirectoryError(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error(
        "failed to start engine due to and error when attempting to read data: {0}"
    )]
    OpenReadError(#[from] tantivy::directory::error::OpenReadError),

    #[error("failed to start engine due to a corrupted schema: {0}")]
    CorruptionError(#[from] lnx_common::schema::SchemaError),

    #[error("failed to start engine due to IO error: {0}")]
    IoError(#[from] std::io::Error),
}
