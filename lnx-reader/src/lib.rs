use lnx_storage::StorageError;
use tantivy::TantivyError;

mod deletes;
mod segment;

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("{0}")]
    StorageError(#[from] StorageError),

    #[error("{0}")]
    TantivyError(#[from] TantivyError),

    #[error("Io Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("The segment data format is unsupported for this version of lnx.")]
    Unsupported,
}
