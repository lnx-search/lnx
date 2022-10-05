mod metadata;
mod readonly;
mod writer;

pub use readonly::ReadOnlyDirectory;
pub use writer::DirectoryWriter;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("The index segment is corrupted. Unable to proceed.")]
    Corrupted,

    #[error("Deserialization Error: {0}")]
    DeserializationError(String),

    #[error("Serialization Error: {0}")]
    SerializationError(String),
}
