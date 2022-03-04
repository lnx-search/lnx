use crate::types::document::Document;
use crate::types::ConversionError;

#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Tantivy Error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Other Error: {0}")]
    Other(#[from] anyhow::Error),

    #[error("Failed to convert document: {0}")]
    ConversionError(#[from] ConversionError),

    #[error("The given document failed validation checks: {0}")]
    ValidationError(String),
}

pub trait DocProcessor {
    fn process_document(&self, doc: Document) -> Result<Document, ProcessingError>;
}
