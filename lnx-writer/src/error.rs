use lnx_common::schema::ConstraintViolation;
use thiserror::Error;

#[derive(Debug)]
pub struct ValidationError {
    pub position: usize,
    pub errors: Vec<ConstraintViolation>,
}

#[derive(Debug, Error)]
pub enum DocumentError {
    #[error("Unknown index {0:?}")]
    UnknownIndex(String),

    #[error("An error occurred while trying to join pending tasks: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("An error occurred while trying to complete this operation: {0}")]
    Error(#[from] anyhow::Error),

    #[error("Some documents do not match the given schema constraints")]
    ValidationError(Vec<ValidationError>),
}
