use scylla::transport::errors::{NewSessionError, QueryError};
use thiserror::Error;


#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("failed to connect to cluster: {0}")]
    ConnectError(#[from] NewSessionError),

    #[error("failed to execute query: {0}")]
    QueryError(#[from] QueryError)
}