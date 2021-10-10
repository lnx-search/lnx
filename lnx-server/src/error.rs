use std::fmt::{Display, Formatter};
use hyper::{Body, Response};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, LnxError>;

#[derive(Error, Debug)]
pub enum LnxError {
    #[error("unable to complete index operation: {0}")]
    Other(#[from] anyhow::Error),

    #[error("failed to process request due to a server error: {0}")]
    ServerError(#[from] hyper::Error),

    #[error("failed serialize / deserialize value: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("invalid request given: {0}")]
    BadRequest(&'static str),

    #[error("missing required authorization context: {0}")]
    UnAuthorized(&'static str),

    #[error("request aborted early")]
    AbortRequest(Response<Body>),
}
