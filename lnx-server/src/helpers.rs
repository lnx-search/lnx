use hyper::{Response, Body, Request};

use crate::error::Result;

pub type LnxRequest = Request<Body>;
pub type LnxResponse = Result<Response<Body>>;

#[macro_export]
macro_rules! abort {
    ($status:expr, $val:expr) => {{
        Err(LnxError::AbortRequest(json_response(
            $status,
            $val,
        )?))
    }}
}

#[macro_export]
macro_rules! unauthorized {
    ($val:expr) => {{
        Err(LnxError::UnAuthorized($val))
    }}
}

#[macro_export]
macro_rules! bad_request {
    ($val:expr) => {{
        Err(LnxError::BadRequest($val))
    }}
}

#[macro_export]
macro_rules! json {
    ($body:expr) => {{
        let body = to_bytes($body).await?;
        serde_json::from_slice(&body)?
    }}
}