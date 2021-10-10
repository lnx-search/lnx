use std::error::Error;
use anyhow::Result;
use hyper::{Request, Response};
use hyper::Body;
use routerify::RequestInfo;

use crate::error::LnxError;
use crate::responders::json_response;

pub async fn handle_404(_request: Request<Body>) -> Result<Response<Body>, LnxError> {
    Ok(json_response(404, "No route matched for path.")?)
}

pub async fn error_handler(err: routerify::RouteError) -> Response<Body> {
    match handle_casting(err).await {
        Ok(cast) => cast,
        Err(e) => return json_response(
            500,
            &format!("{}", e.to_string()),
        ).expect("serialize message")
    }
}

pub async fn handle_casting(err: routerify::RouteError) -> Result<Response<Body>> {
    let cast = match err.downcast::<LnxError>() {
        Ok(cast) => cast,
        Err(e) => return json_response(
            500,
            &format!("{}", e.to_string()),
        ).map_err(anyhow::Error::from)
    };

    let res = match *cast {
        LnxError::BadRequest(msg) =>
            json_response(400, msg).map_err(anyhow::Error::from)?,
        LnxError::UnAuthorized(msg) =>
            json_response(401, msg).map_err(anyhow::Error::from)?,
        LnxError::AbortRequest(resp) => resp,
        LnxError::Other(ref e) if e.source().is_some() => json_response(
            500,
            &format!("error handling request: {}", e.to_string()),
        ).map_err(anyhow::Error::from)?,
        LnxError::Other(e) => json_response(
            400,
            &e.to_string(),
        ).map_err(anyhow::Error::from)?,
        LnxError::ServerError(e) => json_response(
            500,
            &format!("error handling request: {}", e.to_string()),
        ).map_err(anyhow::Error::from)?,
        LnxError::SerializationError(e) => json_response(
            400,
            &e.to_string(),
        ).map_err(anyhow::Error::from)?,
    };

    Ok(res)
}