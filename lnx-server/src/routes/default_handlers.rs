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
        )
    };

    let res = match *cast {
        LnxError::BadRequest(msg) => json_response(400, msg)?,
        LnxError::UnAuthorized(msg) => json_response(401, msg)?,
        LnxError::AbortRequest(resp) => resp,
        LnxError::Other(e) => json_response(
            500,
            &format!("error handling request: {}", e.to_string()),
        )?
    };

    Ok(res)
}