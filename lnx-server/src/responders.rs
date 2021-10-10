use headers::{ContentType, HeaderMapExt};
use hyper::{Body, StatusCode};
use serde::Serialize;

use crate::error::Result;

#[derive(Serialize)]
pub struct Response<'a, T: Serialize + ?Sized> {
    status: u16,
    data: &'a T,
}

pub fn json_response<T: Serialize + ?Sized>(
    status: u16,
    body: &T,
) -> Result<hyper::Response<Body>> {
    let payload = Response { status, data: body };

    let buffer = serde_json::to_vec(&payload)?;
    let mut resp = hyper::Response::new(Body::from(buffer));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().typed_insert(ContentType::json());

    Ok(resp)
}
