use axum::body::Body;
use axum::http::{Response, StatusCode};
use headers::{ContentType, HeaderMapExt};
use serde::Serialize;

/// Produces a standard Response<BoxBody> from a given status code and value.
///
/// The value is expected to implement `Serialize`, this function will
/// panic if the value is unable to be serialized.
///
/// The response is automatically tagged with the `application/json` datatype.
pub fn json_response<T: Serialize + ?Sized>(status: StatusCode, value: &T) -> Response<Body> {
    let val = &json!({
        "status": status.as_u16(),
        "data": value,
    });

    let buff = serde_json::to_vec(val).expect("serialize data");

    let mut resp = Response::builder()
        .status(status)
        .body(axum::body::Body::from(buff))
        .unwrap();

    resp.headers_mut().typed_insert(ContentType::json());

    resp
}
