use axum::body::{box_body, BoxBody};
use axum::http::{Response, StatusCode};
use headers::{ContentType, HeaderMapExt};
use serde::Serialize;

pub fn json_response<T: Serialize + ?Sized>(status: StatusCode, value: &T) -> Response<BoxBody> {
    let val = &json!({
        "status": status.as_u16(),
        "data": value,
    });

    let buff = serde_json::to_vec(val).expect("serialize data");

    let mut resp = Response::builder()
        .status(status)
        .body(box_body(hyper::Body::from(buff)))
        .unwrap();

    resp.headers_mut().typed_insert(ContentType::json());

    resp
}
