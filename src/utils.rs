use axum::http::header;

use hyper::http::{HeaderValue, Request, Response, StatusCode};

use tower_http::auth::AuthorizeRequest;
use tower_http::set_header::MakeHeaderValue;

use uuid::Uuid;


#[derive(Debug, Clone)]
pub struct AuthIfEnabled {
    enabled: bool,
    auth: HeaderValue,
    reject_msg: bytes::Bytes,
}

impl AuthIfEnabled {
    pub fn bearer(token: &str, enabled: bool, reject_msg: &str) -> Self {
        let reject_msg = bytes::Bytes::copy_from_slice(reject_msg.as_bytes());
        let auth = HeaderValue::from_str(token).unwrap();

        Self {
            enabled,
            auth,
            reject_msg,
        }
    }
}

impl AuthorizeRequest for AuthIfEnabled {
    type Output = ();
    type ResponseBody = axum::body::BoxBody;

    fn authorize<B>(&mut self, request: &Request<B>) -> Option<Self::Output> {
        if !self.enabled {
            return Some(());
        };

        if let Some(actual) = request.headers().get(header::AUTHORIZATION) {
            (actual == self.auth).then(|| ())
        } else {
            None
        }
    }

    fn unauthorized_response<B>(&mut self, _request: &Request<B>) -> Response<Self::ResponseBody> {
        let body = axum::body::box_body(hyper::Body::from(self.reject_msg.clone()));
        let mut res = Response::new(body);
        *res.status_mut() = StatusCode::UNAUTHORIZED;
        res
    }
}

#[derive(Copy, Clone)]
pub struct RequestTagger;

impl RequestTagger {
    pub fn new() -> Self {
        Self {}
    }
}

impl<T> MakeHeaderValue<T> for RequestTagger {
    fn make_header_value(&mut self, _message: &T) -> Option<HeaderValue> {
         Some(HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap())
    }
}