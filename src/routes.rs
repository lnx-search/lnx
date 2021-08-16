use axum::body::BoxBody;
use axum::http::{Response, StatusCode};
use axum::response;
use axum::response::IntoResponse;

use crate::responders::json_response;

pub async fn search_index() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn create_index() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn delete_index() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn add_document() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn get_document() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn delete_document() -> impl IntoResponse {
    response::Json(None::<()>)
}

pub async fn delete_all_documents() -> impl IntoResponse {
    response::Json(None::<()>)
}

/// Modifies select responses.
///
/// If a response has a status code of 404, 405 or 500 a custom
/// response is used.
pub fn map_status(resp: Response<BoxBody>) -> Response<BoxBody> {
    let status = resp.status();
    if status == StatusCode::NOT_FOUND {
        return json_response(StatusCode::NOT_FOUND, "route not found");
    } else if status == StatusCode::METHOD_NOT_ALLOWED {
        return json_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    } else if status == StatusCode::INTERNAL_SERVER_ERROR {
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error while handling request",
        );
    }

    resp
}
