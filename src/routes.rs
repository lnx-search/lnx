use axum::response;
use axum::response::IntoResponse;


pub async fn search_index() -> impl IntoResponse {
    response::Json(None::<()>)}

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
