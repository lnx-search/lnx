use std::sync::Arc;
use serde::Deserialize;

use axum::body::{box_body, Body, BoxBody};
use axum::extract::{Extension, Path, Query};
use axum::http::{Response, StatusCode};
use axum::extract;

use engine::structures::{QueryPayload, IndexDeclaration};
use engine::{SearchEngine, LeasedIndex};
use engine::tantivy::Document;
use engine::{FromValue, DocumentPayload};

use crate::responders::json_response;

type SharedEngine = Arc<SearchEngine>;

/// Extracts a leased index or returns a json response
/// with a 400 status code.
macro_rules! get_index_or_reject {
    ($engine:expr, $name:expr) => {{
        match $engine.get_index($name).await {
            None => {
                warn!("rejected request due to unknown index {:?}", $name);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("no index exists with name {:?}", $name),
                )
            }
            Some(index) => index,
        }
    }};
}

/// Checks for any errors in the given operation.
///
/// If the error has a source (meaning it has consumed a underlying source)
/// if is marked as a 500 response and logged.
///
/// If the error has no source the request is rejected and marked as a 400,
/// this is also logged as a warning.
macro_rules! check_error {
    ($result:expr, $action:expr) => {{
        match $result {
            Ok(ok) => ok,
            // The error was not custom
            Err(ref e) if e.source().is_some() => {
                error!("failed to {} due to error: {:?}", $action, e);
                return json_response(StatusCode::INTERNAL_SERVER_ERROR, &())  // this will be over-ridden.
            },
            Err(e) => {
                warn!("rejecting {} operation due to bad request: {:?}", $action, &e);
                return json_response(StatusCode::BAD_REQUEST, &e.to_string())  // this will be over-ridden.

            }
        }
    }}
}

/// Searches an index with a given query.
pub async fn search_index(
    query: Query<QueryPayload>,
    Path(index_name): Path<String>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index: LeasedIndex = get_index_or_reject!(engine, &index_name);
    let results = check_error!(index.search(query.0).await, "search index");

    json_response(StatusCode::OK, &results)
}

#[derive(Deserialize)]
pub struct CreateIndexQueryParams {
    override_if_exists: Option<bool>
}

pub async fn create_index(
    query: Query<CreateIndexQueryParams>,
    payload: extract::Json<IndexDeclaration>,
    Extension(engine): Extension<SharedEngine>
) -> Response<Body> {

    let ignore = query.0;
    check_error!(engine.add_index(
            payload.0,
            ignore.override_if_exists.unwrap_or(false)
        ).await,
        "create index"
    );

    json_response(StatusCode::OK, "index created")
}

pub async fn delete_index(
    Path(index_name): Path<String>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    check_error!(engine.remove_index(&index_name).await, "delete index");

    json_response(StatusCode::OK, "index deleted")
}

#[derive(Deserialize)]
pub struct PendingQueries {
    wait: Option<bool>
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum DocumentOptions {
    Single(DocumentPayload),
    Many(Vec<DocumentPayload>),
}

pub async fn add_document(
    query: Query<PendingQueries>,
    Path(index_name): Path<String>,
    payload: extract::Json<DocumentOptions>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index: LeasedIndex = get_index_or_reject!(engine, &index_name);

    let schema = index.schema();
    let wait = query.0.wait.unwrap_or(true);

    match payload.0 {
        DocumentOptions::Single(doc) => {
            let document = check_error!(Document::from_value_map(doc, &schema), "load document from raw");
            if wait {
                check_error!(index.add_document(document).await, "add document");
            } else {
                tokio::spawn(async move {
                    if let Err(e) = index.add_document(document).await {
                        error!("failed to add document {:?}", e);
                    }
                });
            }
        },
        DocumentOptions::Many(docs) => {
            let documents = check_error!(Document::from_many_value_map(docs, &schema), "load many documents from raw");
            if wait {
                check_error!(index.add_many_documents(documents).await, "add documents");
            } else {
                tokio::spawn(async move {
                    if let Err(e) = index.add_many_documents(documents).await {
                        error!("failed to add documents {:?}", e);
                    }
                });
            }
        }
    }

    json_response(StatusCode::OK, if wait { "added documents" } else {"submitted documents"})
}

pub async fn get_document(
    Path(index_name): Path<String>,
    Path(_document_id): Path<String>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let _index: LeasedIndex = get_index_or_reject!(engine, &index_name);

    json_response(StatusCode::OK, &())
}

pub async fn delete_document(
    Path(index_name): Path<String>,
    Path(_document_id): Path<String>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let _index: LeasedIndex = get_index_or_reject!(engine, &index_name);

    json_response(StatusCode::OK, &())
}

pub async fn delete_all_documents(
    Path(index_name): Path<String>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let _index: LeasedIndex = get_index_or_reject!(engine, &index_name);

    json_response(StatusCode::OK, &())
}

/// Converts an arbitary Response<Body> into Response<BoxBody>
fn to_box_body(resp: Response<Body>) -> Response<BoxBody> {
    let (parts, body) = resp.into_parts();
    let body = box_body(body);

    Response::from_parts(parts, body)
}

/// Modifies select responses.
///
/// If a response has a status code of 404, 405 or 500 a custom
/// response is used.
pub fn map_status(resp: Response<BoxBody>) -> Response<BoxBody> {
    let status = resp.status();
    if status == StatusCode::NOT_FOUND {
        return to_box_body(json_response(StatusCode::NOT_FOUND, "route not found"));
    } else if status == StatusCode::METHOD_NOT_ALLOWED {
        return to_box_body(json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    } else if status == StatusCode::INTERNAL_SERVER_ERROR {
        return to_box_body(json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error while handling request",
        ));
    }

    resp
}
