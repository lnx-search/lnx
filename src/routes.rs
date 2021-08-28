use std::sync::Arc;

use axum::body::{box_body, Body, BoxBody};
use axum::extract::rejection::{JsonRejection, PathParamsRejection, QueryRejection};
use axum::extract::{self, Extension, Path, Query};
use axum::http::{Response, StatusCode};
use engine::structures::{Document, IndexDeclaration, QueryPayload, DocumentValue};
use engine::{LeasedIndex, SearchEngine};
use hashbrown::HashMap;
use serde::Deserialize;

use crate::auth::{AuthManager, Permissions};
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
                    &format!("no index exists with name '{}", $name),
                );
            },
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

/// Checks for any errors in parsing and extracting the path.
///
/// This is used to return a custom error message in a JSON format
/// verses axum's default text/plain response.
macro_rules! check_path {
    ($result:expr) => {{
        match $result {
            Ok(payload) => payload,
            Err(PathParamsRejection::InvalidPathParam(e)) => {
                warn!("rejecting request due to {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid path parameter {}", e),
                );
            },
            Err(PathParamsRejection::MissingRouteParams(e)) => {
                warn!("rejecting request due to {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("missing required route parameters: {}", e),
                );
            },
            Err(e) => {
                warn!("rejecting request due to {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("error with path handling: {}", e),
                );
            },
        }
    }};
}

/// Checks for any errors in parsing and extracting the query.
///
/// This is used to return a custom error message in a JSON format
/// verses axum's default text/plain response.
macro_rules! check_query {
    ($result:expr) => {{
        match $result {
            Ok(payload) => payload,
            Err(QueryRejection::FailedToDeserializeQueryString(e)) => {
                warn!("rejecting request due to {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to deserialize query string: {}", e),
                );
            },
            Err(e) => {
                warn!("rejecting request due to {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("error with query string handling: {}", e),
                );
            },
        }
    }};
}

/// Checks for any errors in parsing and extracting the json payload.
///
/// This is used to return a custom error message in a JSON format
/// verses axum's default text/plain response.
macro_rules! check_json {
    ($result:expr) => {{
        match $result {
            Ok(payload) => payload,
            Err(JsonRejection::MissingJsonContentType(_)) => {
                warn!("rejecting request due to missing json content-type");
                return json_response(
                    StatusCode::BAD_REQUEST,
                    "request missing application/json content type",
                );
            },
            Err(JsonRejection::InvalidJsonBody(e)) => {
                warn!("rejecting request due to invalid body: {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid JSON body: {}", e),
                );
            },
            Err(JsonRejection::BodyAlreadyExtracted(_)) => {
                warn!("rejecting request due to duplicate body extracting");
                return json_response(StatusCode::BAD_REQUEST, "body already extracted");
            },
            Err(e) => {
                warn!("rejecting request due to unknown error: {:?}", e);
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &format!("error with json payload: {}", e),
                );
            },
        }
    }};
}

/// Searches an index with a given query.
pub async fn search_index(
    query: Result<Query<QueryPayload>, QueryRejection>,
    index_name: Result<Path<String>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let query = check_query!(query);
    let index_name = check_path!(index_name);

    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());
    let results = check_error!(index.search(query.0).await, "search index");

    json_response(StatusCode::OK, &results)
}

/// The given set of query parameters available to the create
/// index function.
#[derive(Deserialize)]
pub struct CreateIndexQueryParams {
    /// If true this will delete the old index if it existed. (defaults to false)
    #[serde(default)]
    override_if_exists: bool,
}

/// Creates a index / overrides an index with the given payload.
pub async fn create_index(
    query: Result<Query<CreateIndexQueryParams>, QueryRejection>,
    payload: Result<extract::Json<IndexDeclaration>, JsonRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let query = check_query!(query);
    let payload = check_json!(payload);

    let ignore = query.0;
    check_error!(
        engine.add_index(payload.0, ignore.override_if_exists).await,
        "create index"
    );

    json_response(StatusCode::OK, "index created")
}

/// Deletes the given index if it exists.
pub async fn delete_index(
    index_name: Result<Path<String>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = check_path!(index_name);

    check_error!(
        engine.remove_index(index_name.as_str()).await,
        "delete index"
    );

    json_response(StatusCode::OK, "index deleted")
}

/// The set of query operations that can be given when writing
/// to an index.
#[derive(Deserialize)]
pub struct PendingQueries {
    /// If false this will return immediately without waiting for
    /// all operations to be submitted. (defaults to true).
    ///
    /// It's recommend to wait for the operation to be submitted for
    /// the purposes of backpressure.
    wait: Option<bool>,
}

/// The possible formats for uploading documents.
#[derive(Deserialize)]
#[serde(untagged)]
pub enum DocumentOptions {
    /// A singular document payload.
    Single(Document),

    /// An array of documents acting as a bulk insertion.
    Many(Vec<Document>),
}

/// Adds one or more documents to the given index.
///
/// This can either return immediately or wait for all operations to be
/// submitted depending on the `wait` query parameter.
pub async fn add_document(
    query: Result<Query<PendingQueries>, QueryRejection>,
    index_name: Result<Path<String>, PathParamsRejection>,
    payload: Result<extract::Json<DocumentOptions>, JsonRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = check_path!(index_name);
    let query = check_query!(query);
    let payload = check_json!(payload);

    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());
    let wait = query.0.wait.unwrap_or(true);

    match payload.0 {
        DocumentOptions::Single(doc) => {
            debug!("adding single document to index: {}", index_name.as_str());
            if wait {
                check_error!(index.add_document(doc).await, "add document");
            } else {
                tokio::spawn(async move {
                    if let Err(e) = index.add_document(doc).await {
                        error!("failed to add document {:?}", e);
                    }
                });
            }
        },
        DocumentOptions::Many(docs) => {
            debug!("adding multiple document to index: {}", index_name.as_str());
            if wait {
                check_error!(index.add_many_documents(docs).await, "add documents");
            } else {
                tokio::spawn(async move {
                    if let Err(e) = index.add_many_documents(docs).await {
                        error!("failed to add documents {:?}", e);
                    }
                });
            }
        },
    }

    json_response(
        StatusCode::OK,
        if wait {
            "added documents"
        } else {
            "submitted documents"
        },
    )
}

/// Gets a specific document from the system.
pub async fn get_document(
    params: Result<Path<(String, u64)>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let (index_name, document_id) = check_path!(params).0;

    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());
    let doc = check_error!(index.get_doc(document_id).await, "retrieve doc");

    json_response(StatusCode::OK, &doc)
}

/// Deletes any documents matching the set of given terms.
pub async fn delete_documents(
    index_name: Result<Path<String>, PathParamsRejection>,
    terms: Result<extract::Json<HashMap<String, DocumentValue>>, JsonRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = check_path!(index_name);
    let mut terms = check_json!(terms);

    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());

    for (field, term) in terms.0.drain() {
        let term = check_error!(index.get_term(&field, term), "get term");
        check_error!(
            index.delete_documents_with_term(term).await,
            "delete documents with term"
        );
    }

    json_response(StatusCode::OK, "deleted any document matching term")
}

/// Deletes all documents.
pub async fn delete_all_documents(
    index_name: Result<Path<String>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = check_path!(index_name);
    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());

    check_error!(index.clear_documents().await, "clear documents");

    json_response(StatusCode::OK, &())
}

/// Commits any recent changes since the last commit.
///
/// This will finalise any changes and flush to disk.
pub async fn commit_index_changes(
    index_name: Result<Path<String>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = check_path!(index_name);
    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());

    check_error!(index.commit().await, "commit changes");

    json_response(StatusCode::OK, "changes committed")
}

/// Removes any recent changes since the last commit.
pub async fn rollback_index_changes(
    index_name: Result<Path<String>, PathParamsRejection>,
    Extension(engine): Extension<SharedEngine>,
) -> Response<Body> {
    let index_name = Path(check_path!(index_name));
    let index: LeasedIndex = get_index_or_reject!(engine, index_name.as_str());

    check_error!(index.rollback().await, "rollback changes");

    json_response(StatusCode::OK, "changes rolled back since last commit")
}

/// The query parameters for the revoke token endpoint.
#[derive(Deserialize)]
pub struct CreateTokenQuery {
    username: String,

    /// The set permission flags
    #[serde(default)]
    search: bool, // serde didnt let us flatten a hashmap with them in so, yeah :)

    #[serde(default)]
    documents: bool,

    #[serde(default)]
    indexes: bool,
}

/// Creates a unique authentication access token with a
/// given set of permissions.
pub async fn create_token(
    query: Result<Query<CreateTokenQuery>, QueryRejection>,
    Extension(auth_manager): Extension<Arc<AuthManager>>,
) -> Response<Body> {
    let query = check_query!(query);
    let user = query.username.clone();

    let mut map = HashMap::with_capacity(3);
    map.insert(Permissions::Search, query.search);
    map.insert(Permissions::ModifyDocuments, query.documents);
    map.insert(Permissions::ModifyIndexes, query.indexes);

    let permissions = Permissions::get_flags_from_map(&map);
    let token = check_error!(
        auth_manager.create_token(user, permissions).await,
        "revoke token"
    );

    json_response(
        StatusCode::OK,
        &json!({
            "access_token": token,
            "username": query.username.as_str(),
            "permissions": permissions,
        }),
    )
}

/// The query parameters for the revoke token endpoint.
#[derive(Deserialize)]
pub struct RevokeQuery {
    token: String,
}

/// Revokes a authentication token.
///
/// After a token is revoked any access attempted with the token
/// will be denied.
pub async fn revoke_token(
    query: Result<Query<RevokeQuery>, QueryRejection>,
    Extension(auth_manager): Extension<Arc<AuthManager>>,
) -> Response<Body> {
    let query = check_query!(query);
    check_error!(
        auth_manager.revoke_token(query.token.clone()).await,
        "revoke token"
    );

    json_response(StatusCode::OK, "token revoked")
}

/// Invalidated all authentication tokens currently active.
///
/// After a token is revoked any access attempted with the token
/// will be denied.
pub async fn revoke_all(Extension(auth_manager): Extension<Arc<AuthManager>>) -> Response<Body> {
    check_error!(auth_manager.revoke_all().await, "revoke all tokens");

    json_response(StatusCode::OK, "tokens revoked")
}

/// The query parameters for changing the permissions
/// of an access token.
///
/// Either `set`, `unset` or both must not be null.
#[derive(Deserialize)]
pub struct ModifyPermissionsQuery {
    /// The access token itself.
    token: String,

    /// Permissions from the existing flags.
    #[serde(default)]
    search: bool,

    #[serde(default)]
    documents: bool,

    #[serde(default)]
    indexes: bool,
}

/// Alters the permissions for a given access token.
pub async fn modify_permissions(
    query: Result<Query<ModifyPermissionsQuery>, QueryRejection>,
    Extension(auth_manager): Extension<Arc<AuthManager>>,
) -> Response<Body> {
    let query = check_query!(query);
    let token = query.token.clone();

    let mut map = HashMap::with_capacity(3);
    map.insert(Permissions::Search, query.search);
    map.insert(Permissions::ModifyDocuments, query.documents);
    map.insert(Permissions::ModifyIndexes, query.indexes);

    let set = Permissions::get_flags_from_map(&map);
    check_error!(
        auth_manager.modify_permissions(&token, set).await,
        "set access token permissions"
    );

    json_response(StatusCode::OK, &())
}

/// Converts an arbitrary Response<Body> into Response<BoxBody>
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
