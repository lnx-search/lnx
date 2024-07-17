use poem_openapi::param::Path;
use poem_openapi::payload::Json;
use poem_openapi::{ApiResponse, Enum, Object, OpenApi};

use crate::models::{ApiError, WrapSerde};

/// API routes for managing and handling indexes.
pub struct IndexApiRoutes {}

#[OpenApi(prefix_path = "/index", tag = crate::Tag::Indexes)]
impl IndexApiRoutes {
    #[oai(path = "/", method = "post")]
    /// Create New Search Index
    ///
    /// This `POST` method allows you to create a new search index with a given
    /// configuration which can be provided as either JSON or YAML.
    ///
    /// See the
    async fn create_index(
        &self,
        Path(index_id): Path<WrapSerde>,
    ) -> Result<Json<CreateIndexData>, CreateIndexError> {
        todo!()
    }

    #[oai(path = "/:index_id", method = "delete")]
    /// Delete Search Index
    ///
    /// This `DELETE` method performs a 'soft' deletion of the specified index, this means
    /// internally the index is scheduled for removal and prevents the index from being searched,
    /// but is recoverable for a period specified by the `index_recover_period` field
    /// specified in the system settings (defaults to 7 days).
    ///
    /// An index can be forcefully deleted ignoring this recover period by re-sending the request
    /// with `?force=true` to the query parameter.
    ///
    /// NOTE:
    /// You forceful deletes **always** require 2 API requests, one to soft delete initially and a
    /// second to force the removal.
    async fn delete_index(&self) -> Result<Json<DeleteIndexData>, DeleteIndexError> {
        todo!()
    }

    #[oai(path = "/:index_id", method = "get")]
    /// Inspect Search Index
    ///
    /// This `GET` method provides general information and metrics about the index and its current
    /// state within the system.
    async fn inspect_index(&self) -> Result<Json<bool>, poem::Error> {
        todo!()
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "Self::bad_request_handler")]
/// Possible responses during the create index operation.
pub enum CreateIndexError {
    #[oai(status = 400)]
    /// The provided index payload was unable to be parsed due to an error.
    InvalidData(Json<ApiError>),
    #[oai(status = 422)]
    /// The provided index payload failed validation and cannot be created.
    ValidationError(Json<ApiError>),
    #[oai(status = 500)]
    /// An internal server error occurred.
    Internal(Json<ApiError>),
}

impl CreateIndexError {
    fn bad_request_handler(err: poem::Error) -> Self {
        Self::InvalidData(Json(ApiError::from(err)))
    }
}

#[derive(Object)]
/// The response payload for the newly created index.
pub struct CreateIndexData {
    /// The unique name of the index.
    ///
    /// This value is used for interacting with other
    /// API endpoints targeting the specific index.
    index_name: String,
    /// The unique hash ID of the index.
    ///
    /// This is the identifier used within filepaths and is
    /// a derived cityhash of the `index_name` field.
    index_hash_id: u64,
}

#[derive(Object)]
/// The response payload for the soft deleted index.
pub struct DeleteIndexData {
    /// The kind of deletion that was applied to the index.
    deletion_kind: DeletionKind,
    /// Can the index be recovered within the `index_recover_period` time period.
    is_recoverable: bool,
}

#[derive(Enum)]
#[oai(rename_all = "lowercase")]
/// The kind of deletion that was applied to the index.
pub enum DeletionKind {
    /// The index has been hidden from search but the contents of the index
    /// have not yet been removed and may be recovered.
    Soft,
    /// The index has been completely removed along with all persisted data
    /// and settings, it is no longer recoverable.
    Hard,
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "Self::bad_request_handler")]
/// Possible responses during the create index operation.
pub enum DeleteIndexError {
    #[oai(status = 404)]
    /// The specified index does not exist.
    UnknownIndex(Json<ApiError>),
    #[oai(status = 400)]
    /// The provided index ID was invalid/malformed, or an invalid value was provided
    /// while attempting to parse the query parameters of the request.
    BadRequest(Json<ApiError>),
    #[oai(status = 422)]
    /// The target index is in some existing state that prevents the operation from being applied:
    ///
    /// - The index is already soft deleted and cannot be soft deleted again.
    /// - The index is not yet soft deleted, so cannot be hard deleted/forced.
    Unprocessable(Json<ApiError>),
    #[oai(status = 500)]
    /// An internal server error occurred.
    Internal(Json<ApiError>),
}

impl DeleteIndexError {
    fn bad_request_handler(err: poem::Error) -> Self {
        if err.is::<poem::error::ParseQueryError>() {
            Self::BadRequest(Json(ApiError::bad_request(
                "Unable to parse the provided query parameters",
            )))
        } else {
            // TODO: Add handling of Index ID. & Mi
            Self::Internal(Json(ApiError::from(err)))
        }
    }
}
