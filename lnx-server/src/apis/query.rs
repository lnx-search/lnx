use lnx_query::syntax::SelectQuery;
use poem_openapi::payload::Json;
use poem_openapi::{Object, OpenApi};

use super::Tag;

/// System information API endpoints
pub struct LnxQueryApi;

#[OpenApi(tag = Tag::QueryEndpoints)]
impl LnxQueryApi {
    #[oai(path = "/query/select", method = "post", external_docs = "test")]
    /// Execute Select Query
    async fn execute(
        &self,
        Json(payload): Json<SelectQuery>,
    ) -> poem::Result<Json<bool>> {
        dbg!(payload);
        Ok(Json(true))
    }

    #[oai(path = "/query/explain", method = "post")]
    /// Explain Query
    async fn explain(&self, Json(payload): Json<SelectQuery>) -> Json<bool> {
        dbg!(payload);
        Json(true)
    }
}
