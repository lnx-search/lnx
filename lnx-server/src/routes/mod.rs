mod auth;
mod default_handlers;
mod engine;
mod index;

use headers::{ContentType, HeaderMapExt};
use hyper::http::Response;
use hyper::Body;
use routerify::{Middleware, Router};

use crate::error::LnxError;
use crate::helpers::{LnxRequest, LnxResponse};
use crate::state::State;

pub fn get_router(state: State) -> Router<Body, LnxError> {
    Router::builder()
        .data(state)
        .middleware(Middleware::pre(auth::check_permissions))
        .middleware(Middleware::pre(index::ensure_index_perms))
        .get("/openapi.json", openapi)
        .get("/docs", docs)
        .post("/auth", auth::create_token)
        .delete("/auth", auth::revoke_all_tokens)
        .post("/auth/:token/revoke", auth::revoke_token)
        .post("/auth/:token/edit", auth::edit_token)
        .post("/indexes", engine::create_index)
        .delete("/indexes/:index", engine::delete_index)
        .post("/indexes/:index/commit", index::commit)
        .post("/indexes/:index/rollback", index::rollback)
        .post("/indexes/:index/search", index::search_index)
        .post("/indexes/:index/correct", index::get_corrections)
        .post("/indexes/:index/documents", index::add_documents)
        .post("/indexes/:index/stopwords", index::add_stop_words)
        .delete("/indexes/:index/stopwords", index::remove_stop_words)
        .delete("/indexes/:index/documents", index::delete_documents)
        .delete("/indexes/:index/documents/query", index::delete_documents_by_query)
        .delete("/indexes/:index/documents/clear", index::clear_documents)
        .get(
            "/indexes/:index/documents/:document_id",
            index::get_document,
        )
        .err_handler(default_handlers::error_handler)
        .any(default_handlers::handle_404)
        .build()
        .unwrap()
}

async fn openapi(_req: LnxRequest) -> LnxResponse {
    let json = include_str!("./openapi/openapi.json");
    let mut response = Response::builder()
        .status(200)
        .body(Body::from(json))
        .unwrap();

    response.headers_mut().typed_insert(ContentType::json());

    Ok(response)
}

async fn docs(_req: LnxRequest) -> LnxResponse {
    let html = r#"
    <!DOCTYPE html>
    <html>
    <head>
    <title>Lnx Documentation</title>
    <!-- needed for adaptive design -->
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <link rel="shortcut icon" href="https://github.com/ChillFish8/lnx/raw/master/assets/logo.png">
     <!--
    ReDoc doesn't change outer page styles
    -->
    <style>
      body {
        margin: 0;
        padding: 0;
      }
    </style>
    </head>
    <body>
    <redoc spec-url="/openapi.json"></redoc>
    <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"> </script>
    </body>
    </html>
    "#;

    let mut response = Response::builder()
        .status(200)
        .body(Body::from(html))
        .unwrap();

    response.headers_mut().typed_insert(ContentType::html());

    Ok(response)
}
