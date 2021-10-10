mod default_handlers;
mod auth;
mod engine;
mod index;

use hyper::Body;
use routerify::{Middleware, Router};

use crate::state::State;
use crate::error::LnxError;

pub fn get_router(state: State) -> Router<Body, LnxError> {
     Router::builder()
         .data(state)
         .middleware(Middleware::pre(auth::check_permissions))

         .post("/auth", auth::create_token)
         .delete("/auth", auth::revoke_all_tokens)
         .post("/auth/:token/revoke", auth::revoke_token)
         .post("/auth/:token/edit", auth::edit_token)

         .post("/indexes", engine::create_index)
         .delete("/indexes/:index", engine::delete_index)
         .post("/indexes/:index/search", index::search_index)
         .post("/indexes/:index/documents", index::add_documents)
         .post("/indexes/:index/stopwords", index::add_stop_words)
         .delete("/indexes/:index/stopwords", index::remove_stop_words)
         .delete("/indexes/:index/documents", index::delete_documents)
         .delete("/indexes/:index/documents/clear", index::clear_documents)
         .get("/indexes/:index/documents/:document_id", index::get_document)

         .err_handler(default_handlers::error_handler)
         .any(default_handlers::handle_404)
         .build()
         .unwrap()
}