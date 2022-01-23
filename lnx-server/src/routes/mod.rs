mod auth;
mod default_handlers;
mod engine;
mod index;

use hyper::Body;
use routerify::{Middleware, Router};

use crate::error::LnxError;
use crate::state::State;

pub fn get_router(state: State) -> Router<Body, LnxError> {
    Router::builder()
        .data(state)
        .middleware(Middleware::pre(auth::check_permissions))
        .middleware(Middleware::pre(index::ensure_index_perms))
        .post("/auth", auth::create_token)
        .delete("/auth", auth::revoke_all_tokens)
        .post("/auth/:token/revoke", auth::revoke_token)
        .post("/auth/:token/edit", auth::edit_token)
        .post("/indexes", engine::create_index)
        .delete("/indexes/:index", engine::delete_index)
        .post("/indexes/:index/commit", index::commit)
        .post("/indexes/:index/rollback", index::rollback)
        .post("/indexes/:index/search", index::search_index)
        .post("/indexes/:index/hint", index::get_corrected_query_hint)
        .post("/indexes/:index/documents", index::add_documents)
        .post("/indexes/:index/stopwords", index::add_stop_words)
        .delete("/indexes/:index/stopwords", index::remove_stop_words)
        .delete("/indexes/:index/stopwords/clear", index::clear_stop_words)
        .post("/indexes/:index/synonyms", index::add_synonyms)
        .delete("/indexes/:index/synonyms", index::remove_synonyms)
        .delete("/indexes/:index/synonyms/clear", index::clear_synonyms)
        .delete("/indexes/:index/documents", index::delete_documents)
        .delete(
            "/indexes/:index/documents/query",
            index::delete_documents_by_query,
        )
        .delete("/indexes/:index/documents/clear", index::clear_documents)
        .get(
            "/indexes/:index/documents/:document_id",
            index::get_document,
        )
        .delete(
            "/indexes/:index/documents/:document_id",
            index::delete_document,
        )
        .err_handler(default_handlers::error_handler)
        .any(default_handlers::handle_404)
        .build()
        .unwrap()
}
