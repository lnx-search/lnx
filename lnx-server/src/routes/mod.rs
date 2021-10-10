mod default_handlers;
mod auth;
mod engine;
mod index;

use std::convert::Infallible;
use hyper::Body;
use routerify::{Middleware, Router, RouterService, RequestInfo};

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
         .post("/indexes/:index", engine::delete_index)

         .err_handler(default_handlers::error_handler)
         .any(default_handlers::handle_404)
         .build()
         .unwrap()
}