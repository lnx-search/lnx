pub mod default_handlers;
pub mod auth;
pub mod index;

use std::convert::Infallible;
use hyper::Body;
use routerify::prelude::*;
use routerify::{Middleware, Router, RouterService, RequestInfo};

use crate::state::State;
use crate::error::LnxError;

pub fn get_router(state: State) -> Router<Body, LnxError> {
     Router::builder()
         .data(state)
         .middleware(Middleware::pre(auth::check_permissions))
         .err_handler(default_handlers::error_handler)
        .any(default_handlers::handle_404)
         .build()
         .unwrap()
}