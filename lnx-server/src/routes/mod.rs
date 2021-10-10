pub mod default_handlers;
pub mod auth;
pub mod index;

use std::convert::Infallible;
use anyhow::Error;
use hyper::Body;
use routerify::prelude::*;
use routerify::{Middleware, Router, RouterService, RequestInfo};

use crate::state::State;

pub fn get_router(state: State) -> Router<Body, Error> {
     Router::builder()
        .data(state)
        .middleware(Middleware::pre(auth::check_permissions))
        .build()
        .unwrap()
}