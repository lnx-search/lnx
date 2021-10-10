use std::convert::Infallible;
use anyhow::Result;
use hyper::{Body, Request};
use serde::{Deserialize};

use crate::auth::permissions;
use crate::responders::json_response;
use crate::check_error;

/// A set of metadata to associate with a access token.
#[derive(Deserialize)]
struct CreateTokenPayload {
    /// The permissions of the token.
    permissions: usize,

    /// An optional identifier for a user.
    user: Option<String>,

    /// An optional description for the given token.
    description: Option<String>,

    /// An optional set of indexes the user is allowed to access.
    ///
    /// If None the user can access all tokens.
    allowed_indexes: Option<Vec<String>>
}

/// A middleware that checks the user accessing the endpoint has
/// the required permissions.
///
/// If authorization is disabled then this does no checks.
pub(crate) async fn check_permissions(req: Request<Body>) -> Result<Request<Body>> {
    Ok(req)
}


