use std::convert::Infallible;
use hyper::{Body, Request};
use serde::{Deserialize};
use routerify::ext::RequestExt;

use crate::auth::permissions;
use crate::responders::json_response;
use crate::{bad_request, unauthorized, abort};
use crate::error::{Result, LnxError};
use crate::state::State;

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
    let state = req.data::<State>().expect("get state");

    if !state.auth.enabled() {
        return Ok(req)
    }

    let auth = req.headers().get("Authorization");
    let token = match auth {
        Some(auth) => auth
            .to_str()
            .map_err(|_| LnxError::BadRequest("invalid token provided"))?,
        None => return unauthorized!("missing authorization header"),
    };

    let data = match state.auth.get_token_data(&token) {
        None => return unauthorized!("invalid token provided"),
        Some(v) => v,
    };

    let required_permissions: usize;
    let path = req.uri().path();
    if path.starts_with("/auth") {
        required_permissions = permissions::MODIFY_AUTH;
    } else if path == "/indexes" {
        required_permissions = permissions::MODIFY_ENGINE;
    } else if path.starts_with("/indexes") {
        if path.ends_with("/search") {
            required_permissions = permissions::SEARCH_INDEX;
        } else if path.ends_with("/stopwords") {
            required_permissions = permissions::MODIFY_STOP_WORDS;
        } else {
            required_permissions = permissions::MODIFY_DOCUMENTS
        }
    } else {
        // A safe default is to return a 404.
        return abort!(404, "unknown route.")
    }

    if !data.has_permissions(required_permissions) {
        return unauthorized!("you lack permissions to perform this request")
    }

    Ok(req)
}


