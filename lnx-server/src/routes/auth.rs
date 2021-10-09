use anyhow::Error;
use serde::{Deserialize};

use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};
use thruster::middleware::cookies::HasCookies;

use crate::auth::permissions;
use crate::responders::json_response;
use crate::state::Ctx;
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
#[middleware_fn]
pub async fn check_permissions(mut ctx: Ctx, next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    if !ctx.state.auth.enabled() {
        return next(ctx).await
    }

    let mut maybe_key = ctx.get_header("Authorization");
    if maybe_key.is_empty() {
        return Ok(json_response(ctx, 401, "missing authorization token."))
    }

    let key = maybe_key.remove(0);
    let data = match ctx.state.auth.get_token_data(&key) {
        None => return Ok(json_response(
            ctx,
            401,
            "invalid authorization token given.",
        )),
        Some(v) => v,
    };

    let required_permissions: usize;
    let path = ctx.request().path();
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
        return Ok(json_response(
            ctx,
            404,
            "unknown route.",
        ))
    }


    if !data.has_permissions(required_permissions) {
        return Ok(json_response(
            ctx,
            401,
            "you lack the permissions to perform this action.",
        ))
    }

    ctx = next(ctx).await?;

    Ok(ctx)
}

/// Creates a new access token with 64 characters.
///
/// Each token can have the following metadata associated to it:
/// - permissions*
/// - user
/// - description
/// - allowed_indexes
///
/// `*` - Required.
#[middleware_fn]
pub async fn create_token(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let res = ctx.request()
        .body_json::<CreateTokenPayload>()
        .map_err(Error::from);

    let (body, ctx) = check_error!(
        res,
        ctx,
        "deserialize body"
    );

    let data = ctx.state.auth.create_token(
        body.permissions,
        body.user,
        body.description,
        body.allowed_indexes,
    );

    let storage = ctx.state.storage.clone();
    let (_, ctx) = check_error!(ctx.state.auth.commit(storage).await, ctx, "commit auth tokens");

    Ok(json_response(ctx, 200, data.as_ref())    )
}


/// Revoke all access tokens.
///
/// # WARNING:
///     This is absolutely only designed for use in an emergency.
///     Running this will revoke all tokens including the super user key,
///     run this at your own risk
#[middleware_fn]
pub async fn revoke_all_tokens(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    ctx.state.auth.revoke_all_tokens();

    let storage = ctx.state.storage.clone();
    let (_, ctx) = check_error!(ctx.state.auth.commit(storage).await, ctx, "commit auth tokens");

    Ok(json_response(ctx, 200, "token revoked.")    )
}


/// Revokes a given access token.
#[middleware_fn]
pub async fn revoke_token(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let token = match ctx.request().params() {
        None => return Ok(json_response(
            ctx,
            400,
            "missing required url parameters.",
        )),
        Some(params) => {
            match params.get("token") {
                Some(t) => t,
                None => return Ok(json_response(
                    ctx,
                    400,
                    "missing required url parameters 'token'.",
                )),
            }
        },
    };

    ctx.state.auth.revoke_token(token);

    let storage = ctx.state.storage.clone();
    let (_, ctx) = check_error!(ctx.state.auth.commit(storage).await, ctx, "commit auth tokens");

    Ok(json_response(ctx, 200, "token revoked.")    )
}

/// Edit's an existing token's metadata.
///
/// This will replace all fields with the new metadata other than
/// the created timestamp and token itself.
#[middleware_fn]
pub async fn edit_token(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let token = match ctx.request().params() {
        None => return Ok(json_response(
            ctx,
            400,
            "missing required url parameters.",
        )),
        Some(params) => {
            match params.get("token") {
                Some(t) => t.to_string(),
                None => return Ok(json_response(
                    ctx,
                    400,
                    "missing required url parameters 'token'.",
                )),
            }
        },
    };

    let res = ctx.request()
        .body_json::<CreateTokenPayload>()
        .map_err(Error::from);

    let (body, ctx) = check_error!(
        res,
        ctx,
        "deserialize body"
    );

    let data = ctx.state.auth.update_token(
        &token,
        body.permissions,
        body.user,
        body.description,
        body.allowed_indexes,
    );

    let data = match data {
        None => return Ok(json_response(ctx, 400, "this token does not exist.")),
        Some(d) => d,
    };

    let storage = ctx.state.storage.clone();
    let (_, ctx) = check_error!(ctx.state.auth.commit(storage).await, ctx, "commit auth tokens");

    Ok(json_response(ctx, 200, data.as_ref()))
}