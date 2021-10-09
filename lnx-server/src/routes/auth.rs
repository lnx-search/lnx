use anyhow::Error;
use serde::{Deserialize};

use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};
use thruster::middleware::cookies::HasCookies;

use crate::auth::permissions;
use crate::responders::json_response;
use crate::state::Ctx;
use crate::check_error;

#[derive(Deserialize)]
struct CreateTokenPayload {
    permissions: usize,
    user: Option<String>,
    description: Option<String>,
    allowed_indexes: Option<Vec<String>>
}

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


#[middleware_fn]
pub async fn revoke_all_tokens(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    ctx.state.auth.revoke_all_tokens();

    let storage = ctx.state.storage.clone();
    let (_, ctx) = check_error!(ctx.state.auth.commit(storage).await, ctx, "commit auth tokens");

    Ok(json_response(ctx, 200, "token revoked.")    )
}


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

