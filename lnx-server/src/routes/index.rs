use anyhow::Error;
use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};

use engine::QueryPayload;
use engine::structures::IndexDeclaration;

use crate::{get_index, check_error};
use crate::responders::json_response;
use crate::state::Ctx;


#[middleware_fn]
pub async fn create_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let res = ctx.request()
        .body_json::<IndexDeclaration>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize index payload");

    let res = ctx.state.engine.add_index(&payload).await;
    let (_, ctx) = check_error!(res, ctx, "add index");

    Ok(json_response(ctx, 200, "index created."))
}

#[middleware_fn]
pub async fn delete_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let index = match ctx.request().params() {
        None => return Ok(json_response(
            ctx,
            400,
            "missing required url parameters.",
        )),
        Some(params) => {
            match params.get("index") {
                Some(t) => t.to_string(),
                None => return Ok(json_response(
                    ctx,
                    400,
                    "missing required url parameters 'index'.",
                )),
            }
        },
    };

    let res = ctx.state.engine.remove_index(&index).await;
    let (_, ctx) = check_error!(res, ctx, "remove index");

    Ok(json_response(ctx,200,"index deleted"))
}

#[middleware_fn]
pub async fn search_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<QueryPayload>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize query payload");
    let (results, ctx) = check_error!(index.search(payload).await, ctx, "search index");

    Ok(json_response(ctx, 200, &results))
}


#[middleware_fn]
pub async fn add_stop_words(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<Vec<String>>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize stop words");
    let (_, ctx) = check_error!(index.add_stop_words(payload).await, ctx, "add stop words");

    Ok(json_response(ctx, 200, "stop words added"))
}

#[middleware_fn]
pub async fn remove_stop_words(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<Vec<String>>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize stop words");
    let (_, ctx) = check_error!(index.remove_stop_words(payload).await, ctx, "remove stop words");

    Ok(json_response(ctx, 200, "stop words removed"))
}
