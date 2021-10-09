use anyhow::Error;
use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};

use engine::QueryPayload;

use crate::{get_index, check_error};
use crate::responders::json_response;
use crate::state::Ctx;


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

