use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};

use crate::responders::json_response;
use crate::state::Ctx;

#[middleware_fn]
pub async fn handle_404(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    Ok(json_response(ctx, 404, "No route matched for path."))
}
