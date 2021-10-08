use thruster::middleware_fn;
use thruster::BasicContext as Ctx;
use thruster::{MiddlewareNext, MiddlewareResult};

use crate::responders::json_response;

#[middleware_fn]
pub async fn handle_404(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    Ok(json_response(ctx, 404, "No route matched for path."))
}
