use anyhow::Result;
use hyper::{Request, Response};
use hyper::Body;

use crate::responders::json_response;

pub async fn handle_404(request: Request<Body>) -> Result<Response<Body>> {
    json_response(404, "No route matched for path.")
}
