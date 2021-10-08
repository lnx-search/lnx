use crate::responders::json_response;


/// Checks for any errors in the given operation.
///
/// If the error has a source (meaning it has consumed a underlying source)
/// if is marked as a 500 response and logged.
///
/// If the error has no source the request is rejected and marked as a 400,
/// this is also logged as a warning.
#[macro_export]
macro_rules! check_error {
    ($result:expr, $ctx:expr, $action:expr) => {{
        match $result {
            Ok(ok) => (ok, $ctx),
            // The error was not custom
            Err(ref e) if e.source().is_some() => {
                error!("failed to {} due to error: {:?}", $action, e);
                return Ok(json_response($ctx, 500, "An error occurred while processing this request"))
            },
            Err(e) => {
                warn!("rejecting {} operation due to bad request: {:?}", $action, &e);
                return Ok(json_response($ctx, 400, &e.to_string()))

            }
        }
    }}
}
