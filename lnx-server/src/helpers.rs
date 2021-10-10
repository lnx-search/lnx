
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
                return Ok(json_response(500, "An error occurred while processing this request"))
            },
            Err(e) => {
                debug!("rejecting {} operation due to bad request: {:?}", $action, &e);
                return Err(LnxError::BadRequest(&e.to_string()))

            }
        }
    }}
}

#[macro_export]
macro_rules! abort {
    ($status:expr, $val:expr) => {{
        Err(LnxError::AbortRequest(json_response(
            $status,
            $val,
        )?))
    }}
}

#[macro_export]
macro_rules! unauthorized {
    ($val:expr) => {{
        Err(LnxError::UnAuthorized($val))
    }}
}

#[macro_export]
macro_rules! bad_request {
    ($val:expr) => {{
        Err(LnxError::BadRequest($val))
    }}
}