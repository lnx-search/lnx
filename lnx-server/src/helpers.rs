use anyhow::Context;
use hyper::{Body, Request, Response};
use serde::Serialize;

use crate::error::Result;

pub type LnxRequest = Request<Body>;
pub type LnxResponse = Result<Response<Body>>;

#[macro_export]
macro_rules! abort {
    ($status:expr, $val:expr) => {{
        Err(crate::error::LnxError::AbortRequest(json_response(
            $status, $val,
        )?))
    }};
}

#[macro_export]
macro_rules! unauthorized {
    ($val:expr) => {{
        Err(crate::error::LnxError::UnAuthorized($val))
    }};
}

#[macro_export]
macro_rules! bad_request {
    ($val:expr) => {{
        Err(crate::error::LnxError::BadRequest($val))
    }};
}

#[macro_export]
macro_rules! json {
    ($body:expr) => {{
        use hyper::body::to_bytes;
        let body = to_bytes($body).await?;
        serde_json::from_slice(&body)?
    }};
}

#[macro_export]
macro_rules! get_or_400 {
    ($val:expr, $err_msg:expr) => {{
        match $val {
            None => return crate::bad_request!($err_msg),
            Some(v) => v,
        }
    }};

    ($val:expr) => {{
        match $val {
            None => return crate::bad_request!("missing required url parameter"),
            Some(v) => v,
        }
    }};
}


#[inline]
pub async fn atomic_store<T: Serialize + Sync + Send + 'static + Sized>(
    db: sled::Db,
    keyspace: &'static str,
    v: T,
) -> Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let buff = bincode::serialize(&v).context("failed to serialize base type")?;
        db.insert(keyspace, buff).context("failed to serialize into sled")?;
        db.flush()?;

        Ok(())
    }).await.map_err(anyhow::Error::from)??;

    Ok(())
}